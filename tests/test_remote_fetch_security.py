import socket
import unittest
from unittest.mock import patch

from app.platform.errors import ValidationError
from app.platform.net.remote_fetch import (
    PublicAddressResolver,
    fetch_remote_asset,
    is_public_address,
    validate_public_http_url,
    validate_remote_content,
)


class _FakeResolver:
    def __init__(self, addresses: list[str]) -> None:
        self.addresses = addresses
        self.closed = False

    async def resolve(self, host, port=0, family=socket.AF_INET):
        """返回测试预置的 DNS 地址。"""
        return [
            {
                "hostname": host,
                "host": address,
                "port": port,
                "family": socket.AF_INET6 if ":" in address else socket.AF_INET,
                "proto": 0,
                "flags": 0,
            }
            for address in self.addresses
        ]

    async def close(self):
        """记录解析器关闭状态。"""
        self.closed = True


class _FakeContent:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def iter_chunked(self, _size):
        """按测试预置顺序产生响应块。"""
        for chunk in self._chunks:
            yield chunk


class _FakeResponse:
    def __init__(
        self,
        status: int,
        *,
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
        content_length: int | None = None,
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self.content = _FakeContent(chunks or [])
        self.content_length = content_length

    async def __aenter__(self):
        """模拟 aiohttp 响应上下文入口。"""
        return self

    async def __aexit__(self, *_args):
        """模拟 aiohttp 响应上下文出口。"""
        return None


class _FakeSession:
    responses: list[_FakeResponse] = []
    requests: list[tuple[str, dict[str, object]]] = []

    def __init__(self, *, connector, timeout) -> None:
        self.connector = connector
        self.timeout = timeout

    async def __aenter__(self):
        """模拟 aiohttp 会话上下文入口。"""
        return self

    async def __aexit__(self, *_args):
        """关闭真实构造但未联网使用的连接器。"""
        await self.connector.close()

    def get(self, url, **kwargs):
        """记录请求并返回下一个测试响应。"""
        self.requests.append((url, kwargs))
        return self.responses.pop(0)


class PublicAddressValidationTests(unittest.IsolatedAsyncioTestCase):
    def test_special_addresses_are_rejected(self):
        """覆盖环回、内网、链路本地、文档和特殊 IPv6 网段。"""
        blocked = [
            "0.0.0.1",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.168.1.1",
            "198.18.0.1",
            "203.0.113.8",
            "::1",
            "64:ff9b::7f00:1",
            "2001:db8::1",
            "fc00::1",
            "fe80::1",
        ]
        for address in blocked:
            with self.subTest(address=address):
                self.assertFalse(is_public_address(address))
        self.assertTrue(is_public_address("1.1.1.1"))
        self.assertTrue(is_public_address("2606:4700:4700::1111"))

    def test_url_rejects_credentials_private_hosts_and_unsafe_ports(self):
        """URL 入口阻断凭证、内部主机后缀、私有字面量和非 Web 端口。"""
        blocked = [
            "http://user:pass@example.com/file.png",
            "http://localhost/file.png",
            "http://service.internal/file.png",
            "http://127.0.0.1/file.png",
            "http://[::1]/file.png",
            "https://example.com:8443/file.png",
        ]
        for url in blocked:
            with self.subTest(url=url), self.assertRaises(ValidationError):
                validate_public_http_url(url)
        self.assertEqual(
            validate_public_http_url("HTTPS://EXAMPLE.COM/path?q=1#fragment"),
            "https://example.com/path?q=1",
        )

    async def test_resolver_rejects_mixed_public_and_private_answers(self):
        """DNS 任一答案落入内网时整体拒绝，避免轮询落到内网。"""
        delegate = _FakeResolver(["1.1.1.1", "127.0.0.1"])
        resolver = PublicAddressResolver(delegate)  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            await resolver.resolve("example.com", 443, socket.AF_UNSPEC)
        await resolver.close()
        self.assertTrue(delegate.closed)


class RemoteFetchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """重置每个用例共享的伪会话状态。"""
        _FakeSession.responses = []
        _FakeSession.requests = []

    async def test_redirect_is_validated_and_request_has_no_credentials(self):
        """逐跳校验公开重定向，且抓取请求不携带会话凭证。"""
        png = b"\x89PNG\r\n\x1a\n" + b"payload"
        _FakeSession.responses = [
            _FakeResponse(302, headers={"Location": "https://cdn.example.net/final.png"}),
            _FakeResponse(200, headers={"Content-Type": "image/png"}, chunks=[png]),
        ]
        with patch("app.platform.net.remote_fetch.aiohttp.ClientSession", _FakeSession):
            result = await fetch_remote_asset("https://example.com/start")

        self.assertEqual(result.content, png)
        self.assertEqual(result.filename, "final.png")
        self.assertEqual(result.final_url, "https://cdn.example.net/final.png")
        self.assertEqual(len(_FakeSession.requests), 2)
        for _url, kwargs in _FakeSession.requests:
            headers = kwargs["headers"]
            self.assertNotIn("Cookie", headers)
            self.assertNotIn("Authorization", headers)
            self.assertFalse(kwargs["allow_redirects"])

    async def test_redirect_to_private_address_is_rejected(self):
        """公开入口跳转到私有地址时在发出第二跳前终止。"""
        _FakeSession.responses = [
            _FakeResponse(302, headers={"Location": "http://169.254.169.254/latest/meta-data"}),
        ]
        with patch("app.platform.net.remote_fetch.aiohttp.ClientSession", _FakeSession):
            with self.assertRaises(ValidationError):
                await fetch_remote_asset("https://example.com/start")
        self.assertEqual(len(_FakeSession.requests), 1)

    async def test_streamed_body_size_limit_is_enforced(self):
        """无 Content-Length 时也按累计读取字节数实施硬上限。"""
        _FakeSession.responses = [
            _FakeResponse(200, headers={"Content-Type": "image/png"}, chunks=[b"1234", b"5678"]),
        ]
        with patch("app.platform.net.remote_fetch.aiohttp.ClientSession", _FakeSession):
            with self.assertRaises(ValidationError):
                await fetch_remote_asset("https://example.com/file.png", max_bytes=7)

    def test_mime_signature_mismatch_and_html_are_rejected(self):
        """阻断伪装成图片或文本文件的主动内容。"""
        with self.assertRaises(ValidationError):
            validate_remote_content(b"not-a-png", "image/png")
        with self.assertRaises(ValidationError):
            validate_remote_content(b"<!doctype html><script>alert(1)</script>", "text/plain")


if __name__ == "__main__":
    unittest.main()
