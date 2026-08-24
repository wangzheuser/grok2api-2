import asyncio
import base64
import unittest
from unittest.mock import patch

from app.control.proxy.models import ProxyLease, ProxyProvider
from app.dataplane.proxy.adapters.session import (
    ResettableSession,
    build_session_kwargs,
)
from app.dataplane.reverse.transport.websocket import WebSocketClient
from app.platform.errors import UpstreamError


class _FailingSession:
    """模拟底层连接失败。"""

    async def get(self, *_args, **_kwargs):
        """抛出连接层异常。"""
        raise OSError("connect tunnel failed")

    async def close(self):
        """关闭测试会话。"""


class _ResponseSession:
    """返回指定 HTTP 状态的测试会话。"""

    def __init__(self, status_code):
        self.status_code = status_code

    async def get(self, *_args, **_kwargs):
        """返回最小响应对象。"""
        return type("Response", (), {"status_code": self.status_code})()

    async def close(self):
        """关闭测试会话。"""


class _RawForwardProxy:
    """记录请求行和请求头的本地正向代理测试夹具。"""

    def __init__(self, *, connect_status=407):
        self.connect_status = connect_status
        self.server = None
        self.port = 0
        self.request = asyncio.get_running_loop().create_future()

    async def __aenter__(self):
        """启动仅绑定环回地址的测试服务器。"""
        self.server = await asyncio.start_server(
            self._handle,
            "127.0.0.1",
            0,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *_args):
        """停止本地测试服务器。"""
        self.server.close()
        await self.server.wait_closed()

    async def _handle(self, reader, writer):
        """记录单次代理请求并返回确定性 HTTP 响应。"""
        try:
            raw = await reader.readuntil(b"\r\n\r\n")
            if not self.request.done():
                self.request.set_result(raw.decode("latin-1"))
            if raw.startswith(b"CONNECT "):
                status = self.connect_status
                writer.write(
                    f"HTTP/1.1 {status} Proxy Error\r\nContent-Length: 0\r\n\r\n".encode()
                )
            else:
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


class ResinTransportTests(unittest.IsolatedAsyncioTestCase):
    def _lease(self, proxy_url=None):
        """构造包含 Basic 认证的 Resin 租约。"""
        return ProxyLease(
            lease_id="resin",
            proxy_url=(
                proxy_url
                or "https://node.uuid:token@proxy.test:8443"
            ),
            provider=ProxyProvider.RESIN,
            affinity_key="resin:uuid",
        )

    def test_resin_forward_proxy_is_applied_to_http_and_https(self):
        """curl_cffi 应把同一 Resin 正向代理用于 HTTP 和 HTTPS。"""
        kwargs = build_session_kwargs(lease=self._lease())

        self.assertEqual(
            kwargs["proxies"],
            {
                "http": "https://node.uuid:token@proxy.test:8443",
                "https": "https://node.uuid:token@proxy.test:8443",
            },
        )

    async def test_resin_connect_failure_maps_to_stable_egress_error(self):
        """Resin CONNECT 失败应返回稳定出口错误且不触发其他出口。"""
        with patch.object(ResettableSession, "_create", return_value=_FailingSession()):
            session = ResettableSession(lease=self._lease())
            with self.assertRaises(UpstreamError) as caught:
                await session.get("https://grok.com")

        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(caught.exception.code, "egress_proxy_unavailable")
        self.assertNotIn("token", str(caught.exception))

    async def test_resin_407_maps_to_stable_egress_error(self):
        """Resin 代理认证失败应统一映射为出口不可用。"""
        with patch.object(
            ResettableSession,
            "_create",
            return_value=_ResponseSession(407),
        ):
            session = ResettableSession(lease=self._lease())
            with self.assertRaises(UpstreamError) as caught:
                await session.get("http://grok.com")

        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(caught.exception.code, "egress_proxy_unavailable")

    async def test_resin_gateway_502_and_503_map_to_stable_egress_error(self):
        """Resin 网关无节点时应保持失败关闭并隐藏原响应。"""
        for status_code in (502, 503):
            with self.subTest(status_code=status_code), patch.object(
                ResettableSession,
                "_create",
                return_value=_ResponseSession(status_code),
            ):
                session = ResettableSession(lease=self._lease())
                with self.assertRaises(UpstreamError) as caught:
                    await session.get("https://grok.com")

            self.assertEqual(caught.exception.status, 503)
            self.assertEqual(caught.exception.code, "egress_proxy_unavailable")

    async def test_local_forward_proxy_receives_basic_auth_for_http(self):
        """真实 HTTP 正向代理链路应携带 Resin Basic 认证。"""
        async with _RawForwardProxy() as proxy_server:
            proxy_url = (
                f"http://node.account:proxy-token@127.0.0.1:"
                f"{proxy_server.port}"
            )
            async with ResettableSession(
                lease=self._lease(proxy_url),
                browser_override="",
            ) as session:
                response = await session.get("http://upstream.invalid/resource")
            raw_request = await asyncio.wait_for(proxy_server.request, timeout=2)

        expected_auth = base64.b64encode(
            b"node.account:proxy-token"
        ).decode()
        self.assertEqual(response.status_code, 200)
        self.assertIn("GET http://upstream.invalid/resource", raw_request)
        self.assertIn(f"Proxy-Authorization: Basic {expected_auth}", raw_request)

    async def test_local_forward_proxy_connect_407_is_fail_closed(self):
        """真实 HTTPS CONNECT 认证失败应映射为统一出口错误。"""
        async with _RawForwardProxy(connect_status=407) as proxy_server:
            proxy_url = (
                f"http://node.account:proxy-token@127.0.0.1:"
                f"{proxy_server.port}"
            )
            async with ResettableSession(
                lease=self._lease(proxy_url),
                browser_override="",
            ) as session:
                with self.assertRaises(UpstreamError) as caught:
                    await session.get("https://upstream.invalid/resource")
            raw_request = await asyncio.wait_for(proxy_server.request, timeout=2)

        self.assertTrue(raw_request.startswith("CONNECT upstream.invalid:443"))
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(caught.exception.code, "egress_proxy_unavailable")

    async def test_websocket_connect_uses_same_resin_gateway(self):
        """WSS 握手应通过同一 Resin CONNECT 链路并保持失败关闭。"""
        async with _RawForwardProxy(connect_status=407) as proxy_server:
            proxy_url = (
                f"http://node.account:proxy-token@127.0.0.1:"
                f"{proxy_server.port}"
            )
            with self.assertRaises(UpstreamError) as caught:
                await WebSocketClient().connect(
                    "wss://upstream.invalid/socket",
                    timeout=2,
                    lease=self._lease(proxy_url),
                )
            raw_request = await asyncio.wait_for(proxy_server.request, timeout=2)

        self.assertTrue(raw_request.startswith("CONNECT upstream.invalid:443"))
        self.assertEqual(caught.exception.code, "egress_proxy_unavailable")


if __name__ == "__main__":
    unittest.main()
