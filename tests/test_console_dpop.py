import asyncio
import base64
import hashlib
import time
import unittest
from email.utils import formatdate
from unittest.mock import patch

import orjson
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

from app.control.proxy.models import ProxyFeedbackKind, ProxyLease, ProxyProvider
from app.dataplane.reverse.protocol.xai_console_chat import (
    _classify_console_error_body,
    _post_console_dpop,
    _status_feedback,
)
from app.dataplane.reverse.transport.console_dpop import (
    ConsoleDPoPManager,
    DPoPSession,
    build_dpop_proof,
    clock_skew_from_date_header,
    dpop_htu,
    jwk_thumbprint,
    public_jwk,
)


def _base64url(value: bytes) -> str:
    """返回测试用无填充 Base64URL。"""
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode_base64url(value: str) -> bytes:
    """解码测试用无填充 Base64URL。"""
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _access_token(jwk: dict[str, str], *, sequence: int, server_skew: int = 0) -> str:
    """构造仅用于本地 claims 校验的 DPoP access token。"""
    header = _base64url(orjson.dumps({"alg": "ES256", "typ": "JWT"}))
    claims = _base64url(
        orjson.dumps(
            {
                "exp": int(time.time()) + server_skew + 300,
                "cnf": {"jkt": jwk_thumbprint(jwk)},
                "seq": sequence,
            }
        )
    )
    return f"{header}.{claims}.{_base64url(f'signature-{sequence}'.encode())}"


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        content: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.closed = False

    async def acontent(self) -> bytes:
        """返回已缓存的响应体。"""
        return self.content

    async def aclose(self) -> None:
        """记录流响应关闭状态。"""
        self.closed = True


class _FakeConsoleSession:
    def __init__(self, *, server_skew: int = 0, delay: float = 0.0) -> None:
        self.server_skew = server_skew
        self.delay = delay
        self.mint_calls = 0
        self.response_calls = 0
        self.mint_headers: list[dict[str, str]] = []
        self.response_headers: list[dict[str, str]] = []
        self.response_statuses: list[int] = []
        self.responses: list[_FakeResponse] = []

    async def post(self, url, *, headers, data, timeout, stream=False):
        """模拟 DPoP token 交换及受保护 responses 请求。"""
        if url.endswith("/v1/dpop/token"):
            self.mint_calls += 1
            self.mint_headers.append(dict(headers))
            if self.delay:
                await asyncio.sleep(self.delay)
            jwk = orjson.loads(data)["jwk"]
            token = _access_token(
                jwk,
                sequence=self.mint_calls,
                server_skew=self.server_skew,
            )
            body = orjson.dumps(
                {"access_token": token, "token_type": "DPoP", "expires_in": 300}
            )
            return _FakeResponse(
                200,
                content=body,
                headers={
                    "Date": formatdate(
                        timeval=time.time() + self.server_skew,
                        usegmt=True,
                    )
                },
            )

        self.response_calls += 1
        self.response_headers.append(dict(headers))
        status = self.response_statuses.pop(0) if self.response_statuses else 200
        response = _FakeResponse(status, content=b'{"error":"expired"}' if status == 401 else b"")
        self.responses.append(response)
        return response


def _lease(**updates) -> ProxyLease:
    """构造带稳定账号和代理身份的测试租约。"""
    values = {
        "lease_id": "lease-1",
        "proxy_url": "http://proxy.example:8080",
        "cf_cookies": "cf_clearance=clearance; __cf_bm=bm",
        "user_agent": "Mozilla/5.0 Chrome/136.0.0.0",
        "proxy_id": "proxy-1",
        "generation": 2,
        "runtime_epoch": 3,
        "account_key": "account-1",
        "provider": ProxyProvider.MANAGED_POOL,
    }
    values.update(updates)
    values.setdefault(
        "affinity_key",
        (
            f"managed:{values['proxy_id']}:{values['generation']}:"
            f"{values['runtime_epoch']}"
        ),
    )
    return ProxyLease(**values)


class DPoPProofTests(unittest.TestCase):
    def test_proof_is_valid_es256_and_binds_method_url_and_token(self):
        """验证 proof 的签名格式、HTU、HTM、IAT 和 access token hash。"""
        private_key = ec.generate_private_key(ec.SECP256R1())
        jwk = public_jwk(private_key)
        session = DPoPSession(
            access_token="access-token",
            private_key=private_key,
            jwk=jwk,
            expires_at=9999999999,
            clock_skew_seconds=-80,
        )

        proof = build_dpop_proof(
            session,
            method="post",
            url="https://console.x.ai/v1/responses?ignored=1#fragment",
            now=1_700_000_100.9,
        )
        encoded_header, encoded_claims, encoded_signature = proof.split(".")
        header = orjson.loads(_decode_base64url(encoded_header))
        claims = orjson.loads(_decode_base64url(encoded_claims))
        raw_signature = _decode_base64url(encoded_signature)
        r = int.from_bytes(raw_signature[:32], "big")
        s = int.from_bytes(raw_signature[32:], "big")
        private_key.public_key().verify(
            encode_dss_signature(r, s),
            f"{encoded_header}.{encoded_claims}".encode("ascii"),
            ec.ECDSA(hashes.SHA256()),
        )

        self.assertEqual(header["typ"], "dpop+jwt")
        self.assertEqual(header["alg"], "ES256")
        self.assertEqual(header["jwk"], jwk)
        self.assertEqual(claims["htm"], "POST")
        self.assertEqual(claims["htu"], "https://console.x.ai/v1/responses")
        self.assertEqual(claims["iat"], 1_700_000_020)
        self.assertEqual(
            claims["ath"],
            _base64url(hashlib.sha256(b"access-token").digest()),
        )
        self.assertTrue(claims["jti"])

    def test_htu_and_clock_skew_are_deterministic(self):
        """URL 查询参数被移除，Date 偏差按往返中点取整。"""
        self.assertEqual(
            dpop_htu("https://console.x.ai?query=1"),
            "https://console.x.ai/",
        )
        self.assertEqual(
            clock_skew_from_date_header(
                "Tue, 14 Nov 2023 22:14:20 GMT",
                1_700_000_000,
                1_700_000_002,
            ),
            59,
        )
        self.assertEqual(clock_skew_from_date_header("bad-date", 1, 2), 0)
        self.assertEqual(
            clock_skew_from_date_header(
                "Tue, 14 Nov 2023 22:13:21 GMT",
                1_700_000_000,
                1_700_000_001,
            ),
            1,
        )
        self.assertEqual(
            clock_skew_from_date_header(
                "Tue, 14 Nov 2023 22:13:19 GMT",
                1_700_000_000,
                1_700_000_001,
            ),
            -2,
        )


class DPoPManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_requests_share_one_token_exchange(self):
        """同账号同代理的并发请求只交换一次 token，并各自生成 proof。"""
        manager = ConsoleDPoPManager()
        session = _FakeConsoleSession(server_skew=45, delay=0.01)
        lease = _lease()

        authorizations = await asyncio.gather(
            *[
                manager.authorize(
                    http_session=session,
                    token="sso-token",
                    lease=lease,
                    method="POST",
                    url="https://console.x.ai/v1/responses",
                    timeout_s=10,
                )
                for _ in range(8)
            ]
        )

        self.assertEqual(session.mint_calls, 1)
        self.assertEqual(len({item.access_token for item in authorizations}), 1)
        self.assertEqual(len({item.headers["DPoP"] for item in authorizations}), 8)
        self.assertTrue(all(item.headers["Authorization"].startswith("DPoP ") for item in authorizations))
        self.assertTrue(all(item.headers["x-cluster"] for item in authorizations))
        mint_headers = session.mint_headers[0]
        self.assertNotIn("Authorization", mint_headers)
        self.assertNotIn("DPoP", mint_headers)
        self.assertNotIn("x-cluster", mint_headers)
        self.assertIn("sso=sso-token", mint_headers["Cookie"])

    async def test_cache_is_bound_to_proxy_generation_and_runtime_epoch(self):
        """代理实例、配置代次或运行代次变化都会获取独立 DPoP 会话。"""
        manager = ConsoleDPoPManager()
        session = _FakeConsoleSession()
        common = {
            "http_session": session,
            "token": "sso-token",
            "method": "POST",
            "url": "https://console.x.ai/v1/responses",
            "timeout_s": 10,
        }
        await manager.authorize(lease=_lease(), **common)
        await manager.authorize(lease=_lease(runtime_epoch=4), **common)
        await manager.authorize(lease=_lease(proxy_id="proxy-2"), **common)
        await manager.authorize(lease=_lease(generation=3), **common)

        self.assertEqual(session.mint_calls, 4)

    async def test_conditional_invalidation_refreshes_token_once(self):
        """命中 access token 的失效会刷新，不匹配的旧失效请求不会删除新缓存。"""
        manager = ConsoleDPoPManager()
        session = _FakeConsoleSession()
        kwargs = {
            "http_session": session,
            "token": "sso-token",
            "lease": _lease(),
            "method": "POST",
            "url": "https://console.x.ai/v1/responses",
            "timeout_s": 10,
        }
        first = await manager.authorize(**kwargs)
        await manager.invalidate(first.cache_key, "different-token")
        cached = await manager.authorize(**kwargs)
        self.assertEqual(cached.access_token, first.access_token)

        await manager.invalidate(first.cache_key, first.access_token)
        refreshed = await manager.authorize(**kwargs)
        self.assertNotEqual(refreshed.access_token, first.access_token)
        self.assertEqual(session.mint_calls, 2)

    async def test_request_retries_once_after_401_with_fresh_session(self):
        """受保护请求首次 401 时关闭旧流、清缓存并仅重试一次。"""
        manager = ConsoleDPoPManager()
        session = _FakeConsoleSession()
        session.response_statuses = [401, 200]
        with patch(
            "app.dataplane.reverse.protocol.xai_console_chat.console_dpop_manager",
            manager,
        ):
            response = await _post_console_dpop(
                session=session,
                token="sso-token",
                lease=_lease(),
                endpoint="https://console.x.ai/v1/responses",
                payload_bytes=b"{}",
                timeout_s=10,
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(session.response_calls, 2)
        self.assertEqual(session.mint_calls, 2)
        self.assertTrue(session.responses[0].closed)
        first_auth = session.response_headers[0]["Authorization"]
        second_auth = session.response_headers[1]["Authorization"]
        self.assertNotEqual(first_auth, second_auth)


class ConsoleFailureClassificationTests(unittest.TestCase):
    def test_dpop_and_account_block_do_not_invalidate_clearance(self):
        """协议级 DPoP 和明确账号封禁不会被误判为 Cloudflare challenge。"""
        dpop_body = '{"code":"unauthorized:dpop-required"}'
        blocked_body = '{"error":{"message":"User is blocked"}}'
        self.assertEqual(_classify_console_error_body(dpop_body), "dpop_required")
        self.assertEqual(_classify_console_error_body(blocked_body), "account_blocked")
        self.assertEqual(
            _status_feedback(403, "dpop_required").kind,
            ProxyFeedbackKind.FORBIDDEN,
        )
        self.assertEqual(
            _status_feedback(403, "account_blocked").kind,
            ProxyFeedbackKind.FORBIDDEN,
        )
        self.assertEqual(
            _status_feedback(403, "cloudflare_challenge").kind,
            ProxyFeedbackKind.CHALLENGE,
        )


if __name__ == "__main__":
    unittest.main()
