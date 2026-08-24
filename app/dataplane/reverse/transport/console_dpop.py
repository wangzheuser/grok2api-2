"""Console DPoP 会话交换、证明签名和有界缓存。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse, urlunparse

import orjson
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

from app.control.proxy.models import ProxyLease
from app.dataplane.proxy.adapters.headers import build_console_headers
from app.platform.errors import UpstreamError


_CACHE_LIMIT = 4096
_REFRESH_SKEW_SECONDS = 20
_MAX_TOKEN_LIFETIME_SECONDS = 3600


def _base64url(value: bytes) -> str:
    """返回无填充的 Base64URL 文本。"""
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode_base64url(value: str) -> bytes:
    """解码无填充 Base64URL 文本。"""
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(value + padding)
    except Exception as exc:
        raise UpstreamError("Console DPoP token payload is invalid", status=502) from exc


def public_jwk(key: ec.EllipticCurvePrivateKey) -> dict[str, str]:
    """从 P-256 私钥导出公开 JWK。"""
    numbers = key.public_key().public_numbers()
    return {
        "kty": "EC",
        "crv": "P-256",
        "x": _base64url(numbers.x.to_bytes(32, "big")),
        "y": _base64url(numbers.y.to_bytes(32, "big")),
    }


def jwk_thumbprint(jwk: dict[str, str]) -> str:
    """按 RFC 7638 计算 EC JWK thumbprint。"""
    canonical = {
        "crv": str(jwk.get("crv") or ""),
        "kty": str(jwk.get("kty") or ""),
        "x": str(jwk.get("x") or ""),
        "y": str(jwk.get("y") or ""),
    }
    return _base64url(hashlib.sha256(orjson.dumps(canonical)).digest())


def parse_access_token(value: str) -> tuple[int, str]:
    """读取 DPoP access token 的 exp 与 cnf.jkt 绑定声明。"""
    parts = value.split(".")
    if len(parts) != 3:
        raise UpstreamError("Console DPoP access token format is invalid", status=502)
    try:
        payload = orjson.loads(_decode_base64url(parts[1]))
        expires_at = int(payload.get("exp") or 0)
        confirmation = payload.get("cnf") or {}
        thumbprint = str(confirmation.get("jkt") or "").strip()
    except (TypeError, ValueError, orjson.JSONDecodeError) as exc:
        raise UpstreamError("Console DPoP access token claims are invalid", status=502) from exc
    if expires_at <= 0 or not thumbprint:
        raise UpstreamError("Console DPoP access token claims are invalid", status=502)
    return expires_at, thumbprint


def dpop_htu(url: str) -> str:
    """构造忽略 query/fragment 的 DPoP HTTP URI。"""
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("DPoP URL must be absolute")
    return urlunparse((parsed.scheme.lower(), parsed.netloc, parsed.path or "/", "", "", ""))


def clock_skew_from_date_header(date_header: str, local_before: float, local_after: float) -> int:
    """根据响应 Date 和请求往返时间中点估算服务端时钟偏差秒数。"""
    if not date_header.strip():
        return 0
    try:
        server_timestamp = parsedate_to_datetime(date_header).timestamp()
    except (TypeError, ValueError, OverflowError):
        return 0
    if local_after < local_before:
        local_after = local_before
    local_midpoint = local_before + (local_after - local_before) / 2
    delta = server_timestamp - local_midpoint
    # 与前端/Go Duration.Round 一致：恰好半秒时远离零取整。
    return int(delta + 0.5) if delta >= 0 else int(delta - 0.5)


@dataclass(frozen=True, slots=True)
class DPoPSession:
    """与账号、代理身份和 P-256 私钥绑定的短期访问会话。"""

    access_token: str
    private_key: ec.EllipticCurvePrivateKey
    jwk: dict[str, str]
    expires_at: float
    clock_skew_seconds: int = 0


@dataclass(frozen=True, slots=True)
class DPoPAuthorization:
    """一次上游请求使用的 DPoP 头和缓存身份。"""

    headers: dict[str, str]
    cache_key: str
    access_token: str


class ConsoleDPoPTokenError(UpstreamError):
    """Console DPoP token 交换端点返回的 HTTP 失败。"""


class ConsoleDPoPTransportError(UpstreamError):
    """Console DPoP token 交换阶段发生的网络传输失败。"""


def build_dpop_proof(
    session: DPoPSession,
    *,
    method: str,
    url: str,
    now: float | None = None,
) -> str:
    """为一次 HTTP 请求生成 ES256 DPoP proof。"""
    local_now = time.time() if now is None else now
    protected_header = {"typ": "dpop+jwt", "alg": "ES256", "jwk": session.jwk}
    claims = {
        "jti": str(uuid.uuid4()),
        "htm": method.upper(),
        "htu": dpop_htu(url),
        "iat": int(local_now + session.clock_skew_seconds),
        "ath": _base64url(hashlib.sha256(session.access_token.encode()).digest()),
    }
    encoded_header = _base64url(orjson.dumps(protected_header))
    encoded_claims = _base64url(orjson.dumps(claims))
    signing_input = f"{encoded_header}.{encoded_claims}".encode("ascii")
    der_signature = session.private_key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der_signature)
    raw_signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    return f"{encoded_header}.{encoded_claims}.{_base64url(raw_signature)}"


def dpop_cache_key(base_url: str, token: str, lease: ProxyLease | None) -> str:
    """构造绑定账号、代理实例及其运行代次的缓存键。"""
    normalized_base = base_url.strip().rstrip("/")
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    if lease is None:
        return f"{normalized_base}|{token_hash}|direct|0|0"
    proxy_identity = lease.affinity_key or lease.proxy_id or "direct"
    account_identity = lease.account_key or token_hash
    return "|".join(
        (
            normalized_base,
            token_hash,
            account_identity,
            proxy_identity,
            str(lease.generation),
            str(lease.runtime_epoch),
        )
    )


class ConsoleDPoPManager:
    """管理按账号和出口绑定的 DPoP session，并合并并发 token 交换。"""

    def __init__(self, *, cache_limit: int = _CACHE_LIMIT) -> None:
        self._cache_limit = max(1, cache_limit)
        self._sessions: OrderedDict[str, DPoPSession] = OrderedDict()
        self._loads: dict[str, asyncio.Task[DPoPSession]] = {}
        self._lock = asyncio.Lock()

    async def authorize(
        self,
        *,
        http_session: Any,
        token: str,
        lease: ProxyLease | None,
        method: str,
        url: str,
        timeout_s: float,
    ) -> DPoPAuthorization:
        """取得缓存会话并生成当前请求的认证头。"""
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        session, cache_key = await self.get(
            http_session=http_session,
            token=token,
            lease=lease,
            base_url=base_url,
            timeout_s=timeout_s,
        )
        headers = build_console_headers(token, lease=lease)
        headers["Authorization"] = f"DPoP {session.access_token}"
        headers["DPoP"] = build_dpop_proof(session, method=method, url=url)
        if urlparse(url).path.endswith("/responses"):
            headers["x-cluster"] = "https://us-east-1.api.x.ai"
        return DPoPAuthorization(
            headers=headers,
            cache_key=cache_key,
            access_token=session.access_token,
        )

    async def get(
        self,
        *,
        http_session: Any,
        token: str,
        lease: ProxyLease | None,
        base_url: str,
        timeout_s: float,
    ) -> tuple[DPoPSession, str]:
        """返回有效缓存会话，未命中时仅发起一次共享 token 交换。"""
        cache_key = dpop_cache_key(base_url, token, lease)
        async with self._lock:
            cached = self._cached_locked(cache_key)
            if cached is not None:
                return cached, cache_key
            load = self._loads.get(cache_key)
            if load is None:
                load = asyncio.create_task(
                    self._mint(
                        http_session=http_session,
                        token=token,
                        lease=lease,
                        base_url=base_url,
                        timeout_s=timeout_s,
                    )
                )
                self._loads[cache_key] = load

        try:
            session = await asyncio.shield(load)
        except BaseException:
            if load.done():
                async with self._lock:
                    if self._loads.get(cache_key) is load:
                        self._loads.pop(cache_key, None)
            raise
        async with self._lock:
            existing = self._cached_locked(cache_key)
            if existing is None:
                self._store_locked(cache_key, session)
            else:
                session = existing
            if self._loads.get(cache_key) is load:
                self._loads.pop(cache_key, None)
        return session, cache_key

    async def invalidate(self, cache_key: str, access_token: str = "") -> None:
        """仅在缓存仍指向给定 access token 时删除会话。"""
        async with self._lock:
            current = self._sessions.get(cache_key)
            if current is None:
                return
            if access_token and current.access_token != access_token:
                return
            self._sessions.pop(cache_key, None)

    async def clear(self) -> None:
        """清空缓存；主要供测试和运行态重载使用。"""
        async with self._lock:
            self._sessions.clear()

    def _cached_locked(self, cache_key: str) -> DPoPSession | None:
        """在锁内读取并刷新 LRU 顺序。"""
        session = self._sessions.get(cache_key)
        if session is None:
            return None
        if session.expires_at <= time.time() + _REFRESH_SKEW_SECONDS:
            self._sessions.pop(cache_key, None)
            return None
        self._sessions.move_to_end(cache_key)
        return session

    def _store_locked(self, cache_key: str, session: DPoPSession) -> None:
        """在锁内写入缓存，并驱逐最久未使用项。"""
        self._sessions[cache_key] = session
        self._sessions.move_to_end(cache_key)
        while len(self._sessions) > self._cache_limit:
            self._sessions.popitem(last=False)

    async def _mint(
        self,
        *,
        http_session: Any,
        token: str,
        lease: ProxyLease | None,
        base_url: str,
        timeout_s: float,
    ) -> DPoPSession:
        """通过同一代理会话交换并验证新的 DPoP access token。"""
        private_key = ec.generate_private_key(ec.SECP256R1())
        jwk = public_jwk(private_key)
        endpoint = f"{base_url.rstrip('/')}/v1/dpop/token"
        headers = build_console_headers(token, lease=lease)
        # token 交换只依赖 SSO Cookie，不能带旧 DPoP 或 responses 集群路由头。
        for key in ("Authorization", "DPoP", "x-cluster"):
            headers.pop(key, None)

        local_before = time.time()
        try:
            response = await http_session.post(
                endpoint,
                headers=headers,
                data=orjson.dumps({"jwk": jwk}),
                timeout=min(max(timeout_s, 1.0), 30.0),
            )
        except Exception as exc:
            raise ConsoleDPoPTransportError(
                f"Console DPoP token transport failed: {exc}",
                status=502,
            ) from exc
        local_after = time.time()
        body_bytes = bytes(response.content or b"")
        if response.status_code < 200 or response.status_code >= 300:
            body = body_bytes.decode("utf-8", "replace")[:400]
            raise ConsoleDPoPTokenError(
                f"Console DPoP token endpoint returned {response.status_code}",
                status=response.status_code,
                body=body,
                details={"body_class": "dpop_token_endpoint"},
            )

        try:
            payload = orjson.loads(body_bytes)
            access_token = str(payload.get("access_token") or "").strip()
            token_type = str(payload.get("token_type") or "").strip()
            expires_in = int(payload.get("expires_in") or 0)
        except (TypeError, ValueError, orjson.JSONDecodeError) as exc:
            raise UpstreamError("Console DPoP token response is invalid", status=502) from exc
        if not access_token or token_type.lower() != "dpop":
            raise UpstreamError("Console DPoP token response is invalid", status=502)
        if expires_in <= 0 or expires_in > _MAX_TOKEN_LIFETIME_SECONDS:
            raise UpstreamError("Console DPoP token lifetime is invalid", status=502)

        token_expiry, token_thumbprint = parse_access_token(access_token)
        if token_thumbprint != jwk_thumbprint(jwk):
            raise UpstreamError("Console DPoP token is not bound to the local key", status=502)

        clock_skew = clock_skew_from_date_header(
            str(response.headers.get("Date", "")),
            local_before,
            local_after,
        )
        now = time.time()
        # JWT exp 是服务端时间；先减去已观测偏差，再与 expires_in 的本地期限取较早值。
        expires_at = min(now + expires_in, token_expiry - clock_skew)
        if expires_at <= now + _REFRESH_SKEW_SECONDS:
            raise UpstreamError("Console DPoP token is expired or near expiry", status=502)
        return DPoPSession(
            access_token=access_token,
            private_key=private_key,
            jwk=jwk,
            expires_at=expires_at,
            clock_skew_seconds=clock_skew,
        )


console_dpop_manager = ConsoleDPoPManager()


__all__ = [
    "ConsoleDPoPManager",
    "ConsoleDPoPTokenError",
    "ConsoleDPoPTransportError",
    "DPoPAuthorization",
    "DPoPSession",
    "build_dpop_proof",
    "clock_skew_from_date_header",
    "console_dpop_manager",
    "dpop_cache_key",
    "dpop_htu",
    "jwk_thumbprint",
    "parse_access_token",
    "public_jwk",
]
