"""统一网络代理服务。"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from typing import Any, Protocol

from app.control.account.identity import account_key_for_token
from app.platform.config.snapshot import get_config
from app.platform.errors import UpstreamError

from .clearance import ProxyClearanceManager
from .managed_pool import (
    ManagedProxyPool,
    get_managed_proxy_pool,
    mask_proxy_url,
    sanitize_proxy_error,
)
from .managed_state import ProxyHealthJobKind
from .migration import ensure_proxy_config_migrated, reset_proxy_migration_for_tests
from .models import (
    EgressMode,
    ProxyFeedback,
    ProxyLease,
    ProxyProvider,
    ProxyRequestContext,
    ProxyScope,
    RequestKind,
)
from .validation import (
    ProxyConfigIssue,
    validate_resin_url_template,
    validate_unified_proxy_config,
)


PROJECT_RESIN_NAMESPACE = uuid.UUID("f931b37f-66f1-5e63-b9b7-d37ff02d0b1c")
HEALTH_ACCOUNT_KEY = "health-probe"


def resin_uuid_for_account(account_key: str) -> str:
    """为脱敏账号 key 生成跨进程稳定的 Resin UUID。"""
    if not account_key:
        raise ValueError("account key is required")
    return str(uuid.uuid5(PROJECT_RESIN_NAMESPACE, account_key))


def render_resin_proxy_url(template: str, account_key: str) -> str:
    """使用账号稳定 UUID 渲染 Resin 正向代理模板。"""
    return template.replace("{uuid}", resin_uuid_for_account(account_key))


def proxy_context_for_token(
    token: str,
    *,
    origin: str = "https://grok.com",
    scope: ProxyScope = ProxyScope.APP,
    kind: RequestKind = RequestKind.HTTP,
    request_id: str = "",
) -> ProxyRequestContext:
    """根据账号 token 构造统一代理请求上下文。"""
    return ProxyRequestContext(
        account_key=account_key_for_token(token),
        origin=origin,
        scope=scope,
        kind=kind,
        request_id=request_id,
    )


class ProxyProviderAdapter(Protocol):
    """统一出口提供者协议。"""

    async def acquire(self, context: ProxyRequestContext) -> ProxyLease:
        """根据请求上下文返回代理租约。"""


class DirectProxyProvider:
    """生成直连租约。"""

    def __init__(self, clearance: ProxyClearanceManager) -> None:
        self._clearance = clearance

    async def acquire(self, context: ProxyRequestContext) -> ProxyLease:
        """为请求生成直连租约。"""
        return await self._clearance.acquire_lease(
            proxy_url=None,
            affinity_key="direct",
            provider=ProxyProvider.DIRECT,
            account_key=context.account_key,
            scope=context.scope,
            kind=context.kind,
            clearance_origin=context.origin,
        )


class ManagedProxyProvider:
    """通过共享运行态托管池生成租约。"""

    def __init__(
        self,
        pool: ManagedProxyPool,
        clearance: ProxyClearanceManager,
    ) -> None:
        self._pool = pool
        self._clearance = clearance

    async def acquire(self, context: ProxyRequestContext) -> ProxyLease:
        """按账号绑定选择健康托管节点。"""
        return await self._pool.acquire(
            account_key=context.account_key,
            lease_factory=self._clearance.acquire_lease,
            scope=context.scope,
            kind=context.kind,
            clearance_origin=context.origin,
        )


class ResinProxyProvider:
    """根据账号稳定 UUID 使用 Resin 正向代理网关。"""

    def __init__(self, clearance: ProxyClearanceManager) -> None:
        self._clearance = clearance

    async def acquire(self, context: ProxyRequestContext) -> ProxyLease:
        """渲染 Resin 模板并生成 fail-closed 租约。"""
        cfg = get_config()
        template = cfg.get_str("proxy.resin.url_template", "").strip()
        try:
            validate_resin_url_template(template)
        except ProxyConfigIssue as exc:
            raise UpstreamError(
                f"Egress proxy configuration is invalid: {exc}",
                status=503,
                code="egress_proxy_unavailable",
            ) from exc
        resin_uuid = resin_uuid_for_account(context.account_key)
        rendered = render_resin_proxy_url(template, context.account_key)
        lease = await self._clearance.acquire_lease(
            proxy_url=rendered,
            affinity_key=f"resin:{resin_uuid}",
            provider=ProxyProvider.RESIN,
            account_key=context.account_key,
            scope=context.scope,
            kind=context.kind,
            clearance_origin=context.origin,
        )
        lease.proxy_id = "resin-gateway"
        lease.proxy_mode = "uuid_template"
        lease.generation = int(
            hashlib.sha256(template.encode("utf-8", "ignore")).hexdigest()[:8],
            16,
        )
        return lease


class ProxyService:
    """统一选择出口、生成租约并应用反馈。"""

    def __init__(
        self,
        pool: ManagedProxyPool,
        clearance: ProxyClearanceManager | None = None,
    ) -> None:
        self._pool = pool
        self._clearance = clearance or ProxyClearanceManager()
        self._direct = DirectProxyProvider(self._clearance)
        self._managed = ManagedProxyProvider(self._pool, self._clearance)
        self._resin = ResinProxyProvider(self._clearance)
        self._mode = EgressMode.DIRECT
        self._route_lock = asyncio.Lock()
        self._last_probe: dict[str, Any] = {}

    @property
    def clearance_manager(self) -> ProxyClearanceManager:
        """返回调度器复用的 clearance 管理器。"""
        return self._clearance

    @property
    def managed_pool(self) -> ManagedProxyPool:
        """返回管理 API 复用的本地托管池。"""
        return self._pool

    @property
    def mode(self) -> EgressMode:
        """返回当前统一出口模式。"""
        return self._mode

    async def initialize(self) -> None:
        """完成配置迁移并加载托管池与 clearance。"""
        await ensure_proxy_config_migrated()
        await self.reload_config(load_managed_pool=True)

    async def reload_config(
        self,
        *,
        load_managed_pool: bool = False,
    ) -> None:
        """热加载统一出口配置，并按需同步托管池运行态。"""
        try:
            validated = validate_unified_proxy_config(get_config().raw())
        except ProxyConfigIssue as exc:
            raise UpstreamError(
                f"Egress proxy configuration is invalid: {exc}",
                status=503,
                code="egress_proxy_unavailable",
            ) from exc
        # Resin 与直连切换不依赖托管池共享状态，避免无关仓储拖慢设置保存。
        if load_managed_pool:
            await self._pool.load()
        await self._clearance.load()
        self._mode = EgressMode(validated.mode)

    async def acquire(self, context: ProxyRequestContext) -> ProxyLease:
        """按当前唯一出口模式获取代理租约。"""
        await self._synchronize_route()
        provider: ProxyProviderAdapter
        if self._mode == EgressMode.DIRECT:
            provider = self._direct
        elif self._mode == EgressMode.MANAGED_POOL:
            provider = self._managed
        else:
            provider = self._resin
        return await provider.acquire(context)

    async def _synchronize_route(self) -> None:
        """仅在其他 Worker 热切换模式后重新加载有效出口。"""
        raw_mode = get_config().get_str("proxy.egress.mode", "direct")
        if raw_mode == self._mode.value:
            return
        async with self._route_lock:
            raw_mode = get_config().get_str("proxy.egress.mode", "direct")
            if raw_mode != self._mode.value:
                await self.reload_config(
                    load_managed_pool=raw_mode == EgressMode.MANAGED_POOL.value,
                )

    async def feedback(self, lease: ProxyLease, result: ProxyFeedback) -> None:
        """统一应用 origin clearance 与托管节点反馈。"""
        await self._clearance.feedback(lease, result)
        if lease.provider == ProxyProvider.MANAGED_POOL:
            await self._pool.feedback(lease, result)

    async def derive(
        self,
        lease: ProxyLease,
        *,
        origin: str,
        scope: ProxyScope,
        kind: RequestKind,
    ) -> ProxyLease:
        """保持出口身份不变，为子步骤附加目标 origin 的 clearance。"""
        if (
            lease.origin == origin
            and lease.scope == scope
            and lease.kind == kind
        ):
            return lease
        derived = await self._clearance.acquire_lease(
            proxy_url=lease.proxy_url,
            affinity_key=lease.affinity_key,
            provider=lease.provider,
            account_key=lease.account_key,
            scope=scope,
            kind=kind,
            clearance_origin=origin,
        )
        # 派生租约只更新目标上下文，出口节点与运行态版本必须保持不变。
        derived.proxy_id = lease.proxy_id
        derived.proxy_mode = lease.proxy_mode
        derived.generation = lease.generation
        derived.runtime_epoch = lease.runtime_epoch
        return derived

    async def probe_effective(self) -> dict[str, Any]:
        """通过当前出口访问检测地址并记录脱敏结果。"""
        from app.dataplane.proxy.adapters.session import ResettableSession

        cfg = get_config()
        target = cfg.get_str("proxy.health.check_url", "https://console.x.ai/")
        timeout = max(1, cfg.get_int("proxy.health.check_timeout_sec", 15))
        context = ProxyRequestContext(
            account_key=HEALTH_ACCOUNT_KEY,
            origin=target,
            scope=ProxyScope.APP,
            kind=RequestKind.HTTP,
            request_id="health-probe",
        )
        started = time.monotonic()
        try:
            lease = await self.acquire(context)
            async with ResettableSession(lease=lease) as session:
                response = await session.get(target, timeout=timeout)
            latency_ms = int((time.monotonic() - started) * 1000)
            ok = 200 <= response.status_code < 400
            result = {
                "ok": ok,
                "provider": lease.provider.value,
                "status_code": response.status_code,
                "latency_ms": latency_ms,
                "checked_at": int(time.time() * 1000),
                "error": "",
            }
        except Exception as exc:
            result = {
                "ok": False,
                "provider": self.mode.value,
                "status_code": None,
                "latency_ms": int((time.monotonic() - started) * 1000),
                "checked_at": int(time.time() * 1000),
                "error": sanitize_proxy_error(str(exc)),
            }
        self._last_probe = result
        return dict(result)

    async def enqueue_probe(self) -> str:
        """在共享健康任务仓储创建有效出口检测任务。"""
        job = await self._pool.create_health_job(
            ProxyHealthJobKind.PROVIDER_MANUAL,
        )
        return job.job_id

    async def snapshot(self) -> dict[str, Any]:
        """返回统一代理模式、Resin 和本地节点快照。"""
        await self._synchronize_route()
        pool_snapshot = await self._pool.snapshot()
        template = get_config().get_str("proxy.resin.url_template", "").strip()
        pool_snapshot.update(
            {
                "mode": self.mode.value,
                "skip_ssl_verify": get_config().get_bool(
                    "proxy.egress.skip_ssl_verify",
                    False,
                ),
                "resin": {
                    "configured": bool(template),
                    "url_template": mask_proxy_url(template),
                    "last_probe": dict(self._last_probe),
                },
            }
        )
        return pool_snapshot


_proxy_service: ProxyService | None = None


async def get_proxy_service() -> ProxyService:
    """返回进程内统一代理服务。"""
    global _proxy_service
    if _proxy_service is None:
        # 代理池初始化会同步共享运行态，迁移必须先保留旧节点身份。
        await ensure_proxy_config_migrated()
        pool = await get_managed_proxy_pool()
        _proxy_service = ProxyService(pool)
        await _proxy_service.initialize()
    return _proxy_service


async def reset_proxy_service_for_tests() -> None:
    """清理统一代理服务单例。"""
    global _proxy_service
    _proxy_service = None
    reset_proxy_migration_for_tests()


__all__ = [
    "DirectProxyProvider",
    "HEALTH_ACCOUNT_KEY",
    "ManagedProxyProvider",
    "PROJECT_RESIN_NAMESPACE",
    "ProxyService",
    "ResinProxyProvider",
    "get_proxy_service",
    "proxy_context_for_token",
    "render_resin_proxy_url",
    "reset_proxy_service_for_tests",
    "resin_uuid_for_account",
]
