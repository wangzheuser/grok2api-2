"""统一代理服务的数据面门面。"""

from app.control.proxy import ProxyService, get_proxy_service
from app.control.proxy.models import (
    EgressMode,
    ProxyFeedback,
    ProxyLease,
    ProxyRequestContext,
    ProxyScope,
    RequestKind,
)
from app.control.proxy.service import proxy_context_for_token


class ProxyRuntime:
    """向传输层暴露稳定的租约获取和反馈接口。"""

    def __init__(self, service: ProxyService) -> None:
        self._service = service

    async def acquire(
        self,
        context: ProxyRequestContext | None = None,
        *,
        token: str = "",
        scope: ProxyScope = ProxyScope.APP,
        kind: RequestKind = RequestKind.HTTP,
        clearance_origin: str | None = None,
        request_id: str = "",
    ) -> ProxyLease:
        """根据账号和目标上下文获取当前统一出口租约。"""
        if context is None:
            context = (
                proxy_context_for_token(
                    token,
                    origin=clearance_origin or "https://grok.com",
                    scope=scope,
                    kind=kind,
                    request_id=request_id,
                )
                if token
                else ProxyRequestContext(
                    account_key="system",
                    origin=clearance_origin or "https://grok.com",
                    scope=scope,
                    kind=kind,
                    request_id=request_id,
                )
            )
        return await self._service.acquire(context)

    async def feedback(self, lease: ProxyLease, result: ProxyFeedback) -> None:
        """把请求结果统一回写到 clearance 和托管池。"""
        await self._service.feedback(lease, result)

    async def derive(
        self,
        lease: ProxyLease,
        *,
        origin: str,
        scope: ProxyScope,
        kind: RequestKind,
    ) -> ProxyLease:
        """为复合请求子步骤派生同出口、独立 origin 的租约。"""
        return await self._service.derive(
            lease,
            origin=origin,
            scope=scope,
            kind=kind,
        )

    @property
    def has_proxy(self) -> bool:
        """返回当前模式是否使用代理。"""
        return self._service.mode != EgressMode.DIRECT

    @property
    def service(self) -> ProxyService:
        """返回管理与调度复用的统一代理服务。"""
        return self._service


_runtime: ProxyRuntime | None = None


async def get_proxy_runtime() -> ProxyRuntime:
    """返回进程内数据面代理门面。"""
    global _runtime
    service = await get_proxy_service()
    if _runtime is None or _runtime.service is not service:
        _runtime = ProxyRuntime(service)
    return _runtime


__all__ = ["ProxyRuntime", "get_proxy_runtime"]
