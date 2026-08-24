"""统一代理控制面领域模型。"""

from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Self

from pydantic import BaseModel


class ProxyScope(StrEnum):
    """请求用途，仅用于日志、clearance 和反馈上下文。"""

    APP = "app"
    ASSET = "asset"


class RequestKind(StrEnum):
    """业务传输类型。"""

    HTTP = "http"
    WEBSOCKET = "websocket"
    GRPC = "grpc"


class EgressMode(StrEnum):
    """统一出口模式。"""

    DIRECT = "direct"
    MANAGED_POOL = "managed_pool"
    RESIN = "resin"


class ProxyProvider(StrEnum):
    """实际生成租约的出口提供者。"""

    DIRECT = "direct"
    MANAGED_POOL = "managed_pool"
    RESIN = "resin"


class ClearanceMode(StrEnum):
    """Cloudflare clearance 获取模式。"""

    NONE = "none"
    MANUAL = "manual"
    FLARESOLVERR = "flaresolverr"

    @classmethod
    def parse(cls, value: str | Self) -> Self:
        """把配置文本解析为 clearance 模式。"""
        if isinstance(value, cls):
            return value
        normalized = str(value or "").strip().lower()
        if not normalized:
            return cls.NONE
        return cls(normalized)


class ClearanceBundleState(IntEnum):
    """Clearance bundle 生命周期状态。"""

    VALID = 0
    STALE = 1
    INVALID = 2


class ProxyFeedbackKind(StrEnum):
    """统一反馈分类。"""

    SUCCESS = "success"
    CHALLENGE = "challenge"
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    RATE_LIMITED = "rate_limited"
    UPSTREAM_5XX = "upstream_5xx"
    TRANSPORT_ERROR = "transport_error"


class ClearanceBundle(BaseModel):
    """与出口身份和 origin 绑定的 Cloudflare 凭据。"""

    bundle_id: str
    cf_cookies: str = ""
    user_agent: str = ""
    state: ClearanceBundleState = ClearanceBundleState.VALID
    affinity_key: str = ""
    clearance_host: str = "grok.com"
    last_refresh_at: int | None = None


class ProxyLease(BaseModel):
    """一次逻辑请求在其子步骤间传递的统一出口租约。"""

    lease_id: str
    proxy_url: str | None = None
    cf_cookies: str = ""
    user_agent: str = ""
    clearance_host: str = "grok.com"
    scope: ProxyScope = ProxyScope.APP
    kind: RequestKind = RequestKind.HTTP
    acquired_at: int = 0
    proxy_id: str = ""
    proxy_mode: str = ""
    generation: int = 0
    runtime_epoch: int = 0
    account_key: str = ""
    provider: ProxyProvider = ProxyProvider.DIRECT
    affinity_key: str = "direct"
    origin: str = "https://grok.com"

    @property
    def has_proxy(self) -> bool:
        """返回租约是否包含正向代理 URL。"""
        return bool(self.proxy_url)


class ProxyFeedback(BaseModel):
    """业务请求结束后回写的代理相关观测。"""

    kind: ProxyFeedbackKind
    status_code: int | None = None
    reason: str = ""
    retry_after_ms: int | None = None


@dataclass(frozen=True, slots=True)
class ProxyRequestContext:
    """一次上游请求参与代理选择的稳定上下文。"""

    account_key: str
    origin: str = "https://grok.com"
    scope: ProxyScope = ProxyScope.APP
    kind: RequestKind = RequestKind.HTTP
    request_id: str = ""


__all__ = [
    "ClearanceBundle",
    "ClearanceBundleState",
    "ClearanceMode",
    "EgressMode",
    "ProxyFeedback",
    "ProxyFeedbackKind",
    "ProxyLease",
    "ProxyProvider",
    "ProxyRequestContext",
    "ProxyScope",
    "RequestKind",
]
