"""代理配置的规范化与一致性校验。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit


SUPPORTED_PROXY_SCHEMES = frozenset(
    {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}
)
SUPPORTED_EGRESS_MODES = frozenset({"direct", "single_proxy", "proxy_pool"})
SUPPORTED_ROTATION_STRATEGIES = frozenset(
    {"sticky_failover", "round_robin", "random"}
)


@dataclass(frozen=True, slots=True)
class ProxyConfigIssue(ValueError):
    """描述一个可映射到管理端字段的代理配置错误。"""

    message: str
    path: str
    code: str

    def __str__(self) -> str:
        """返回适合日志和 API 展示的错误消息。"""
        return self.message


@dataclass(frozen=True, slots=True)
class ValidatedEgressConfig:
    """运行时可直接装载的全局出口代理配置。"""

    mode: str
    proxy_url: str
    proxy_pool: tuple[str, ...]
    rotation_strategy: str
    resource_proxy_url: str
    resource_proxy_pool: tuple[str, ...]

    @property
    def has_proxy(self) -> bool:
        """返回当前模式是否配置了真实代理出口。"""
        if self.mode == "single_proxy":
            return bool(self.proxy_url)
        if self.mode == "proxy_pool":
            return bool(self.proxy_pool)
        return False


def normalize_proxy_list(value: Any, *, path: str) -> list[str]:
    """把数组、逗号文本或多行文本规范化为去重后的代理 URL 列表。"""
    if value in (None, ""):
        return []
    if isinstance(value, str):
        items = re.split(r"[,\r\n]+", value)
    elif isinstance(value, (list, tuple)):
        items = value
    else:
        raise ProxyConfigIssue(
            "Proxy pool must be a list or separated text",
            path,
            "invalid_proxy_pool",
        )

    normalized: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(items, start=1):
        url = str(item or "").strip()
        if not url:
            continue
        validate_proxy_url(url, path=f"{path}[{index - 1}]")
        if url in seen:
            continue
        seen.add(url)
        normalized.append(url)
    return normalized


def validate_proxy_url(value: Any, *, path: str, allow_empty: bool = False) -> str:
    """校验并返回去除首尾空白后的代理 URL。"""
    url = str(value or "").strip()
    if not url:
        if allow_empty:
            return ""
        raise ProxyConfigIssue(
            "Proxy URL is required",
            path,
            "proxy_url_required",
        )

    try:
        parsed = urlsplit(url.replace("{time}", "0"))
        port = parsed.port
    except ValueError as exc:
        raise ProxyConfigIssue(
            "Invalid proxy URL",
            path,
            "invalid_proxy_url",
        ) from exc
    if parsed.scheme.lower() not in SUPPORTED_PROXY_SCHEMES or not parsed.hostname:
        raise ProxyConfigIssue(
            "Invalid proxy URL",
            path,
            "invalid_proxy_url",
        )
    if port is not None and not 1 <= port <= 65535:
        raise ProxyConfigIssue(
            "Invalid proxy port",
            path,
            "invalid_proxy_url",
        )
    return url


def validate_egress_config(value: dict[str, Any]) -> ValidatedEgressConfig:
    """校验全局出口代理配置并返回规范化结构。"""
    mode = str(value.get("mode", "direct") or "direct").strip()
    if mode not in SUPPORTED_EGRESS_MODES:
        raise ProxyConfigIssue(
            "Unsupported proxy mode",
            "proxy.egress.mode",
            "invalid_proxy_mode",
        )

    strategy = str(
        value.get("rotation_strategy", "sticky_failover") or "sticky_failover"
    ).strip()
    if strategy not in SUPPORTED_ROTATION_STRATEGIES:
        raise ProxyConfigIssue(
            "Unsupported proxy rotation strategy",
            "proxy.egress.rotation_strategy",
            "invalid_proxy_rotation_strategy",
        )

    proxy_url = validate_proxy_url(
        value.get("proxy_url", ""),
        path="proxy.egress.proxy_url",
        allow_empty=mode != "single_proxy",
    )
    proxy_pool = normalize_proxy_list(
        value.get("proxy_pool", []),
        path="proxy.egress.proxy_pool",
    )
    resource_proxy_url = validate_proxy_url(
        value.get("resource_proxy_url", ""),
        path="proxy.egress.resource_proxy_url",
        allow_empty=True,
    )
    resource_proxy_pool = normalize_proxy_list(
        value.get("resource_proxy_pool", []),
        path="proxy.egress.resource_proxy_pool",
    )

    if mode == "proxy_pool" and not proxy_pool:
        raise ProxyConfigIssue(
            "Proxy pool mode requires at least one proxy",
            "proxy.egress.proxy_pool",
            "proxy_pool_required",
        )

    return ValidatedEgressConfig(
        mode=mode,
        proxy_url=proxy_url,
        proxy_pool=tuple(proxy_pool),
        rotation_strategy=strategy,
        resource_proxy_url=resource_proxy_url,
        resource_proxy_pool=tuple(resource_proxy_pool),
    )


def validate_effective_proxy_config(value: dict[str, Any]) -> ValidatedEgressConfig:
    """校验完整配置中的全局出口以及 Console 回退组合。"""
    proxy = value.get("proxy")
    proxy = proxy if isinstance(proxy, dict) else {}
    egress = proxy.get("egress")
    egress = egress if isinstance(egress, dict) else {}
    validated = validate_egress_config(egress)

    console = value.get("console")
    console = console if isinstance(console, dict) else {}
    pool = console.get("proxy_pool")
    pool = pool if isinstance(pool, dict) else {}
    enabled = _as_bool(pool.get("enabled", False))
    fallback = _as_bool(pool.get("fallback_to_global_proxy", False))
    concurrency = _as_int(
        pool.get("health_check_concurrency", 20),
        path="console.proxy_pool.health_check_concurrency",
    )
    if not 1 <= concurrency <= 100:
        raise ProxyConfigIssue(
            "Console proxy health check concurrency must be between 1 and 100",
            "console.proxy_pool.health_check_concurrency",
            "invalid_console_proxy_health_concurrency",
        )
    binding_idle_ttl_sec = _as_int(
        pool.get("binding_idle_ttl_sec", 604800),
        path="console.proxy_pool.binding_idle_ttl_sec",
    )
    if binding_idle_ttl_sec <= 0:
        raise ProxyConfigIssue(
            "Console proxy binding idle TTL must be greater than zero",
            "console.proxy_pool.binding_idle_ttl_sec",
            "invalid_console_proxy_binding_ttl",
        )
    if enabled and fallback and not validated.has_proxy:
        raise ProxyConfigIssue(
            "Console proxy fallback requires a configured global proxy",
            "console.proxy_pool.fallback_to_global_proxy",
            "invalid_console_proxy_fallback",
        )
    return validated


def _as_bool(value: Any) -> bool:
    """按配置快照约定解析布尔值。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _as_int(value: Any, *, path: str) -> int:
    """解析管理端数值字段并保留稳定的字段级错误。"""
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ProxyConfigIssue(
            "Console proxy numeric setting must be an integer",
            path,
            "invalid_console_proxy_setting",
        ) from exc


__all__ = [
    "ProxyConfigIssue",
    "SUPPORTED_EGRESS_MODES",
    "SUPPORTED_PROXY_SCHEMES",
    "SUPPORTED_ROTATION_STRATEGIES",
    "ValidatedEgressConfig",
    "normalize_proxy_list",
    "validate_effective_proxy_config",
    "validate_egress_config",
    "validate_proxy_url",
]
