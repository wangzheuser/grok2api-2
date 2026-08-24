"""代理配置的规范化与一致性校验。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit


SUPPORTED_UNIFIED_EGRESS_MODES = frozenset({"direct", "managed_pool", "resin"})
RESIN_UUID_PLACEHOLDER = "{uuid}"
_PLACEHOLDER_RE = re.compile(r"\{[^{}]+\}")
_VALIDATION_UUID = "00000000-0000-5000-8000-000000000000"


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
class ValidatedUnifiedProxyConfig:
    """统一出口运行时配置。"""

    mode: str
    resin_url_template: str
    enabled_pool_entries: int


def validate_resin_url_template(value: Any) -> str:
    """校验 Resin 正向代理 URL 模板。"""
    template = str(value or "").strip()
    path = "proxy.resin.url_template"
    if not template:
        raise ProxyConfigIssue(
            "Resin 代理 URL 模板为必填项",
            path,
            "resin_proxy_url_required",
        )
    placeholders = set(_PLACEHOLDER_RE.findall(template))
    if RESIN_UUID_PLACEHOLDER not in placeholders:
        raise ProxyConfigIssue(
            "Resin 代理 URL 模板必须包含 {uuid}",
            path,
            "resin_uuid_placeholder_required",
        )
    unknown = sorted(placeholders - {RESIN_UUID_PLACEHOLDER})
    if unknown:
        raise ProxyConfigIssue(
            f"Resin 代理 URL 模板包含未支持的占位符：{unknown[0]}",
            path,
            "invalid_resin_proxy_placeholder",
        )
    rendered = template.replace(RESIN_UUID_PLACEHOLDER, _VALIDATION_UUID)
    if "{" in rendered or "}" in rendered:
        raise ProxyConfigIssue(
            "Resin 代理 URL 模板的占位符语法有误",
            path,
            "invalid_resin_proxy_placeholder",
        )
    try:
        parsed = urlsplit(rendered)
        port = parsed.port
    except ValueError as exc:
        raise ProxyConfigIssue(
            "Resin 代理 URL 模板格式有误",
            path,
            "invalid_resin_proxy_url",
        ) from exc
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ProxyConfigIssue(
            "Resin 代理 URL 必须使用 http 或 https，并包含主机名",
            path,
            "invalid_resin_proxy_url",
        )
    if parsed.username is None or parsed.password is None:
        raise ProxyConfigIssue(
            "Resin 代理 URL 必须包含用户名和密码",
            path,
            "invalid_resin_proxy_url",
        )
    if port is not None and not 1 <= port <= 65535:
        raise ProxyConfigIssue(
            "Resin 代理端口有误",
            path,
            "invalid_resin_proxy_url",
        )
    return template


def validate_unified_proxy_config(value: dict[str, Any]) -> ValidatedUnifiedProxyConfig:
    """校验统一出口模式、托管池和 Resin 配置。"""
    proxy = value.get("proxy")
    proxy = proxy if isinstance(proxy, dict) else {}
    egress = proxy.get("egress")
    egress = egress if isinstance(egress, dict) else {}
    mode = str(egress.get("mode", "direct") or "direct").strip()
    if mode not in SUPPORTED_UNIFIED_EGRESS_MODES:
        raise ProxyConfigIssue(
            "代理出口模式未受支持",
            "proxy.egress.mode",
            "invalid_proxy_mode",
        )

    pool = proxy.get("pool")
    pool = pool if isinstance(pool, dict) else {}
    entries = pool.get("entries", []) or []
    if not isinstance(entries, list):
        raise ProxyConfigIssue(
            "托管代理节点必须是列表",
            "proxy.pool.entries",
            "invalid_proxy_pool",
        )
    from .managed_pool import ProxyEntry

    enabled = 0
    for index, item in enumerate(entries):
        try:
            entry = ProxyEntry.model_validate(item)
        except ValueError as exc:
            raise ProxyConfigIssue(
                str(exc),
                f"proxy.pool.entries[{index}]",
                "invalid_proxy",
            ) from exc
        enabled += int(entry.enabled)
    if mode == "managed_pool" and enabled == 0:
        raise ProxyConfigIssue(
            "托管代理池模式至少需要一个已启用节点",
            "proxy.pool.entries",
            "proxy_pool_required",
        )

    resin = proxy.get("resin")
    resin = resin if isinstance(resin, dict) else {}
    resin_template = str(resin.get("url_template", "") or "").strip()
    if resin_template or mode == "resin":
        resin_template = validate_resin_url_template(resin_template)
    health = proxy.get("health")
    health = health if isinstance(health, dict) else {}
    concurrency = _as_int(
        health.get("concurrency", 20),
        path="proxy.health.concurrency",
    )
    if not 1 <= concurrency <= 100:
        raise ProxyConfigIssue(
            "代理健康检查并发数必须在 1 到 100 之间",
            "proxy.health.concurrency",
            "invalid_proxy_health_concurrency",
        )
    binding_ttl = _as_int(
        pool.get("binding_idle_ttl_sec", 604800),
        path="proxy.pool.binding_idle_ttl_sec",
    )
    if binding_ttl <= 0:
        raise ProxyConfigIssue(
            "代理绑定闲置时间必须大于零",
            "proxy.pool.binding_idle_ttl_sec",
            "invalid_proxy_binding_ttl",
        )
    return ValidatedUnifiedProxyConfig(
        mode=mode,
        resin_url_template=resin_template,
        enabled_pool_entries=enabled,
    )


def _as_int(value: Any, *, path: str) -> int:
    """解析管理端数值字段并保留稳定的字段级错误。"""
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ProxyConfigIssue(
            "代理数值配置必须为整数",
            path,
            "invalid_proxy_setting",
        ) from exc


__all__ = [
    "ProxyConfigIssue",
    "RESIN_UUID_PLACEHOLDER",
    "SUPPORTED_UNIFIED_EGRESS_MODES",
    "ValidatedUnifiedProxyConfig",
    "validate_resin_url_template",
    "validate_unified_proxy_config",
]
