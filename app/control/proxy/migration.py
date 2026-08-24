"""统一代理配置的幂等迁移。"""

from __future__ import annotations

import asyncio
from typing import Any

from app.platform.config.snapshot import config, get_config
from app.platform.logging.logger import logger

from .managed_pool import (
    ProxyEntry,
    _coerce_entry,
    _deduplicate_entries,
    _entry_identity,
    _stable_entry_id,
)


PROXY_SCHEMA_VERSION = 2
_migration_lock = asyncio.Lock()
_migration_checked = False


def _entry_from_url(url: str, *, enabled: bool) -> ProxyEntry:
    """把旧 URL 转换成结构化托管节点。"""
    return ProxyEntry(
        id=_stable_entry_id({"url": url}),
        url=url,
        enabled=enabled,
    )


def _legacy_urls(cfg: Any) -> list[str]:
    """按旧全局模式返回实际参与基础出口的代理 URL。"""
    mode = cfg.get_str("proxy.egress.mode", "direct")
    if mode == "single_proxy":
        url = cfg.get_str("proxy.egress.proxy_url", "").strip()
        return [url] if url else []
    if mode == "proxy_pool":
        return [
            str(url).strip()
            for url in cfg.get_list("proxy.egress.proxy_pool", [])
            if str(url).strip()
        ]
    return []


def _legacy_resource_urls(cfg: Any) -> list[str]:
    """返回旧资源出口中的独有代理 URL。"""
    url = cfg.get_str("proxy.egress.resource_proxy_url", "").strip()
    pool = [
        str(item).strip()
        for item in cfg.get_list("proxy.egress.resource_proxy_pool", [])
        if str(item).strip()
    ]
    return ([url] if url else []) + pool


def _legacy_console_entries(cfg: Any) -> list[ProxyEntry]:
    """解析旧 Console 托管节点。"""
    entries: list[ProxyEntry] = []
    for item in cfg.get("console.proxy_pool.entries", []) or []:
        entries.append(_coerce_entry(item))
    return _deduplicate_entries(entries)


def _append_unique_legacy_urls(
    entries: list[ProxyEntry],
    urls: list[str],
    *,
    enabled: bool,
) -> None:
    """追加旧 URL 独有节点，不覆盖优先级更高的结构化条目。"""
    identities = {_entry_identity(entry) for entry in entries}
    for url in urls:
        candidate = _entry_from_url(url, enabled=enabled)
        identity = _entry_identity(candidate)
        if identity in identities:
            continue
        identities.add(identity)
        entries.append(candidate)


def build_proxy_migration_patch(cfg: Any) -> dict[str, Any]:
    """根据旧配置生成 schema v2 补丁。"""
    console_enabled = cfg.get_bool("console.proxy_pool.enabled", False)
    old_mode = cfg.get_str("proxy.egress.mode", "direct")
    current_entries = cfg.get("proxy.pool.entries", []) or []
    current_resin = cfg.get_str("proxy.resin.url_template", "").strip()
    if old_mode in {"managed_pool", "resin"} or current_entries or current_resin:
        # 已使用新结构时只补迁移版本，保留操作者显式配置。
        return {"proxy": {"schema_version": PROXY_SCHEMA_VERSION}}
    global_urls = _legacy_urls(cfg)
    resource_urls = _legacy_resource_urls(cfg)
    console_entries = _legacy_console_entries(cfg)

    migrated: list[ProxyEntry] = []
    if console_enabled:
        migrated.extend(console_entries)
        _append_unique_legacy_urls(migrated, global_urls, enabled=False)
        mode = "managed_pool"
    elif old_mode in {"single_proxy", "proxy_pool"} and global_urls:
        global_entries = [
            _entry_from_url(url, enabled=True) for url in global_urls
        ]
        global_identities = {_entry_identity(entry) for entry in global_entries}
        # 先放旧结构化节点，重复端点即可保留原稳定 ID 与 generation。
        migrated.extend(
            ProxyEntry.model_validate(
                {
                    **entry.public_dict(include_secret=True),
                    "enabled": _entry_identity(entry) in global_identities,
                }
            )
            for entry in console_entries
        )
        _append_unique_legacy_urls(migrated, global_urls, enabled=True)
        mode = "managed_pool"
    else:
        migrated.extend(console_entries)
        mode = "direct"
    _append_unique_legacy_urls(migrated, resource_urls, enabled=False)
    migrated = _deduplicate_entries(migrated)

    return {
        "proxy": {
            "schema_version": PROXY_SCHEMA_VERSION,
            "egress": {
                "mode": mode,
                "skip_ssl_verify": cfg.get_bool(
                    "proxy.egress.skip_ssl_verify",
                    False,
                ),
            },
            "pool": {
                "entries": [
                    entry.public_dict(include_secret=True) for entry in migrated
                ],
                "max_proxy_retries_per_request": cfg.get_int(
                    "console.proxy_pool.max_proxy_retries_per_request",
                    1,
                ),
                "challenge_failure_threshold": cfg.get_int(
                    "console.proxy_pool.challenge_failure_threshold",
                    2,
                ),
                "static_cooldown_sec": cfg.get_int(
                    "console.proxy_pool.static_cooldown_sec",
                    300,
                ),
                "dynamic_retry_base_sec": cfg.get_int(
                    "console.proxy_pool.dynamic_retry_base_sec",
                    60,
                ),
                "dynamic_retry_max_sec": cfg.get_int(
                    "console.proxy_pool.dynamic_retry_max_sec",
                    600,
                ),
                "dynamic_backoff_factor": cfg.get_float(
                    "console.proxy_pool.dynamic_backoff_factor",
                    2.0,
                ),
                "binding_idle_ttl_sec": cfg.get_int(
                    "console.proxy_pool.binding_idle_ttl_sec",
                    604800,
                ),
            },
            "health": {
                "enabled": cfg.get_bool(
                    "console.proxy_pool.health_check_enabled",
                    True,
                ),
                "check_url": cfg.get_str(
                    "console.proxy_pool.health_check_url",
                    "https://console.x.ai/",
                ),
                "check_interval_sec": cfg.get_int(
                    "console.proxy_pool.health_check_interval_sec",
                    300,
                ),
                "check_timeout_sec": cfg.get_int(
                    "console.proxy_pool.health_check_timeout_sec",
                    15,
                ),
                "concurrency": cfg.get_int(
                    "console.proxy_pool.health_check_concurrency",
                    20,
                ),
            },
            "resin": {"url_template": ""},
        }
    }


async def ensure_proxy_config_migrated() -> bool:
    """把旧代理配置原子迁移为 schema v2，并返回是否写入。"""
    global _migration_checked
    if _migration_checked:
        return False
    async with _migration_lock:
        if _migration_checked:
            return False
        await config.load()
        overrides = await config.user_overrides()
        if not _proxy_overrides_need_migration(overrides):
            _migration_checked = True
            return False
        cfg = get_config()
        patch = build_proxy_migration_patch(cfg)
        await config.update(patch)
        await config.load()
        migrated_cfg = get_config()
        logger.info(
            "proxy config migrated: schema_version={} mode={} entries={}",
            PROXY_SCHEMA_VERSION,
            migrated_cfg.get_str("proxy.egress.mode", "direct"),
            len(migrated_cfg.get("proxy.pool.entries", []) or []),
        )
        _migration_checked = True
        return True


def _proxy_overrides_need_migration(overrides: dict[str, Any]) -> bool:
    """判断持久化覆盖中是否存在旧代理结构或未标版的新结构。"""
    proxy = overrides.get("proxy")
    proxy = proxy if isinstance(proxy, dict) else {}
    raw_version = proxy.get("schema_version")
    try:
        if raw_version is not None and int(raw_version) >= PROXY_SCHEMA_VERSION:
            return False
    except (TypeError, ValueError):
        return True
    if raw_version is not None:
        return True

    egress = proxy.get("egress")
    egress = egress if isinstance(egress, dict) else {}
    mode = str(egress.get("mode", "") or "")
    has_proxy_override = bool(
        mode
        or any(
            key in egress
            for key in (
                "proxy_url",
                "proxy_pool",
                "resource_proxy_url",
                "resource_proxy_pool",
                "rotation_strategy",
            )
        )
        or "pool" in proxy
        or "resin" in proxy
    )
    console = overrides.get("console")
    has_console_inventory = isinstance(console, dict) and "proxy_pool" in console
    return has_proxy_override or has_console_inventory


def reset_proxy_migration_for_tests() -> None:
    """清理进程内迁移检查标记。"""
    global _migration_checked
    _migration_checked = False


__all__ = [
    "PROXY_SCHEMA_VERSION",
    "build_proxy_migration_patch",
    "ensure_proxy_config_migrated",
    "reset_proxy_migration_for_tests",
]
