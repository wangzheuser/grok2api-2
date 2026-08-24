"""Admin API — router aggregator, shared DI, lightweight endpoints.

All admin endpoints live under ``/admin/api`` with ``verify_admin_key`` guard.
Heavy handlers are split into ``tokens`` and ``batch`` sub-modules.
"""

import copy
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

import orjson
from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response
from pydantic import RootModel

from app.control.account.backends.factory import get_repository_backend
from app.control.proxy.validation import (
    ProxyConfigIssue,
    normalize_proxy_list,
    validate_effective_proxy_config,
    validate_egress_config,
)
from app.platform.auth.middleware import verify_admin_key
from app.platform.config.snapshot import config
from app.platform.errors import AppError, ErrorKind, ValidationError
from app.platform.logging.logger import logger, reload_file_logging
from app.platform.storage import reconcile_local_media_cache_async

if TYPE_CHECKING:
    from app.control.account.refresh import AccountRefreshService
    from app.control.account.repository import AccountRepository

# ---------------------------------------------------------------------------
# Shared DI dependencies — inject via Depends, no try/except per call
# ---------------------------------------------------------------------------

_CFG_CHAR_REPLACEMENTS = str.maketrans(
    {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u00a0": " ",
        "\u2007": " ",
        "\u202f": " ",
        "\u200b": "",
        "\u200c": "",
        "\u200d": "",
        "\ufeff": "",
    }
)

_STARTUP_ONLY_CONFIG_PREFIXES = (
    "account.storage",
    "account.local",
    "account.redis",
    "account.mysql",
    "account.postgresql",
)


class ConfigPatchRequest(RootModel[dict[str, Any]]):
    """Loose config patch payload with explicit root typing."""


def _sanitize_text(value: Any, *, remove_all_spaces: bool = False) -> str:
    text = "" if value is None else str(value)
    text = text.translate(_CFG_CHAR_REPLACEMENTS)
    if remove_all_spaces:
        text = re.sub(r"\s+", "", text)
    else:
        text = text.strip()
    return text.encode("latin-1", errors="ignore").decode("latin-1")


def _proxy_endpoint_identity(value: Any) -> tuple[Any, ...] | None:
    """返回忽略密码后的代理端点身份，用于匹配脱敏回传值。"""
    try:
        parts = urlsplit(str(value or ""))
        return (
            parts.scheme.lower(),
            parts.username or "",
            (parts.hostname or "").lower(),
            parts.port,
            parts.path,
            parts.query,
            parts.fragment,
        )
    except ValueError:
        return None


def _restore_masked_proxy_url(
    value: Any,
    current_values: list[Any],
    *,
    path: str,
) -> str:
    """把管理页面回传的密码掩码替换为当前持久化 URL。"""
    text = str(value or "").strip()
    try:
        masked = urlsplit(text).password == "***"
    except ValueError:
        return text
    if not masked:
        return text
    identity = _proxy_endpoint_identity(text)
    for current in current_values:
        candidate = str(current or "").strip()
        if identity == _proxy_endpoint_identity(candidate):
            return candidate
    raise ValidationError(
        "Masked proxy password does not match the current endpoint",
        param=path,
        code="masked_proxy_secret_mismatch",
    )


def _proxy_list_items(value: Any) -> list[Any]:
    """展开数组、逗号或多行代理输入，供密码掩码恢复使用。"""
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return re.split(r"[,\r\n]+", value)
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _sanitize_proxy_config(payload: dict[str, Any]) -> dict[str, Any]:
    proxy = payload.get("proxy")
    if not isinstance(proxy, dict):
        return dict(payload)

    sanitized = dict(proxy)
    changed = False

    def _sanitize_fields(target: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        normalized = dict(target)
        local_changed = False
        for key, strip_spaces in [
            ("user_agent", False),
            ("cf_cookies", False),
            ("cf_clearance", True),
        ]:
            if key not in normalized:
                continue
            raw = normalized[key]
            val = _sanitize_text(raw, remove_all_spaces=strip_spaces)
            if val != raw:
                normalized[key] = val
                local_changed = True
        return normalized, local_changed

    sanitized, changed = _sanitize_fields(sanitized)

    clearance = sanitized.get("clearance")
    if isinstance(clearance, dict):
        sanitized_clearance, clearance_changed = _sanitize_fields(clearance)
        if clearance_changed:
            sanitized["clearance"] = sanitized_clearance
            changed = True

    egress = sanitized.get("egress")
    if isinstance(egress, dict):
        normalized_egress = dict(egress)
        current_proxy = config.raw().get("proxy", {})
        current_egress = (
            current_proxy.get("egress", {})
            if isinstance(current_proxy, dict)
            else {}
        )
        current_egress = current_egress if isinstance(current_egress, dict) else {}
        for key in ("proxy_url", "resource_proxy_url"):
            if key not in normalized_egress:
                continue
            restored = _restore_masked_proxy_url(
                normalized_egress[key],
                [current_egress.get(key, "")],
                path=f"proxy.egress.{key}",
            )
            if restored != normalized_egress[key]:
                normalized_egress[key] = restored
                changed = True
        for key in ("proxy_pool", "resource_proxy_pool"):
            if key not in normalized_egress:
                continue
            try:
                current_items = _proxy_list_items(current_egress.get(key, []))
                restored_items = [
                    _restore_masked_proxy_url(
                        item,
                        current_items,
                        path=f"proxy.egress.{key}[{index}]",
                    )
                    for index, item in enumerate(
                        _proxy_list_items(normalized_egress[key])
                    )
                ]
                normalized = normalize_proxy_list(
                    restored_items,
                    path=f"proxy.egress.{key}",
                )
            except ProxyConfigIssue as exc:
                raise ValidationError(
                    str(exc),
                    param=exc.path,
                    code=exc.code,
                ) from exc
            if normalized != normalized_egress[key]:
                normalized_egress[key] = normalized
                changed = True
        if normalized_egress != egress:
            sanitized["egress"] = normalized_egress

    if not changed:
        return dict(payload)

    logger.warning("admin config payload sanitized before save: section=proxy")
    result = dict(payload)
    result["proxy"] = sanitized
    return result


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    """递归合并配置补丁，不修改调用方对象。"""
    merged = dict(base)
    for key, value in patch.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = _deep_merge(current, value)
        else:
            merged[key] = value
    return merged


def _validate_effective_proxy_patch(patch: dict[str, Any]) -> None:
    """用当前快照与补丁合并后的最终值校验代理配置。"""
    effective = _deep_merge(config.raw(), patch)
    try:
        validate_effective_proxy_config(effective)
    except ProxyConfigIssue as exc:
        raise ValidationError(
            str(exc),
            param=exc.path,
            code=exc.code,
        ) from exc


def _normalize_console_proxy_fallback_patch(
    patch: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """在没有真实全局代理时关闭无效的 Console 回退。"""
    normalized = copy.deepcopy(patch)
    current = config.raw()
    effective = _deep_merge(current, normalized)
    proxy = effective.get("proxy")
    proxy = proxy if isinstance(proxy, dict) else {}
    egress = proxy.get("egress")
    egress = egress if isinstance(egress, dict) else {}
    try:
        has_global_proxy = validate_egress_config(egress).has_proxy
    except ProxyConfigIssue:
        # 出口本身的字段错误交给后续统一校验，避免被联动规则掩盖。
        return normalized, False
    if has_global_proxy:
        return normalized, False

    console = effective.get("console")
    console = console if isinstance(console, dict) else {}
    pool = console.get("proxy_pool")
    pool = pool if isinstance(pool, dict) else {}
    fallback = _as_config_bool(pool.get("fallback_to_global_proxy", False))

    patch_console = normalized.get("console")
    patch_console = patch_console if isinstance(patch_console, dict) else {}
    patch_pool = patch_console.get("proxy_pool")
    patch_pool = patch_pool if isinstance(patch_pool, dict) else {}
    current_console = current.get("console")
    current_console = current_console if isinstance(current_console, dict) else {}
    current_pool = current_console.get("proxy_pool")
    current_pool = current_pool if isinstance(current_pool, dict) else {}
    current_enabled = _as_config_bool(current_pool.get("enabled", False))
    current_fallback = _as_config_bool(
        current_pool.get("fallback_to_global_proxy", False)
    )
    fallback_explicitly_enabled = (
        "fallback_to_global_proxy" in patch_pool
        and _as_config_bool(patch_pool.get("fallback_to_global_proxy"))
    )
    enables_console = (
        "enabled" in patch_pool
        and _as_config_bool(patch_pool.get("enabled"))
        and not current_enabled
    )
    patch_proxy = normalized.get("proxy")
    egress_changed = isinstance(patch_proxy, dict) and "egress" in patch_proxy
    active_fallback_lost_egress = (
        egress_changed and current_enabled and current_fallback
    )
    should_auto_disable = fallback_explicitly_enabled or (
        fallback and (enables_console or active_fallback_lost_egress)
    )
    if not should_auto_disable:
        return normalized, False

    console_patch = normalized.setdefault("console", {})
    if not isinstance(console_patch, dict):
        console_patch = {}
        normalized["console"] = console_patch
    pool_patch = console_patch.setdefault("proxy_pool", {})
    if not isinstance(pool_patch, dict):
        pool_patch = {}
        console_patch["proxy_pool"] = pool_patch
    pool_patch["fallback_to_global_proxy"] = False
    return normalized, True


def _as_config_bool(value: Any) -> bool:
    """按配置快照规则解析布尔值。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _redact_proxy_url(value: Any) -> str:
    """脱敏配置接口中的代理 URL 内嵌密码。"""
    text = str(value or "")
    try:
        parts = urlsplit(text)
        if not parts.password:
            return text
        username = parts.username or ""
        hostname = parts.hostname or ""
        host = f"[{hostname}]" if ":" in hostname else hostname
        if parts.port:
            host = f"{host}:{parts.port}"
        return urlunsplit(
            (
                parts.scheme,
                f"{username}:***@{host}",
                parts.path,
                parts.query,
                parts.fragment,
            )
        )
    except ValueError:
        return "<invalid>"


def _public_config_snapshot() -> dict[str, Any]:
    """返回可供管理页面读取且不包含代理密码的配置快照。"""
    value = copy.deepcopy(config.raw())
    egress = value.get("proxy", {}).get("egress", {})
    if isinstance(egress, dict):
        for key in ("proxy_url", "resource_proxy_url"):
            if key in egress:
                egress[key] = _redact_proxy_url(egress[key])
        for key in ("proxy_pool", "resource_proxy_pool"):
            if isinstance(egress.get(key), list):
                egress[key] = [_redact_proxy_url(item) for item in egress[key]]

    console_pool = value.get("console", {}).get("proxy_pool", {})
    entries = console_pool.get("entries") if isinstance(console_pool, dict) else None
    if isinstance(entries, list):
        public_entries = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            public = dict(item)
            public.pop("password", None)
            if "url" in public:
                public["url"] = _redact_proxy_url(public["url"])
            public_entries.append(public)
        console_pool["entries"] = public_entries
    return value


def _iter_patch_paths(value: Any, prefix: str = ""):
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(child, dict):
                yield from _iter_patch_paths(child, path)
            else:
                yield path


def _ensure_runtime_patch_allowed(payload: dict[str, Any]) -> None:
    for path in _iter_patch_paths(payload):
        for blocked in _STARTUP_ONLY_CONFIG_PREFIXES:
            if path == blocked or path.startswith(f"{blocked}."):
                raise ValidationError(
                    "Storage config is startup-only and must be set via env",
                    param=path,
                    code="startup_only_config",
                )


def _validate_console_config(payload: dict[str, Any]) -> None:
    console = payload.get("console")
    if not isinstance(console, dict):
        return
    fallback = console.get("fallback")
    if not isinstance(fallback, dict) or "rules" not in fallback:
        return
    from app.dataplane.reverse.protocol.console_model_guard import validate_fallback_rules

    try:
        validate_fallback_rules(fallback.get("rules"))
    except ValueError as exc:
        raise ValidationError(
            str(exc),
            param="console.fallback.rules",
            code="invalid_fallback_rules",
        ) from exc


def _patch_touches_prefix(payload: dict[str, Any], prefix: str) -> bool:
    return any(
        path == prefix or path.startswith(f"{prefix}.")
        for path in _iter_patch_paths(payload)
    )


def get_repo(request: Request) -> "AccountRepository":
    """Resolve the singleton AccountRepository from app state."""
    return request.app.state.repository


def get_refresh_svc(request: Request) -> "AccountRefreshService":
    """Resolve the singleton AccountRefreshService from app state."""
    return request.app.state.refresh_service


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/admin/api", dependencies=[Depends(verify_admin_key)])
_TAG_ADMIN_SYSTEM = "Admin - System"

# Mount sub-modules
from .tokens import router as _tokens_router  # noqa: E402
from .batch import router as _batch_router  # noqa: E402
from .assets import router as _assets_router  # noqa: E402
from .cache import router as _cache_router  # noqa: E402
from .models import router as _models_router  # noqa: E402
from .proxies import router as _proxies_router  # noqa: E402

router.include_router(_tokens_router)
router.include_router(_batch_router)
router.include_router(_assets_router)
router.include_router(_cache_router)
router.include_router(_models_router)
router.include_router(_proxies_router)


# ---------------------------------------------------------------------------
# Lightweight inline endpoints (no separate file needed)
# ---------------------------------------------------------------------------


@router.get("/verify", tags=[_TAG_ADMIN_SYSTEM])
async def admin_verify():
    return {"status": "success"}


@router.get("/config", tags=[_TAG_ADMIN_SYSTEM])
async def get_config_endpoint():
    return Response(
        content=orjson.dumps(_public_config_snapshot()),
        media_type="application/json",
    )


@router.post("/config", tags=[_TAG_ADMIN_SYSTEM])
async def update_config(req: ConfigPatchRequest):
    from app.control.account.runtime import reconcile_refresh_runtime

    patch = _sanitize_proxy_config(req.root)
    patch, fallback_auto_disabled = _normalize_console_proxy_fallback_patch(
        patch
    )
    _ensure_runtime_patch_allowed(patch)
    _validate_console_config(patch)
    _validate_effective_proxy_patch(patch)
    old_console_enabled = config.get_bool("console.proxy_pool.enabled", False)
    console_pool_changed = _patch_touches_prefix(patch, "console.proxy_pool")
    cache_local_changed = _patch_touches_prefix(patch, "cache.local")
    await config.update(patch)
    # config.update() only writes to the backend and invalidates the in-memory
    # snapshot (_version = None); it does not refresh the data.  load() is
    # required here so that get_str/get_int calls below return the new values.
    await config.load()
    reload_file_logging(
        file_level=config.get_str("logging.file_level", "") or None,
        max_files=config.get_int("logging.max_files", 7),
    )
    if cache_local_changed:
        await reconcile_local_media_cache_async()
    bootstrap_job_id = None
    if console_pool_changed:
        from app.control.proxy.console_pool import get_console_proxy_pool
        from app.control.proxy.console_state import ConsoleProxyHealthJobKind

        pool = await get_console_proxy_pool()
        if (
            not old_console_enabled
            and config.get_bool("console.proxy_pool.enabled", False)
        ):
            job = await pool.create_health_job(
                ConsoleProxyHealthJobKind.BOOTSTRAP
            )
            bootstrap_job_id = job.job_id
    strategy_name = reconcile_refresh_runtime()
    return {
        "status": "success",
        "message": "配置已更新",
        "selection_strategy": strategy_name,
        "console_proxy_bootstrap_job_id": bootstrap_job_id,
        "fallback_auto_disabled": fallback_auto_disabled,
    }


@router.get("/storage", tags=[_TAG_ADMIN_SYSTEM])
async def get_storage_mode():
    return {"type": get_repository_backend()}


@router.get("/status", tags=[_TAG_ADMIN_SYSTEM])
async def runtime_status():
    from app.control.account.runtime import reconcile_refresh_runtime
    from app.dataplane.account import _directory

    if _directory is None:
        raise AppError(
            "Account directory not initialised",
            kind=ErrorKind.SERVER,
            code="directory_not_initialised",
            status=503,
        )
    strategy_name = reconcile_refresh_runtime()
    return Response(
        content=orjson.dumps(
            {
                "status": "ok",
                "size": _directory.size,
                "revision": _directory.revision,
                "selection_strategy": strategy_name,
            }
        ),
        media_type="application/json",
    )


@router.post("/sync", tags=[_TAG_ADMIN_SYSTEM])
async def force_sync():
    from app.dataplane.account import _directory

    if _directory is None:
        raise AppError(
            "Account directory not initialised",
            kind=ErrorKind.SERVER,
            code="directory_not_initialised",
            status=503,
        )
    changed = await _directory.sync_if_changed()
    return Response(
        content=orjson.dumps({"changed": changed, "revision": _directory.revision}),
        media_type="application/json",
    )


__all__ = ["router", "get_repo", "get_refresh_svc"]
