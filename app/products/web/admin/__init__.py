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
    validate_unified_proxy_config,
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

    resin = sanitized.get("resin")
    if isinstance(resin, dict) and "url_template" in resin:
        current_proxy = config.raw().get("proxy", {})
        current_resin = (
            current_proxy.get("resin", {})
            if isinstance(current_proxy, dict)
            else {}
        )
        current_resin = current_resin if isinstance(current_resin, dict) else {}
        normalized_resin = dict(resin)
        restored = _restore_masked_proxy_url(
            normalized_resin["url_template"],
            [current_resin.get("url_template", "")],
            path="proxy.resin.url_template",
        )
        if restored != normalized_resin["url_template"]:
            normalized_resin["url_template"] = restored
            changed = True
        if normalized_resin != resin:
            sanitized["resin"] = normalized_resin

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
        validate_unified_proxy_config(effective)
    except ProxyConfigIssue as exc:
        raise ValidationError(
            str(exc),
            param=exc.path,
            code=exc.code,
        ) from exc


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
    proxy = value.get("proxy", {})
    egress = proxy.get("egress", {}) if isinstance(proxy, dict) else {}
    if isinstance(egress, dict):
        for legacy_key in (
            "proxy_url",
            "proxy_pool",
            "resource_proxy_url",
            "resource_proxy_pool",
            "rotation_strategy",
        ):
            egress.pop(legacy_key, None)
    resin = proxy.get("resin", {}) if isinstance(proxy, dict) else {}
    if isinstance(resin, dict) and "url_template" in resin:
        resin["url_template"] = _redact_proxy_url(resin["url_template"])
    managed_pool = proxy.get("pool", {}) if isinstance(proxy, dict) else {}
    if isinstance(managed_pool, dict):
        # 托管节点只经 /admin/api/proxies 读取，避免通用配置页重复传输库存。
        managed_pool.pop("entries", None)
    console = value.get("console")
    if isinstance(console, dict):
        console.pop("proxy_pool", None)
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
    _ensure_runtime_patch_allowed(patch)
    _validate_console_config(patch)
    _validate_effective_proxy_patch(patch)
    old_proxy_mode = config.get_str("proxy.egress.mode", "direct")
    proxy_pool_changed = _patch_touches_prefix(patch, "proxy.pool")
    proxy_settings_changed = _patch_touches_prefix(patch, "proxy")
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
    if proxy_settings_changed:
        from app.control.proxy import get_proxy_service
        from app.control.proxy.managed_pool import get_managed_proxy_pool
        from app.control.proxy.managed_state import ProxyHealthJobKind

        service = await get_proxy_service()
        new_proxy_mode = config.get_str("proxy.egress.mode", "direct")
        await service.reload_config(
            load_managed_pool=(
                proxy_pool_changed or new_proxy_mode == "managed_pool"
            ),
        )
        pool = await get_managed_proxy_pool()
        if (
            (old_proxy_mode != "managed_pool" or proxy_pool_changed)
            and new_proxy_mode == "managed_pool"
        ):
            job = await pool.create_health_job(
                ProxyHealthJobKind.BOOTSTRAP
            )
            bootstrap_job_id = job.job_id
    strategy_name = reconcile_refresh_runtime()
    return {
        "status": "success",
        "message": "配置已更新",
        "selection_strategy": strategy_name,
        "proxy_bootstrap_job_id": bootstrap_job_id,
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
