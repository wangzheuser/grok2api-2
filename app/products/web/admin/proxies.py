"""Admin 统一网络代理与托管库存管理接口。"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from app.control.proxy.managed_health import (
    ManagedProxyHealthScheduler,
    probe_managed_proxy,
)
from app.control.proxy.managed_pool import (
    ProxyEntry,
    get_managed_proxy_pool,
    mask_proxy_url,
    parse_proxy_line,
    sanitize_proxy_error,
)
from app.control.proxy.managed_state import ProxyHealthJobKind
from app.control.proxy.service import get_proxy_service
from app.control.proxy.validation import (
    ProxyConfigIssue,
    validate_unified_proxy_config,
)
from app.platform.config.snapshot import config
from app.platform.errors import ValidationError


router = APIRouter(prefix="/proxies", tags=["Admin - Proxies"])


class ProxyCreateRequest(BaseModel):
    """创建托管代理请求。"""

    url: str
    username: str = ""
    password: str = ""
    mode: str | None = None
    enabled: bool = True


class ProxyUpdateRequest(BaseModel):
    """更新托管代理请求，password 为空表示不修改。"""

    url: str | None = None
    username: str | None = None
    password: str | None = None
    mode: str | None = None
    enabled: bool | None = None


class ProxyImportRequest(BaseModel):
    """批量导入代理请求。"""

    text: str


class ProxyEnabledRequest(BaseModel):
    """单节点启停请求。"""

    enabled: bool


class ProxySettingsRequest(BaseModel):
    """统一出口模式和 Resin 设置。"""

    mode: Literal["direct", "managed_pool", "resin"] | None = None
    skip_ssl_verify: bool | None = None
    resin_url_template: str | None = None


class ProxyBatchRequest(BaseModel):
    """按稳定代理 ID 执行批量操作的请求。"""

    proxy_ids: list[str] = Field(min_length=1)

    @field_validator("proxy_ids", mode="after")
    @classmethod
    def _normalize_proxy_ids(cls, value: list[str]) -> list[str]:
        """清理空白并按首次出现顺序去重。"""
        normalized = list(
            dict.fromkeys(str(proxy_id).strip() for proxy_id in value)
        )
        if not normalized or any(not proxy_id for proxy_id in normalized):
            raise ValueError("proxy_ids must not contain empty values")
        return normalized


class ProxyBatchEnabledRequest(ProxyBatchRequest):
    """批量启用或禁用代理请求。"""

    enabled: bool


@router.get("")
async def list_managed_proxies() -> dict[str, Any]:
    """返回统一网络代理快照。"""
    service = await get_proxy_service()
    return await service.snapshot()


@router.patch("/settings")
async def update_proxy_settings(
    req: ProxySettingsRequest,
    request: Request,
) -> dict[str, Any]:
    """更新统一出口设置，并在切入托管池时创建 bootstrap 任务。"""
    from app.products.web.admin import _deep_merge, _restore_masked_proxy_url

    old_mode = config.get_str("proxy.egress.mode", "direct")
    values = {
        key: value
        for key, value in req.model_dump().items()
        if value is not None
    }
    if not values:
        return {
            "status": "success",
            "mode": old_mode,
            "skip_ssl_verify": config.get_bool(
                "proxy.egress.skip_ssl_verify", False
            ),
            "resin_configured": bool(
                config.get_str("proxy.resin.url_template", "").strip()
            ),
            "resin_url_template": mask_proxy_url(
                config.get_str("proxy.resin.url_template", "")
            ),
            "job_id": None,
        }
    patch: dict[str, Any] = {"proxy": {}}
    egress: dict[str, Any] = {}
    if "mode" in values:
        egress["mode"] = values["mode"]
    if "skip_ssl_verify" in values:
        egress["skip_ssl_verify"] = values["skip_ssl_verify"]
    if egress:
        patch["proxy"]["egress"] = egress
    if "resin_url_template" in values:
        current = config.get_str("proxy.resin.url_template", "")
        restored = _restore_masked_proxy_url(
            values["resin_url_template"],
            [current],
            path="proxy.resin.url_template",
        )
        patch["proxy"]["resin"] = {"url_template": restored}
    effective = _deep_merge(config.raw(), patch)
    try:
        validate_unified_proxy_config(effective)
    except ProxyConfigIssue as exc:
        raise ValidationError(str(exc), param=exc.path, code=exc.code) from exc
    await config.update(patch)
    await config.load()
    service = await get_proxy_service()
    job_id = None
    new_mode = config.get_str("proxy.egress.mode", "direct")
    await service.reload_config(
        load_managed_pool=new_mode == "managed_pool",
    )
    if old_mode != "managed_pool" and new_mode == "managed_pool":
        pool = await get_managed_proxy_pool()
        job = await _scheduler(request, pool).enqueue(
            kind=ProxyHealthJobKind.BOOTSTRAP
        )
        job_id = job.job_id
    return {
        "status": "success",
        "mode": new_mode,
        "skip_ssl_verify": config.get_bool(
            "proxy.egress.skip_ssl_verify", False
        ),
        "resin_configured": bool(
            config.get_str("proxy.resin.url_template", "").strip()
        ),
        "resin_url_template": mask_proxy_url(
            config.get_str("proxy.resin.url_template", "")
        ),
        "job_id": job_id,
    }


@router.post("")
async def create_managed_proxy(req: ProxyCreateRequest) -> dict[str, Any]:
    """新增一个托管代理并创建增量检测任务。"""
    try:
        entry = ProxyEntry.model_validate(req.model_dump())
    except ValueError as exc:
        raise ValidationError(str(exc), param="url", code="invalid_proxy") from exc
    pool = await get_managed_proxy_pool()
    result = await pool.add_entries([entry])
    saved = result.entries[0]
    job = await pool.create_health_job(
        ProxyHealthJobKind.INCREMENTAL,
        [saved],
    )
    return {
        "status": "success",
        "item": saved.public_dict(include_secret=False),
        "added": result.added,
        "updated": result.updated,
        "unchanged": result.unchanged,
        "job_id": job.job_id,
    }


@router.post("/import")
async def import_managed_proxies(req: ProxyImportRequest) -> dict[str, Any]:
    """按行批量导入托管代理并检测受影响节点。"""
    entries: list[ProxyEntry] = []
    errors: list[dict[str, Any]] = []
    for line_no, line in enumerate(req.text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            entries.append(parse_proxy_line(line))
        except ValueError as exc:
            errors.append({"line": line_no, "message": str(exc)})
    if errors:
        raise ValidationError(
            f"代理导入失败，第 {errors[0]['line']} 行：{errors[0]['message']}",
            param="text",
            code="invalid_proxy_import",
        )
    pool = await get_managed_proxy_pool()
    result = await pool.add_entries(entries)
    job = await pool.create_health_job(
        ProxyHealthJobKind.INCREMENTAL,
        list(result.entries),
    )
    return {
        "status": "success",
        "imported": result.added,
        "updated": result.updated,
        "skipped": result.unchanged,
        "job_id": job.job_id,
    }


@router.post("/batch/test", status_code=202)
async def test_selected_managed_proxies(
    req: ProxyBatchRequest,
    request: Request,
) -> dict[str, Any]:
    """创建仅包含所选节点的异步健康检查任务。"""
    pool = await get_managed_proxy_pool()
    entries = await _selected_entries(pool, req.proxy_ids)
    job = await _scheduler(request, pool).enqueue(
        kind=ProxyHealthJobKind.MANUAL_SELECTION,
        entries=entries,
    )
    return {
        "status": "accepted",
        "selected": len(entries),
        "job_id": job.job_id,
    }


@router.post("/batch/reset", status_code=202)
async def reset_selected_managed_proxies(
    req: ProxyBatchRequest,
    request: Request,
) -> dict[str, Any]:
    """重置所选节点并创建一个异步检测任务。"""
    pool = await get_managed_proxy_pool()
    await _selected_entries(pool, req.proxy_ids)
    reset_entries = await pool.reset_entries(req.proxy_ids)
    if not reset_entries:
        raise HTTPException(409, "Proxy runtime state is unavailable")
    job = await _scheduler(request, pool).enqueue(
        kind=ProxyHealthJobKind.MANUAL_SELECTION,
        entries=list(reset_entries),
    )
    return {
        "status": "accepted",
        "selected": len(req.proxy_ids),
        "reset": len(reset_entries),
        "job_id": job.job_id,
    }


@router.post("/batch/enabled")
async def set_selected_managed_proxies_enabled(
    req: ProxyBatchEnabledRequest,
    request: Request,
) -> dict[str, Any]:
    """一次持久化启用或禁用所选节点。"""
    pool = await get_managed_proxy_pool()
    await _selected_entries(pool, req.proxy_ids)
    result = await pool.set_entries_enabled(req.proxy_ids, req.enabled)
    job_id = None
    if req.enabled and result.entries:
        job = await _scheduler(request, pool).enqueue(
            kind=ProxyHealthJobKind.INCREMENTAL,
            entries=list(result.entries),
        )
        job_id = job.job_id
    return {
        "status": "success",
        "selected": len(req.proxy_ids),
        "changed": result.changed,
        "unchanged": result.unchanged,
        "enabled": req.enabled,
        "job_id": job_id,
    }


@router.post("/batch/clear-bindings")
async def clear_selected_managed_proxy_bindings(
    req: ProxyBatchRequest,
) -> dict[str, Any]:
    """清空所选节点关联的账号 sticky 绑定。"""
    pool = await get_managed_proxy_pool()
    await _selected_entries(pool, req.proxy_ids)
    cleared = await pool.clear_entry_bindings(req.proxy_ids)
    return {
        "status": "success",
        "selected": len(req.proxy_ids),
        "cleared": cleared,
    }


@router.post("/batch/delete")
async def delete_selected_managed_proxies(
    req: ProxyBatchRequest,
) -> dict[str, Any]:
    """一次持久化删除所选节点。"""
    pool = await get_managed_proxy_pool()
    await _selected_entries(pool, req.proxy_ids)
    deleted = await pool.remove_entries(req.proxy_ids)
    return {
        "status": "success",
        "selected": len(req.proxy_ids),
        "deleted": deleted,
    }


@router.post("/test-all", status_code=202)
async def test_all_managed_proxies(request: Request) -> dict[str, Any]:
    """创建全量异步健康检查任务。"""
    pool = await get_managed_proxy_pool()
    job = await _scheduler(request, pool).enqueue(
        kind=ProxyHealthJobKind.MANUAL_ALL
    )
    return {"status": "accepted", "job_id": job.job_id}


@router.post("/test-egress", status_code=202)
async def test_effective_egress() -> dict[str, Any]:
    """创建当前统一出口的异步检测任务。"""
    service = await get_proxy_service()
    job_id = await service.enqueue_probe()
    return {"status": "accepted", "job_id": job_id}


@router.get("/test-jobs/{job_id}")
async def get_proxy_test_job(job_id: str) -> dict[str, Any]:
    """返回异步健康检查任务进度。"""
    pool = await get_managed_proxy_pool()
    job = await pool.get_health_job(job_id)
    if job is None:
        raise HTTPException(404, "Health job not found")
    return {
        "job_id": job.job_id,
        "kind": job.kind.value,
        "status": job.status.value,
        "total": job.total,
        "completed": job.completed,
        "healthy": job.healthy,
        "unhealthy": job.unhealthy,
        "inconclusive": job.inconclusive,
        "skipped": job.skipped,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "updated_at": job.updated_at,
        "finished_at": job.finished_at,
        "error": job.error,
    }


@router.put("/{proxy_id}")
async def update_managed_proxy(
    proxy_id: str,
    req: ProxyUpdateRequest,
) -> dict[str, Any]:
    """更新指定托管代理并创建增量检测任务。"""
    patch = {key: value for key, value in req.model_dump().items() if value is not None}
    try:
        pool = await get_managed_proxy_pool()
        item = await pool.update_entry(proxy_id, patch)
    except KeyError as exc:
        raise HTTPException(404, "Proxy not found") from exc
    except ValueError as exc:
        raise ValidationError(str(exc), param="proxy", code="invalid_proxy") from exc
    job = await pool.create_health_job(
        ProxyHealthJobKind.INCREMENTAL,
        [item],
    )
    return {
        "status": "success",
        "item": item.public_dict(include_secret=False),
        "job_id": job.job_id,
    }


@router.delete("/bindings")
async def clear_managed_proxy_bindings() -> dict[str, Any]:
    """清空所有账号 sticky 绑定。"""
    pool = await get_managed_proxy_pool()
    count = await pool.clear_bindings()
    return {"status": "success", "cleared": count}


@router.delete("/{proxy_id}")
async def delete_managed_proxy(proxy_id: str) -> dict[str, Any]:
    """删除指定托管代理。"""
    try:
        pool = await get_managed_proxy_pool()
        await pool.remove_entry(proxy_id)
    except KeyError as exc:
        raise HTTPException(404, "Proxy not found") from exc
    return {"status": "success"}


@router.post("/{proxy_id}/enabled")
async def set_managed_proxy_enabled(
    proxy_id: str,
    req: ProxyEnabledRequest,
) -> dict[str, Any]:
    """启用或禁用指定托管代理。"""
    try:
        pool = await get_managed_proxy_pool()
        item = await pool.set_enabled(proxy_id, req.enabled)
    except KeyError as exc:
        raise HTTPException(404, "Proxy not found") from exc
    job = None
    if req.enabled:
        job = await pool.create_health_job(
            ProxyHealthJobKind.INCREMENTAL,
            [item],
        )
    return {
        "status": "success",
        "item": item.public_dict(include_secret=False),
        "job_id": job.job_id if job else None,
    }


@router.post("/{proxy_id}/reset", status_code=202)
async def reset_managed_proxy(proxy_id: str) -> dict[str, Any]:
    """把节点重置为 unknown 并创建单节点检测任务。"""
    pool = await get_managed_proxy_pool()
    if not await pool.reset_entry(proxy_id):
        raise HTTPException(404, "Proxy not found")
    entry = next(
        (
            item
            for item in await pool.entries(include_secret=True)
            if item.id == proxy_id
        ),
        None,
    )
    if entry is None:
        raise HTTPException(404, "Proxy not found")
    job = await pool.create_health_job(
        ProxyHealthJobKind.MANUAL_SINGLE,
        [entry],
    )
    return {"status": "accepted", "job_id": job.job_id}


@router.post("/{proxy_id}/test")
async def test_managed_proxy(proxy_id: str) -> dict[str, Any]:
    """同步测试单个托管代理并返回结构化判定。"""
    pool = await get_managed_proxy_pool()
    entry = next(
        (
            item
            for item in await pool.entries(include_secret=True)
            if item.id == proxy_id
        ),
        None,
    )
    if entry is None:
        raise HTTPException(404, "Proxy not found")
    result = await probe_managed_proxy(entry)
    await pool.record_health_result(
        entry.id,
        generation=entry.generation,
        outcome=result.outcome,
        message=result.message,
        latency_ms=result.latency_ms,
        status_code=result.status_code,
    )
    return {
        "status": "success",
        "ok": result.ok,
        "outcome": result.outcome.value,
        "status_code": result.status_code,
        "message": sanitize_proxy_error(result.message, entry),
        "latency_ms": result.latency_ms,
    }


def _scheduler(request: Request, pool) -> ManagedProxyHealthScheduler:
    """返回应用调度器或构造一个只负责共享任务入队的实例。"""
    scheduler = getattr(request.app.state, "proxy_health_scheduler", None)
    return scheduler or ManagedProxyHealthScheduler(pool)


async def _selected_entries(pool, proxy_ids: list[str]) -> list[ProxyEntry]:
    """校验批量请求中的代理 ID，并映射为稳定字段错误。"""
    try:
        return await pool.selected_entries(proxy_ids)
    except KeyError as exc:
        missing = list(exc.args[0]) if exc.args else proxy_ids
        preview = "、".join(str(proxy_id) for proxy_id in missing[:3])
        suffix = "…" if len(missing) > 3 else ""
        raise ValidationError(
            f"代理不存在：{preview}{suffix}",
            param="proxy_ids",
            code="proxy_not_found",
        ) from exc


__all__ = ["router"]
