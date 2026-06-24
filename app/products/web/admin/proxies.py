"""Admin Console proxy pool endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.control.proxy.console_pool import (
    ConsoleProxyEntry,
    get_console_proxy_pool,
    parse_proxy_line,
)
from app.platform.config.snapshot import config
from app.platform.errors import ValidationError

router = APIRouter(prefix="/proxies", tags=["Admin - Proxies"])


class ProxyCreateRequest(BaseModel):
    """创建 Console 代理请求。"""

    url: str
    username: str = ""
    password: str = ""
    mode: str | None = None
    enabled: bool = True


class ProxyUpdateRequest(BaseModel):
    """更新 Console 代理请求，password 为空表示不修改。"""

    url: str | None = None
    username: str | None = None
    password: str | None = None
    mode: str | None = None
    enabled: bool | None = None


class ProxyImportRequest(BaseModel):
    """批量导入代理请求。"""

    text: str


class ProxyEnabledRequest(BaseModel):
    """启停代理请求。"""

    enabled: bool


@router.get("")
async def list_console_proxies() -> dict[str, Any]:
    """返回 Console 专用代理池快照。"""
    pool = await get_console_proxy_pool()
    return await pool.snapshot()


@router.post("")
async def create_console_proxy(req: ProxyCreateRequest) -> dict[str, Any]:
    """新增一个 Console 代理。"""
    try:
        entry = ConsoleProxyEntry.model_validate(req.model_dump())
    except ValueError as exc:
        raise ValidationError(str(exc), param="url", code="invalid_proxy") from exc
    pool = await get_console_proxy_pool()
    await pool.add_entries([entry])
    return {"status": "success", "item": entry.public_dict(include_secret=False)}


@router.post("/import")
async def import_console_proxies(req: ProxyImportRequest) -> dict[str, Any]:
    """按行批量导入 Console 代理。"""
    entries: list[ConsoleProxyEntry] = []
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
    pool = await get_console_proxy_pool()
    count = await pool.add_entries(entries)
    return {"status": "success", "imported": count}


@router.put("/{proxy_id}")
async def update_console_proxy(proxy_id: str, req: ProxyUpdateRequest) -> dict[str, Any]:
    """更新指定 Console 代理。"""
    patch = {k: v for k, v in req.model_dump().items() if v is not None}
    try:
        pool = await get_console_proxy_pool()
        item = await pool.update_entry(proxy_id, patch)
    except KeyError as exc:
        raise HTTPException(404, "Proxy not found") from exc
    except ValueError as exc:
        raise ValidationError(str(exc), param="proxy", code="invalid_proxy") from exc
    return {"status": "success", "item": item.public_dict(include_secret=False)}


@router.delete("/bindings")
async def clear_console_proxy_bindings() -> dict[str, Any]:
    """清空所有账号 sticky 绑定。"""
    pool = await get_console_proxy_pool()
    count = await pool.clear_bindings()
    return {"status": "success", "cleared": count}


@router.delete("/{proxy_id}")
async def delete_console_proxy(proxy_id: str) -> dict[str, Any]:
    """删除指定 Console 代理。"""
    try:
        pool = await get_console_proxy_pool()
        await pool.remove_entry(proxy_id)
    except KeyError as exc:
        raise HTTPException(404, "Proxy not found") from exc
    return {"status": "success"}


@router.post("/{proxy_id}/enabled")
async def set_console_proxy_enabled(proxy_id: str, req: ProxyEnabledRequest) -> dict[str, Any]:
    """启用或禁用指定 Console 代理。"""
    try:
        pool = await get_console_proxy_pool()
        item = await pool.set_enabled(proxy_id, req.enabled)
    except KeyError as exc:
        raise HTTPException(404, "Proxy not found") from exc
    return {"status": "success", "item": item.public_dict(include_secret=False)}


@router.post("/{proxy_id}/reset")
async def reset_console_proxy(proxy_id: str) -> dict[str, Any]:
    """重置指定 Console 代理运行态。"""
    pool = await get_console_proxy_pool()
    if not await pool.reset_entry(proxy_id):
        raise HTTPException(404, "Proxy not found")
    return {"status": "success"}


@router.post("/{proxy_id}/test")
async def test_console_proxy(proxy_id: str) -> dict[str, Any]:
    """测试指定 Console 代理的基础连通性。"""
    pool = await get_console_proxy_pool()
    entries = await pool.entries(include_secret=True)
    entry = next((item for item in entries if item.id == proxy_id), None)
    if entry is None:
        raise HTTPException(404, "Proxy not found")
    ok, message, latency_ms = await _probe_proxy(entry)
    return {"status": "success", "ok": ok, "message": message, "latency_ms": latency_ms}


async def _probe_proxy(entry: ConsoleProxyEntry) -> tuple[bool, str, int]:
    """通过 curl-cffi 检测代理是否能访问配置的检测地址。"""
    import time

    from app.control.proxy.models import ProxyLease
    from app.dataplane.proxy.adapters.session import ResettableSession
    from app.platform.runtime.ids import next_hex

    cfg = config
    check_url = cfg.get_str("console.proxy_pool.health_check_url", "https://console.x.ai/")
    timeout_s = max(1, cfg.get_int("console.proxy_pool.health_check_timeout_sec", 15))
    from app.control.proxy.console_pool import _render_proxy_url  # 局部导入，避免公开管理接口依赖。

    proxy_url = _render_proxy_url(entry, int(time.time() * 1000))
    lease = ProxyLease(
        lease_id=next_hex(),
        proxy_url=proxy_url,
        proxy_pool="console",
        proxy_id=entry.id,
        proxy_mode=entry.inferred_mode().value,
    )
    started = time.monotonic()
    try:
        async with ResettableSession(lease=lease) as session:
            response = await session.get(check_url, timeout=timeout_s)
        latency_ms = int((time.monotonic() - started) * 1000)
        if response.status_code < 500:
            return True, f"HTTP {response.status_code}", latency_ms
        return False, f"HTTP {response.status_code}", latency_ms
    except Exception as exc:
        latency_ms = int((time.monotonic() - started) * 1000)
        return False, str(exc)[:300], latency_ms


__all__ = ["router"]
