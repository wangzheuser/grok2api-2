"""Admin model catalog endpoints."""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.control.account.state_machine import is_manageable
from app.control.account.quota_defaults import supports_mode
from app.control.model import registry as model_registry
from app.control.model.enums import Capability
from app.dataplane.reverse.protocol.console_model_guard import fallback_target_ids
from app.dataplane.reverse.protocol.xai_console_chat import (
    MODEL_FIXED_EFFORT,
    console_model_for,
)

router = APIRouter(prefix="/models", tags=["Admin - Models"])

_POOL_ID_TO_NAME = {0: "basic", 1: "super", 2: "heavy"}


@router.get("")
async def list_admin_models(request: Request):
    """Return all registered models with admin-facing metadata."""
    pools = await _available_pools(request)
    fallback_targets = fallback_target_ids()
    created = int(time.time())
    data = [
        _model_payload(spec, pools=pools, fallback_targets=fallback_targets, created=created)
        for spec in model_registry.MODELS
    ]
    return JSONResponse({"object": "list", "data": data})


async def _available_pools(request: Request) -> frozenset[str]:
    repo = getattr(request.app.state, "repository", None)
    if repo is None:
        return frozenset()
    snapshot = await repo.runtime_snapshot()
    return frozenset(record.pool for record in snapshot.items if is_manageable(record))


def _model_payload(spec, *, pools: frozenset[str], fallback_targets: set[str], created: int) -> dict[str, Any]:
    pool_candidates = [
        _POOL_ID_TO_NAME[pool_id]
        for pool_id in spec.pool_candidates()
        if pool_id in _POOL_ID_TO_NAME
    ]
    available = any(
        pool in pools and supports_mode(pool, int(spec.mode_id))
        for pool in pool_candidates
    )
    is_console = spec.is_console_chat()
    return {
        "id": spec.model_name,
        "object": "model",
        "created": created,
        "owned_by": "xai",
        "name": spec.public_name,
        "capabilities": _capability_names(spec.capability),
        "tier": spec.tier.name.lower(),
        "mode": spec.mode_id.name.lower(),
        "pool_candidates": pool_candidates,
        "enabled": spec.enabled,
        "available": available,
        "is_console": is_console,
        "console_model": console_model_for(spec.model_name) if is_console else "",
        "fixed_effort": MODEL_FIXED_EFFORT.get(spec.model_name, ""),
        "fallback_target": spec.model_name in fallback_targets,
    }


def _capability_names(capability: Capability) -> list[str]:
    names: list[str] = []
    for item in Capability:
        if capability & item:
            names.append(item.name.lower())
    return names


__all__ = ["router"]
