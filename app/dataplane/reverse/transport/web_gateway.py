"""Grok Web MGW WebSocket 会话传输。"""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
import uuid
from collections import OrderedDict
from typing import Any, AsyncGenerator
from urllib.parse import urlencode, urlparse, urlunparse

import aiohttp
import orjson

from app.control.model.enums import ModeId
from app.control.proxy.models import ProxyLease
from app.dataplane.proxy.adapters.headers import (
    build_http_headers,
    build_sso_cookie,
    build_ws_headers,
)
from app.dataplane.proxy.adapters.session import ResettableSession
from app.dataplane.reverse.runtime.endpoint_table import BASE
from app.dataplane.reverse.transport.websocket import WebSocketClient
from app.platform.config.snapshot import get_config
from app.platform.errors import UpstreamError


_HANDSHAKE_TIMEOUT_SECONDS = 20.0
_HEARTBEAT_SECONDS = 25.0
_MAX_FRAME_BYTES = 16 * 1024 * 1024
_SESSION_BODY_LIMIT = 64 * 1024
_IDENTITY_CACHE_LIMIT = 4096
_IDENTITY_CACHE_TTL_SECONDS = 3600.0
_X_USER_ID_RE = re.compile(r"(?:^|;\s*)x-userid=([^;]+)", re.IGNORECASE)

_identity_cache: OrderedDict[str, tuple[str, float]] = OrderedDict()
_identity_lock = asyncio.Lock()


def normalize_gateway_user_id(value: str) -> str:
    """校验并规范化 Gateway 要求的 UUID 用户标识。"""
    try:
        return str(uuid.UUID(str(value or "").strip()))
    except (ValueError, AttributeError, TypeError) as exc:
        raise UpstreamError("Grok Web account is missing a valid user_id", status=401) from exc


def gateway_endpoint(base_url: str, user_id: str) -> tuple[str, str]:
    """从 Web Base URL 构造 MGW WebSocket endpoint 与 Origin。"""
    parsed = urlparse(base_url.strip())
    if not parsed.hostname or parsed.scheme not in {"http", "https"}:
        raise ValueError("Grok Web base URL is invalid")
    scheme = "wss" if parsed.scheme == "https" else "ws"
    origin = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
    endpoint = urlunparse(
        (scheme, parsed.netloc, "/ws/mgw/", "", urlencode({"uid": normalize_gateway_user_id(user_id)}), "")
    )
    return endpoint, origin


def parse_session_user_id(body: bytes) -> str:
    """从 /api/auth/session 响应提取稳定用户 UUID。"""
    try:
        payload = orjson.loads(body)
    except orjson.JSONDecodeError as exc:
        raise UpstreamError("Grok session response is invalid", status=502) from exc
    if not isinstance(payload, dict):
        raise UpstreamError("Grok session response is invalid", status=502)

    status = str(payload.get("status") or "").strip().lower()
    if status in {"blocked", "unauthenticated"}:
        raise UpstreamError(f"Grok session status is {status}", status=401)

    session = payload.get("session") if isinstance(payload.get("session"), dict) else {}
    user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
    candidates = (
        session.get("userId"),
        user.get("id"),
        user.get("userId"),
        user.get("sub"),
        payload.get("id"),
        payload.get("userId"),
        payload.get("sub"),
    )
    for candidate in candidates:
        if candidate:
            return normalize_gateway_user_id(str(candidate))
    raise UpstreamError("Grok session response does not contain user_id", status=401)


def _cookie_user_id(token: str, lease: ProxyLease | None) -> str | None:
    """读取已有 Cookie 中的 x-userid，格式异常时继续走 Session 同步。"""
    match = _X_USER_ID_RE.search(build_sso_cookie(token, lease=lease))
    if not match:
        return None
    try:
        return normalize_gateway_user_id(match.group(1))
    except UpstreamError:
        return None


async def resolve_gateway_user_id(
    token: str,
    lease: ProxyLease,
    *,
    base_url: str = BASE,
) -> str:
    """复用 Cookie 或同一出口的 Session 接口解析 Gateway user_id。"""
    cookie_user_id = _cookie_user_id(token, lease)
    if cookie_user_id:
        return cookie_user_id

    cache_key = hashlib.sha256(token.encode()).hexdigest()
    async with _identity_lock:
        cached = _identity_cache.get(cache_key)
        if cached and cached[1] > time.time():
            _identity_cache.move_to_end(cache_key)
            return cached[0]
        _identity_cache.pop(cache_key, None)

    endpoint = f"{base_url.rstrip('/')}/api/auth/session"
    headers = build_http_headers(
        token,
        origin=base_url,
        referer=f"{base_url.rstrip('/')}/",
        lease=lease,
    )
    try:
        async with ResettableSession(lease=lease) as session:
            response = await session.get(endpoint, headers=headers, timeout=15.0)
    except Exception as exc:
        if isinstance(exc, UpstreamError):
            raise
        raise UpstreamError(f"Grok session transport failed: {exc}", status=502) from exc

    body = bytes(response.content or b"")
    if len(body) > _SESSION_BODY_LIMIT:
        raise UpstreamError("Grok session response exceeds the size limit", status=502)
    if response.status_code == 401:
        raise UpstreamError("Grok session is unauthorized", status=401, body=body[:400].decode("utf-8", "replace"))
    if response.status_code < 200 or response.status_code >= 300:
        raise UpstreamError(
            f"Grok session endpoint returned {response.status_code}",
            status=response.status_code,
            body=body[:400].decode("utf-8", "replace"),
        )
    user_id = parse_session_user_id(body)

    async with _identity_lock:
        _identity_cache[cache_key] = (user_id, time.time() + _IDENTITY_CACHE_TTL_SECONDS)
        _identity_cache.move_to_end(cache_key)
        while len(_identity_cache) > _IDENTITY_CACHE_LIMIT:
            _identity_cache.popitem(last=False)
    return user_id


def gateway_session(
    mode_id: ModeId,
    *,
    request_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """构造新临时会话的 session.create 配置。"""
    cfg = get_config()
    overrides = request_overrides or {}
    try:
        image_count = int(overrides.get("imageGenerationCount", 2))
    except (TypeError, ValueError):
        image_count = 2
    image_count = max(1, min(image_count, 4))
    x_grok = {
        "protocol_capabilities": ["conversation_attached", "custom_methods_v1"],
        "use_chunk": True,
        "enable_side_by_side": True,
        "force_side_by_side": bool(overrides.get("forceSideBySide", False)),
        "enable_image_generation": bool(overrides.get("enableImageGeneration", True)),
        "image_generation_count": image_count,
        "disable_text_follow_ups": bool(overrides.get("disableTextFollowUps", False)),
        "disable_artifact": True,
        "force_concise": bool(overrides.get("forceConcise", False)),
        "keep_context": False,
        "is_temporary": bool(overrides.get("temporary", cfg.get_bool("features.temporary", True))),
        "disable_memory": bool(
            overrides.get("disableMemory", not cfg.get_bool("features.memory", False))
        ),
    }
    if "disableSearch" in overrides:
        x_grok["disable_search"] = bool(overrides["disableSearch"])
    return {"model": mode_id.to_api_str(), "x_grok": x_grok}


def gateway_turn_events(
    session_id: str,
    prompt: str,
    attachments: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """构造 conversation.item.create 与 response.create 事件。"""
    chunks: list[dict[str, Any]] = [
        {
            "mention": {
                "target": {"file_mention": {"file_id": attachment}}
            }
        }
        for attachment in attachments
    ]
    chunks.append({"text": {"text": prompt}})
    item: dict[str, Any] = {
        "type": "message",
        "role": "user",
        "x_grok": {
            "client_message_id": str(uuid.uuid4()),
            "input_chunks": chunks,
        },
    }
    if attachments:
        item["file_attachment_ids"] = list(attachments)

    timestamp_ms = int(time.time() * 1000)
    item_event: dict[str, Any] = {
        "session_id": session_id,
        "event": {
            "type": "conversation.item.create",
            "event_id": f"evt_msg_{timestamp_ms}",
            "item": item,
        },
    }
    if attachments:
        item_event["event"]["file_attachment_ids"] = list(attachments)
    response_event = {
        "session_id": session_id,
        "event": {
            "type": "response.create",
            "event_id": f"evt_resp_{timestamp_ms}",
        },
    }
    return item_event, response_event


def gateway_headers(
    token: str,
    user_id: str,
    origin: str,
    lease: ProxyLease,
) -> dict[str, str]:
    """构造包含规范 x-userid 的 WebSocket 握手头。"""
    headers = build_ws_headers(token, origin=origin, lease=lease)
    cookie = re.sub(r"(^|;\s*)x-userid=[^;]*", "", headers.get("Cookie", ""), flags=re.IGNORECASE)
    headers["Cookie"] = f"{cookie.strip('; ')}; x-userid={normalize_gateway_user_id(user_id)}"
    return headers


async def _send_json(
    ws: aiohttp.ClientWebSocketResponse,
    send_lock: asyncio.Lock,
    value: dict[str, Any],
) -> None:
    """串行发送一个文本 JSON frame。"""
    async with send_lock:
        await ws.send_str(orjson.dumps(value).decode())


async def _heartbeat(
    ws: aiohttp.ClientWebSocketResponse,
    send_lock: asyncio.Lock,
) -> None:
    """在会话存续期间发送 MGW 应用层 ping。"""
    try:
        while True:
            await asyncio.sleep(_HEARTBEAT_SECONDS)
            await _send_json(
                ws,
                send_lock,
                {
                    "event": {
                        "type": "ping",
                        "event_id": f"evt_hb_{int(time.time() * 1000)}",
                    }
                },
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        # 发送失败时主动结束读循环，由主状态机统一报告“提前关闭”。
        await ws.close()
        raise


async def stream_gateway_chat(
    *,
    token: str,
    mode_id: ModeId,
    prompt: str,
    attachments: list[str],
    lease: ProxyLease,
    timeout_s: float,
    request_overrides: dict[str, Any] | None = None,
) -> AsyncGenerator[str, None]:
    """运行 MGW 状态机并逐帧输出 Gateway JSON envelope。"""
    user_id = await resolve_gateway_user_id(token, lease)
    endpoint, origin = gateway_endpoint(BASE, user_id)
    headers = gateway_headers(token, user_id, origin, lease)
    client = WebSocketClient()

    try:
        connection = await client.connect(
            endpoint,
            headers=headers,
            timeout=min(max(timeout_s, 1.0), _HANDSHAKE_TIMEOUT_SECONDS),
            ws_kwargs={"max_msg_size": _MAX_FRAME_BYTES, "autoping": True},
            lease=lease,
        )
    except aiohttp.WSServerHandshakeError as exc:
        raise UpstreamError(
            f"Grok Gateway handshake returned {exc.status}",
            status=exc.status,
            body=str(exc)[:400],
        ) from exc
    except Exception as exc:
        if isinstance(exc, UpstreamError):
            raise
        raise UpstreamError(f"Grok Gateway handshake failed: {exc}", status=502) from exc

    send_lock = asyncio.Lock()
    heartbeat_task: asyncio.Task[None] | None = None
    initial_event_id = f"evt_init_{uuid.uuid4()}"
    session_create = {
        "event": {
            "type": "session.create",
            "event_id": initial_event_id,
            "session": gateway_session(mode_id, request_overrides=request_overrides),
        }
    }

    try:
        async with asyncio.timeout(max(timeout_s, 1.0)):
            async with connection as ws:
                await _send_json(ws, send_lock, session_create)
                heartbeat_task = asyncio.create_task(_heartbeat(ws, send_lock))
                current_session_id = ""
                created = False
                attached = False
                turn_sent = False

                async for message in ws:
                    if message.type == aiohttp.WSMsgType.ERROR:
                        raise UpstreamError(f"Grok Gateway WebSocket error: {ws.exception()}", status=502)
                    if message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING}:
                        break
                    if message.type != aiohttp.WSMsgType.TEXT:
                        continue

                    data = str(message.data)
                    if len(data.encode("utf-8")) > _MAX_FRAME_BYTES:
                        raise UpstreamError("Grok Gateway frame exceeds the size limit", status=502)
                    try:
                        envelope = orjson.loads(data)
                    except orjson.JSONDecodeError:
                        continue
                    if not isinstance(envelope, dict) or not isinstance(envelope.get("event"), dict):
                        continue

                    event = envelope["event"]
                    event_type = str(event.get("type") or "")
                    yield data

                    if event_type == "session.created":
                        client_event_id = str(event.get("client_event_id") or "")
                        if client_event_id and client_event_id != initial_event_id:
                            continue
                        created = True
                        current_session_id = current_session_id or str(envelope.get("session_id") or "")
                    elif event_type == "conversation.attached":
                        conversation = event.get("conversation") if isinstance(event.get("conversation"), dict) else {}
                        conversation_id = str(conversation.get("id") or "")
                        current_session_id = current_session_id or conversation_id
                        if not conversation_id or conversation_id != current_session_id:
                            raise UpstreamError("Grok Gateway returned an inconsistent conversation id", status=502)
                        attached = True
                    elif event_type in {"response.done", "error"}:
                        return
                    elif event_type == "session.ended":
                        raise UpstreamError("Grok Gateway session ended before response completion", status=502)

                    if created and attached and not turn_sent:
                        turn_sent = True
                        item_event, response_event = gateway_turn_events(
                            current_session_id,
                            prompt,
                            attachments,
                        )
                        await _send_json(ws, send_lock, item_event)
                        await _send_json(ws, send_lock, response_event)

                raise UpstreamError("Grok Gateway closed before response completion", status=502)
    except TimeoutError as exc:
        raise UpstreamError("Grok Gateway request timed out", status=504) from exc
    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)


__all__ = [
    "gateway_endpoint",
    "gateway_headers",
    "gateway_session",
    "gateway_turn_events",
    "normalize_gateway_user_id",
    "parse_session_user_id",
    "resolve_gateway_user_id",
    "stream_gateway_chat",
]
