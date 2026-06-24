"""Console 专用代理池。

该模块只服务 console.x.ai 请求，不改变全局 proxy.egress 行为。
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, Field, field_validator, model_validator

from app.control.proxy.models import ProxyFeedback, ProxyFeedbackKind, RequestKind, ProxyLease, ProxyScope
from app.platform.config.snapshot import config, get_config
from app.platform.errors import UpstreamError
from app.platform.logging.logger import logger
from app.platform.runtime.clock import now_ms

TIME_PLACEHOLDER = "{time}"


class ConsoleProxyMode(StrEnum):
    """Console 代理模式。"""

    STATIC = "static"
    DYNAMIC_TEMPLATE = "dynamic_template"


class ConsoleProxyStatus(StrEnum):
    """Console 代理运行态状态。"""

    ALIVE = "alive"
    AVAILABLE = "available"
    COOLING_DOWN = "cooling_down"
    DISABLED = "disabled"
    DEAD = "dead"


class ConsoleProxyEntry(BaseModel):
    """Console 代理条目的持久化结构。"""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    url: str
    username: str = ""
    password: str = ""
    mode: ConsoleProxyMode | str | None = None
    enabled: bool = True
    generation: int = 0

    @field_validator("url", mode="before")
    @classmethod
    def _normalize_url(cls, value: Any) -> str:
        """规范化并校验代理 URL。"""
        text = str(value or "").strip()
        if not text:
            raise ValueError("proxy url cannot be empty")
        parsed = urlsplit(text.replace(TIME_PLACEHOLDER, "0"))
        if parsed.scheme.lower() not in {"http", "https", "socks", "socks4", "socks4a", "socks5", "socks5h"}:
            raise ValueError("unsupported proxy scheme")
        if not parsed.netloc:
            raise ValueError("proxy url must include host")
        return text

    @field_validator("username", "password", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str:
        """把可选文本字段收敛为空字符串。"""
        return "" if value is None else str(value).strip()

    @field_validator("mode", mode="after")
    @classmethod
    def _normalize_mode(cls, value: ConsoleProxyMode | str | None) -> ConsoleProxyMode | None:
        """允许旧配置缺失 mode，由运行时按 URL/username 推断。"""
        if value in (None, ""):
            return None
        return ConsoleProxyMode(str(value))

    @model_validator(mode="after")
    def _split_embedded_auth(self) -> "ConsoleProxyEntry":
        """拆分 URL 内嵌认证信息，避免后台接口回显明文密码。"""
        parts = urlsplit(self.url)
        if not parts.username and not parts.password:
            return self
        if not self.username and parts.username:
            self.username = parts.username
        if not self.password and parts.password:
            self.password = parts.password
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        self.url = urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))
        return self

    def inferred_mode(self) -> ConsoleProxyMode:
        """返回显式 mode 或按模板占位符推断出的 mode。"""
        if self.mode:
            return ConsoleProxyMode(str(self.mode))
        if TIME_PLACEHOLDER in self.url or TIME_PLACEHOLDER in self.username:
            return ConsoleProxyMode.DYNAMIC_TEMPLATE
        return ConsoleProxyMode.STATIC

    def public_dict(self, *, include_secret: bool = False) -> dict[str, Any]:
        """返回可持久化结构，默认不包含明文密码。"""
        data = {
            "id": self.id,
            "url": self.url,
            "username": self.username,
            "mode": self.inferred_mode().value,
            "enabled": self.enabled,
            "generation": self.generation,
        }
        if include_secret:
            data["password"] = self.password
        return data


@dataclass(slots=True)
class _ConsoleProxyRuntime:
    """Console 代理条目的内存运行态。"""

    alive: bool = True
    runtime_epoch: int = 0
    last_error: str = ""
    last_failure_at: int | None = None
    next_retry_at: int | None = None
    consecutive_failures: int = 0
    success_count: int = 0
    failure_count: int = 0
    challenge_count: int = 0


@dataclass(frozen=True, slots=True)
class _AssignedConsoleProxy:
    """一次 Console 请求实际分配到的代理。"""

    entry: ConsoleProxyEntry
    proxy_url: str
    runtime_epoch: int
    account_key: str


@dataclass(slots=True)
class _ConsolePoolState:
    """Console 代理池内存态。"""

    entries: list[ConsoleProxyEntry] = field(default_factory=list)
    runtimes: dict[str, _ConsoleProxyRuntime] = field(default_factory=dict)
    bindings: dict[str, str] = field(default_factory=dict)
    config_sig: tuple[Any, ...] | None = None


class ConsoleProxyPool:
    """账号 sticky 的 Console 专用代理池。"""

    def __init__(self) -> None:
        self._state = _ConsolePoolState()
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        *,
        token: str,
        fallback_lease_factory,
        scope: ProxyScope = ProxyScope.APP,
        kind: RequestKind = RequestKind.HTTP,
        clearance_origin: str | None = "https://console.x.ai",
    ) -> ProxyLease:
        """为 Console 请求获取代理租约。"""
        cfg = get_config()
        if not cfg.get_bool("console.proxy_pool.enabled", False):
            return await fallback_lease_factory(
                scope=scope,
                kind=kind,
                clearance_origin=clearance_origin,
            )

        await self.load()
        # load() may parse no entries but should not fail open on malformed config.
        account_key = account_key_for_token(token)
        assigned = await self._assign(account_key)
        if assigned is None:
            if cfg.get_bool("console.proxy_pool.fallback_to_global_proxy", True):
                lease = await fallback_lease_factory(
                    scope=scope,
                    kind=kind,
                    clearance_origin=clearance_origin,
                )
                lease.proxy_pool = "global"
                lease.account_key = account_key
                return lease
            raise UpstreamError(
                "No schedulable console proxy",
                status=429,
                code="console_proxy_unavailable",
            )

        lease = await fallback_lease_factory(
            scope=scope,
            kind=kind,
            clearance_origin=clearance_origin,
            proxy_url_override=assigned.proxy_url,
        )
        lease.proxy_pool = "console"
        lease.proxy_id = assigned.entry.id
        lease.proxy_mode = assigned.entry.inferred_mode().value
        lease.generation = assigned.entry.generation
        lease.runtime_epoch = assigned.runtime_epoch
        lease.account_key = assigned.account_key
        return lease

    async def feedback(self, lease: ProxyLease, result: ProxyFeedback) -> None:
        """根据请求结果回写 Console 代理运行态。"""
        if lease.proxy_pool != "console" or not lease.proxy_id:
            return

        if result.kind == ProxyFeedbackKind.SUCCESS:
            await self.mark_success(lease)
            return

        if result.kind == ProxyFeedbackKind.CHALLENGE:
            await self._record_challenge(lease, result.reason or result.kind.value)
            return

        if not _is_proxy_failure(result):
            return

        await self.mark_failure(lease, result.reason or result.kind.value)


    async def _record_challenge(self, lease: ProxyLease, reason: str = "") -> None:
        """记录 403/challenge 软失败，达到阈值后才切换代理。"""
        if not lease.proxy_id:
            return
        cfg = get_config()
        threshold = max(2, cfg.get_int("console.proxy_pool.challenge_failure_threshold", 2))
        should_fail = False
        async with self._lock:
            runtime = self._state.runtimes.get(lease.proxy_id)
            entry = self._entry_by_id(lease.proxy_id)
            if not entry or not runtime or entry.generation != lease.generation:
                return
            if runtime.runtime_epoch != lease.runtime_epoch:
                return
            runtime.challenge_count += 1
            should_fail = runtime.challenge_count >= threshold
        if should_fail:
            await self.mark_failure(lease, reason or "challenge_threshold")

    async def mark_success(self, lease: ProxyLease) -> bool:
        """按租约身份标记代理成功。"""
        if not lease.proxy_id:
            return False
        async with self._lock:
            entry = self._entry_by_id(lease.proxy_id)
            runtime = self._state.runtimes.get(lease.proxy_id)
            if not entry or not runtime or entry.generation != lease.generation:
                return False
            if runtime.runtime_epoch != lease.runtime_epoch:
                return False
            runtime.success_count += 1
            runtime.alive = True
            runtime.last_error = ""
            runtime.last_failure_at = None
            runtime.next_retry_at = None
            runtime.consecutive_failures = 0
            runtime.challenge_count = 0
            if entry.inferred_mode() == ConsoleProxyMode.DYNAMIC_TEMPLATE:
                runtime.runtime_epoch += 1
            return True

    async def mark_failure(self, lease: ProxyLease, reason: str = "") -> bool:
        """按租约身份标记代理失败并清理 sticky 绑定。"""
        if not lease.proxy_id:
            return False
        cfg = get_config()
        async with self._lock:
            entry = self._entry_by_id(lease.proxy_id)
            runtime = self._state.runtimes.get(lease.proxy_id)
            if not entry or not runtime or entry.generation != lease.generation:
                return False
            if runtime.runtime_epoch != lease.runtime_epoch:
                return False

            runtime.failure_count += 1
            runtime.alive = False
            runtime.last_error = str(reason or "proxy_failure")[:300]
            runtime.last_failure_at = now_ms()
            runtime.consecutive_failures += 1
            runtime.runtime_epoch += 1
            if entry.inferred_mode() == ConsoleProxyMode.DYNAMIC_TEMPLATE:
                base = max(1, cfg.get_int("console.proxy_pool.dynamic_retry_base_sec", 60))
                max_sec = max(base, cfg.get_int("console.proxy_pool.dynamic_retry_max_sec", 600))
                factor = max(1.0, cfg.get_float("console.proxy_pool.dynamic_backoff_factor", 2.0))
                delay = min(max_sec, int(base * (factor ** max(0, runtime.consecutive_failures - 1))))
            else:
                delay = max(1, cfg.get_int("console.proxy_pool.static_cooldown_sec", 300))
            runtime.next_retry_at = now_ms() + delay * 1000
            if lease.account_key:
                self._state.bindings.pop(lease.account_key, None)
            logger.warning(
                "console proxy marked failed: proxy_id={} mode={} reason={} next_retry_at={}",
                entry.id,
                entry.inferred_mode().value,
                runtime.last_error,
                runtime.next_retry_at,
            )
            return True

    async def load(self) -> None:
        """从热配置加载代理池条目，保留可复用运行态。"""
        cfg = get_config()
        raw_entries = cfg.get("console.proxy_pool.entries", []) or []
        config_sig = (tuple(_entry_sig(item) for item in raw_entries),)
        async with self._lock:
            if self._state.config_sig == config_sig:
                return
            entries = [_coerce_entry(item) for item in raw_entries]
            ids = {entry.id for entry in entries}
            runtimes = {
                proxy_id: runtime
                for proxy_id, runtime in self._state.runtimes.items()
                if proxy_id in ids
            }
            for entry in entries:
                runtimes.setdefault(entry.id, _ConsoleProxyRuntime())
            bindings = {
                account_key: proxy_id
                for account_key, proxy_id in self._state.bindings.items()
                if proxy_id in ids
            }
            self._state = _ConsolePoolState(
                entries=entries,
                runtimes=runtimes,
                bindings=bindings,
                config_sig=config_sig,
            )
            logger.info("console proxy pool loaded: count={}", len(entries))

    async def snapshot(self) -> dict[str, Any]:
        """返回后台展示用代理池快照。"""
        await self.load()
        cfg = get_config()
        async with self._lock:
            now = now_ms()
            rows = [self._snapshot_entry(entry, now) for entry in self._state.entries]
            return {
                "enabled": cfg.get_bool("console.proxy_pool.enabled", False),
                "fallback_to_global_proxy": cfg.get_bool("console.proxy_pool.fallback_to_global_proxy", True),
                "items": rows,
                "binding_count": len(self._state.bindings),
            }

    async def replace_entries(self, entries: list[ConsoleProxyEntry]) -> None:
        """整体保存代理池条目并热加载。"""
        await config.update({"console": {"proxy_pool": {"entries": [e.public_dict(include_secret=True) for e in entries]}}})
        await config.load()
        async with self._lock:
            self._state.config_sig = None
        await self.load()

    async def add_entries(self, entries: list[ConsoleProxyEntry]) -> int:
        """追加代理条目。"""
        current = await self.entries(include_secret=True)
        current.extend(entries)
        await self.replace_entries(current)
        return len(entries)

    async def entries(self, *, include_secret: bool = False) -> list[ConsoleProxyEntry]:
        """返回当前配置中的代理条目。"""
        cfg = get_config()
        return [_coerce_entry(item) for item in (cfg.get("console.proxy_pool.entries", []) or [])]

    async def update_entry(self, proxy_id: str, patch: dict[str, Any]) -> ConsoleProxyEntry:
        """更新指定代理条目。"""
        entries = await self.entries(include_secret=True)
        for idx, entry in enumerate(entries):
            if entry.id != proxy_id:
                continue
            data = entry.public_dict(include_secret=True)
            for key in ("url", "username", "mode", "enabled"):
                if key in patch:
                    data[key] = patch[key]
            if patch.get("password") not in (None, ""):
                data["password"] = patch["password"]
            data["generation"] = int(data.get("generation", 0)) + 1
            updated = _coerce_entry(data)
            entries[idx] = updated
            await self.replace_entries(entries)
            await self._clear_binding_for_proxy(proxy_id)
            return updated
        raise KeyError(proxy_id)

    async def remove_entry(self, proxy_id: str) -> None:
        """删除指定代理条目。"""
        entries = await self.entries(include_secret=True)
        next_entries = [entry for entry in entries if entry.id != proxy_id]
        if len(next_entries) == len(entries):
            raise KeyError(proxy_id)
        await self.replace_entries(next_entries)
        await self._clear_binding_for_proxy(proxy_id)

    async def set_enabled(self, proxy_id: str, enabled: bool) -> ConsoleProxyEntry:
        """启用或禁用指定代理。"""
        return await self.update_entry(proxy_id, {"enabled": enabled})

    async def reset_entry(self, proxy_id: str) -> bool:
        """重置指定代理运行态。"""
        async with self._lock:
            runtime = self._state.runtimes.get(proxy_id)
            if runtime is None:
                return False
            runtime.alive = True
            runtime.last_error = ""
            runtime.last_failure_at = None
            runtime.next_retry_at = None
            runtime.consecutive_failures = 0
            runtime.runtime_epoch += 1
            return True

    async def clear_bindings(self) -> int:
        """清空所有账号 sticky 绑定。"""
        async with self._lock:
            count = len(self._state.bindings)
            self._state.bindings.clear()
            return count

    async def _clear_binding_for_proxy(self, proxy_id: str) -> None:
        """清理命中指定代理的账号绑定。"""
        async with self._lock:
            self._state.bindings = {
                account_key: bound_proxy_id
                for account_key, bound_proxy_id in self._state.bindings.items()
                if bound_proxy_id != proxy_id
            }

    async def _assign(self, account_key: str) -> _AssignedConsoleProxy | None:
        """按账号 sticky 规则分配代理。"""
        async with self._lock:
            now = now_ms()
            bound_id = self._state.bindings.get(account_key)
            if bound_id:
                entry = self._entry_by_id(bound_id)
                if entry and self._is_schedulable(entry, now):
                    runtime = self._state.runtimes[entry.id]
                    return self._assigned(entry, runtime, account_key, now)
                self._state.bindings.pop(account_key, None)

            candidates = [entry for entry in self._state.entries if self._is_schedulable(entry, now)]
            if not candidates:
                return None

            bind_counts = {entry.id: 0 for entry in candidates}
            for proxy_id in self._state.bindings.values():
                if proxy_id in bind_counts:
                    bind_counts[proxy_id] += 1
            entry = min(candidates, key=lambda item: bind_counts.get(item.id, 0))
            self._state.bindings[account_key] = entry.id
            runtime = self._state.runtimes[entry.id]
            return self._assigned(entry, runtime, account_key, now)

    def _assigned(
        self,
        entry: ConsoleProxyEntry,
        runtime: _ConsoleProxyRuntime,
        account_key: str,
        timestamp_ms: int,
    ) -> _AssignedConsoleProxy:
        """构建一次请求的代理分配结果。"""
        return _AssignedConsoleProxy(
            entry=entry,
            proxy_url=_render_proxy_url(entry, timestamp_ms),
            runtime_epoch=runtime.runtime_epoch,
            account_key=account_key,
        )

    def _entry_by_id(self, proxy_id: str) -> ConsoleProxyEntry | None:
        """按稳定 ID 查找代理条目。"""
        return next((entry for entry in self._state.entries if entry.id == proxy_id), None)

    def _is_schedulable(self, entry: ConsoleProxyEntry, timestamp_ms: int) -> bool:
        """判断代理当前是否可调度。"""
        if not entry.enabled:
            return False
        runtime = self._state.runtimes.get(entry.id)
        if runtime is None:
            return False
        if runtime.next_retry_at and timestamp_ms < runtime.next_retry_at:
            return False
        if runtime.next_retry_at and timestamp_ms >= runtime.next_retry_at:
            runtime.alive = True
            runtime.next_retry_at = None
        return runtime.alive

    def _snapshot_entry(self, entry: ConsoleProxyEntry, timestamp_ms: int) -> dict[str, Any]:
        """构建单个代理的后台快照。"""
        runtime = self._state.runtimes.setdefault(entry.id, _ConsoleProxyRuntime())
        status = _status_for(entry, runtime, timestamp_ms)
        bound_count = sum(1 for proxy_id in self._state.bindings.values() if proxy_id == entry.id)
        return {
            "id": entry.id,
            "url": _display_proxy_url(entry),
            "raw_url": entry.url,
            "username": entry.username,
            "has_password": bool(entry.password),
            "mode": entry.inferred_mode().value,
            "enabled": entry.enabled,
            "generation": entry.generation,
            "status": status.value,
            "runtime_epoch": runtime.runtime_epoch,
            "last_error": runtime.last_error,
            "last_failure_at": runtime.last_failure_at,
            "next_retry_at": runtime.next_retry_at,
            "consecutive_failures": runtime.consecutive_failures,
            "success_count": runtime.success_count,
            "failure_count": runtime.failure_count,
            "challenge_count": runtime.challenge_count,
            "bound_account_count": bound_count,
            "fallback_target": True,
        }


def _display_proxy_url(entry: ConsoleProxyEntry) -> str:
    """返回包含脱敏认证信息的代理展示 URL。"""
    if not entry.username:
        return mask_proxy_url(entry.url)
    try:
        parts = urlsplit(entry.url)
        host = parts.netloc
        userinfo = f"{entry.username}:***" if entry.password else entry.username
        return urlunsplit((parts.scheme, f"{userinfo}@{host}", parts.path, parts.query, parts.fragment))
    except Exception:
        return mask_proxy_url(entry.url)


def account_key_for_token(token: str) -> str:
    """返回账号 sticky 绑定使用的脱敏 key。"""
    normalized = token[4:] if token.startswith("sso=") else token
    return hashlib.sha256(normalized.encode("utf-8", "ignore")).hexdigest()


def mask_proxy_url(url: str) -> str:
    """脱敏展示代理 URL。"""
    if not url:
        return ""
    try:
        parts = urlsplit(url)
        netloc = parts.netloc
        if "@" in netloc:
            userinfo, host = netloc.rsplit("@", 1)
            username = userinfo.split(":", 1)[0]
            netloc = f"{username}:***@{host}"
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        return "<invalid>"


def parse_proxy_line(line: str) -> ConsoleProxyEntry:
    """解析后台批量导入的一行代理配置。"""
    stripped = line.strip()
    if not stripped:
        raise ValueError("proxy line is empty")
    return _coerce_entry({"url": stripped})


def _render_proxy_url(entry: ConsoleProxyEntry, timestamp_ms: int) -> str:
    """渲染本次请求真实使用的代理 URL。"""
    mode = entry.inferred_mode()
    def render(value: str) -> str:
        return value.replace(TIME_PLACEHOLDER, str(timestamp_ms)) if mode == ConsoleProxyMode.DYNAMIC_TEMPLATE else value

    raw_url = render(entry.url)
    if not entry.username:
        return raw_url
    parts = urlsplit(raw_url)
    username = render(entry.username)
    password = render(entry.password)
    auth = username if not password else f"{username}:{password}"
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, f"{auth}@{host}", parts.path, parts.query, parts.fragment))


def _status_for(entry: ConsoleProxyEntry, runtime: _ConsoleProxyRuntime, timestamp_ms: int) -> ConsoleProxyStatus:
    """返回代理运行态状态。"""
    if not entry.enabled:
        return ConsoleProxyStatus.DISABLED
    if runtime.next_retry_at and timestamp_ms < runtime.next_retry_at:
        return ConsoleProxyStatus.COOLING_DOWN
    if runtime.next_retry_at and timestamp_ms >= runtime.next_retry_at:
        runtime.alive = True
        runtime.next_retry_at = None
    if runtime.alive:
        return ConsoleProxyStatus.AVAILABLE if entry.inferred_mode() == ConsoleProxyMode.DYNAMIC_TEMPLATE else ConsoleProxyStatus.ALIVE
    return ConsoleProxyStatus.DEAD


def _is_proxy_failure(result: ProxyFeedback) -> bool:
    """判断反馈是否应计为代理失败。"""
    if result.kind == ProxyFeedbackKind.TRANSPORT_ERROR:
        return True
    if result.status_code == 407:
        return True
    if result.kind == ProxyFeedbackKind.UNAUTHORIZED:
        return True
    return False


def _coerce_entry(item: Any) -> ConsoleProxyEntry:
    """把配置项转换为 ConsoleProxyEntry。"""
    if isinstance(item, ConsoleProxyEntry):
        entry = item
    elif isinstance(item, str):
        entry = ConsoleProxyEntry(id=_stable_entry_id({"url": item}), url=item)
    elif isinstance(item, dict):
        data = dict(item)
        if not str(data.get("id") or "").strip():
            data["id"] = _stable_entry_id(data)
        entry = ConsoleProxyEntry.model_validate(data)
    else:
        raise ValueError("invalid console proxy entry")
    if not entry.id:
        entry.id = _stable_entry_id(entry.public_dict(include_secret=True))
    if entry.mode is None:
        entry.mode = entry.inferred_mode()
    return entry


def _stable_entry_id(data: dict[str, Any]) -> str:
    """为手写配置生成稳定代理 ID。"""
    raw = "|".join(str(data.get(key, "")) for key in ("url", "username", "password"))
    return "cp_" + hashlib.sha256(raw.encode("utf-8", "ignore")).hexdigest()[:24]


def _entry_sig(item: Any) -> str:
    """生成热加载配置签名。"""
    entry = _coerce_entry(item)
    return repr(entry.public_dict(include_secret=True))


_console_proxy_pool: ConsoleProxyPool | None = None


async def get_console_proxy_pool() -> ConsoleProxyPool:
    """返回 Console 专用代理池单例。"""
    global _console_proxy_pool
    if _console_proxy_pool is None:
        _console_proxy_pool = ConsoleProxyPool()
    await _console_proxy_pool.load()
    return _console_proxy_pool


async def reset_console_proxy_pool_for_tests() -> None:
    """重置测试用代理池单例。"""
    global _console_proxy_pool
    _console_proxy_pool = ConsoleProxyPool()


__all__ = [
    "ConsoleProxyEntry",
    "ConsoleProxyMode",
    "ConsoleProxyPool",
    "ConsoleProxyStatus",
    "TIME_PLACEHOLDER",
    "account_key_for_token",
    "get_console_proxy_pool",
    "mask_proxy_url",
    "parse_proxy_line",
    "reset_console_proxy_pool_for_tests",
]
