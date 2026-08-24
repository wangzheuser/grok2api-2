"""统一出口使用的本地托管代理池。"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any, Awaitable, Callable
from urllib.parse import quote, urlsplit, urlunsplit

from pydantic import BaseModel, Field, field_validator, model_validator

from app.control.account.identity import account_key_for_token
from app.control.proxy.models import (
    ProxyFeedback,
    ProxyFeedbackKind,
    ProxyLease,
    ProxyProvider,
    ProxyScope,
    RequestKind,
)
from app.platform.config.snapshot import config, get_config
from app.platform.errors import UpstreamError
from app.platform.logging.logger import logger
from app.platform.runtime.clock import now_ms

from .managed_state import (
    ProxyBindingCandidate,
    ProxyHealthJob,
    ProxyHealthJobKind,
    ProxyHealthState,
    ProxyProbeOutcome,
    ProxyRuntimeRecord,
    ManagedProxyStateRepository,
    ProxyStateSeed,
    InMemoryManagedProxyStateRepository,
)
from .managed_state_factory import create_managed_proxy_state_repository


TIME_PLACEHOLDER = "{time}"


class ProxyEntryMode(StrEnum):
    """托管代理节点模式。"""

    STATIC = "static"
    DYNAMIC_TEMPLATE = "dynamic_template"


class ProxyStatus(StrEnum):
    """托管代理后台展示状态。"""

    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    COOLING_DOWN = "cooling_down"
    DISABLED = "disabled"
    DEAD = "dead"


class ProxyEntry(BaseModel):
    """托管代理条目的持久化结构。"""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    url: str
    username: str = ""
    password: str = ""
    mode: ProxyEntryMode | str | None = None
    enabled: bool = True
    generation: int = 0

    @field_validator("url", mode="before")
    @classmethod
    def _normalize_url(cls, value: Any) -> str:
        """规范化并校验代理 URL。"""
        text = str(value or "").strip()
        if not text:
            raise ValueError("代理 URL 为空")
        try:
            parsed = urlsplit(text.replace(TIME_PLACEHOLDER, "0"))
            port = parsed.port
        except ValueError as exc:
            raise ValueError("代理 URL 格式有误") from exc
        if parsed.scheme.lower() not in {
            "http",
            "https",
            "socks4",
            "socks4a",
            "socks5",
            "socks5h",
        }:
            raise ValueError("代理协议未受支持")
        if not parsed.hostname:
            raise ValueError("代理 URL 缺少主机名")
        if port is not None and not 1 <= port <= 65535:
            raise ValueError("代理端口有误")
        return text

    @field_validator("username", "password", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str:
        """把可选文本字段收敛为空字符串。"""
        return "" if value is None else str(value).strip()

    @field_validator("mode", mode="after")
    @classmethod
    def _normalize_mode(
        cls,
        value: ProxyEntryMode | str | None,
    ) -> ProxyEntryMode | None:
        """允许旧配置缺失 mode，由运行时推断。"""
        if value in (None, ""):
            return None
        return ProxyEntryMode(str(value))

    @model_validator(mode="after")
    def _split_embedded_auth(self) -> "ProxyEntry":
        """拆分 URL 内嵌认证信息，避免后台接口回显明文密码。"""
        parts = urlsplit(self.url)
        if not parts.username and not parts.password:
            return self
        if not self.username and parts.username:
            self.username = parts.username
        if not self.password and parts.password:
            self.password = parts.password
        host = _proxy_hostport(parts)
        self.url = urlunsplit(
            (parts.scheme, host, parts.path, parts.query, parts.fragment)
        )
        return self

    def inferred_mode(self) -> ProxyEntryMode:
        """返回显式 mode 或按模板占位符推断出的 mode。"""
        if self.mode:
            return ProxyEntryMode(str(self.mode))
        if any(
            TIME_PLACEHOLDER in value
            for value in (self.url, self.username, self.password)
        ):
            return ProxyEntryMode.DYNAMIC_TEMPLATE
        return ProxyEntryMode.STATIC

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


@dataclass(frozen=True, slots=True)
class ProxyUpsertResult:
    """批量写入代理后的新增、更新和未变化统计。"""

    entries: tuple[ProxyEntry, ...]
    added: int
    updated: int
    unchanged: int


@dataclass(frozen=True, slots=True)
class ProxyBatchUpdateResult:
    """批量修改代理后的变更条目和计数。"""

    entries: tuple[ProxyEntry, ...]
    changed: int
    unchanged: int


@dataclass(frozen=True, slots=True)
class _AssignedManagedProxy:
    """一次业务请求实际分配到的托管代理。"""

    entry: ProxyEntry
    proxy_url: str
    runtime: ProxyRuntimeRecord
    account_key: str


LeaseFactory = Callable[..., Awaitable[ProxyLease]]


class ManagedProxyPool:
    """由共享状态仓储提供账号粘性绑定的托管代理池。"""

    def __init__(self, state_repo: ManagedProxyStateRepository | None = None) -> None:
        self._state_repo = state_repo or InMemoryManagedProxyStateRepository()
        self._entries: list[ProxyEntry] = []
        self._config_entries_ref: object | None = None

    @property
    def state_repository(self) -> ManagedProxyStateRepository:
        """返回健康任务和管理 API 共用的状态仓储。"""
        return self._state_repo

    async def initialize(self) -> None:
        """初始化共享仓储并同步配置条目。"""
        await self._state_repo.initialize()
        await self.load()

    async def acquire(
        self,
        *,
        account_key: str = "",
        lease_factory: LeaseFactory,
        scope: ProxyScope = ProxyScope.APP,
        kind: RequestKind = RequestKind.HTTP,
        clearance_origin: str | None = "https://console.x.ai",
    ) -> ProxyLease:
        """按严格健康门禁为业务账号获取托管代理租约。"""
        try:
            await self.load()
            if not account_key:
                raise ValueError("account key is required")
            assigned = await self._assign(account_key)
        except Exception as exc:
            logger.exception(
                "managed proxy shared state unavailable: error_type={} error={}",
                type(exc).__name__,
                exc,
            )
            raise UpstreamError(
                "Managed proxy shared state is unavailable",
                status=503,
                code="egress_proxy_state_unavailable",
            ) from exc

        if assigned is None:
            raise UpstreamError(
                "No schedulable managed proxy",
                status=503,
                code="egress_proxy_unavailable",
            )

        lease = await lease_factory(
            scope=scope,
            kind=kind,
            clearance_origin=clearance_origin,
            proxy_url=assigned.proxy_url,
            affinity_key=(
                f"managed:{assigned.entry.id}:{assigned.entry.generation}:"
                f"{assigned.runtime.runtime_epoch}"
            ),
            provider=ProxyProvider.MANAGED_POOL,
            account_key=assigned.account_key,
        )
        lease.proxy_id = assigned.entry.id
        lease.proxy_mode = assigned.entry.inferred_mode().value
        lease.generation = assigned.entry.generation
        lease.runtime_epoch = assigned.runtime.runtime_epoch
        lease.account_key = assigned.account_key
        lease.provider = ProxyProvider.MANAGED_POOL
        return lease

    async def feedback(self, lease: ProxyLease, result: ProxyFeedback) -> None:
        """根据请求结果条件更新共享运行态。"""
        if lease.provider != ProxyProvider.MANAGED_POOL or not lease.proxy_id:
            return
        if result.kind == ProxyFeedbackKind.SUCCESS:
            await self.mark_success(lease)
            return
        if result.kind == ProxyFeedbackKind.CHALLENGE:
            await self._record_challenge(
                lease,
                result.reason or result.kind.value,
            )
            return
        if _is_proxy_failure(result):
            await self.mark_failure(
                lease,
                result.reason or result.kind.value,
                dead=result.status_code == 407,
            )

    async def mark_success(self, lease: ProxyLease) -> bool:
        """按租约身份标记代理成功。"""
        entry = self._entry_by_id(lease.proxy_id)
        if entry is None:
            return False

        def mutate(runtime: ProxyRuntimeRecord) -> ProxyRuntimeRecord:
            epoch = runtime.runtime_epoch
            if entry.inferred_mode() == ProxyEntryMode.DYNAMIC_TEMPLATE:
                epoch += 1
            return replace(
                runtime,
                health_state=ProxyHealthState.HEALTHY,
                runtime_epoch=epoch,
                last_error="",
                last_failure_at=None,
                next_retry_at=None,
                consecutive_failures=0,
                success_count=runtime.success_count + 1,
                challenge_count=0,
                updated_at=now_ms(),
            )

        return await self._mutate_lease_runtime(lease, mutate)

    async def mark_failure(
        self,
        lease: ProxyLease,
        reason: str = "",
        *,
        dead: bool = False,
    ) -> bool:
        """按租约身份标记代理失败并清理共享绑定。"""
        entry = self._entry_by_id(lease.proxy_id)
        if entry is None:
            return False
        timestamp_ms = now_ms()
        safe_reason = sanitize_proxy_error(reason, entry)

        def mutate(runtime: ProxyRuntimeRecord) -> ProxyRuntimeRecord:
            return _failed_runtime(
                entry,
                runtime,
                get_config(),
                reason=safe_reason,
                timestamp_ms=timestamp_ms,
                count_request=True,
                dead=dead,
            )

        updated = await self._mutate_lease_runtime(
            lease,
            mutate,
            clear_bindings=True,
        )
        if updated:
            logger.warning(
                "managed proxy marked failed: proxy_id={} mode={} reason={}",
                entry.id,
                entry.inferred_mode().value,
                safe_reason,
            )
        return updated

    async def _record_challenge(self, lease: ProxyLease, reason: str) -> None:
        """记录 Challenge，达到阈值后进入冷却并解绑。"""
        threshold = max(
            2,
            get_config().get_int(
                "proxy.pool.challenge_failure_threshold",
                2,
            ),
        )
        for _ in range(5):
            runtime = await self._state_repo.get_runtime(lease.proxy_id)
            if not _runtime_matches_lease(runtime, lease):
                return
            assert runtime is not None
            count = runtime.challenge_count + 1
            if count >= threshold:
                await self.mark_failure(lease, reason or "challenge_threshold")
                return
            stored = await self._state_repo.compare_and_swap_runtime(
                runtime,
                replace(
                    runtime,
                    challenge_count=count,
                    last_error=sanitize_proxy_error(reason, self._entry_by_id(lease.proxy_id)),
                    updated_at=now_ms(),
                ),
            )
            if stored is not None:
                return

    async def record_health_result(
        self,
        proxy_id: str,
        *,
        generation: int,
        outcome: ProxyProbeOutcome,
        message: str,
        latency_ms: int,
        status_code: int | None = None,
    ) -> bool:
        """按探测分类更新共享健康状态。"""
        entry = self._entry_by_id(proxy_id)
        if entry is None or entry.generation != generation:
            return False
        safe_message = sanitize_proxy_error(message, entry)
        for _ in range(5):
            runtime = await self._state_repo.get_runtime(proxy_id)
            if runtime is None or runtime.generation != generation:
                return False
            timestamp_ms = now_ms()
            common = {
                "checking": False,
                "last_checked_at": timestamp_ms,
                "last_latency_ms": max(0, int(latency_ms)),
                "last_probe_outcome": outcome.value,
                "updated_at": timestamp_ms,
            }
            clear_bindings = False
            if outcome == ProxyProbeOutcome.HEALTHY:
                recovery_blocked = (
                    runtime.health_state == ProxyHealthState.DEAD
                    or (
                        runtime.health_state
                        == ProxyHealthState.COOLING_DOWN
                        and runtime.next_retry_at is not None
                        and runtime.next_retry_at > timestamp_ms
                    )
                )
                if recovery_blocked:
                    # 永久错误或尚未到期的冷却不能被探测结果提前解除。
                    updated = replace(
                        runtime,
                        **common,
                        health_success_count=runtime.health_success_count + 1,
                    )
                else:
                    was_unhealthy = (
                        runtime.health_state != ProxyHealthState.HEALTHY
                    )
                    updated = replace(
                        runtime,
                        **common,
                        health_state=ProxyHealthState.HEALTHY,
                        runtime_epoch=(
                            runtime.runtime_epoch + 1
                            if was_unhealthy
                            else runtime.runtime_epoch
                        ),
                        last_error="",
                        last_failure_at=None,
                        next_retry_at=None,
                        consecutive_failures=0,
                        challenge_count=0,
                        health_success_count=runtime.health_success_count + 1,
                    )
            elif outcome == ProxyProbeOutcome.UNHEALTHY:
                updated = replace(
                    _failed_runtime(
                        entry,
                        runtime,
                        get_config(),
                        reason=safe_message or "health_check_failed",
                        timestamp_ms=timestamp_ms,
                        count_request=False,
                        dead=status_code == 407,
                    ),
                    **common,
                    health_failure_count=runtime.health_failure_count + 1,
                )
                clear_bindings = True
            else:
                # 不确定响应只记录观测，不清理既有冷却或错误。
                updated = replace(
                    runtime,
                    **common,
                    health_failure_count=runtime.health_failure_count + 1,
                )
            stored = await self._state_repo.compare_and_swap_runtime(
                runtime,
                updated,
                clear_bindings=clear_bindings,
            )
            if stored is not None:
                return True
        return False

    async def mark_checking(self, proxy_id: str, generation: int) -> bool:
        """把节点标记为正在检测而不覆盖原健康状态。"""
        for _ in range(5):
            runtime = await self._state_repo.get_runtime(proxy_id)
            if runtime is None or runtime.generation != generation:
                return False
            stored = await self._state_repo.compare_and_swap_runtime(
                runtime,
                replace(runtime, checking=True, updated_at=now_ms()),
            )
            if stored is not None:
                return True
        return False

    async def is_probe_eligible(self, proxy_id: str, generation: int) -> bool:
        """判断节点是否可执行本轮主动探测。"""
        runtime = await self._state_repo.get_runtime(proxy_id)
        return bool(
            runtime
            and runtime.generation == generation
            and runtime.health_state
            not in {
                ProxyHealthState.COOLING_DOWN,
                ProxyHealthState.DEAD,
            }
        )

    async def load(self) -> None:
        """从热配置加载条目并同步共享运行态身份。"""
        cfg = get_config()
        raw_entries = cfg.get("proxy.pool.entries", []) or []
        if self._config_entries_ref is raw_entries:
            return
        entries = _deduplicate_entries([_coerce_entry(item) for item in raw_entries])
        await self._state_repo.sync_entries(
            [ProxyStateSeed(entry.id, entry.generation) for entry in entries],
            timestamp_ms=now_ms(),
        )
        self._entries = entries
        self._config_entries_ref = raw_entries
        logger.info("managed proxy pool loaded: count={}", len(entries))

    async def snapshot(self) -> dict[str, Any]:
        """返回后台展示用代理池、路由和活动任务快照。"""
        await self.load()
        await self.expire_cooldowns()
        cfg = get_config()
        runtimes = await self._state_repo.runtime_snapshot()
        binding_counts = await self._state_repo.binding_counts()
        active_job = await self._state_repo.get_active_health_job()
        rows = [
            self._snapshot_entry(
                entry,
                runtimes.get(entry.id),
                binding_counts.get(entry.id, 0),
            )
            for entry in self._entries
        ]
        status_counts: dict[str, int] = {}
        for row in rows:
            status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
        status_counts["checking"] = sum(row["checking"] for row in rows)
        return {
            "mode": cfg.get_str("proxy.egress.mode", "direct"),
            "items": rows,
            "binding_count": sum(binding_counts.values()),
            "status_counts": status_counts,
            "active_job": _job_dict(active_job) if active_job else None,
            "route": _route_summary(cfg, rows),
        }

    async def replace_entries(self, entries: list[ProxyEntry]) -> None:
        """整体保存代理池条目并热加载。"""
        unique_entries = _deduplicate_entries(entries)
        await config.update(
            {
                "proxy": {
                    "pool": {
                        "entries": [
                            entry.public_dict(include_secret=True)
                            for entry in unique_entries
                        ]
                    }
                }
            }
        )
        await config.load()
        self._config_entries_ref = None
        await self.load()

    async def add_entries(
        self,
        entries: list[ProxyEntry],
    ) -> ProxyUpsertResult:
        """按代理端点身份批量新增或更新条目。"""
        current = await self.entries(include_secret=True)
        positions = {_entry_identity(entry): idx for idx, entry in enumerate(current)}
        affected: list[ProxyEntry] = []
        added = 0
        updated = 0
        unchanged = 0
        for incoming in entries:
            identity = _entry_identity(incoming)
            index = positions.get(identity)
            if index is None:
                positions[identity] = len(current)
                current.append(incoming)
                affected.append(incoming)
                added += 1
                continue
            existing = current[index]
            data = incoming.public_dict(include_secret=True)
            data["id"] = existing.id
            data["generation"] = existing.generation
            if not incoming.password and existing.password:
                data["password"] = existing.password
            candidate = _coerce_entry(data)
            if candidate.public_dict(
                include_secret=True
            ) == existing.public_dict(include_secret=True):
                affected.append(existing)
                unchanged += 1
                continue
            candidate.generation = existing.generation + 1
            current[index] = candidate
            affected.append(candidate)
            updated += 1
        if added or updated:
            await self.replace_entries(current)
        return ProxyUpsertResult(
            entries=tuple(affected),
            added=added,
            updated=updated,
            unchanged=unchanged,
        )

    async def entries(self, *, include_secret: bool = False) -> list[ProxyEntry]:
        """返回当前配置中的去重代理条目。"""
        _ = include_secret
        cfg = get_config()
        return _deduplicate_entries(
            [
                _coerce_entry(item)
                for item in (cfg.get("proxy.pool.entries", []) or [])
            ]
        )

    async def selected_entries(
        self,
        proxy_ids: list[str],
    ) -> list[ProxyEntry]:
        """按请求顺序返回已校验且去重的代理条目。"""
        entries = await self.entries(include_secret=True)
        return _select_entries(entries, proxy_ids)

    async def update_entry(
        self,
        proxy_id: str,
        patch: dict[str, Any],
    ) -> ProxyEntry:
        """更新指定代理条目并清除旧绑定。"""
        entries = await self.entries(include_secret=True)
        for index, entry in enumerate(entries):
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
            if any(
                item.id != proxy_id
                and _entry_identity(item) == _entry_identity(updated)
                for item in entries
            ):
                raise ValueError("proxy endpoint already exists")
            entries[index] = updated
            await self.replace_entries(entries)
            await self._state_repo.clear_bindings(proxy_id)
            return updated
        raise KeyError(proxy_id)

    async def remove_entry(self, proxy_id: str) -> None:
        """删除指定代理条目和共享运行态。"""
        entries = await self.entries(include_secret=True)
        next_entries = [entry for entry in entries if entry.id != proxy_id]
        if len(next_entries) == len(entries):
            raise KeyError(proxy_id)
        await self.replace_entries(next_entries)
        await self._state_repo.clear_bindings(proxy_id)

    async def remove_entries(self, proxy_ids: list[str]) -> int:
        """一次持久化删除多个代理条目。"""
        entries = await self.entries(include_secret=True)
        selected = _select_entries(entries, proxy_ids)
        selected_ids = {entry.id for entry in selected}
        await self.replace_entries(
            [entry for entry in entries if entry.id not in selected_ids]
        )
        return len(selected)

    async def set_enabled(
        self,
        proxy_id: str,
        enabled: bool,
    ) -> ProxyEntry:
        """启用或禁用指定代理。"""
        return await self.update_entry(proxy_id, {"enabled": enabled})

    async def set_entries_enabled(
        self,
        proxy_ids: list[str],
        enabled: bool,
    ) -> ProxyBatchUpdateResult:
        """一次持久化启用或禁用多个代理条目。"""
        entries = await self.entries(include_secret=True)
        selected = _select_entries(entries, proxy_ids)
        selected_ids = {entry.id for entry in selected}
        changed_entries: list[ProxyEntry] = []
        next_entries: list[ProxyEntry] = []
        for entry in entries:
            if entry.id not in selected_ids or entry.enabled == enabled:
                next_entries.append(entry)
                continue
            data = entry.public_dict(include_secret=True)
            data["enabled"] = enabled
            data["generation"] = entry.generation + 1
            updated = _coerce_entry(data)
            next_entries.append(updated)
            changed_entries.append(updated)
        if changed_entries:
            await self.replace_entries(next_entries)
        return ProxyBatchUpdateResult(
            entries=tuple(changed_entries),
            changed=len(changed_entries),
            unchanged=len(selected) - len(changed_entries),
        )

    async def reset_entry(self, proxy_id: str) -> bool:
        """把指定节点重置为 unknown 并清理绑定。"""
        for _ in range(5):
            runtime = await self._state_repo.get_runtime(proxy_id)
            if runtime is None:
                return False
            stored = await self._state_repo.compare_and_swap_runtime(
                runtime,
                replace(
                    runtime,
                    health_state=ProxyHealthState.UNKNOWN,
                    checking=False,
                    runtime_epoch=runtime.runtime_epoch + 1,
                    last_error="",
                    last_failure_at=None,
                    next_retry_at=None,
                    consecutive_failures=0,
                    challenge_count=0,
                    updated_at=now_ms(),
                ),
                clear_bindings=True,
            )
            if stored is not None:
                return True
        return False

    async def reset_entries(
        self,
        proxy_ids: list[str],
    ) -> tuple[ProxyEntry, ...]:
        """按单节点语义重置多个代理并返回成功条目。"""
        selected = await self.selected_entries(proxy_ids)
        reset: list[ProxyEntry] = []
        for entry in selected:
            if await self.reset_entry(entry.id):
                reset.append(entry)
        return tuple(reset)

    async def clear_bindings(self) -> int:
        """清空所有共享 sticky 绑定。"""
        return await self._state_repo.clear_bindings()

    async def clear_entry_bindings(self, proxy_ids: list[str]) -> int:
        """仅清空指定代理关联的共享 sticky 绑定。"""
        selected = await self.selected_entries(proxy_ids)
        cleared = 0
        for entry in selected:
            cleared += await self._state_repo.clear_bindings(entry.id)
        return cleared

    async def create_health_job(
        self,
        kind: ProxyHealthJobKind,
        entries: list[ProxyEntry] | None = None,
    ) -> ProxyHealthJob:
        """为给定节点创建或复用异步健康任务。"""
        if kind == ProxyHealthJobKind.PROVIDER_MANUAL:
            identities = [("__effective_egress__", 0)]
        else:
            selected = (
                entries
                if entries is not None
                else [entry for entry in self._entries if entry.enabled]
            )
            identities = sorted(
                (entry.id, entry.generation) for entry in selected
            )
        digest = hashlib.sha256(repr(identities).encode()).hexdigest()[:20]
        if kind == ProxyHealthJobKind.PROVIDER_MANUAL:
            scope = "provider"
        elif kind in {
            ProxyHealthJobKind.BOOTSTRAP,
            ProxyHealthJobKind.PERIODIC,
            ProxyHealthJobKind.MANUAL_ALL,
        }:
            scope = "all"
        else:
            scope = digest
        return await self._state_repo.create_health_job(
            kind=kind,
            # 同一节点范围只保留一个活动任务，避免 bootstrap、周期和手工检测重叠。
            dedupe_key=f"scope:{scope}",
            items=[ProxyStateSeed(*identity) for identity in identities],
            timestamp_ms=now_ms(),
        )

    async def get_health_job(self, job_id: str) -> ProxyHealthJob | None:
        """返回指定健康任务。"""
        return await self._state_repo.get_health_job(job_id)

    async def cleanup_shared_state(self) -> tuple[int, int]:
        """清理闲置绑定和过期健康任务。"""
        cfg = get_config()
        timestamp_ms = now_ms()
        idle_sec = max(
            60,
            cfg.get_int("proxy.pool.binding_idle_ttl_sec", 604800),
        )
        bindings = await self._state_repo.cleanup_bindings(
            cutoff_ms=timestamp_ms - idle_sec * 1000
        )
        jobs = await self._state_repo.prune_health_jobs(
            cutoff_ms=timestamp_ms - 86400 * 1000
        )
        return bindings, jobs

    async def _assign(self, account_key: str) -> _AssignedManagedProxy | None:
        """通过共享仓储原子复用或创建账号绑定。"""
        await self.expire_cooldowns()
        timestamp_ms = now_ms()
        entries = {entry.id: entry for entry in self._entries if entry.enabled}
        assignment = await self._state_repo.acquire_binding(
            account_key,
            [
                ProxyBindingCandidate(entry.id, entry.generation)
                for entry in entries.values()
            ],
            timestamp_ms=timestamp_ms,
        )
        if assignment is None:
            return None
        entry = entries.get(assignment.binding.proxy_id)
        if entry is None or entry.generation != assignment.binding.generation:
            await self._state_repo.clear_bindings(assignment.binding.proxy_id)
            return None
        return _AssignedManagedProxy(
            entry=entry,
            proxy_url=render_proxy_url(entry, timestamp_ms),
            runtime=assignment.runtime,
            account_key=account_key,
        )

    async def expire_cooldowns(self) -> int:
        """把到期冷却节点转为 unknown，等待下一次探测重新放行。"""
        timestamp_ms = now_ms()
        changed = 0
        for runtime in (await self._state_repo.runtime_snapshot()).values():
            if (
                runtime.health_state != ProxyHealthState.COOLING_DOWN
                or runtime.next_retry_at is None
                or runtime.next_retry_at > timestamp_ms
            ):
                continue
            stored = await self._state_repo.compare_and_swap_runtime(
                runtime,
                replace(
                    runtime,
                    health_state=ProxyHealthState.UNKNOWN,
                    checking=False,
                    next_retry_at=None,
                    updated_at=timestamp_ms,
                ),
                clear_bindings=True,
            )
            changed += int(stored is not None)
        return changed

    async def _mutate_lease_runtime(
        self,
        lease: ProxyLease,
        mutate: Callable[[ProxyRuntimeRecord], ProxyRuntimeRecord],
        *,
        clear_bindings: bool = False,
    ) -> bool:
        """按租约的 generation 和 epoch 条件更新共享运行态。"""
        for _ in range(5):
            runtime = await self._state_repo.get_runtime(lease.proxy_id)
            if not _runtime_matches_lease(runtime, lease):
                return False
            assert runtime is not None
            stored = await self._state_repo.compare_and_swap_runtime(
                runtime,
                mutate(runtime),
                clear_bindings=clear_bindings,
            )
            if stored is not None:
                return True
        return False

    def _entry_by_id(self, proxy_id: str) -> ProxyEntry | None:
        """按稳定 ID 查找代理条目。"""
        return next((entry for entry in self._entries if entry.id == proxy_id), None)

    def _snapshot_entry(
        self,
        entry: ProxyEntry,
        runtime: ProxyRuntimeRecord | None,
        bound_count: int,
    ) -> dict[str, Any]:
        """构建单个代理的后台快照。"""
        runtime = runtime or ProxyRuntimeRecord(
            proxy_id=entry.id,
            generation=entry.generation,
        )
        status = (
            ProxyStatus.DISABLED.value
            if not entry.enabled
            else runtime.health_state.value
        )
        return {
            "id": entry.id,
            "url": _display_proxy_url(entry),
            "raw_url": entry.url,
            "username": entry.username,
            "has_password": bool(entry.password),
            "mode": entry.inferred_mode().value,
            "enabled": entry.enabled,
            "generation": entry.generation,
            "status": status,
            "checking": runtime.checking,
            "runtime_epoch": runtime.runtime_epoch,
            "last_error": runtime.last_error,
            "last_failure_at": runtime.last_failure_at,
            "next_retry_at": runtime.next_retry_at,
            "consecutive_failures": runtime.consecutive_failures,
            "success_count": runtime.success_count,
            "failure_count": runtime.failure_count,
            "challenge_count": runtime.challenge_count,
            "health_success_count": runtime.health_success_count,
            "health_failure_count": runtime.health_failure_count,
            "last_checked_at": runtime.last_checked_at,
            "last_latency_ms": runtime.last_latency_ms,
            "last_probe_outcome": runtime.last_probe_outcome,
            "bound_account_count": bound_count,
        }


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
        return urlunsplit(
            (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
        )
    except Exception:
        return "<invalid>"


def sanitize_proxy_error(message: str, entry: ProxyEntry | None = None) -> str:
    """清除错误文本中的代理密码和 URL 内嵌凭据。"""
    text = str(message or "")[:1000]
    text = re.sub(
        r"(?i)\b(https?|socks4a?|socks5h?)://([^/@\s:]+):([^@\s]+)@",
        r"\1://\2:***@",
        text,
    )
    if entry is not None and entry.password:
        text = text.replace(entry.password, "***")
    return text[:300]


def parse_proxy_line(line: str) -> ProxyEntry:
    """解析后台批量导入的一行代理配置。"""
    stripped = line.strip()
    if not stripped:
        raise ValueError("代理行为空")
    return _coerce_entry({"url": stripped})


def render_proxy_url(entry: ProxyEntry, timestamp_ms: int) -> str:
    """渲染本次请求真实使用的代理 URL。"""
    mode = entry.inferred_mode()

    def render(value: str) -> str:
        """按动态模板模式替换时间占位符。"""
        if mode == ProxyEntryMode.DYNAMIC_TEMPLATE:
            return value.replace(TIME_PLACEHOLDER, str(timestamp_ms))
        return value

    raw_url = render(entry.url)
    if not entry.username:
        return raw_url
    parts = urlsplit(raw_url)
    username = render(entry.username)
    password = render(entry.password)
    encoded_username = quote(username, safe="%")
    encoded_password = quote(password, safe="%")
    auth = (
        encoded_username
        if not encoded_password
        else f"{encoded_username}:{encoded_password}"
    )
    host = _proxy_hostport(parts)
    return urlunsplit(
        (parts.scheme, f"{auth}@{host}", parts.path, parts.query, parts.fragment)
    )


def _failed_runtime(
    entry: ProxyEntry,
    runtime: ProxyRuntimeRecord,
    cfg: Any,
    *,
    reason: str,
    timestamp_ms: int,
    count_request: bool,
    dead: bool,
) -> ProxyRuntimeRecord:
    """构建硬失败或健康失败后的共享运行态。"""
    failures = runtime.consecutive_failures + 1
    next_retry_at: int | None
    state = (
        ProxyHealthState.DEAD
        if dead
        else ProxyHealthState.COOLING_DOWN
    )
    if dead:
        next_retry_at = None
    elif entry.inferred_mode() == ProxyEntryMode.DYNAMIC_TEMPLATE:
        base = max(1, cfg.get_int("proxy.pool.dynamic_retry_base_sec", 60))
        max_sec = max(
            base,
            cfg.get_int("proxy.pool.dynamic_retry_max_sec", 600),
        )
        factor = max(
            1.0,
            cfg.get_float("proxy.pool.dynamic_backoff_factor", 2.0),
        )
        delay = min(max_sec, int(base * (factor ** max(0, failures - 1))))
        next_retry_at = timestamp_ms + delay * 1000
    else:
        delay = max(
            1,
            cfg.get_int("proxy.pool.static_cooldown_sec", 300),
        )
        next_retry_at = timestamp_ms + delay * 1000
    return replace(
        runtime,
        health_state=state,
        checking=False,
        runtime_epoch=runtime.runtime_epoch + 1,
        last_error=sanitize_proxy_error(reason or "proxy_failure", entry),
        last_failure_at=timestamp_ms,
        next_retry_at=next_retry_at,
        consecutive_failures=failures,
        failure_count=runtime.failure_count + (1 if count_request else 0),
        challenge_count=0,
        updated_at=timestamp_ms,
    )


def _runtime_matches_lease(
    runtime: ProxyRuntimeRecord | None,
    lease: ProxyLease,
) -> bool:
    """判断运行态是否仍对应当前请求租约。"""
    return bool(
        runtime
        and runtime.generation == lease.generation
        and runtime.runtime_epoch == lease.runtime_epoch
    )


def _is_proxy_failure(result: ProxyFeedback) -> bool:
    """判断反馈是否应计为代理硬失败。"""
    return bool(
        result.kind == ProxyFeedbackKind.TRANSPORT_ERROR
        or result.status_code == 407
    )


def _display_proxy_url(entry: ProxyEntry) -> str:
    """返回包含脱敏认证信息的代理展示 URL。"""
    if not entry.username:
        return mask_proxy_url(entry.url)
    try:
        parts = urlsplit(entry.url)
        host = parts.netloc
        userinfo = f"{entry.username}:***" if entry.password else entry.username
        return urlunsplit(
            (
                parts.scheme,
                f"{userinfo}@{host}",
                parts.path,
                parts.query,
                parts.fragment,
            )
        )
    except Exception:
        return mask_proxy_url(entry.url)


def _proxy_hostport(parts: Any) -> str:
    """按 URL 语法重建支持 IPv6 的 host:port。"""
    hostname = parts.hostname or ""
    host = f"[{hostname}]" if ":" in hostname else hostname
    return f"{host}:{parts.port}" if parts.port else host


def _coerce_entry(item: Any) -> ProxyEntry:
    """把配置项转换为 ProxyEntry。"""
    if isinstance(item, ProxyEntry):
        entry = item
    elif isinstance(item, str):
        entry = ProxyEntry(id=_stable_entry_id({"url": item}), url=item)
    elif isinstance(item, dict):
        data = dict(item)
        if not str(data.get("id") or "").strip():
            data["id"] = _stable_entry_id(data)
        entry = ProxyEntry.model_validate(data)
    else:
        raise ValueError("托管代理节点格式有误")
    if entry.mode is None:
        entry.mode = entry.inferred_mode()
    return entry


def _entry_identity(
    entry: ProxyEntry,
) -> tuple[str, str, int | None, str, str, str]:
    """生成忽略密码和展示 ID 的代理端点身份。"""
    parts = urlsplit(entry.url.replace(TIME_PLACEHOLDER, "0"))
    default_port = {"http": 80, "https": 443}.get(parts.scheme.lower())
    return (
        parts.scheme.lower(),
        (parts.hostname or "").lower(),
        parts.port or default_port,
        parts.path.rstrip("/"),
        parts.query,
        entry.username,
    )


def _deduplicate_entries(entries: list[ProxyEntry]) -> list[ProxyEntry]:
    """按代理端点身份去重，后出现的认证配置覆盖先前配置。"""
    unique: list[ProxyEntry] = []
    positions: dict[tuple[str, str, int | None, str, str, str], int] = {}
    for entry in entries:
        identity = _entry_identity(entry)
        index = positions.get(identity)
        if index is None:
            positions[identity] = len(unique)
            unique.append(entry)
            continue
        existing = unique[index]
        data = entry.public_dict(include_secret=True)
        data["id"] = existing.id
        data["generation"] = max(existing.generation, entry.generation)
        if not entry.password and existing.password:
            data["password"] = existing.password
        unique[index] = _coerce_entry(data)
    return unique


def _stable_entry_id(data: dict[str, Any]) -> str:
    """为手写配置生成稳定代理 ID。"""
    raw = "|".join(
        str(data.get(key, "")) for key in ("url", "username", "password")
    )
    return "cp_" + hashlib.sha256(raw.encode("utf-8", "ignore")).hexdigest()[:24]


def _select_entries(
    entries: list[ProxyEntry],
    proxy_ids: list[str],
) -> list[ProxyEntry]:
    """校验代理 ID 全部存在，并按首次出现顺序返回条目。"""
    unique_ids = list(dict.fromkeys(str(proxy_id).strip() for proxy_id in proxy_ids))
    by_id = {entry.id: entry for entry in entries}
    missing = [proxy_id for proxy_id in unique_ids if not proxy_id or proxy_id not in by_id]
    if missing:
        raise KeyError(tuple(missing))
    return [by_id[proxy_id] for proxy_id in unique_ids]


def _job_dict(job: ProxyHealthJob) -> dict[str, Any]:
    """把共享健康任务转换为管理 API 结构。"""
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


def _route_summary(cfg: Any, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """返回不包含代理凭据的有效出口摘要。"""
    mode = cfg.get_str("proxy.egress.mode", "direct")
    healthy = sum(row["enabled"] and row["status"] == "healthy" for row in rows)
    resin_configured = bool(cfg.get_str("proxy.resin.url_template", "").strip())
    if mode == "direct":
        summary = "统一出口：直连"
        fail_closed = False
        available = True
    elif mode == "resin":
        summary = "统一出口：Resin" if resin_configured else "Resin 配置缺失 → 失败关闭"
        fail_closed = True
        available = resin_configured
    elif healthy:
        summary = f"统一出口：托管代理池（{healthy} 个健康节点）"
        fail_closed = True
        available = True
    else:
        summary = "托管代理池无健康节点 → 失败关闭"
        fail_closed = True
        available = False
    return {
        "summary": summary,
        "provider": mode,
        "healthy_count": healthy,
        "resin_configured": resin_configured,
        "fail_closed": fail_closed,
        "available": available,
    }


_managed_proxy_pool: ManagedProxyPool | None = None


async def get_managed_proxy_pool() -> ManagedProxyPool:
    """返回进程内托管代理池门面。"""
    global _managed_proxy_pool
    # 首次同步共享运行态前完成旧配置迁移，保留节点 ID、generation 与绑定。
    from .migration import ensure_proxy_config_migrated

    await ensure_proxy_config_migrated()
    if _managed_proxy_pool is None:
        _managed_proxy_pool = ManagedProxyPool(
            create_managed_proxy_state_repository()
        )
        await _managed_proxy_pool.initialize()
    else:
        await _managed_proxy_pool.load()
    return _managed_proxy_pool


async def reset_managed_proxy_pool_for_tests() -> None:
    """重置测试用代理池单例。"""
    global _managed_proxy_pool
    _managed_proxy_pool = ManagedProxyPool()
    await _managed_proxy_pool.initialize()


__all__ = [
    "ProxyBatchUpdateResult",
    "ProxyEntry",
    "ProxyEntryMode",
    "ManagedProxyPool",
    "ProxyStatus",
    "ProxyUpsertResult",
    "TIME_PLACEHOLDER",
    "account_key_for_token",
    "get_managed_proxy_pool",
    "mask_proxy_url",
    "parse_proxy_line",
    "render_proxy_url",
    "reset_managed_proxy_pool_for_tests",
    "sanitize_proxy_error",
]
