"""托管代理池共享运行态模型与仓储协议。"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Protocol, runtime_checkable


class ProxyHealthState(StrEnum):
    """托管代理节点的共享健康状态。"""

    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    COOLING_DOWN = "cooling_down"
    DEAD = "dead"


class ProxyProbeOutcome(StrEnum):
    """一次主动探测的业务判定。"""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    INCONCLUSIVE = "inconclusive"
    SKIPPED = "skipped"


class ProxyHealthJobKind(StrEnum):
    """健康检查任务的触发来源。"""

    BOOTSTRAP = "bootstrap"
    PERIODIC = "periodic"
    MANUAL_ALL = "manual_all"
    INCREMENTAL = "incremental"
    MANUAL_SINGLE = "manual_single"
    MANUAL_SELECTION = "manual_selection"
    PROVIDER_MANUAL = "provider_manual"


class ProxyHealthJobStatus(StrEnum):
    """健康检查后台任务状态。"""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True, slots=True)
class ProxyStateSeed:
    """配置条目同步到运行态时使用的非敏感身份。"""

    proxy_id: str
    generation: int


@dataclass(frozen=True, slots=True)
class ProxyRuntimeRecord:
    """一个代理节点的共享运行态。"""

    proxy_id: str
    generation: int
    health_state: ProxyHealthState = ProxyHealthState.UNKNOWN
    checking: bool = False
    runtime_epoch: int = 0
    last_error: str = ""
    last_failure_at: int | None = None
    next_retry_at: int | None = None
    consecutive_failures: int = 0
    success_count: int = 0
    failure_count: int = 0
    challenge_count: int = 0
    health_success_count: int = 0
    health_failure_count: int = 0
    last_checked_at: int | None = None
    last_latency_ms: int | None = None
    last_probe_outcome: str = ""
    version: int = 0
    updated_at: int = 0

    def is_schedulable(self, timestamp_ms: int) -> bool:
        """返回节点在指定时间是否可参与新绑定。"""
        return (
            self.health_state == ProxyHealthState.HEALTHY
            and (self.next_retry_at is None or timestamp_ms >= self.next_retry_at)
        )


@dataclass(frozen=True, slots=True)
class ProxyBindingCandidate:
    """一次账号绑定可选择的节点身份。"""

    proxy_id: str
    generation: int


@dataclass(frozen=True, slots=True)
class ProxyBinding:
    """共享账号 sticky 绑定。"""

    account_key: str
    proxy_id: str
    generation: int
    created_at: int
    last_used_at: int


@dataclass(frozen=True, slots=True)
class ProxyBindingAssignment:
    """原子绑定结果及其对应运行态版本。"""

    binding: ProxyBinding
    runtime: ProxyRuntimeRecord


@dataclass(frozen=True, slots=True)
class ProxyHealthJobItem:
    """健康任务捕获的节点配置版本。"""

    proxy_id: str
    generation: int
    completed: bool = False
    outcome: str = ""


@dataclass(frozen=True, slots=True)
class ProxyHealthJob:
    """可跨 Worker 恢复的健康检查任务。"""

    job_id: str
    kind: ProxyHealthJobKind
    dedupe_key: str
    status: ProxyHealthJobStatus
    total: int
    completed: int = 0
    healthy: int = 0
    unhealthy: int = 0
    inconclusive: int = 0
    skipped: int = 0
    created_at: int = 0
    started_at: int | None = None
    updated_at: int = 0
    finished_at: int | None = None
    lease_owner: str = ""
    lease_expires_at: int | None = None
    error: str = ""


@runtime_checkable
class ManagedProxyStateRepository(Protocol):
    """托管代理共享运行态的存储契约。"""

    async def initialize(self) -> None:
        """创建所需表、索引或 Redis 元数据。"""
        ...

    async def sync_entries(
        self,
        entries: list[ProxyStateSeed],
        *,
        timestamp_ms: int,
    ) -> None:
        """同步配置身份，新增或变更 generation 的节点重置为 unknown。"""
        ...

    async def runtime_snapshot(self) -> dict[str, ProxyRuntimeRecord]:
        """返回所有节点运行态。"""
        ...

    async def get_runtime(self, proxy_id: str) -> ProxyRuntimeRecord | None:
        """返回指定节点运行态。"""
        ...

    async def compare_and_swap_runtime(
        self,
        expected: ProxyRuntimeRecord,
        updated: ProxyRuntimeRecord,
        *,
        clear_bindings: bool = False,
    ) -> ProxyRuntimeRecord | None:
        """按 generation 和 version 条件更新运行态。"""
        ...

    async def acquire_binding(
        self,
        account_key: str,
        candidates: list[ProxyBindingCandidate],
        *,
        timestamp_ms: int,
    ) -> ProxyBindingAssignment | None:
        """原子复用或创建账号绑定。"""
        ...

    async def clear_bindings(self, proxy_id: str | None = None) -> int:
        """清除全部或指定节点的账号绑定。"""
        ...

    async def binding_counts(self) -> dict[str, int]:
        """返回各节点绑定数。"""
        ...

    async def cleanup_bindings(self, *, cutoff_ms: int) -> int:
        """清除超过闲置期限的绑定。"""
        ...

    async def create_health_job(
        self,
        *,
        kind: ProxyHealthJobKind,
        dedupe_key: str,
        items: list[ProxyStateSeed],
        timestamp_ms: int,
    ) -> ProxyHealthJob:
        """创建健康任务，存在同范围活动任务时返回已有任务。"""
        ...

    async def get_health_job(self, job_id: str) -> ProxyHealthJob | None:
        """返回指定健康任务。"""
        ...

    async def get_active_health_job(self) -> ProxyHealthJob | None:
        """返回最近一个未结束任务。"""
        ...

    async def claim_health_job(
        self,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> ProxyHealthJob | None:
        """认领排队中或租约已过期的任务。"""
        ...

    async def heartbeat_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> bool:
        """续期当前 Worker 持有的任务租约。"""
        ...

    async def pending_health_job_items(
        self,
        job_id: str,
    ) -> list[ProxyHealthJobItem]:
        """返回任务尚未完成的节点。"""
        ...

    async def complete_health_job_item(
        self,
        job_id: str,
        *,
        proxy_id: str,
        generation: int,
        outcome: ProxyProbeOutcome,
        timestamp_ms: int,
    ) -> bool:
        """幂等完成一个任务节点并累加任务计数。"""
        ...

    async def finish_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        error: str = "",
    ) -> ProxyHealthJob | None:
        """完成或标记失败一个健康任务。"""
        ...

    async def prune_health_jobs(self, *, cutoff_ms: int) -> int:
        """清理超过保留期限的已结束任务。"""
        ...

    async def close(self) -> None:
        """释放仓储连接。"""
        ...


@dataclass(slots=True)
class _InMemoryState:
    """内存仓储内部状态。"""

    runtimes: dict[str, ProxyRuntimeRecord] = field(default_factory=dict)
    bindings: dict[str, ProxyBinding] = field(default_factory=dict)
    jobs: dict[str, ProxyHealthJob] = field(default_factory=dict)
    job_items: dict[str, dict[str, ProxyHealthJobItem]] = field(
        default_factory=dict
    )


class InMemoryManagedProxyStateRepository:
    """测试和单进程构造使用的内存共享状态仓储。"""

    def __init__(self) -> None:
        self._state = _InMemoryState()
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """内存仓储无需初始化外部资源。"""

    async def sync_entries(
        self,
        entries: list[ProxyStateSeed],
        *,
        timestamp_ms: int,
    ) -> None:
        """同步配置身份并清理已删除节点。"""
        seeds = {entry.proxy_id: entry for entry in entries}
        async with self._lock:
            for proxy_id, seed in seeds.items():
                current = self._state.runtimes.get(proxy_id)
                if current is None or current.generation != seed.generation:
                    self._state.runtimes[proxy_id] = ProxyRuntimeRecord(
                        proxy_id=proxy_id,
                        generation=seed.generation,
                        runtime_epoch=(current.runtime_epoch + 1 if current else 0),
                        updated_at=timestamp_ms,
                    )
            removed = set(self._state.runtimes) - set(seeds)
            for proxy_id in removed:
                self._state.runtimes.pop(proxy_id, None)
            self._state.bindings = {
                key: binding
                for key, binding in self._state.bindings.items()
                if binding.proxy_id in seeds
                and seeds[binding.proxy_id].generation == binding.generation
            }

    async def runtime_snapshot(self) -> dict[str, ProxyRuntimeRecord]:
        """返回不可变运行态对象的字典副本。"""
        async with self._lock:
            return dict(self._state.runtimes)

    async def get_runtime(self, proxy_id: str) -> ProxyRuntimeRecord | None:
        """返回指定节点运行态。"""
        async with self._lock:
            return self._state.runtimes.get(proxy_id)

    async def compare_and_swap_runtime(
        self,
        expected: ProxyRuntimeRecord,
        updated: ProxyRuntimeRecord,
        *,
        clear_bindings: bool = False,
    ) -> ProxyRuntimeRecord | None:
        """按版本原子更新内存运行态。"""
        async with self._lock:
            current = self._state.runtimes.get(expected.proxy_id)
            if (
                current is None
                or current.generation != expected.generation
                or current.version != expected.version
            ):
                return None
            stored = replace(updated, version=expected.version + 1)
            self._state.runtimes[expected.proxy_id] = stored
            if clear_bindings:
                self._state.bindings = {
                    key: binding
                    for key, binding in self._state.bindings.items()
                    if binding.proxy_id != expected.proxy_id
                }
            return stored

    async def acquire_binding(
        self,
        account_key: str,
        candidates: list[ProxyBindingCandidate],
        *,
        timestamp_ms: int,
    ) -> ProxyBindingAssignment | None:
        """原子复用绑定或选择当前绑定数最少的健康节点。"""
        candidate_map = {item.proxy_id: item for item in candidates}
        async with self._lock:
            existing = self._state.bindings.get(account_key)
            if existing:
                candidate = candidate_map.get(existing.proxy_id)
                runtime = self._state.runtimes.get(existing.proxy_id)
                if (
                    candidate
                    and runtime
                    and existing.generation == candidate.generation
                    and runtime.generation == candidate.generation
                    and runtime.is_schedulable(timestamp_ms)
                ):
                    touched = replace(existing, last_used_at=timestamp_ms)
                    self._state.bindings[account_key] = touched
                    return ProxyBindingAssignment(touched, runtime)
                self._state.bindings.pop(account_key, None)

            eligible: list[tuple[ProxyBindingCandidate, ProxyRuntimeRecord]] = []
            for candidate in candidates:
                runtime = self._state.runtimes.get(candidate.proxy_id)
                if (
                    runtime
                    and runtime.generation == candidate.generation
                    and runtime.is_schedulable(timestamp_ms)
                ):
                    eligible.append((candidate, runtime))
            if not eligible:
                return None

            counts = {candidate.proxy_id: 0 for candidate, _ in eligible}
            for binding in self._state.bindings.values():
                if binding.proxy_id in counts:
                    counts[binding.proxy_id] += 1
            candidate, runtime = min(
                eligible,
                key=lambda item: (counts[item[0].proxy_id], item[0].proxy_id),
            )
            binding = ProxyBinding(
                account_key=account_key,
                proxy_id=candidate.proxy_id,
                generation=candidate.generation,
                created_at=timestamp_ms,
                last_used_at=timestamp_ms,
            )
            self._state.bindings[account_key] = binding
            return ProxyBindingAssignment(binding, runtime)

    async def clear_bindings(self, proxy_id: str | None = None) -> int:
        """清除全部或指定节点绑定。"""
        async with self._lock:
            keys = [
                key
                for key, binding in self._state.bindings.items()
                if proxy_id is None or binding.proxy_id == proxy_id
            ]
            for key in keys:
                self._state.bindings.pop(key, None)
            return len(keys)

    async def binding_counts(self) -> dict[str, int]:
        """统计内存绑定数量。"""
        async with self._lock:
            counts: dict[str, int] = {}
            for binding in self._state.bindings.values():
                counts[binding.proxy_id] = counts.get(binding.proxy_id, 0) + 1
            return counts

    async def cleanup_bindings(self, *, cutoff_ms: int) -> int:
        """清理闲置绑定。"""
        async with self._lock:
            keys = [
                key
                for key, binding in self._state.bindings.items()
                if binding.last_used_at < cutoff_ms
            ]
            for key in keys:
                self._state.bindings.pop(key, None)
            return len(keys)

    async def create_health_job(
        self,
        *,
        kind: ProxyHealthJobKind,
        dedupe_key: str,
        items: list[ProxyStateSeed],
        timestamp_ms: int,
    ) -> ProxyHealthJob:
        """创建或复用相同范围的活动健康任务。"""
        async with self._lock:
            for job in self._state.jobs.values():
                if (
                    job.dedupe_key == dedupe_key
                    and job.status
                    in {
                        ProxyHealthJobStatus.QUEUED,
                        ProxyHealthJobStatus.RUNNING,
                    }
                ):
                    return job
            unique = {(item.proxy_id, item.generation): item for item in items}
            if kind == ProxyHealthJobKind.BOOTSTRAP:
                # 新 bootstrap 在创建任务的同一临界区关闭旧健康租约，
                # 多 Worker 复用活动任务时不会重复抬升 runtime_epoch。
                reset_ids: set[str] = set()
                for proxy_id, generation in unique:
                    runtime = self._state.runtimes.get(proxy_id)
                    if (
                        runtime is None
                        or runtime.generation != generation
                        or runtime.health_state != ProxyHealthState.HEALTHY
                    ):
                        continue
                    self._state.runtimes[proxy_id] = replace(
                        runtime,
                        health_state=ProxyHealthState.UNKNOWN,
                        checking=False,
                        runtime_epoch=runtime.runtime_epoch + 1,
                        last_error="",
                        last_failure_at=None,
                        next_retry_at=None,
                        consecutive_failures=0,
                        challenge_count=0,
                        last_probe_outcome="",
                        version=runtime.version + 1,
                        updated_at=timestamp_ms,
                    )
                    reset_ids.add(proxy_id)
                if reset_ids:
                    self._state.bindings = {
                        key: binding
                        for key, binding in self._state.bindings.items()
                        if binding.proxy_id not in reset_ids
                    }
            job_id = uuid.uuid4().hex
            job = ProxyHealthJob(
                job_id=job_id,
                kind=kind,
                dedupe_key=dedupe_key,
                status=ProxyHealthJobStatus.QUEUED,
                total=len(unique),
                created_at=timestamp_ms,
                updated_at=timestamp_ms,
            )
            self._state.jobs[job_id] = job
            self._state.job_items[job_id] = {
                proxy_id: ProxyHealthJobItem(proxy_id, generation)
                for proxy_id, generation in unique
            }
            return job

    async def get_health_job(self, job_id: str) -> ProxyHealthJob | None:
        """返回指定内存任务。"""
        async with self._lock:
            return self._state.jobs.get(job_id)

    async def get_active_health_job(self) -> ProxyHealthJob | None:
        """返回最近创建的活动任务。"""
        async with self._lock:
            active = [
                job
                for job in self._state.jobs.values()
                if job.status
                in {
                    ProxyHealthJobStatus.QUEUED,
                    ProxyHealthJobStatus.RUNNING,
                }
            ]
            return max(active, key=lambda item: item.created_at, default=None)

    async def claim_health_job(
        self,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> ProxyHealthJob | None:
        """认领最早排队或租约已过期的任务。"""
        async with self._lock:
            if any(
                job.status == ProxyHealthJobStatus.RUNNING
                and (job.lease_expires_at or 0) > timestamp_ms
                for job in self._state.jobs.values()
            ):
                return None
            candidates = [
                job
                for job in self._state.jobs.values()
                if job.status == ProxyHealthJobStatus.QUEUED
                or (
                    job.status == ProxyHealthJobStatus.RUNNING
                    and (job.lease_expires_at or 0) <= timestamp_ms
                )
            ]
            if not candidates:
                return None
            job = min(candidates, key=lambda item: item.created_at)
            claimed = replace(
                job,
                status=ProxyHealthJobStatus.RUNNING,
                started_at=job.started_at or timestamp_ms,
                updated_at=timestamp_ms,
                lease_owner=owner,
                lease_expires_at=timestamp_ms + lease_ms,
            )
            self._state.jobs[job.job_id] = claimed
            return claimed

    async def heartbeat_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> bool:
        """续期当前内存任务租约。"""
        async with self._lock:
            job = self._state.jobs.get(job_id)
            if (
                job is None
                or job.status != ProxyHealthJobStatus.RUNNING
                or job.lease_owner != owner
            ):
                return False
            self._state.jobs[job_id] = replace(
                job,
                updated_at=timestamp_ms,
                lease_expires_at=timestamp_ms + lease_ms,
            )
            return True

    async def pending_health_job_items(
        self,
        job_id: str,
    ) -> list[ProxyHealthJobItem]:
        """返回内存任务未完成节点。"""
        async with self._lock:
            return [
                item
                for item in self._state.job_items.get(job_id, {}).values()
                if not item.completed
            ]

    async def complete_health_job_item(
        self,
        job_id: str,
        *,
        proxy_id: str,
        generation: int,
        outcome: ProxyProbeOutcome,
        timestamp_ms: int,
    ) -> bool:
        """幂等完成一个内存任务节点。"""
        async with self._lock:
            job = self._state.jobs.get(job_id)
            item = self._state.job_items.get(job_id, {}).get(proxy_id)
            if job is None or item is None or item.completed or item.generation != generation:
                return False
            self._state.job_items[job_id][proxy_id] = replace(
                item,
                completed=True,
                outcome=outcome.value,
            )
            field_name = {
                ProxyProbeOutcome.HEALTHY: "healthy",
                ProxyProbeOutcome.UNHEALTHY: "unhealthy",
                ProxyProbeOutcome.INCONCLUSIVE: "inconclusive",
                ProxyProbeOutcome.SKIPPED: "skipped",
            }[outcome]
            self._state.jobs[job_id] = replace(
                job,
                completed=job.completed + 1,
                updated_at=timestamp_ms,
                **{field_name: getattr(job, field_name) + 1},
            )
            return True

    async def finish_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        error: str = "",
    ) -> ProxyHealthJob | None:
        """结束当前 Worker 持有的任务。"""
        async with self._lock:
            job = self._state.jobs.get(job_id)
            if job is None or job.lease_owner != owner:
                return None
            status = (
                ProxyHealthJobStatus.FAILED
                if error
                else ProxyHealthJobStatus.COMPLETED
            )
            finished = replace(
                job,
                status=status,
                updated_at=timestamp_ms,
                finished_at=timestamp_ms,
                lease_owner="",
                lease_expires_at=None,
                error=error[:500],
            )
            self._state.jobs[job_id] = finished
            return finished

    async def prune_health_jobs(self, *, cutoff_ms: int) -> int:
        """清理超过保留期的内存任务。"""
        async with self._lock:
            ids = [
                job_id
                for job_id, job in self._state.jobs.items()
                if job.finished_at is not None and job.finished_at < cutoff_ms
            ]
            for job_id in ids:
                self._state.jobs.pop(job_id, None)
                self._state.job_items.pop(job_id, None)
            return len(ids)

    async def close(self) -> None:
        """内存仓储没有外部连接需要释放。"""


__all__ = [
    "ProxyBinding",
    "ProxyBindingAssignment",
    "ProxyBindingCandidate",
    "ProxyHealthJob",
    "ProxyHealthJobItem",
    "ProxyHealthJobKind",
    "ProxyHealthJobStatus",
    "ProxyHealthState",
    "ProxyProbeOutcome",
    "ProxyRuntimeRecord",
    "ManagedProxyStateRepository",
    "ProxyStateSeed",
    "InMemoryManagedProxyStateRepository",
]
