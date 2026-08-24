"""Console 代理池共享运行态模型与仓储协议。"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Protocol, runtime_checkable


class ConsoleProxyHealthState(StrEnum):
    """Console 代理节点的共享健康状态。"""

    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    COOLING_DOWN = "cooling_down"
    DEAD = "dead"


class ConsoleProxyProbeOutcome(StrEnum):
    """一次主动探测的业务判定。"""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    INCONCLUSIVE = "inconclusive"
    SKIPPED = "skipped"


class ConsoleProxyHealthJobKind(StrEnum):
    """健康检查任务的触发来源。"""

    BOOTSTRAP = "bootstrap"
    PERIODIC = "periodic"
    MANUAL_ALL = "manual_all"
    INCREMENTAL = "incremental"
    MANUAL_SINGLE = "manual_single"
    MANUAL_SELECTION = "manual_selection"


class ConsoleProxyHealthJobStatus(StrEnum):
    """健康检查后台任务状态。"""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True, slots=True)
class ConsoleProxyStateSeed:
    """配置条目同步到运行态时使用的非敏感身份。"""

    proxy_id: str
    generation: int


@dataclass(frozen=True, slots=True)
class ConsoleProxyRuntimeRecord:
    """一个代理节点的共享运行态。"""

    proxy_id: str
    generation: int
    health_state: ConsoleProxyHealthState = ConsoleProxyHealthState.UNKNOWN
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
            self.health_state == ConsoleProxyHealthState.HEALTHY
            and (self.next_retry_at is None or timestamp_ms >= self.next_retry_at)
        )


@dataclass(frozen=True, slots=True)
class ConsoleProxyBindingCandidate:
    """一次账号绑定可选择的节点身份。"""

    proxy_id: str
    generation: int


@dataclass(frozen=True, slots=True)
class ConsoleProxyBinding:
    """共享账号 sticky 绑定。"""

    account_key: str
    proxy_id: str
    generation: int
    created_at: int
    last_used_at: int


@dataclass(frozen=True, slots=True)
class ConsoleProxyBindingAssignment:
    """原子绑定结果及其对应运行态版本。"""

    binding: ConsoleProxyBinding
    runtime: ConsoleProxyRuntimeRecord


@dataclass(frozen=True, slots=True)
class ConsoleProxyHealthJobItem:
    """健康任务捕获的节点配置版本。"""

    proxy_id: str
    generation: int
    completed: bool = False
    outcome: str = ""


@dataclass(frozen=True, slots=True)
class ConsoleProxyHealthJob:
    """可跨 Worker 恢复的健康检查任务。"""

    job_id: str
    kind: ConsoleProxyHealthJobKind
    dedupe_key: str
    status: ConsoleProxyHealthJobStatus
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
class ConsoleProxyStateRepository(Protocol):
    """Console 代理共享运行态的存储契约。"""

    async def initialize(self) -> None:
        """创建所需表、索引或 Redis 元数据。"""
        ...

    async def sync_entries(
        self,
        entries: list[ConsoleProxyStateSeed],
        *,
        timestamp_ms: int,
    ) -> None:
        """同步配置身份，新增或变更 generation 的节点重置为 unknown。"""
        ...

    async def runtime_snapshot(self) -> dict[str, ConsoleProxyRuntimeRecord]:
        """返回所有节点运行态。"""
        ...

    async def get_runtime(self, proxy_id: str) -> ConsoleProxyRuntimeRecord | None:
        """返回指定节点运行态。"""
        ...

    async def compare_and_swap_runtime(
        self,
        expected: ConsoleProxyRuntimeRecord,
        updated: ConsoleProxyRuntimeRecord,
        *,
        clear_bindings: bool = False,
    ) -> ConsoleProxyRuntimeRecord | None:
        """按 generation 和 version 条件更新运行态。"""
        ...

    async def acquire_binding(
        self,
        account_key: str,
        candidates: list[ConsoleProxyBindingCandidate],
        *,
        timestamp_ms: int,
    ) -> ConsoleProxyBindingAssignment | None:
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
        kind: ConsoleProxyHealthJobKind,
        dedupe_key: str,
        items: list[ConsoleProxyStateSeed],
        timestamp_ms: int,
    ) -> ConsoleProxyHealthJob:
        """创建健康任务，存在同范围活动任务时返回已有任务。"""
        ...

    async def get_health_job(self, job_id: str) -> ConsoleProxyHealthJob | None:
        """返回指定健康任务。"""
        ...

    async def get_active_health_job(self) -> ConsoleProxyHealthJob | None:
        """返回最近一个未结束任务。"""
        ...

    async def claim_health_job(
        self,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> ConsoleProxyHealthJob | None:
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
    ) -> list[ConsoleProxyHealthJobItem]:
        """返回任务尚未完成的节点。"""
        ...

    async def complete_health_job_item(
        self,
        job_id: str,
        *,
        proxy_id: str,
        generation: int,
        outcome: ConsoleProxyProbeOutcome,
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
    ) -> ConsoleProxyHealthJob | None:
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

    runtimes: dict[str, ConsoleProxyRuntimeRecord] = field(default_factory=dict)
    bindings: dict[str, ConsoleProxyBinding] = field(default_factory=dict)
    jobs: dict[str, ConsoleProxyHealthJob] = field(default_factory=dict)
    job_items: dict[str, dict[str, ConsoleProxyHealthJobItem]] = field(
        default_factory=dict
    )


class InMemoryConsoleProxyStateRepository:
    """测试和单进程构造使用的内存共享状态仓储。"""

    def __init__(self) -> None:
        self._state = _InMemoryState()
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """内存仓储无需初始化外部资源。"""

    async def sync_entries(
        self,
        entries: list[ConsoleProxyStateSeed],
        *,
        timestamp_ms: int,
    ) -> None:
        """同步配置身份并清理已删除节点。"""
        seeds = {entry.proxy_id: entry for entry in entries}
        async with self._lock:
            for proxy_id, seed in seeds.items():
                current = self._state.runtimes.get(proxy_id)
                if current is None or current.generation != seed.generation:
                    self._state.runtimes[proxy_id] = ConsoleProxyRuntimeRecord(
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

    async def runtime_snapshot(self) -> dict[str, ConsoleProxyRuntimeRecord]:
        """返回不可变运行态对象的字典副本。"""
        async with self._lock:
            return dict(self._state.runtimes)

    async def get_runtime(self, proxy_id: str) -> ConsoleProxyRuntimeRecord | None:
        """返回指定节点运行态。"""
        async with self._lock:
            return self._state.runtimes.get(proxy_id)

    async def compare_and_swap_runtime(
        self,
        expected: ConsoleProxyRuntimeRecord,
        updated: ConsoleProxyRuntimeRecord,
        *,
        clear_bindings: bool = False,
    ) -> ConsoleProxyRuntimeRecord | None:
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
        candidates: list[ConsoleProxyBindingCandidate],
        *,
        timestamp_ms: int,
    ) -> ConsoleProxyBindingAssignment | None:
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
                    return ConsoleProxyBindingAssignment(touched, runtime)
                self._state.bindings.pop(account_key, None)

            eligible: list[tuple[ConsoleProxyBindingCandidate, ConsoleProxyRuntimeRecord]] = []
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
            binding = ConsoleProxyBinding(
                account_key=account_key,
                proxy_id=candidate.proxy_id,
                generation=candidate.generation,
                created_at=timestamp_ms,
                last_used_at=timestamp_ms,
            )
            self._state.bindings[account_key] = binding
            return ConsoleProxyBindingAssignment(binding, runtime)

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
        kind: ConsoleProxyHealthJobKind,
        dedupe_key: str,
        items: list[ConsoleProxyStateSeed],
        timestamp_ms: int,
    ) -> ConsoleProxyHealthJob:
        """创建或复用相同范围的活动健康任务。"""
        async with self._lock:
            for job in self._state.jobs.values():
                if (
                    job.dedupe_key == dedupe_key
                    and job.status
                    in {
                        ConsoleProxyHealthJobStatus.QUEUED,
                        ConsoleProxyHealthJobStatus.RUNNING,
                    }
                ):
                    return job
            unique = {(item.proxy_id, item.generation): item for item in items}
            if kind == ConsoleProxyHealthJobKind.BOOTSTRAP:
                # 新 bootstrap 在创建任务的同一临界区关闭旧健康租约，
                # 多 Worker 复用活动任务时不会重复抬升 runtime_epoch。
                reset_ids: set[str] = set()
                for proxy_id, generation in unique:
                    runtime = self._state.runtimes.get(proxy_id)
                    if (
                        runtime is None
                        or runtime.generation != generation
                        or runtime.health_state != ConsoleProxyHealthState.HEALTHY
                    ):
                        continue
                    self._state.runtimes[proxy_id] = replace(
                        runtime,
                        health_state=ConsoleProxyHealthState.UNKNOWN,
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
            job = ConsoleProxyHealthJob(
                job_id=job_id,
                kind=kind,
                dedupe_key=dedupe_key,
                status=ConsoleProxyHealthJobStatus.QUEUED,
                total=len(unique),
                created_at=timestamp_ms,
                updated_at=timestamp_ms,
            )
            self._state.jobs[job_id] = job
            self._state.job_items[job_id] = {
                proxy_id: ConsoleProxyHealthJobItem(proxy_id, generation)
                for proxy_id, generation in unique
            }
            return job

    async def get_health_job(self, job_id: str) -> ConsoleProxyHealthJob | None:
        """返回指定内存任务。"""
        async with self._lock:
            return self._state.jobs.get(job_id)

    async def get_active_health_job(self) -> ConsoleProxyHealthJob | None:
        """返回最近创建的活动任务。"""
        async with self._lock:
            active = [
                job
                for job in self._state.jobs.values()
                if job.status
                in {
                    ConsoleProxyHealthJobStatus.QUEUED,
                    ConsoleProxyHealthJobStatus.RUNNING,
                }
            ]
            return max(active, key=lambda item: item.created_at, default=None)

    async def claim_health_job(
        self,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> ConsoleProxyHealthJob | None:
        """认领最早排队或租约已过期的任务。"""
        async with self._lock:
            if any(
                job.status == ConsoleProxyHealthJobStatus.RUNNING
                and (job.lease_expires_at or 0) > timestamp_ms
                for job in self._state.jobs.values()
            ):
                return None
            candidates = [
                job
                for job in self._state.jobs.values()
                if job.status == ConsoleProxyHealthJobStatus.QUEUED
                or (
                    job.status == ConsoleProxyHealthJobStatus.RUNNING
                    and (job.lease_expires_at or 0) <= timestamp_ms
                )
            ]
            if not candidates:
                return None
            job = min(candidates, key=lambda item: item.created_at)
            claimed = replace(
                job,
                status=ConsoleProxyHealthJobStatus.RUNNING,
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
                or job.status != ConsoleProxyHealthJobStatus.RUNNING
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
    ) -> list[ConsoleProxyHealthJobItem]:
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
        outcome: ConsoleProxyProbeOutcome,
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
                ConsoleProxyProbeOutcome.HEALTHY: "healthy",
                ConsoleProxyProbeOutcome.UNHEALTHY: "unhealthy",
                ConsoleProxyProbeOutcome.INCONCLUSIVE: "inconclusive",
                ConsoleProxyProbeOutcome.SKIPPED: "skipped",
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
    ) -> ConsoleProxyHealthJob | None:
        """结束当前 Worker 持有的任务。"""
        async with self._lock:
            job = self._state.jobs.get(job_id)
            if job is None or job.lease_owner != owner:
                return None
            status = (
                ConsoleProxyHealthJobStatus.FAILED
                if error
                else ConsoleProxyHealthJobStatus.COMPLETED
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
    "ConsoleProxyBinding",
    "ConsoleProxyBindingAssignment",
    "ConsoleProxyBindingCandidate",
    "ConsoleProxyHealthJob",
    "ConsoleProxyHealthJobItem",
    "ConsoleProxyHealthJobKind",
    "ConsoleProxyHealthJobStatus",
    "ConsoleProxyHealthState",
    "ConsoleProxyProbeOutcome",
    "ConsoleProxyRuntimeRecord",
    "ConsoleProxyStateRepository",
    "ConsoleProxyStateSeed",
    "InMemoryConsoleProxyStateRepository",
]
