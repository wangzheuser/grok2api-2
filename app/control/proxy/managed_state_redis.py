"""Redis 托管代理共享状态仓储。"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, replace
from typing import Any

from redis.asyncio import Redis

from .managed_state import (
    ProxyBinding,
    ProxyBindingAssignment,
    ProxyBindingCandidate,
    ProxyHealthJob,
    ProxyHealthJobItem,
    ProxyHealthJobKind,
    ProxyHealthJobStatus,
    ProxyHealthState,
    ProxyProbeOutcome,
    ProxyRuntimeRecord,
    ProxyStateSeed,
)


_RUNTIMES = "{console_proxy}:runtimes"
_BINDINGS = "{console_proxy}:bindings"
_JOBS = "{console_proxy}:jobs"
_JOB_ITEMS = "{{console_proxy}}:job_items:{job_id}"

_CAS_RUNTIME_LUA = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local current = cjson.decode(raw)
if tonumber(current.generation) ~= tonumber(ARGV[2]) or
   tonumber(current.version) ~= tonumber(ARGV[3]) then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
if ARGV[5] == '1' then
  local bindings = redis.call('HGETALL', KEYS[2])
  for i = 1, #bindings, 2 do
    local binding = cjson.decode(bindings[i + 1])
    if binding.proxy_id == ARGV[1] then
      redis.call('HDEL', KEYS[2], bindings[i])
    end
  end
end
return 1
"""

_ACQUIRE_BINDING_LUA = """
local account_key = ARGV[1]
local timestamp_ms = tonumber(ARGV[2])
local candidates = cjson.decode(ARGV[3])
local candidate_map = {}
for _, candidate in ipairs(candidates) do
  candidate_map[candidate.proxy_id] = candidate
end

local function eligible(candidate, runtime)
  if not candidate or not runtime then return false end
  if tonumber(runtime.generation) ~= tonumber(candidate.generation) then return false end
  if runtime.health_state ~= 'healthy' then return false end
  if runtime.next_retry_at ~= nil and runtime.next_retry_at ~= cjson.null and
     tonumber(runtime.next_retry_at) > timestamp_ms then
    return false
  end
  return true
end

local existing_raw = redis.call('HGET', KEYS[2], account_key)
if existing_raw then
  local existing = cjson.decode(existing_raw)
  local candidate = candidate_map[existing.proxy_id]
  local runtime_raw = redis.call('HGET', KEYS[1], existing.proxy_id)
  local runtime = runtime_raw and cjson.decode(runtime_raw) or nil
  if candidate and tonumber(existing.generation) == tonumber(candidate.generation)
     and eligible(candidate, runtime) then
    existing.last_used_at = timestamp_ms
    redis.call('HSET', KEYS[2], account_key, cjson.encode(existing))
    return cjson.encode({binding = existing, runtime = runtime})
  end
  redis.call('HDEL', KEYS[2], account_key)
end

local counts = {}
for _, candidate in ipairs(candidates) do counts[candidate.proxy_id] = 0 end
local bindings = redis.call('HVALS', KEYS[2])
for _, raw_binding in ipairs(bindings) do
  local binding = cjson.decode(raw_binding)
  if counts[binding.proxy_id] ~= nil then
    counts[binding.proxy_id] = counts[binding.proxy_id] + 1
  end
end

local selected = nil
local selected_runtime = nil
local selected_count = nil
for _, candidate in ipairs(candidates) do
  local runtime_raw = redis.call('HGET', KEYS[1], candidate.proxy_id)
  local runtime = runtime_raw and cjson.decode(runtime_raw) or nil
  if eligible(candidate, runtime) then
    local count = counts[candidate.proxy_id] or 0
    if selected == nil or count < selected_count or
       (count == selected_count and candidate.proxy_id < selected.proxy_id) then
      selected = candidate
      selected_runtime = runtime
      selected_count = count
    end
  end
end
if selected == nil then return nil end

local binding = {
  account_key = account_key,
  proxy_id = selected.proxy_id,
  generation = tonumber(selected.generation),
  created_at = timestamp_ms,
  last_used_at = timestamp_ms
}
redis.call('HSET', KEYS[2], account_key, cjson.encode(binding))
return cjson.encode({binding = binding, runtime = selected_runtime})
"""


class RedisManagedProxyStateRepository:
    """使用 Redis Hash、Lua 和 WATCH 事务共享托管代理状态。"""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def initialize(self) -> None:
        """Redis 仓储按需创建 Key，无需预建结构。"""

    async def sync_entries(
        self,
        entries: list[ProxyStateSeed],
        *,
        timestamp_ms: int,
    ) -> None:
        """用 WATCH 原子同步配置节点与关联绑定。"""
        seeds = {entry.proxy_id: entry for entry in entries}
        for _ in range(8):
            async with self._redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(_RUNTIMES, _BINDINGS)
                    raw_runtimes = await pipe.hgetall(_RUNTIMES)
                    current = {
                        _text(key): _load_runtime(value)
                        for key, value in raw_runtimes.items()
                    }
                    runtime_mapping: dict[str, str] = {}
                    reset_ids: set[str] = set()
                    for proxy_id, seed in seeds.items():
                        runtime = current.get(proxy_id)
                        if runtime is None or runtime.generation != seed.generation:
                            replacement = ProxyRuntimeRecord(
                                proxy_id=proxy_id,
                                generation=seed.generation,
                                runtime_epoch=(
                                    runtime.runtime_epoch + 1 if runtime else 0
                                ),
                                updated_at=timestamp_ms,
                            )
                            runtime_mapping[proxy_id] = _dump_runtime(replacement)
                            if runtime is not None:
                                reset_ids.add(proxy_id)
                    removed_ids = set(current) - set(seeds)
                    reset_ids.update(removed_ids)
                    binding_keys: list[str] = []
                    if reset_ids:
                        raw_bindings = await pipe.hgetall(_BINDINGS)
                        binding_keys = [
                            _text(key)
                            for key, value in raw_bindings.items()
                            if _load_binding(value).proxy_id in reset_ids
                        ]

                    pipe.multi()
                    if runtime_mapping:
                        pipe.hset(_RUNTIMES, mapping=runtime_mapping)
                    if removed_ids:
                        pipe.hdel(_RUNTIMES, *removed_ids)
                    if binding_keys:
                        pipe.hdel(_BINDINGS, *binding_keys)
                    await pipe.execute()
                    return
                except Exception as exc:
                    if exc.__class__.__name__ != "WatchError":
                        raise
        raise RuntimeError("failed to sync managed proxy entries")

    async def runtime_snapshot(self) -> dict[str, ProxyRuntimeRecord]:
        """读取全部 Redis 运行态。"""
        raw = await self._redis.hgetall(_RUNTIMES)
        return {_text(key): _load_runtime(value) for key, value in raw.items()}

    async def get_runtime(self, proxy_id: str) -> ProxyRuntimeRecord | None:
        """读取指定 Redis 运行态。"""
        raw = await self._redis.hget(_RUNTIMES, proxy_id)
        return _load_runtime(raw) if raw else None

    async def compare_and_swap_runtime(
        self,
        expected: ProxyRuntimeRecord,
        updated: ProxyRuntimeRecord,
        *,
        clear_bindings: bool = False,
    ) -> ProxyRuntimeRecord | None:
        """使用 Lua 按 generation 和 version 原子更新运行态并解绑。"""
        stored = replace(updated, version=expected.version + 1)
        result = await self._redis.eval(
            _CAS_RUNTIME_LUA,
            2,
            _RUNTIMES,
            _BINDINGS,
            expected.proxy_id,
            expected.generation,
            expected.version,
            _dump_runtime(stored),
            int(clear_bindings),
        )
        return stored if int(result or 0) == 1 else None

    async def acquire_binding(
        self,
        account_key: str,
        candidates: list[ProxyBindingCandidate],
        *,
        timestamp_ms: int,
    ) -> ProxyBindingAssignment | None:
        """使用 Lua 原子复用或创建账号绑定。"""
        if not candidates:
            return None
        payload = json.dumps(
            [asdict(candidate) for candidate in candidates],
            separators=(",", ":"),
        )
        raw = await self._redis.eval(
            _ACQUIRE_BINDING_LUA,
            2,
            _RUNTIMES,
            _BINDINGS,
            account_key,
            timestamp_ms,
            payload,
        )
        if not raw:
            return None
        result = _load(raw)
        return ProxyBindingAssignment(
            ProxyBinding(**result["binding"]),
            _load_runtime(json.dumps(result["runtime"])),
        )

    async def clear_bindings(self, proxy_id: str | None = None) -> int:
        """清除全部或指定节点 Redis 绑定。"""
        if proxy_id is None:
            count = await self._redis.hlen(_BINDINGS)
            await self._redis.delete(_BINDINGS)
            return int(count)
        raw = await self._redis.hgetall(_BINDINGS)
        keys = [
            _text(key)
            for key, value in raw.items()
            if _load_binding(value).proxy_id == proxy_id
        ]
        return int(await self._redis.hdel(_BINDINGS, *keys)) if keys else 0

    async def binding_counts(self) -> dict[str, int]:
        """统计 Redis 绑定数量。"""
        counts: dict[str, int] = {}
        for raw in await self._redis.hvals(_BINDINGS):
            binding = _load_binding(raw)
            counts[binding.proxy_id] = counts.get(binding.proxy_id, 0) + 1
        return counts

    async def cleanup_bindings(self, *, cutoff_ms: int) -> int:
        """清理超过闲置期限的 Redis 绑定。"""
        raw = await self._redis.hgetall(_BINDINGS)
        keys = [
            _text(key)
            for key, value in raw.items()
            if _load_binding(value).last_used_at < cutoff_ms
        ]
        return int(await self._redis.hdel(_BINDINGS, *keys)) if keys else 0

    async def create_health_job(
        self,
        *,
        kind: ProxyHealthJobKind,
        dedupe_key: str,
        items: list[ProxyStateSeed],
        timestamp_ms: int,
    ) -> ProxyHealthJob:
        """使用 WATCH 创建或复用 Redis 活动任务。"""
        unique = {(item.proxy_id, item.generation): item for item in items}
        for _ in range(8):
            async with self._redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(_JOBS, _RUNTIMES, _BINDINGS)
                    raw_jobs = await pipe.hvals(_JOBS)
                    for raw in raw_jobs:
                        job = _load_job(raw)
                        if job.dedupe_key == dedupe_key and job.status in {
                            ProxyHealthJobStatus.QUEUED,
                            ProxyHealthJobStatus.RUNNING,
                        }:
                            await pipe.reset()
                            return job
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
                    item_mapping = {
                        proxy_id: _dump_job_item(
                            ProxyHealthJobItem(proxy_id, generation)
                        )
                        for proxy_id, generation in unique
                    }
                    runtime_mapping: dict[str, str] = {}
                    reset_ids: set[str] = set()
                    binding_keys: list[str] = []
                    if kind == ProxyHealthJobKind.BOOTSTRAP and unique:
                        identities = list(unique)
                        raw_runtimes = await pipe.hmget(
                            _RUNTIMES,
                            [proxy_id for proxy_id, _ in identities],
                        )
                        for (proxy_id, generation), raw_runtime in zip(
                            identities,
                            raw_runtimes,
                            strict=True,
                        ):
                            if not raw_runtime:
                                continue
                            runtime = _load_runtime(raw_runtime)
                            if (
                                runtime.generation != generation
                                or runtime.health_state
                                != ProxyHealthState.HEALTHY
                            ):
                                continue
                            runtime_mapping[proxy_id] = _dump_runtime(
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
                                    last_probe_outcome="",
                                    version=runtime.version + 1,
                                    updated_at=timestamp_ms,
                                )
                            )
                            reset_ids.add(proxy_id)
                        if reset_ids:
                            raw_bindings = await pipe.hgetall(_BINDINGS)
                            binding_keys = [
                                _text(key)
                                for key, raw_binding in raw_bindings.items()
                                if _load_binding(raw_binding).proxy_id in reset_ids
                            ]
                    pipe.multi()
                    if runtime_mapping:
                        pipe.hset(_RUNTIMES, mapping=runtime_mapping)
                    if binding_keys:
                        pipe.hdel(_BINDINGS, *binding_keys)
                    pipe.hset(_JOBS, job_id, _dump_job(job))
                    if item_mapping:
                        pipe.hset(_JOB_ITEMS.format(job_id=job_id), mapping=item_mapping)
                    await pipe.execute()
                    return job
                except Exception as exc:
                    if exc.__class__.__name__ != "WatchError":
                        raise
        raise RuntimeError("failed to create managed proxy health job")

    async def get_health_job(self, job_id: str) -> ProxyHealthJob | None:
        """读取指定 Redis 健康任务。"""
        raw = await self._redis.hget(_JOBS, job_id)
        return _load_job(raw) if raw else None

    async def get_active_health_job(self) -> ProxyHealthJob | None:
        """读取最近一个 Redis 活动任务。"""
        jobs = [
            _load_job(raw)
            for raw in await self._redis.hvals(_JOBS)
            if _load_job(raw).status
            in {
                ProxyHealthJobStatus.QUEUED,
                ProxyHealthJobStatus.RUNNING,
            }
        ]
        return max(jobs, key=lambda item: item.created_at, default=None)

    async def claim_health_job(
        self,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> ProxyHealthJob | None:
        """使用 WATCH 认领最早可执行 Redis 任务。"""
        for _ in range(8):
            async with self._redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(_JOBS)
                    jobs = [_load_job(raw) for raw in await pipe.hvals(_JOBS)]
                    if any(
                        job.status == ProxyHealthJobStatus.RUNNING
                        and (job.lease_expires_at or 0) > timestamp_ms
                        for job in jobs
                    ):
                        await pipe.reset()
                        return None
                    candidates = [
                        job
                        for job in jobs
                        if job.status == ProxyHealthJobStatus.QUEUED
                        or (
                            job.status == ProxyHealthJobStatus.RUNNING
                            and (job.lease_expires_at or 0) <= timestamp_ms
                        )
                    ]
                    if not candidates:
                        await pipe.reset()
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
                    pipe.multi()
                    pipe.hset(_JOBS, job.job_id, _dump_job(claimed))
                    await pipe.execute()
                    return claimed
                except Exception as exc:
                    if exc.__class__.__name__ != "WatchError":
                        raise
        return None

    async def heartbeat_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> bool:
        """使用 CAS 续期 Redis 任务租约。"""
        for _ in range(5):
            async with self._redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(_JOBS)
                    raw = await pipe.hget(_JOBS, job_id)
                    job = _load_job(raw) if raw else None
                    if (
                        job is None
                        or job.status != ProxyHealthJobStatus.RUNNING
                        or job.lease_owner != owner
                    ):
                        await pipe.reset()
                        return False
                    updated = replace(
                        job,
                        updated_at=timestamp_ms,
                        lease_expires_at=timestamp_ms + lease_ms,
                    )
                    pipe.multi()
                    pipe.hset(_JOBS, job_id, _dump_job(updated))
                    await pipe.execute()
                    return True
                except Exception as exc:
                    if exc.__class__.__name__ != "WatchError":
                        raise
        return False

    async def pending_health_job_items(
        self,
        job_id: str,
    ) -> list[ProxyHealthJobItem]:
        """读取 Redis 健康任务未完成节点。"""
        return [
            item
            for item in (
                _load_job_item(raw)
                for raw in await self._redis.hvals(
                    _JOB_ITEMS.format(job_id=job_id)
                )
            )
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
        """使用 WATCH 幂等完成 Redis 任务节点。"""
        items_key = _JOB_ITEMS.format(job_id=job_id)
        for _ in range(8):
            async with self._redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(_JOBS, items_key)
                    raw_job = await pipe.hget(_JOBS, job_id)
                    raw_item = await pipe.hget(items_key, proxy_id)
                    job = _load_job(raw_job) if raw_job else None
                    item = _load_job_item(raw_item) if raw_item else None
                    if (
                        job is None
                        or item is None
                        or item.completed
                        or item.generation != generation
                    ):
                        await pipe.reset()
                        return False
                    field_name = {
                        ProxyProbeOutcome.HEALTHY: "healthy",
                        ProxyProbeOutcome.UNHEALTHY: "unhealthy",
                        ProxyProbeOutcome.INCONCLUSIVE: "inconclusive",
                        ProxyProbeOutcome.SKIPPED: "skipped",
                    }[outcome]
                    completed = replace(item, completed=True, outcome=outcome.value)
                    updated = replace(
                        job,
                        completed=job.completed + 1,
                        updated_at=timestamp_ms,
                        **{field_name: getattr(job, field_name) + 1},
                    )
                    pipe.multi()
                    pipe.hset(items_key, proxy_id, _dump_job_item(completed))
                    pipe.hset(_JOBS, job_id, _dump_job(updated))
                    await pipe.execute()
                    return True
                except Exception as exc:
                    if exc.__class__.__name__ != "WatchError":
                        raise
        return False

    async def finish_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        error: str = "",
    ) -> ProxyHealthJob | None:
        """结束当前 Worker 持有的 Redis 任务。"""
        for _ in range(5):
            async with self._redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(_JOBS)
                    raw = await pipe.hget(_JOBS, job_id)
                    job = _load_job(raw) if raw else None
                    if job is None or job.lease_owner != owner:
                        await pipe.reset()
                        return None
                    finished = replace(
                        job,
                        status=(
                            ProxyHealthJobStatus.FAILED
                            if error
                            else ProxyHealthJobStatus.COMPLETED
                        ),
                        updated_at=timestamp_ms,
                        finished_at=timestamp_ms,
                        lease_owner="",
                        lease_expires_at=None,
                        error=error[:500],
                    )
                    pipe.multi()
                    pipe.hset(_JOBS, job_id, _dump_job(finished))
                    await pipe.execute()
                    return finished
                except Exception as exc:
                    if exc.__class__.__name__ != "WatchError":
                        raise
        return None

    async def prune_health_jobs(self, *, cutoff_ms: int) -> int:
        """清理超过保留期的 Redis 已结束任务。"""
        raw = await self._redis.hgetall(_JOBS)
        jobs = [
            (_text(key), _load_job(value))
            for key, value in raw.items()
            if (_load_job(value).finished_at or cutoff_ms) < cutoff_ms
        ]
        if not jobs:
            return 0
        pipe = self._redis.pipeline(transaction=True)
        pipe.hdel(_JOBS, *(job_id for job_id, _ in jobs))
        for job_id, _ in jobs:
            pipe.delete(_JOB_ITEMS.format(job_id=job_id))
        await pipe.execute()
        return len(jobs)

    async def close(self) -> None:
        """关闭 Redis 客户端连接。"""
        await self._redis.aclose()


def _dump_runtime(record: ProxyRuntimeRecord) -> str:
    """序列化运行态。"""
    return _dump(record)


def _load_runtime(value: Any) -> ProxyRuntimeRecord:
    """反序列化运行态。"""
    data = _load(value)
    data["health_state"] = ProxyHealthState(data["health_state"])
    return ProxyRuntimeRecord(**data)


def _dump_binding(record: ProxyBinding) -> str:
    """序列化绑定。"""
    return _dump(record)


def _load_binding(value: Any) -> ProxyBinding:
    """反序列化绑定。"""
    return ProxyBinding(**_load(value))


def _dump_job(record: ProxyHealthJob) -> str:
    """序列化健康任务。"""
    return _dump(record)


def _load_job(value: Any) -> ProxyHealthJob:
    """反序列化健康任务。"""
    data = _load(value)
    data["kind"] = ProxyHealthJobKind(data["kind"])
    data["status"] = ProxyHealthJobStatus(data["status"])
    return ProxyHealthJob(**data)


def _dump_job_item(record: ProxyHealthJobItem) -> str:
    """序列化任务节点。"""
    return _dump(record)


def _load_job_item(value: Any) -> ProxyHealthJobItem:
    """反序列化任务节点。"""
    return ProxyHealthJobItem(**_load(value))


def _dump(record: Any) -> str:
    """把 dataclass 序列化为紧凑 JSON。"""
    data = asdict(record)
    for key, value in list(data.items()):
        if hasattr(value, "value"):
            data[key] = value.value
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _load(value: Any) -> dict[str, Any]:
    """把 Redis 文本反序列化为字典。"""
    return json.loads(_text(value))


def _text(value: Any) -> str:
    """统一 Redis bytes 和 str。"""
    return value.decode() if isinstance(value, bytes) else str(value)


__all__ = ["RedisManagedProxyStateRepository"]
