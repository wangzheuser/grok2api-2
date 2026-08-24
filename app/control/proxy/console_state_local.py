"""SQLite Console 代理共享状态仓储。"""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, TypeVar

from .console_state import (
    ConsoleProxyBinding,
    ConsoleProxyBindingAssignment,
    ConsoleProxyBindingCandidate,
    ConsoleProxyHealthJob,
    ConsoleProxyHealthJobItem,
    ConsoleProxyHealthJobKind,
    ConsoleProxyHealthJobStatus,
    ConsoleProxyHealthState,
    ConsoleProxyProbeOutcome,
    ConsoleProxyRuntimeRecord,
    ConsoleProxyStateSeed,
)


_RUNTIME = "console_proxy_runtime"
_BINDING = "console_proxy_binding"
_JOB = "console_proxy_health_job"
_JOB_ITEM = "console_proxy_health_job_item"
_T = TypeVar("_T")


class LocalConsoleProxyStateRepository:
    """使用账户 SQLite 文件共享 Console 代理运行态。"""

    def __init__(self, db_path: Path) -> None:
        self._path = Path(db_path)
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        """创建启用 WAL 与外键的 SQLite 连接。"""
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.OperationalError:
            pass
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    async def _call(self, fn: Callable[..., _T], *args: Any) -> _T:
        """在线程中执行同步 SQLite 操作并串行化本实例写入。"""
        async with self._lock:
            return await asyncio.to_thread(fn, *args)

    async def initialize(self) -> None:
        """幂等创建运行态、绑定和健康任务表。"""
        await self._call(self._initialize_sync)

    def _initialize_sync(self) -> None:
        """同步创建 SQLite 表结构。"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn:
            conn.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS {_RUNTIME} (
                    proxy_id TEXT PRIMARY KEY,
                    generation INTEGER NOT NULL,
                    health_state TEXT NOT NULL,
                    checking INTEGER NOT NULL DEFAULT 0,
                    runtime_epoch INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    last_failure_at INTEGER,
                    next_retry_at INTEGER,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    challenge_count INTEGER NOT NULL DEFAULT 0,
                    health_success_count INTEGER NOT NULL DEFAULT 0,
                    health_failure_count INTEGER NOT NULL DEFAULT 0,
                    last_checked_at INTEGER,
                    last_latency_ms INTEGER,
                    last_probe_outcome TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS {_BINDING} (
                    account_key TEXT PRIMARY KEY,
                    proxy_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    last_used_at INTEGER NOT NULL,
                    FOREIGN KEY(proxy_id) REFERENCES {_RUNTIME}(proxy_id)
                        ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_console_proxy_binding_proxy
                    ON {_BINDING}(proxy_id);
                CREATE INDEX IF NOT EXISTS idx_console_proxy_binding_last_used
                    ON {_BINDING}(last_used_at);

                CREATE TABLE IF NOT EXISTS {_JOB} (
                    job_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total INTEGER NOT NULL,
                    completed INTEGER NOT NULL DEFAULT 0,
                    healthy INTEGER NOT NULL DEFAULT 0,
                    unhealthy INTEGER NOT NULL DEFAULT 0,
                    inconclusive INTEGER NOT NULL DEFAULT 0,
                    skipped INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    started_at INTEGER,
                    updated_at INTEGER NOT NULL,
                    finished_at INTEGER,
                    lease_owner TEXT NOT NULL DEFAULT '',
                    lease_expires_at INTEGER,
                    error TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_console_proxy_job_active
                    ON {_JOB}(status, created_at);
                CREATE INDEX IF NOT EXISTS idx_console_proxy_job_dedupe
                    ON {_JOB}(dedupe_key, status);

                CREATE TABLE IF NOT EXISTS {_JOB_ITEM} (
                    job_id TEXT NOT NULL,
                    proxy_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    completed INTEGER NOT NULL DEFAULT 0,
                    outcome TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(job_id, proxy_id),
                    FOREIGN KEY(job_id) REFERENCES {_JOB}(job_id)
                        ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_console_proxy_job_item_pending
                    ON {_JOB_ITEM}(job_id, completed);
                """
            )
            conn.commit()

    async def sync_entries(
        self,
        entries: list[ConsoleProxyStateSeed],
        *,
        timestamp_ms: int,
    ) -> None:
        """同步配置节点并重置 generation 已变化的运行态。"""
        await self._call(self._sync_entries_sync, entries, timestamp_ms)

    def _sync_entries_sync(
        self,
        entries: list[ConsoleProxyStateSeed],
        timestamp_ms: int,
    ) -> None:
        """在一个写事务中同步节点身份。"""
        seeds = {entry.proxy_id: entry for entry in entries}
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            for seed in seeds.values():
                row = conn.execute(
                    f"SELECT generation, runtime_epoch FROM {_RUNTIME} WHERE proxy_id = ?",
                    (seed.proxy_id,),
                ).fetchone()
                if row is None:
                    conn.execute(
                        f"""
                        INSERT INTO {_RUNTIME}
                            (proxy_id, generation, health_state, updated_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            seed.proxy_id,
                            seed.generation,
                            ConsoleProxyHealthState.UNKNOWN.value,
                            timestamp_ms,
                        ),
                    )
                elif int(row["generation"]) != seed.generation:
                    # generation 变化会隔离旧 DPoP 和旧请求反馈。
                    conn.execute(
                        f"""
                        UPDATE {_RUNTIME}
                        SET generation = ?, health_state = ?, checking = 0,
                            runtime_epoch = ?, last_error = '',
                            last_failure_at = NULL, next_retry_at = NULL,
                            consecutive_failures = 0, success_count = 0,
                            failure_count = 0, challenge_count = 0,
                            health_success_count = 0, health_failure_count = 0,
                            last_checked_at = NULL, last_latency_ms = NULL,
                            last_probe_outcome = '', version = version + 1,
                            updated_at = ?
                        WHERE proxy_id = ?
                        """,
                        (
                            seed.generation,
                            ConsoleProxyHealthState.UNKNOWN.value,
                            int(row["runtime_epoch"]) + 1,
                            timestamp_ms,
                            seed.proxy_id,
                        ),
                    )
                    conn.execute(
                        f"DELETE FROM {_BINDING} WHERE proxy_id = ?",
                        (seed.proxy_id,),
                    )

            if seeds:
                placeholders = ",".join("?" for _ in seeds)
                conn.execute(
                    f"DELETE FROM {_RUNTIME} WHERE proxy_id NOT IN ({placeholders})",
                    tuple(seeds),
                )
            else:
                conn.execute(f"DELETE FROM {_RUNTIME}")
            conn.commit()

    async def runtime_snapshot(self) -> dict[str, ConsoleProxyRuntimeRecord]:
        """读取全部运行态。"""
        return await self._call(self._runtime_snapshot_sync)

    def _runtime_snapshot_sync(self) -> dict[str, ConsoleProxyRuntimeRecord]:
        """同步读取全部运行态。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(f"SELECT * FROM {_RUNTIME}").fetchall()
        return {str(row["proxy_id"]): _runtime_from_row(row) for row in rows}

    async def get_runtime(self, proxy_id: str) -> ConsoleProxyRuntimeRecord | None:
        """读取指定节点运行态。"""
        return await self._call(self._get_runtime_sync, proxy_id)

    def _get_runtime_sync(self, proxy_id: str) -> ConsoleProxyRuntimeRecord | None:
        """同步读取指定节点运行态。"""
        with closing(self._connect()) as conn:
            row = conn.execute(
                f"SELECT * FROM {_RUNTIME} WHERE proxy_id = ?",
                (proxy_id,),
            ).fetchone()
        return _runtime_from_row(row) if row else None

    async def compare_and_swap_runtime(
        self,
        expected: ConsoleProxyRuntimeRecord,
        updated: ConsoleProxyRuntimeRecord,
        *,
        clear_bindings: bool = False,
    ) -> ConsoleProxyRuntimeRecord | None:
        """按 generation 和 version 原子更新运行态。"""
        return await self._call(
            self._compare_and_swap_runtime_sync,
            expected,
            updated,
            clear_bindings,
        )

    def _compare_and_swap_runtime_sync(
        self,
        expected: ConsoleProxyRuntimeRecord,
        updated: ConsoleProxyRuntimeRecord,
        clear_bindings: bool,
    ) -> ConsoleProxyRuntimeRecord | None:
        """同步执行运行态 CAS 和可选解绑。"""
        stored = replace(updated, version=expected.version + 1)
        values = _runtime_update_values(stored)
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                f"""
                UPDATE {_RUNTIME}
                SET health_state = ?, checking = ?, runtime_epoch = ?,
                    last_error = ?, last_failure_at = ?, next_retry_at = ?,
                    consecutive_failures = ?, success_count = ?,
                    failure_count = ?, challenge_count = ?,
                    health_success_count = ?, health_failure_count = ?,
                    last_checked_at = ?, last_latency_ms = ?,
                    last_probe_outcome = ?, version = ?, updated_at = ?
                WHERE proxy_id = ? AND generation = ? AND version = ?
                """,
                (*values, expected.proxy_id, expected.generation, expected.version),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return None
            if clear_bindings:
                conn.execute(
                    f"DELETE FROM {_BINDING} WHERE proxy_id = ?",
                    (expected.proxy_id,),
                )
            conn.commit()
        return stored

    async def acquire_binding(
        self,
        account_key: str,
        candidates: list[ConsoleProxyBindingCandidate],
        *,
        timestamp_ms: int,
    ) -> ConsoleProxyBindingAssignment | None:
        """在 SQLite 写事务中原子复用或创建账号绑定。"""
        return await self._call(
            self._acquire_binding_sync,
            account_key,
            candidates,
            timestamp_ms,
        )

    def _acquire_binding_sync(
        self,
        account_key: str,
        candidates: list[ConsoleProxyBindingCandidate],
        timestamp_ms: int,
    ) -> ConsoleProxyBindingAssignment | None:
        """同步选择绑定数量最少的健康节点。"""
        candidate_map = {item.proxy_id: item for item in candidates}
        if not candidate_map:
            return None
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                f"SELECT * FROM {_BINDING} WHERE account_key = ?",
                (account_key,),
            ).fetchone()
            if existing is not None:
                candidate = candidate_map.get(str(existing["proxy_id"]))
                runtime_row = conn.execute(
                    f"SELECT * FROM {_RUNTIME} WHERE proxy_id = ?",
                    (existing["proxy_id"],),
                ).fetchone()
                runtime = _runtime_from_row(runtime_row) if runtime_row else None
                if (
                    candidate
                    and runtime
                    and int(existing["generation"]) == candidate.generation
                    and runtime.generation == candidate.generation
                    and runtime.is_schedulable(timestamp_ms)
                ):
                    conn.execute(
                        f"UPDATE {_BINDING} SET last_used_at = ? WHERE account_key = ?",
                        (timestamp_ms, account_key),
                    )
                    conn.commit()
                    binding = _binding_from_row(existing)
                    return ConsoleProxyBindingAssignment(
                        replace(binding, last_used_at=timestamp_ms),
                        runtime,
                    )
                conn.execute(
                    f"DELETE FROM {_BINDING} WHERE account_key = ?",
                    (account_key,),
                )

            ids = tuple(candidate_map)
            placeholders = ",".join("?" for _ in ids)
            generation_cases = " OR ".join(
                "(r.proxy_id = ? AND r.generation = ?)" for _ in ids
            )
            generation_args: list[Any] = []
            for proxy_id in ids:
                generation_args.extend((proxy_id, candidate_map[proxy_id].generation))
            row = conn.execute(
                f"""
                SELECT r.*, COUNT(b.account_key) AS binding_count
                FROM {_RUNTIME} r
                LEFT JOIN {_BINDING} b ON b.proxy_id = r.proxy_id
                WHERE r.proxy_id IN ({placeholders})
                  AND r.health_state = ?
                  AND (r.next_retry_at IS NULL OR r.next_retry_at <= ?)
                  AND ({generation_cases})
                GROUP BY r.proxy_id
                ORDER BY binding_count ASC, r.proxy_id ASC
                LIMIT 1
                """,
                (
                    *ids,
                    ConsoleProxyHealthState.HEALTHY.value,
                    timestamp_ms,
                    *generation_args,
                ),
            ).fetchone()
            if row is None:
                conn.commit()
                return None

            proxy_id = str(row["proxy_id"])
            generation = int(row["generation"])
            conn.execute(
                f"""
                INSERT INTO {_BINDING}
                    (account_key, proxy_id, generation, created_at, last_used_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (account_key, proxy_id, generation, timestamp_ms, timestamp_ms),
            )
            conn.commit()
            binding = ConsoleProxyBinding(
                account_key=account_key,
                proxy_id=proxy_id,
                generation=generation,
                created_at=timestamp_ms,
                last_used_at=timestamp_ms,
            )
            return ConsoleProxyBindingAssignment(binding, _runtime_from_row(row))

    async def clear_bindings(self, proxy_id: str | None = None) -> int:
        """清除全部或指定节点绑定。"""
        return await self._call(self._clear_bindings_sync, proxy_id)

    def _clear_bindings_sync(self, proxy_id: str | None) -> int:
        """同步删除绑定并返回数量。"""
        with closing(self._connect()) as conn:
            if proxy_id is None:
                cursor = conn.execute(f"DELETE FROM {_BINDING}")
            else:
                cursor = conn.execute(
                    f"DELETE FROM {_BINDING} WHERE proxy_id = ?",
                    (proxy_id,),
                )
            conn.commit()
            return max(0, cursor.rowcount)

    async def binding_counts(self) -> dict[str, int]:
        """返回各节点绑定数量。"""
        return await self._call(self._binding_counts_sync)

    def _binding_counts_sync(self) -> dict[str, int]:
        """同步统计各节点绑定数量。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT proxy_id, COUNT(*) AS count FROM {_BINDING} GROUP BY proxy_id"
            ).fetchall()
        return {str(row["proxy_id"]): int(row["count"]) for row in rows}

    async def cleanup_bindings(self, *, cutoff_ms: int) -> int:
        """清理超过闲置期限的绑定。"""
        return await self._call(self._cleanup_bindings_sync, cutoff_ms)

    def _cleanup_bindings_sync(self, cutoff_ms: int) -> int:
        """同步清理闲置绑定。"""
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                f"DELETE FROM {_BINDING} WHERE last_used_at < ?",
                (cutoff_ms,),
            )
            conn.commit()
            return max(0, cursor.rowcount)

    async def create_health_job(
        self,
        *,
        kind: ConsoleProxyHealthJobKind,
        dedupe_key: str,
        items: list[ConsoleProxyStateSeed],
        timestamp_ms: int,
    ) -> ConsoleProxyHealthJob:
        """创建或复用相同范围的活动健康任务。"""
        return await self._call(
            self._create_health_job_sync,
            kind,
            dedupe_key,
            items,
            timestamp_ms,
        )

    def _create_health_job_sync(
        self,
        kind: ConsoleProxyHealthJobKind,
        dedupe_key: str,
        items: list[ConsoleProxyStateSeed],
        timestamp_ms: int,
    ) -> ConsoleProxyHealthJob:
        """同步创建健康任务及节点快照。"""
        unique = {(item.proxy_id, item.generation): item for item in items}
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"""
                SELECT * FROM {_JOB}
                WHERE dedupe_key = ? AND status IN (?, ?)
                ORDER BY created_at DESC LIMIT 1
                """,
                (
                    dedupe_key,
                    ConsoleProxyHealthJobStatus.QUEUED.value,
                    ConsoleProxyHealthJobStatus.RUNNING.value,
                ),
            ).fetchone()
            if row is not None:
                conn.commit()
                return _job_from_row(row)

            if kind == ConsoleProxyHealthJobKind.BOOTSTRAP and unique:
                # 仅新建 bootstrap 时撤销旧健康状态；活动任务复用路径保持幂等。
                identities = list(unique)
                conn.executemany(
                    f"""
                    UPDATE {_RUNTIME}
                    SET health_state = ?, checking = 0,
                        runtime_epoch = runtime_epoch + 1,
                        last_error = '', last_failure_at = NULL,
                        next_retry_at = NULL, consecutive_failures = 0,
                        challenge_count = 0, last_probe_outcome = '',
                        version = version + 1, updated_at = ?
                    WHERE proxy_id = ? AND generation = ? AND health_state = ?
                    """,
                    [
                        (
                            ConsoleProxyHealthState.UNKNOWN.value,
                            timestamp_ms,
                            proxy_id,
                            generation,
                            ConsoleProxyHealthState.HEALTHY.value,
                        )
                        for proxy_id, generation in identities
                    ],
                )
                conn.executemany(
                    f"""
                    DELETE FROM {_BINDING}
                    WHERE proxy_id = ? AND generation = ?
                      AND EXISTS (
                          SELECT 1 FROM {_RUNTIME}
                          WHERE proxy_id = ? AND generation = ?
                            AND health_state = ? AND updated_at = ?
                      )
                    """,
                    [
                        (
                            proxy_id,
                            generation,
                            proxy_id,
                            generation,
                            ConsoleProxyHealthState.UNKNOWN.value,
                            timestamp_ms,
                        )
                        for proxy_id, generation in identities
                    ],
                )

            job_id = uuid.uuid4().hex
            conn.execute(
                f"""
                INSERT INTO {_JOB}
                    (job_id, kind, dedupe_key, status, total, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    kind.value,
                    dedupe_key,
                    ConsoleProxyHealthJobStatus.QUEUED.value,
                    len(unique),
                    timestamp_ms,
                    timestamp_ms,
                ),
            )
            conn.executemany(
                f"""
                INSERT INTO {_JOB_ITEM}(job_id, proxy_id, generation)
                VALUES (?, ?, ?)
                """,
                [
                    (job_id, proxy_id, generation)
                    for proxy_id, generation in unique
                ],
            )
            conn.commit()
        return ConsoleProxyHealthJob(
            job_id=job_id,
            kind=kind,
            dedupe_key=dedupe_key,
            status=ConsoleProxyHealthJobStatus.QUEUED,
            total=len(unique),
            created_at=timestamp_ms,
            updated_at=timestamp_ms,
        )

    async def get_health_job(self, job_id: str) -> ConsoleProxyHealthJob | None:
        """读取指定健康任务。"""
        return await self._call(self._get_health_job_sync, job_id)

    def _get_health_job_sync(self, job_id: str) -> ConsoleProxyHealthJob | None:
        """同步读取指定健康任务。"""
        with closing(self._connect()) as conn:
            row = conn.execute(
                f"SELECT * FROM {_JOB} WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return _job_from_row(row) if row else None

    async def get_active_health_job(self) -> ConsoleProxyHealthJob | None:
        """读取最近一个活动健康任务。"""
        return await self._call(self._get_active_health_job_sync)

    def _get_active_health_job_sync(self) -> ConsoleProxyHealthJob | None:
        """同步读取最近活动任务。"""
        with closing(self._connect()) as conn:
            row = conn.execute(
                f"""
                SELECT * FROM {_JOB}
                WHERE status IN (?, ?)
                ORDER BY created_at DESC LIMIT 1
                """,
                (
                    ConsoleProxyHealthJobStatus.QUEUED.value,
                    ConsoleProxyHealthJobStatus.RUNNING.value,
                ),
            ).fetchone()
        return _job_from_row(row) if row else None

    async def claim_health_job(
        self,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> ConsoleProxyHealthJob | None:
        """认领排队中或租约已过期的任务。"""
        return await self._call(
            self._claim_health_job_sync,
            owner,
            timestamp_ms,
            lease_ms,
        )

    def _claim_health_job_sync(
        self,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> ConsoleProxyHealthJob | None:
        """同步认领最早可执行任务。"""
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                f"""
                SELECT 1 FROM {_JOB}
                WHERE status = ? AND COALESCE(lease_expires_at, 0) > ?
                LIMIT 1
                """,
                (ConsoleProxyHealthJobStatus.RUNNING.value, timestamp_ms),
            ).fetchone()
            if active is not None:
                conn.commit()
                return None
            row = conn.execute(
                f"""
                SELECT * FROM {_JOB}
                WHERE status = ?
                   OR (status = ? AND COALESCE(lease_expires_at, 0) <= ?)
                ORDER BY created_at ASC LIMIT 1
                """,
                (
                    ConsoleProxyHealthJobStatus.QUEUED.value,
                    ConsoleProxyHealthJobStatus.RUNNING.value,
                    timestamp_ms,
                ),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            started_at = row["started_at"] or timestamp_ms
            conn.execute(
                f"""
                UPDATE {_JOB}
                SET status = ?, started_at = ?, updated_at = ?,
                    lease_owner = ?, lease_expires_at = ?
                WHERE job_id = ?
                """,
                (
                    ConsoleProxyHealthJobStatus.RUNNING.value,
                    started_at,
                    timestamp_ms,
                    owner,
                    timestamp_ms + lease_ms,
                    row["job_id"],
                ),
            )
            conn.commit()
        return replace(
            _job_from_row(row),
            status=ConsoleProxyHealthJobStatus.RUNNING,
            started_at=int(started_at),
            updated_at=timestamp_ms,
            lease_owner=owner,
            lease_expires_at=timestamp_ms + lease_ms,
        )

    async def heartbeat_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> bool:
        """续期当前 Worker 持有的任务。"""
        return await self._call(
            self._heartbeat_health_job_sync,
            job_id,
            owner,
            timestamp_ms,
            lease_ms,
        )

    def _heartbeat_health_job_sync(
        self,
        job_id: str,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> bool:
        """同步续期任务租约。"""
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                f"""
                UPDATE {_JOB}
                SET updated_at = ?, lease_expires_at = ?
                WHERE job_id = ? AND status = ? AND lease_owner = ?
                """,
                (
                    timestamp_ms,
                    timestamp_ms + lease_ms,
                    job_id,
                    ConsoleProxyHealthJobStatus.RUNNING.value,
                    owner,
                ),
            )
            conn.commit()
            return cursor.rowcount == 1

    async def pending_health_job_items(
        self,
        job_id: str,
    ) -> list[ConsoleProxyHealthJobItem]:
        """读取健康任务尚未完成的节点。"""
        return await self._call(self._pending_health_job_items_sync, job_id)

    def _pending_health_job_items_sync(
        self,
        job_id: str,
    ) -> list[ConsoleProxyHealthJobItem]:
        """同步读取未完成任务节点。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT * FROM {_JOB_ITEM} WHERE job_id = ? AND completed = 0",
                (job_id,),
            ).fetchall()
        return [_job_item_from_row(row) for row in rows]

    async def complete_health_job_item(
        self,
        job_id: str,
        *,
        proxy_id: str,
        generation: int,
        outcome: ConsoleProxyProbeOutcome,
        timestamp_ms: int,
    ) -> bool:
        """幂等完成一个健康任务节点并更新计数。"""
        return await self._call(
            self._complete_health_job_item_sync,
            job_id,
            proxy_id,
            generation,
            outcome,
            timestamp_ms,
        )

    def _complete_health_job_item_sync(
        self,
        job_id: str,
        proxy_id: str,
        generation: int,
        outcome: ConsoleProxyProbeOutcome,
        timestamp_ms: int,
    ) -> bool:
        """同步完成任务节点。"""
        counter = {
            ConsoleProxyProbeOutcome.HEALTHY: "healthy",
            ConsoleProxyProbeOutcome.UNHEALTHY: "unhealthy",
            ConsoleProxyProbeOutcome.INCONCLUSIVE: "inconclusive",
            ConsoleProxyProbeOutcome.SKIPPED: "skipped",
        }[outcome]
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                f"""
                UPDATE {_JOB_ITEM}
                SET completed = 1, outcome = ?
                WHERE job_id = ? AND proxy_id = ? AND generation = ?
                  AND completed = 0
                """,
                (outcome.value, job_id, proxy_id, generation),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return False
            conn.execute(
                f"""
                UPDATE {_JOB}
                SET completed = completed + 1, {counter} = {counter} + 1,
                    updated_at = ?
                WHERE job_id = ?
                """,
                (timestamp_ms, job_id),
            )
            conn.commit()
            return True

    async def finish_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        error: str = "",
    ) -> ConsoleProxyHealthJob | None:
        """完成或标记失败当前 Worker 持有的任务。"""
        return await self._call(
            self._finish_health_job_sync,
            job_id,
            owner,
            timestamp_ms,
            error,
        )

    def _finish_health_job_sync(
        self,
        job_id: str,
        owner: str,
        timestamp_ms: int,
        error: str,
    ) -> ConsoleProxyHealthJob | None:
        """同步结束任务并返回最新快照。"""
        status = (
            ConsoleProxyHealthJobStatus.FAILED
            if error
            else ConsoleProxyHealthJobStatus.COMPLETED
        )
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                f"""
                UPDATE {_JOB}
                SET status = ?, updated_at = ?, finished_at = ?,
                    lease_owner = '', lease_expires_at = NULL, error = ?
                WHERE job_id = ? AND lease_owner = ?
                """,
                (status.value, timestamp_ms, timestamp_ms, error[:500], job_id, owner),
            )
            conn.commit()
            if cursor.rowcount != 1:
                return None
            row = conn.execute(
                f"SELECT * FROM {_JOB} WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return _job_from_row(row) if row else None

    async def prune_health_jobs(self, *, cutoff_ms: int) -> int:
        """清理超过保留期的已结束任务。"""
        return await self._call(self._prune_health_jobs_sync, cutoff_ms)

    def _prune_health_jobs_sync(self, cutoff_ms: int) -> int:
        """同步清理过期任务。"""
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                f"DELETE FROM {_JOB} WHERE finished_at IS NOT NULL AND finished_at < ?",
                (cutoff_ms,),
            )
            conn.commit()
            return max(0, cursor.rowcount)

    async def close(self) -> None:
        """SQLite 仓储每次操作使用短连接，无需关闭。"""


def _runtime_from_row(row: Any) -> ConsoleProxyRuntimeRecord:
    """把 SQLite 行转换为运行态模型。"""
    return ConsoleProxyRuntimeRecord(
        proxy_id=str(row["proxy_id"]),
        generation=int(row["generation"]),
        health_state=ConsoleProxyHealthState(str(row["health_state"])),
        checking=bool(row["checking"]),
        runtime_epoch=int(row["runtime_epoch"]),
        last_error=str(row["last_error"] or ""),
        last_failure_at=_optional_int(row["last_failure_at"]),
        next_retry_at=_optional_int(row["next_retry_at"]),
        consecutive_failures=int(row["consecutive_failures"]),
        success_count=int(row["success_count"]),
        failure_count=int(row["failure_count"]),
        challenge_count=int(row["challenge_count"]),
        health_success_count=int(row["health_success_count"]),
        health_failure_count=int(row["health_failure_count"]),
        last_checked_at=_optional_int(row["last_checked_at"]),
        last_latency_ms=_optional_int(row["last_latency_ms"]),
        last_probe_outcome=str(row["last_probe_outcome"] or ""),
        version=int(row["version"]),
        updated_at=int(row["updated_at"]),
    )


def _runtime_update_values(record: ConsoleProxyRuntimeRecord) -> tuple[Any, ...]:
    """返回运行态 UPDATE 语句的字段值。"""
    return (
        record.health_state.value,
        int(record.checking),
        record.runtime_epoch,
        record.last_error,
        record.last_failure_at,
        record.next_retry_at,
        record.consecutive_failures,
        record.success_count,
        record.failure_count,
        record.challenge_count,
        record.health_success_count,
        record.health_failure_count,
        record.last_checked_at,
        record.last_latency_ms,
        record.last_probe_outcome,
        record.version,
        record.updated_at,
    )


def _binding_from_row(row: Any) -> ConsoleProxyBinding:
    """把 SQLite 行转换为绑定模型。"""
    return ConsoleProxyBinding(
        account_key=str(row["account_key"]),
        proxy_id=str(row["proxy_id"]),
        generation=int(row["generation"]),
        created_at=int(row["created_at"]),
        last_used_at=int(row["last_used_at"]),
    )


def _job_from_row(row: Any) -> ConsoleProxyHealthJob:
    """把 SQLite 行转换为健康任务模型。"""
    return ConsoleProxyHealthJob(
        job_id=str(row["job_id"]),
        kind=ConsoleProxyHealthJobKind(str(row["kind"])),
        dedupe_key=str(row["dedupe_key"]),
        status=ConsoleProxyHealthJobStatus(str(row["status"])),
        total=int(row["total"]),
        completed=int(row["completed"]),
        healthy=int(row["healthy"]),
        unhealthy=int(row["unhealthy"]),
        inconclusive=int(row["inconclusive"]),
        skipped=int(row["skipped"]),
        created_at=int(row["created_at"]),
        started_at=_optional_int(row["started_at"]),
        updated_at=int(row["updated_at"]),
        finished_at=_optional_int(row["finished_at"]),
        lease_owner=str(row["lease_owner"] or ""),
        lease_expires_at=_optional_int(row["lease_expires_at"]),
        error=str(row["error"] or ""),
    )


def _job_item_from_row(row: Any) -> ConsoleProxyHealthJobItem:
    """把 SQLite 行转换为任务节点模型。"""
    return ConsoleProxyHealthJobItem(
        proxy_id=str(row["proxy_id"]),
        generation=int(row["generation"]),
        completed=bool(row["completed"]),
        outcome=str(row["outcome"] or ""),
    )


def _optional_int(value: Any) -> int | None:
    """把数据库可空整数转换为 Python 值。"""
    return int(value) if value is not None else None


__all__ = ["LocalConsoleProxyStateRepository"]
