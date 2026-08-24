"""MySQL/PostgreSQL Console 代理共享状态仓储。"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import asdict, replace
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.ext.asyncio import AsyncEngine

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


metadata = sa.MetaData()

runtime_table = sa.Table(
    "console_proxy_runtime",
    metadata,
    sa.Column("proxy_id", sa.String(128), primary_key=True),
    sa.Column("generation", sa.Integer, nullable=False),
    sa.Column("health_state", sa.String(32), nullable=False),
    sa.Column("checking", sa.Boolean, nullable=False, default=False),
    sa.Column("runtime_epoch", sa.BigInteger, nullable=False, default=0),
    sa.Column("last_error", sa.Text, nullable=False, default=""),
    sa.Column("last_failure_at", sa.BigInteger),
    sa.Column("next_retry_at", sa.BigInteger),
    sa.Column("consecutive_failures", sa.Integer, nullable=False, default=0),
    sa.Column("success_count", sa.BigInteger, nullable=False, default=0),
    sa.Column("failure_count", sa.BigInteger, nullable=False, default=0),
    sa.Column("challenge_count", sa.Integer, nullable=False, default=0),
    sa.Column("health_success_count", sa.BigInteger, nullable=False, default=0),
    sa.Column("health_failure_count", sa.BigInteger, nullable=False, default=0),
    sa.Column("last_checked_at", sa.BigInteger),
    sa.Column("last_latency_ms", sa.Integer),
    sa.Column("last_probe_outcome", sa.String(32), nullable=False, default=""),
    sa.Column("version", sa.BigInteger, nullable=False, default=0),
    sa.Column("updated_at", sa.BigInteger, nullable=False),
)

binding_table = sa.Table(
    "console_proxy_binding",
    metadata,
    sa.Column("account_key", sa.String(64), primary_key=True),
    sa.Column("proxy_id", sa.String(128), nullable=False, index=True),
    sa.Column("generation", sa.Integer, nullable=False),
    sa.Column("created_at", sa.BigInteger, nullable=False),
    sa.Column("last_used_at", sa.BigInteger, nullable=False, index=True),
)

job_table = sa.Table(
    "console_proxy_health_job",
    metadata,
    sa.Column("job_id", sa.String(64), primary_key=True),
    sa.Column("kind", sa.String(32), nullable=False),
    sa.Column("dedupe_key", sa.String(256), nullable=False, index=True),
    sa.Column("status", sa.String(32), nullable=False, index=True),
    sa.Column("total", sa.Integer, nullable=False),
    sa.Column("completed", sa.Integer, nullable=False, default=0),
    sa.Column("healthy", sa.Integer, nullable=False, default=0),
    sa.Column("unhealthy", sa.Integer, nullable=False, default=0),
    sa.Column("inconclusive", sa.Integer, nullable=False, default=0),
    sa.Column("skipped", sa.Integer, nullable=False, default=0),
    sa.Column("created_at", sa.BigInteger, nullable=False),
    sa.Column("started_at", sa.BigInteger),
    sa.Column("updated_at", sa.BigInteger, nullable=False),
    sa.Column("finished_at", sa.BigInteger),
    sa.Column("lease_owner", sa.String(128), nullable=False, default=""),
    sa.Column("lease_expires_at", sa.BigInteger),
    sa.Column("error", sa.Text, nullable=False, default=""),
)

job_item_table = sa.Table(
    "console_proxy_health_job_item",
    metadata,
    sa.Column("job_id", sa.String(64), primary_key=True),
    sa.Column("proxy_id", sa.String(128), primary_key=True),
    sa.Column("generation", sa.Integer, nullable=False),
    sa.Column("completed", sa.Boolean, nullable=False, default=False, index=True),
    sa.Column("outcome", sa.String(32), nullable=False, default=""),
)

job_lock_table = sa.Table(
    "console_proxy_health_lock",
    metadata,
    sa.Column("lock_id", sa.Integer, primary_key=True),
)


class SqlConsoleProxyStateRepository:
    """基于 SQLAlchemy 事务的 Console 代理共享状态仓储。"""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        dialect: str,
        dispose_engine: bool = False,
    ) -> None:
        self._engine = engine
        self._dialect = dialect
        self._dispose_engine = dispose_engine
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """幂等创建 SQL 状态表和索引。"""
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            async with self._engine.begin() as conn:
                mysql_lock_acquired = False
                if self._dialect == "mysql":
                    result = await conn.execute(
                        sa.text(
                            "SELECT GET_LOCK('grok2api_console_proxy_schema', 30)"
                        )
                    )
                    mysql_lock_acquired = result.scalar_one() == 1
                    if not mysql_lock_acquired:
                        raise RuntimeError(
                            "timed out acquiring console proxy schema lock"
                        )
                else:
                    await conn.execute(
                        sa.text("SELECT pg_advisory_xact_lock(1736728149)")
                    )
                try:
                    await conn.run_sync(metadata.create_all)
                    await conn.execute(
                        self._insert_ignore(job_lock_table, {"lock_id": 1})
                    )
                finally:
                    if mysql_lock_acquired:
                        await conn.execute(
                            sa.text(
                                "SELECT RELEASE_LOCK('grok2api_console_proxy_schema')"
                            )
                        )
            self._initialized = True

    def _insert_ignore(self, table: sa.Table, values: dict[str, Any]):
        """构造当前数据库方言的幂等插入语句。"""
        if self._dialect == "mysql":
            stmt = mysql_insert(table).values(**values)
            primary_key = next(iter(table.primary_key.columns)).name
            return stmt.on_duplicate_key_update(
                **{primary_key: getattr(stmt.inserted, primary_key)}
            )
        return postgresql_insert(table).values(**values).on_conflict_do_nothing()

    async def _lock_health_queue(self, conn) -> None:
        """锁定健康任务队列的单例协调行。"""
        await conn.execute(
            sa.select(job_lock_table)
            .where(job_lock_table.c.lock_id == 1)
            .with_for_update()
        )

    async def sync_entries(
        self,
        entries: list[ConsoleProxyStateSeed],
        *,
        timestamp_ms: int,
    ) -> None:
        """同步配置节点并重置变更 generation 的运行态。"""
        await self.initialize()
        seeds = {entry.proxy_id: entry for entry in entries}
        async with self._engine.begin() as conn:
            # 配置同步是低频操作，串行化可避免多 Worker 首次 upsert 死锁。
            await self._lock_health_queue(conn)
            rows = (
                await conn.execute(sa.select(runtime_table).with_for_update())
            ).mappings()
            current = {str(row["proxy_id"]): _runtime_from_mapping(row) for row in rows}
            for proxy_id, seed in seeds.items():
                runtime = current.get(proxy_id)
                if runtime is None:
                    await conn.execute(
                        self._insert_ignore(
                            runtime_table,
                            {
                                "proxy_id": proxy_id,
                                "generation": seed.generation,
                                "health_state": ConsoleProxyHealthState.UNKNOWN.value,
                                "checking": False,
                                "runtime_epoch": 0,
                                "last_error": "",
                                "consecutive_failures": 0,
                                "success_count": 0,
                                "failure_count": 0,
                                "challenge_count": 0,
                                "health_success_count": 0,
                                "health_failure_count": 0,
                                "last_probe_outcome": "",
                                "version": 0,
                                "updated_at": timestamp_ms,
                            },
                        )
                    )
                elif runtime.generation != seed.generation:
                    await conn.execute(
                        runtime_table.update()
                        .where(runtime_table.c.proxy_id == proxy_id)
                        .values(
                            generation=seed.generation,
                            health_state=ConsoleProxyHealthState.UNKNOWN.value,
                            checking=False,
                            runtime_epoch=runtime.runtime_epoch + 1,
                            last_error="",
                            last_failure_at=None,
                            next_retry_at=None,
                            consecutive_failures=0,
                            success_count=0,
                            failure_count=0,
                            challenge_count=0,
                            health_success_count=0,
                            health_failure_count=0,
                            last_checked_at=None,
                            last_latency_ms=None,
                            last_probe_outcome="",
                            version=runtime.version + 1,
                            updated_at=timestamp_ms,
                        )
                    )
                    await conn.execute(
                        binding_table.delete().where(binding_table.c.proxy_id == proxy_id)
                    )
            removed = set(current) - set(seeds)
            if removed:
                await conn.execute(
                    binding_table.delete().where(binding_table.c.proxy_id.in_(removed))
                )
                await conn.execute(
                    runtime_table.delete().where(runtime_table.c.proxy_id.in_(removed))
                )

    async def runtime_snapshot(self) -> dict[str, ConsoleProxyRuntimeRecord]:
        """读取全部 SQL 运行态。"""
        await self.initialize()
        async with self._engine.connect() as conn:
            rows = (await conn.execute(sa.select(runtime_table))).mappings()
            return {str(row["proxy_id"]): _runtime_from_mapping(row) for row in rows}

    async def get_runtime(self, proxy_id: str) -> ConsoleProxyRuntimeRecord | None:
        """读取指定 SQL 运行态。"""
        await self.initialize()
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    sa.select(runtime_table).where(runtime_table.c.proxy_id == proxy_id)
                )
            ).mappings().first()
            return _runtime_from_mapping(row) if row else None

    async def compare_and_swap_runtime(
        self,
        expected: ConsoleProxyRuntimeRecord,
        updated: ConsoleProxyRuntimeRecord,
        *,
        clear_bindings: bool = False,
    ) -> ConsoleProxyRuntimeRecord | None:
        """在可重试事务中按 generation 和 version 更新运行态。"""
        for attempt in range(5):
            try:
                return await self._compare_and_swap_runtime_transaction(
                    expected,
                    updated,
                    clear_bindings=clear_bindings,
                )
            except sa.exc.OperationalError as exc:
                if not _is_retryable_sql_conflict(exc) or attempt == 4:
                    raise
                await asyncio.sleep(0.01 * (attempt + 1))
        return None

    async def _compare_and_swap_runtime_transaction(
        self,
        expected: ConsoleProxyRuntimeRecord,
        updated: ConsoleProxyRuntimeRecord,
        *,
        clear_bindings: bool = False,
    ) -> ConsoleProxyRuntimeRecord | None:
        """执行一次运行态 CAS 数据库事务。"""
        await self.initialize()
        stored = replace(updated, version=expected.version + 1)
        async with self._engine.begin() as conn:
            result = await conn.execute(
                runtime_table.update()
                .where(
                    runtime_table.c.proxy_id == expected.proxy_id,
                    runtime_table.c.generation == expected.generation,
                    runtime_table.c.version == expected.version,
                )
                .values(**_runtime_values(stored))
            )
            if result.rowcount != 1:
                return None
            if clear_bindings:
                await conn.execute(
                    binding_table.delete().where(
                        binding_table.c.proxy_id == expected.proxy_id
                    )
                )
        return stored

    async def acquire_binding(
        self,
        account_key: str,
        candidates: list[ConsoleProxyBindingCandidate],
        *,
        timestamp_ms: int,
    ) -> ConsoleProxyBindingAssignment | None:
        """在可重试事务中原子复用或创建账号绑定。"""
        for attempt in range(5):
            try:
                return await self._acquire_binding_transaction(
                    account_key,
                    candidates,
                    timestamp_ms=timestamp_ms,
                )
            except sa.exc.OperationalError as exc:
                if not _is_retryable_sql_conflict(exc) or attempt == 4:
                    raise
                await asyncio.sleep(0.01 * (attempt + 1))
        return None

    async def _acquire_binding_transaction(
        self,
        account_key: str,
        candidates: list[ConsoleProxyBindingCandidate],
        *,
        timestamp_ms: int,
    ) -> ConsoleProxyBindingAssignment | None:
        """执行一次账号绑定数据库事务。"""
        await self.initialize()
        candidate_map = {item.proxy_id: item for item in candidates}
        if not candidate_map:
            return None
        async with self._engine.begin() as conn:
            existing = (
                await conn.execute(
                    sa.select(binding_table)
                    .where(binding_table.c.account_key == account_key)
                    .with_for_update()
                )
            ).mappings().first()
            if existing:
                runtime_row = (
                    await conn.execute(
                        sa.select(runtime_table)
                        .where(runtime_table.c.proxy_id == existing["proxy_id"])
                        .with_for_update()
                    )
                ).mappings().first()
                runtime = _runtime_from_mapping(runtime_row) if runtime_row else None
                candidate = candidate_map.get(str(existing["proxy_id"]))
                if (
                    runtime
                    and candidate
                    and int(existing["generation"]) == candidate.generation
                    and runtime.generation == candidate.generation
                    and runtime.is_schedulable(timestamp_ms)
                ):
                    await conn.execute(
                        binding_table.update()
                        .where(binding_table.c.account_key == account_key)
                        .values(last_used_at=timestamp_ms)
                    )
                    return ConsoleProxyBindingAssignment(
                        ConsoleProxyBinding(
                            account_key=account_key,
                            proxy_id=str(existing["proxy_id"]),
                            generation=int(existing["generation"]),
                            created_at=int(existing["created_at"]),
                            last_used_at=timestamp_ms,
                        ),
                        runtime,
                    )
                await conn.execute(
                    binding_table.delete().where(binding_table.c.account_key == account_key)
                )

            counts = (
                sa.select(
                    binding_table.c.proxy_id,
                    sa.func.count(binding_table.c.account_key).label("binding_count"),
                )
                .group_by(binding_table.c.proxy_id)
                .subquery()
            )
            conditions = [
                sa.and_(
                    runtime_table.c.proxy_id == candidate.proxy_id,
                    runtime_table.c.generation == candidate.generation,
                )
                for candidate in candidates
            ]
            row = (
                await conn.execute(
                    sa.select(runtime_table)
                    .outerjoin(counts, counts.c.proxy_id == runtime_table.c.proxy_id)
                    .where(
                        sa.or_(*conditions),
                        runtime_table.c.health_state
                        == ConsoleProxyHealthState.HEALTHY.value,
                        sa.or_(
                            runtime_table.c.next_retry_at.is_(None),
                            runtime_table.c.next_retry_at <= timestamp_ms,
                        ),
                    )
                    .order_by(
                        sa.func.coalesce(counts.c.binding_count, 0),
                        runtime_table.c.proxy_id,
                    )
                    .limit(1)
                )
            ).mappings().first()
            if row is None:
                return None
            runtime = _runtime_from_mapping(row)
            binding = ConsoleProxyBinding(
                account_key=account_key,
                proxy_id=runtime.proxy_id,
                generation=runtime.generation,
                created_at=timestamp_ms,
                last_used_at=timestamp_ms,
            )
            if self._dialect == "mysql":
                stmt = mysql_insert(binding_table).values(**asdict(binding))
                stmt = stmt.on_duplicate_key_update(
                    account_key=stmt.inserted.account_key
                )
            else:
                stmt = postgresql_insert(binding_table).values(**asdict(binding))
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=[binding_table.c.account_key]
                )
            await conn.execute(stmt)
            # 并发首次绑定时，唯一约束的胜出结果是唯一可信结果。
            winner = (
                await conn.execute(
                    sa.select(binding_table).where(
                        binding_table.c.account_key == account_key
                    )
                )
            ).mappings().first()
            if winner is None:
                return None
            winner_runtime_row = (
                await conn.execute(
                    sa.select(runtime_table).where(
                        runtime_table.c.proxy_id == winner["proxy_id"]
                    )
                )
            ).mappings().first()
            winner_runtime = (
                _runtime_from_mapping(winner_runtime_row)
                if winner_runtime_row
                else None
            )
            winner_candidate = candidate_map.get(str(winner["proxy_id"]))
            if (
                winner_runtime is None
                or winner_candidate is None
                or int(winner["generation"]) != winner_candidate.generation
                or not winner_runtime.is_schedulable(timestamp_ms)
            ):
                return None
            return ConsoleProxyBindingAssignment(
                ConsoleProxyBinding(
                    account_key=account_key,
                    proxy_id=str(winner["proxy_id"]),
                    generation=int(winner["generation"]),
                    created_at=int(winner["created_at"]),
                    last_used_at=int(winner["last_used_at"]),
                ),
                winner_runtime,
            )

    async def clear_bindings(self, proxy_id: str | None = None) -> int:
        """清除全部或指定节点 SQL 绑定。"""
        await self.initialize()
        async with self._engine.begin() as conn:
            stmt = binding_table.delete()
            if proxy_id is not None:
                stmt = stmt.where(binding_table.c.proxy_id == proxy_id)
            result = await conn.execute(stmt)
            return max(0, result.rowcount)

    async def binding_counts(self) -> dict[str, int]:
        """统计 SQL 节点绑定数量。"""
        await self.initialize()
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(
                    sa.select(
                        binding_table.c.proxy_id,
                        sa.func.count(binding_table.c.account_key).label("count"),
                    ).group_by(binding_table.c.proxy_id)
                )
            ).mappings()
            return {str(row["proxy_id"]): int(row["count"]) for row in rows}

    async def cleanup_bindings(self, *, cutoff_ms: int) -> int:
        """清理超过闲置期限的 SQL 绑定。"""
        await self.initialize()
        async with self._engine.begin() as conn:
            result = await conn.execute(
                binding_table.delete().where(binding_table.c.last_used_at < cutoff_ms)
            )
            return max(0, result.rowcount)

    async def create_health_job(
        self,
        *,
        kind: ConsoleProxyHealthJobKind,
        dedupe_key: str,
        items: list[ConsoleProxyStateSeed],
        timestamp_ms: int,
    ) -> ConsoleProxyHealthJob:
        """创建或复用 SQL 活动健康任务。"""
        await self.initialize()
        unique = {(item.proxy_id, item.generation): item for item in items}
        async with self._engine.begin() as conn:
            await self._lock_health_queue(conn)
            row = (
                await conn.execute(
                    sa.select(job_table)
                    .where(
                        job_table.c.dedupe_key == dedupe_key,
                        job_table.c.status.in_(
                            [
                                ConsoleProxyHealthJobStatus.QUEUED.value,
                                ConsoleProxyHealthJobStatus.RUNNING.value,
                            ]
                        ),
                    )
                    .order_by(job_table.c.created_at.desc())
                    .limit(1)
                    .with_for_update()
                )
            ).mappings().first()
            if row:
                return _job_from_mapping(row)
            if kind == ConsoleProxyHealthJobKind.BOOTSTRAP and unique:
                identities = list(unique)
                identity_filter = sa.tuple_(
                    runtime_table.c.proxy_id,
                    runtime_table.c.generation,
                ).in_(identities)
                await conn.execute(
                    runtime_table.update()
                    .where(
                        identity_filter,
                        runtime_table.c.health_state
                        == ConsoleProxyHealthState.HEALTHY.value,
                    )
                    .values(
                        health_state=ConsoleProxyHealthState.UNKNOWN.value,
                        checking=False,
                        runtime_epoch=runtime_table.c.runtime_epoch + 1,
                        last_error="",
                        last_failure_at=None,
                        next_retry_at=None,
                        consecutive_failures=0,
                        challenge_count=0,
                        last_probe_outcome="",
                        version=runtime_table.c.version + 1,
                        updated_at=timestamp_ms,
                    )
                )
                # 仅删除本次确实被重置的节点绑定，保留 cooling/dead 门禁。
                reset_rows = (
                    await conn.execute(
                        sa.select(
                            runtime_table.c.proxy_id,
                            runtime_table.c.generation,
                        ).where(
                            identity_filter,
                            runtime_table.c.health_state
                            == ConsoleProxyHealthState.UNKNOWN.value,
                            runtime_table.c.updated_at == timestamp_ms,
                        )
                    )
                ).all()
                if reset_rows:
                    await conn.execute(
                        binding_table.delete().where(
                            sa.tuple_(
                                binding_table.c.proxy_id,
                                binding_table.c.generation,
                            ).in_(reset_rows)
                        )
                    )
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
            await conn.execute(job_table.insert().values(**_job_values(job)))
            if unique:
                await conn.execute(
                    job_item_table.insert(),
                    [
                        {
                            "job_id": job_id,
                            "proxy_id": proxy_id,
                            "generation": generation,
                            "completed": False,
                            "outcome": "",
                        }
                        for proxy_id, generation in unique
                    ],
                )
            return job

    async def get_health_job(self, job_id: str) -> ConsoleProxyHealthJob | None:
        """读取指定 SQL 健康任务。"""
        await self.initialize()
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    sa.select(job_table).where(job_table.c.job_id == job_id)
                )
            ).mappings().first()
            return _job_from_mapping(row) if row else None

    async def get_active_health_job(self) -> ConsoleProxyHealthJob | None:
        """读取最近一个 SQL 活动任务。"""
        await self.initialize()
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    sa.select(job_table)
                    .where(
                        job_table.c.status.in_(
                            [
                                ConsoleProxyHealthJobStatus.QUEUED.value,
                                ConsoleProxyHealthJobStatus.RUNNING.value,
                            ]
                        )
                    )
                    .order_by(job_table.c.created_at.desc())
                    .limit(1)
                )
            ).mappings().first()
            return _job_from_mapping(row) if row else None

    async def claim_health_job(
        self,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> ConsoleProxyHealthJob | None:
        """认领最早排队或租约过期的 SQL 任务。"""
        await self.initialize()
        async with self._engine.begin() as conn:
            await self._lock_health_queue(conn)
            active = (
                await conn.execute(
                    sa.select(job_table.c.job_id)
                    .where(
                        job_table.c.status
                        == ConsoleProxyHealthJobStatus.RUNNING.value,
                        sa.func.coalesce(job_table.c.lease_expires_at, 0)
                        > timestamp_ms,
                    )
                    .limit(1)
                    .with_for_update()
                )
            ).first()
            if active is not None:
                return None
            row = (
                await conn.execute(
                    sa.select(job_table)
                    .where(
                        sa.or_(
                            job_table.c.status
                            == ConsoleProxyHealthJobStatus.QUEUED.value,
                            sa.and_(
                                job_table.c.status
                                == ConsoleProxyHealthJobStatus.RUNNING.value,
                                sa.func.coalesce(job_table.c.lease_expires_at, 0)
                                <= timestamp_ms,
                            ),
                        )
                    )
                    .order_by(job_table.c.created_at)
                    .limit(1)
                    .with_for_update()
                )
            ).mappings().first()
            if row is None:
                return None
            job = _job_from_mapping(row)
            claimed = replace(
                job,
                status=ConsoleProxyHealthJobStatus.RUNNING,
                started_at=job.started_at or timestamp_ms,
                updated_at=timestamp_ms,
                lease_owner=owner,
                lease_expires_at=timestamp_ms + lease_ms,
            )
            await conn.execute(
                job_table.update()
                .where(job_table.c.job_id == job.job_id)
                .values(**_job_values(claimed))
            )
            return claimed

    async def heartbeat_health_job(
        self,
        job_id: str,
        *,
        owner: str,
        timestamp_ms: int,
        lease_ms: int,
    ) -> bool:
        """续期当前 Worker 持有的 SQL 任务。"""
        await self.initialize()
        async with self._engine.begin() as conn:
            result = await conn.execute(
                job_table.update()
                .where(
                    job_table.c.job_id == job_id,
                    job_table.c.status == ConsoleProxyHealthJobStatus.RUNNING.value,
                    job_table.c.lease_owner == owner,
                )
                .values(
                    updated_at=timestamp_ms,
                    lease_expires_at=timestamp_ms + lease_ms,
                )
            )
            return result.rowcount == 1

    async def pending_health_job_items(
        self,
        job_id: str,
    ) -> list[ConsoleProxyHealthJobItem]:
        """读取 SQL 健康任务未完成节点。"""
        await self.initialize()
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(
                    sa.select(job_item_table).where(
                        job_item_table.c.job_id == job_id,
                        job_item_table.c.completed.is_(False),
                    )
                )
            ).mappings()
            return [_job_item_from_mapping(row) for row in rows]

    async def complete_health_job_item(
        self,
        job_id: str,
        *,
        proxy_id: str,
        generation: int,
        outcome: ConsoleProxyProbeOutcome,
        timestamp_ms: int,
    ) -> bool:
        """幂等完成 SQL 健康任务节点并累计计数。"""
        await self.initialize()
        counter = {
            ConsoleProxyProbeOutcome.HEALTHY: job_table.c.healthy,
            ConsoleProxyProbeOutcome.UNHEALTHY: job_table.c.unhealthy,
            ConsoleProxyProbeOutcome.INCONCLUSIVE: job_table.c.inconclusive,
            ConsoleProxyProbeOutcome.SKIPPED: job_table.c.skipped,
        }[outcome]
        async with self._engine.begin() as conn:
            result = await conn.execute(
                job_item_table.update()
                .where(
                    job_item_table.c.job_id == job_id,
                    job_item_table.c.proxy_id == proxy_id,
                    job_item_table.c.generation == generation,
                    job_item_table.c.completed.is_(False),
                )
                .values(completed=True, outcome=outcome.value)
            )
            if result.rowcount != 1:
                return False
            await conn.execute(
                job_table.update()
                .where(job_table.c.job_id == job_id)
                .values(
                    completed=job_table.c.completed + 1,
                    updated_at=timestamp_ms,
                    **{counter.name: counter + 1},
                )
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
        """完成或标记失败当前 Worker 持有的 SQL 任务。"""
        await self.initialize()
        async with self._engine.begin() as conn:
            result = await conn.execute(
                job_table.update()
                .where(job_table.c.job_id == job_id, job_table.c.lease_owner == owner)
                .values(
                    status=(
                        ConsoleProxyHealthJobStatus.FAILED.value
                        if error
                        else ConsoleProxyHealthJobStatus.COMPLETED.value
                    ),
                    updated_at=timestamp_ms,
                    finished_at=timestamp_ms,
                    lease_owner="",
                    lease_expires_at=None,
                    error=error[:500],
                )
            )
            if result.rowcount != 1:
                return None
            row = (
                await conn.execute(
                    sa.select(job_table).where(job_table.c.job_id == job_id)
                )
            ).mappings().first()
            return _job_from_mapping(row) if row else None

    async def prune_health_jobs(self, *, cutoff_ms: int) -> int:
        """清理超过保留期的 SQL 健康任务。"""
        await self.initialize()
        async with self._engine.begin() as conn:
            ids = [
                str(row[0])
                for row in (
                    await conn.execute(
                        sa.select(job_table.c.job_id).where(
                            job_table.c.finished_at.is_not(None),
                            job_table.c.finished_at < cutoff_ms,
                        )
                    )
                ).all()
            ]
            if not ids:
                return 0
            await conn.execute(
                job_item_table.delete().where(job_item_table.c.job_id.in_(ids))
            )
            result = await conn.execute(
                job_table.delete().where(job_table.c.job_id.in_(ids))
            )
            return max(0, result.rowcount)

    async def close(self) -> None:
        """按构造参数决定是否释放共享 SQL Engine。"""
        if self._dispose_engine:
            await self._engine.dispose()


def _runtime_values(record: ConsoleProxyRuntimeRecord) -> dict[str, Any]:
    """把运行态转换为 SQL values。"""
    return {
        "health_state": record.health_state.value,
        "checking": record.checking,
        "runtime_epoch": record.runtime_epoch,
        "last_error": record.last_error,
        "last_failure_at": record.last_failure_at,
        "next_retry_at": record.next_retry_at,
        "consecutive_failures": record.consecutive_failures,
        "success_count": record.success_count,
        "failure_count": record.failure_count,
        "challenge_count": record.challenge_count,
        "health_success_count": record.health_success_count,
        "health_failure_count": record.health_failure_count,
        "last_checked_at": record.last_checked_at,
        "last_latency_ms": record.last_latency_ms,
        "last_probe_outcome": record.last_probe_outcome,
        "version": record.version,
        "updated_at": record.updated_at,
    }


def _runtime_from_mapping(row: Any) -> ConsoleProxyRuntimeRecord:
    """把 SQL 行转换为运行态模型。"""
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


def _job_values(job: ConsoleProxyHealthJob) -> dict[str, Any]:
    """把健康任务转换为 SQL values。"""
    return {
        "job_id": job.job_id,
        "kind": job.kind.value,
        "dedupe_key": job.dedupe_key,
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
        "lease_owner": job.lease_owner,
        "lease_expires_at": job.lease_expires_at,
        "error": job.error,
    }


def _job_from_mapping(row: Any) -> ConsoleProxyHealthJob:
    """把 SQL 行转换为健康任务。"""
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


def _job_item_from_mapping(row: Any) -> ConsoleProxyHealthJobItem:
    """把 SQL 行转换为健康任务节点。"""
    return ConsoleProxyHealthJobItem(
        proxy_id=str(row["proxy_id"]),
        generation=int(row["generation"]),
        completed=bool(row["completed"]),
        outcome=str(row["outcome"] or ""),
    )


def _optional_int(value: Any) -> int | None:
    """把可空数据库整数转换为 Python 值。"""
    return int(value) if value is not None else None


def _is_retryable_sql_conflict(exc: sa.exc.OperationalError) -> bool:
    """判断 MySQL/PostgreSQL 事务冲突是否适合有界重试。"""
    original = exc.orig
    args = getattr(original, "args", ())
    mysql_code = args[0] if args else None
    sqlstate = getattr(original, "sqlstate", None) or getattr(
        original,
        "pgcode",
        None,
    )
    return mysql_code in {1205, 1213} or sqlstate in {"40001", "40P01"}


__all__ = ["SqlConsoleProxyStateRepository"]
