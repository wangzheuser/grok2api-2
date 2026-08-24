"""Console 专用代理池的主动连通性检测。"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.control.proxy.console_pool import (
    ConsoleProxyEntry,
    ConsoleProxyPool,
    render_proxy_url,
)
from app.control.proxy.console_state import (
    ConsoleProxyHealthJob,
    ConsoleProxyHealthJobKind,
    ConsoleProxyProbeOutcome,
)
from app.control.proxy.models import ProxyLease
from app.dataplane.proxy.adapters.session import ResettableSession
from app.platform.config.snapshot import get_config
from app.platform.logging.logger import logger
from app.platform.runtime.ids import next_hex


@dataclass(frozen=True, slots=True)
class ConsoleProxyProbeResult:
    """单个代理节点的连通性检测结果。"""

    proxy_id: str
    generation: int
    outcome: ConsoleProxyProbeOutcome
    message: str
    latency_ms: int
    status_code: int | None = None

    @property
    def ok(self) -> bool:
        """兼容旧管理接口的布尔成功标记。"""
        return self.outcome == ConsoleProxyProbeOutcome.HEALTHY


ProbeFunction = Callable[[ConsoleProxyEntry], Awaitable[ConsoleProxyProbeResult]]


class _HealthJobLeaseLost(RuntimeError):
    """健康任务租约已由其他 Worker 接管。"""


async def probe_console_proxy(entry: ConsoleProxyEntry) -> ConsoleProxyProbeResult:
    """通过指定代理访问检测地址，并返回结构化结果。"""
    cfg = get_config()
    check_url = cfg.get_str(
        "console.proxy_pool.health_check_url",
        "https://console.x.ai/",
    )
    timeout_s = max(
        1,
        cfg.get_int("console.proxy_pool.health_check_timeout_sec", 15),
    )
    proxy_url = render_proxy_url(entry, int(time.time() * 1000))
    lease = ProxyLease(
        lease_id=next_hex(),
        proxy_url=proxy_url,
        proxy_pool="console",
        proxy_id=entry.id,
        proxy_mode=entry.inferred_mode().value,
        generation=entry.generation,
    )
    started = time.monotonic()
    try:
        async with ResettableSession(lease=lease) as session:
            response = await session.get(check_url, timeout=timeout_s)
        latency_ms = int((time.monotonic() - started) * 1000)
        outcome = _outcome_for_status(response.status_code)
        return ConsoleProxyProbeResult(
            proxy_id=entry.id,
            generation=entry.generation,
            outcome=outcome,
            message=f"HTTP {response.status_code}",
            latency_ms=latency_ms,
            status_code=response.status_code,
        )
    except Exception as exc:
        latency_ms = int((time.monotonic() - started) * 1000)
        return ConsoleProxyProbeResult(
            proxy_id=entry.id,
            generation=entry.generation,
            outcome=ConsoleProxyProbeOutcome.UNHEALTHY,
            message=str(exc)[:300],
            latency_ms=latency_ms,
        )


class ConsoleProxyHealthScheduler:
    """按配置周期并发检测 Console 代理节点。"""

    def __init__(
        self,
        pool: ConsoleProxyPool,
        *,
        probe: ProbeFunction = probe_console_proxy,
    ) -> None:
        self._pool = pool
        self._probe = probe
        self._repo = pool.state_repository
        self._owner = next_hex()
        self._task: asyncio.Task | None = None
        self._running = False
        self._periodic_due_at = 0
        self._cleanup_due_at = 0

    def start(self) -> None:
        """启动后台检测循环。"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(
            self._loop(),
            name="console-proxy-health",
        )
        logger.info("console proxy health scheduler started")

    async def stop(self) -> None:
        """停止后台检测循环。"""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        logger.info("console proxy health scheduler stopped")

    async def enqueue(
        self,
        *,
        kind: ConsoleProxyHealthJobKind,
        entries: list[ConsoleProxyEntry] | None = None,
    ) -> ConsoleProxyHealthJob:
        """创建或复用一项共享健康任务。"""
        await self._pool.load()
        return await self._pool.create_health_job(kind, entries)

    async def run_once(self, *, force: bool = False) -> list[ConsoleProxyProbeResult]:
        """兼容测试入口：直接检测节点并回写共享状态。"""
        cfg = get_config()
        if not force and not cfg.get_bool(
            "console.proxy_pool.health_check_enabled",
            True,
        ):
            return []
        await self._pool.load()
        await self._pool.expire_cooldowns()
        entries = []
        for entry in await self._pool.entries(include_secret=True):
            if entry.enabled and await self._pool.is_probe_eligible(
                entry.id,
                entry.generation,
            ):
                entries.append(entry)
        return await self._probe_entries(entries)

    async def _probe_entries(
        self,
        entries: list[ConsoleProxyEntry],
    ) -> list[ConsoleProxyProbeResult]:
        """按配置并发限制检测给定节点。"""
        if not entries:
            return []
        concurrency = min(
            max(
                1,
                get_config().get_int(
                    "console.proxy_pool.health_check_concurrency",
                    20,
                ),
            ),
            100,
            len(entries),
        )
        semaphore = asyncio.Semaphore(concurrency)

        async def check(entry: ConsoleProxyEntry) -> ConsoleProxyProbeResult:
            """限制单轮探测并发并回写节点运行态。"""
            async with semaphore:
                await self._pool.mark_checking(entry.id, entry.generation)
                try:
                    result = await self._probe(entry)
                except Exception as exc:
                    result = ConsoleProxyProbeResult(
                        proxy_id=entry.id,
                        generation=entry.generation,
                        outcome=ConsoleProxyProbeOutcome.UNHEALTHY,
                        message=str(exc)[:300],
                        latency_ms=0,
                    )
            await self._pool.record_health_result(
                entry.id,
                generation=entry.generation,
                outcome=result.outcome,
                message=result.message,
                latency_ms=result.latency_ms,
                status_code=result.status_code,
            )
            return result

        results = await asyncio.gather(*(check(entry) for entry in entries))
        logger.info(
            "console proxy health check completed: total={} unhealthy={}",
            len(results),
            sum(result.outcome == ConsoleProxyProbeOutcome.UNHEALTHY for result in results),
        )
        return results

    async def _loop(self) -> None:
        """持续认领共享任务并按周期创建维护任务。"""
        while self._running:
            try:
                await self._enqueue_due_tasks()
                job = await self._repo.claim_health_job(
                    owner=self._owner,
                    timestamp_ms=int(time.time() * 1000),
                    lease_ms=60000,
                )
                if job is None:
                    await asyncio.sleep(1)
                    continue
                await self._execute_job(job)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    "console proxy health check failed: error_type={} error={}",
                    type(exc).__name__,
                    exc,
                )
                await asyncio.sleep(2)

    async def _execute_job(self, job: ConsoleProxyHealthJob) -> None:
        """执行已认领任务，逐项幂等提交进度。"""
        entries = {
            entry.id: entry
            for entry in await self._pool.entries(include_secret=True)
        }
        await self._pool.expire_cooldowns()
        items = await self._repo.pending_health_job_items(job.job_id)
        semaphore = asyncio.Semaphore(
            min(
                100,
                max(
                    1,
                    get_config().get_int(
                        "console.proxy_pool.health_check_concurrency",
                        20,
                    ),
                ),
            )
        )
        force_selected = job.kind == ConsoleProxyHealthJobKind.MANUAL_SELECTION

        async def process(item) -> None:
            """检测任务节点并提交共享进度。"""
            entry = entries.get(item.proxy_id)
            if (
                entry is None
                or entry.generation != item.generation
                or (
                    not force_selected
                    and (
                        not entry.enabled
                        or not await self._pool.is_probe_eligible(
                            item.proxy_id,
                            item.generation,
                        )
                    )
                )
            ):
                outcome = ConsoleProxyProbeOutcome.SKIPPED
            else:
                async with semaphore:
                    await self._pool.mark_checking(entry.id, entry.generation)
                    try:
                        result = await self._probe(entry)
                    except Exception as exc:
                        result = ConsoleProxyProbeResult(
                            proxy_id=entry.id,
                            generation=entry.generation,
                            outcome=ConsoleProxyProbeOutcome.UNHEALTHY,
                            message=str(exc)[:300],
                            latency_ms=0,
                        )
                outcome = result.outcome
                await self._pool.record_health_result(
                    entry.id,
                    generation=entry.generation,
                    outcome=outcome,
                    message=result.message,
                    latency_ms=result.latency_ms,
                    status_code=result.status_code,
                )
            await self._repo.complete_health_job_item(
                job.job_id,
                proxy_id=item.proxy_id,
                generation=item.generation,
                outcome=outcome,
                timestamp_ms=int(time.time() * 1000),
            )

        heartbeat = asyncio.create_task(self._heartbeat(job.job_id))
        work = asyncio.ensure_future(asyncio.gather(*(process(item) for item in items)))
        try:
            done, _ = await asyncio.wait(
                {work, heartbeat},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat in done:
                if not work.done():
                    work.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await work
                heartbeat.result()
                raise _HealthJobLeaseLost("console proxy health job lease lost")
            await work
            finished = await self._repo.finish_health_job(
                job.job_id,
                owner=self._owner,
                timestamp_ms=int(time.time() * 1000),
            )
            if finished is None:
                raise _HealthJobLeaseLost("console proxy health job lease lost")
        except _HealthJobLeaseLost:
            # 旧 Worker 停止写入，等待当前租约持有者继续未完成节点。
            raise
        except Exception as exc:
            await self._repo.finish_health_job(
                job.job_id,
                owner=self._owner,
                timestamp_ms=int(time.time() * 1000),
                # 共享任务表只记录错误类型，不持久化底层连接串或代理上下文。
                error=f"{type(exc).__name__}: health job execution failed",
            )
            raise
        finally:
            if not work.done():
                work.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await work
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _heartbeat(self, job_id: str) -> None:
        """执行任务期间持续续期共享租约。"""
        while True:
            await asyncio.sleep(20)
            if not await self._repo.heartbeat_health_job(
                job_id,
                owner=self._owner,
                timestamp_ms=int(time.time() * 1000),
                lease_ms=60000,
            ):
                return

    async def _enqueue_due_tasks(self) -> None:
        """按配置创建周期健康任务并清理共享历史。"""
        timestamp_ms = int(time.time() * 1000)
        cfg = get_config()
        if (
            timestamp_ms >= self._periodic_due_at
            and cfg.get_bool("console.proxy_pool.enabled", False)
            and cfg.get_bool("console.proxy_pool.health_check_enabled", True)
        ):
            await self.enqueue(kind=ConsoleProxyHealthJobKind.PERIODIC)
            self._periodic_due_at = timestamp_ms + self._interval_seconds() * 1000
        if timestamp_ms >= self._cleanup_due_at:
            await self._pool.cleanup_shared_state()
            self._cleanup_due_at = timestamp_ms + 3600 * 1000

    @staticmethod
    def _interval_seconds() -> int:
        """返回当前配置的探测间隔。"""
        return max(
            5,
            get_config().get_int(
                "console.proxy_pool.health_check_interval_sec",
                300,
            ),
        )


def _outcome_for_status(status_code: int) -> ConsoleProxyProbeOutcome:
    """按 Console 出口可用性分类 HTTP 状态。"""
    if 200 <= status_code < 400:
        return ConsoleProxyProbeOutcome.HEALTHY
    if status_code in {403, 407}:
        return ConsoleProxyProbeOutcome.UNHEALTHY
    return ConsoleProxyProbeOutcome.INCONCLUSIVE


__all__ = [
    "ConsoleProxyHealthScheduler",
    "ConsoleProxyProbeResult",
    "probe_console_proxy",
]
