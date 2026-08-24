import unittest
import asyncio
from dataclasses import replace
from unittest.mock import patch

from app.control.proxy.managed_health import (
    ManagedProxyHealthScheduler,
    ProxyProbeResult,
    _outcome_for_status,
)
from app.control.proxy.managed_pool import ManagedProxyPool
from app.control.proxy.managed_state import (
    ProxyHealthJobKind,
    ProxyHealthState,
    ProxyProbeOutcome,
)


class _PoolConfig:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)

    def get_bool(self, key, default=False):
        value = self.values.get(key, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def get_int(self, key, default=0):
        return int(self.values.get(key, default))

    def get_float(self, key, default=0.0):
        return float(self.values.get(key, default))

    def get_str(self, key, default=""):
        return str(self.values.get(key, default))

    def get_list(self, key, default=None):
        return self.values.get(key, default or [])


class ManagedProxyHealthSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def test_http_status_classification(self):
        """探测状态码应按健康、异常和不确定三类处理。"""
        for status in (200, 204, 301, 399):
            self.assertEqual(
                _outcome_for_status(status),
                ProxyProbeOutcome.HEALTHY,
            )
        for status in (403, 407):
            self.assertEqual(
                _outcome_for_status(status),
                ProxyProbeOutcome.UNHEALTHY,
            )
        for status in (400, 401, 429, 500, 503):
            self.assertEqual(
                _outcome_for_status(status),
                ProxyProbeOutcome.INCONCLUSIVE,
            )

    async def test_run_once_probes_enabled_entries_and_records_results(self):
        """单轮健康检查应跳过禁用节点并把结果写回代理池。"""
        cfg = _PoolConfig(
            {
                "proxy.health.enabled": True,
                "proxy.pool.static_cooldown_sec": 60,
                "proxy.pool.entries": [
                    {"id": "ok", "url": "http://ok:8080", "enabled": True},
                    {"id": "bad", "url": "http://bad:8080", "enabled": True},
                    {"id": "off", "url": "http://off:8080", "enabled": False},
                ],
            }
        )
        pool = ManagedProxyPool()

        async def probe(entry):
            """按节点 ID 返回确定性的本地探测结果。"""
            ok = entry.id == "ok"
            return ProxyProbeResult(
                proxy_id=entry.id,
                generation=entry.generation,
                outcome=(
                    ProxyProbeOutcome.HEALTHY
                    if ok
                    else ProxyProbeOutcome.UNHEALTHY
                ),
                message="HTTP 200" if ok else "connect failed",
                latency_ms=20,
                status_code=200 if ok else None,
            )

        scheduler = ManagedProxyHealthScheduler(pool, probe=probe)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg), patch(
            "app.control.proxy.managed_health.get_config",
            return_value=cfg,
        ):
            results = await scheduler.run_once()
            snapshot = await pool.snapshot()

        self.assertEqual({result.proxy_id for result in results}, {"ok", "bad"})
        rows = {item["id"]: item for item in snapshot["items"]}
        self.assertEqual(rows["ok"]["health_success_count"], 1)
        self.assertEqual(rows["bad"]["health_failure_count"], 1)
        self.assertEqual(rows["bad"]["status"], "cooling_down")
        self.assertIsNone(rows["off"]["last_checked_at"])

    async def test_probe_concurrency_never_exceeds_configured_limit(self):
        """单个共享任务的实际探测并发不得超过配置值。"""
        cfg = _PoolConfig(
            {
                "proxy.health.enabled": True,
                "proxy.health.concurrency": 3,
                "proxy.pool.entries": [
                    {"id": f"p{index}", "url": f"http://p{index}:8080"}
                    for index in range(20)
                ],
            }
        )
        pool = ManagedProxyPool()
        active = 0
        maximum = 0
        lock = asyncio.Lock()

        async def probe(entry):
            """记录当前并发后返回健康结果。"""
            nonlocal active, maximum
            async with lock:
                active += 1
                maximum = max(maximum, active)
            await asyncio.sleep(0.005)
            async with lock:
                active -= 1
            return ProxyProbeResult(
                proxy_id=entry.id,
                generation=entry.generation,
                outcome=ProxyProbeOutcome.HEALTHY,
                message="HTTP 200",
                latency_ms=5,
                status_code=200,
            )

        scheduler = ManagedProxyHealthScheduler(pool, probe=probe)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg), patch(
            "app.control.proxy.managed_health.get_config",
            return_value=cfg,
        ):
            results = await scheduler.run_once()

        self.assertEqual(len(results), 20)
        self.assertEqual(maximum, 3)

    async def test_worker_stops_job_items_after_lease_loss(self):
        """续租失败后旧 Worker 应取消未完成检测，交给新持有者续跑。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy:8080"}
                ],
            }
        )
        probe_started = asyncio.Event()
        probe_cancelled = asyncio.Event()

        async def slow_probe(entry):
            """保持探测挂起，便于模拟任务租约丢失。"""
            probe_started.set()
            try:
                await asyncio.sleep(10)
            finally:
                probe_cancelled.set()

        async def lose_lease(job_id):
            """立即模拟共享仓储拒绝续租。"""
            await probe_started.wait()

        pool = ManagedProxyPool()
        scheduler = ManagedProxyHealthScheduler(pool, probe=slow_probe)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg), patch(
            "app.control.proxy.managed_health.get_config",
            return_value=cfg,
        ), patch.object(scheduler, "_heartbeat", side_effect=lose_lease):
            await pool.initialize()
            job = await scheduler.enqueue(kind=ProxyHealthJobKind.MANUAL_ALL)
            claimed = await pool.state_repository.claim_health_job(
                owner=scheduler._owner,
                timestamp_ms=1000,
                lease_ms=60000,
            )
            with self.assertRaisesRegex(RuntimeError, "lease lost"):
                await scheduler._execute_job(claimed)

        self.assertEqual(claimed.job_id, job.job_id)
        self.assertTrue(probe_cancelled.is_set())

    async def test_manual_selection_probes_gated_nodes_without_recovery(self):
        """显式批量测试应探测禁用、冷却和死亡节点，但保留恢复门禁。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {
                        "id": "off",
                        "url": "http://off:8080",
                        "enabled": False,
                    },
                    {"id": "cool", "url": "http://cool:8080"},
                    {"id": "dead", "url": "http://dead:8080"},
                ],
            }
        )
        probed: list[str] = []

        async def probe(entry):
            """记录显式选择节点并返回健康结果。"""
            probed.append(entry.id)
            return ProxyProbeResult(
                proxy_id=entry.id,
                generation=entry.generation,
                outcome=ProxyProbeOutcome.HEALTHY,
                message="HTTP 200",
                latency_ms=5,
                status_code=200,
            )

        pool = ManagedProxyPool()
        scheduler = ManagedProxyHealthScheduler(pool, probe=probe)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg), patch(
            "app.control.proxy.managed_health.get_config",
            return_value=cfg,
        ):
            await pool.initialize()
            dead = await pool.state_repository.get_runtime("dead")
            await pool.state_repository.compare_and_swap_runtime(
                dead,
                replace(dead, health_state=ProxyHealthState.DEAD),
            )
            cool = await pool.state_repository.get_runtime("cool")
            await pool.state_repository.compare_and_swap_runtime(
                cool,
                replace(
                    cool,
                    health_state=ProxyHealthState.COOLING_DOWN,
                    next_retry_at=10**15,
                ),
            )
            entries = await pool.selected_entries(["off", "cool", "dead"])
            job = await scheduler.enqueue(
                kind=ProxyHealthJobKind.MANUAL_SELECTION,
                entries=entries,
            )
            claimed = await pool.state_repository.claim_health_job(
                owner=scheduler._owner,
                timestamp_ms=1000,
                lease_ms=60000,
            )
            await scheduler._execute_job(claimed)
            snapshot = await pool.snapshot()
            finished = await pool.get_health_job(job.job_id)

        rows = {item["id"]: item for item in snapshot["items"]}
        self.assertEqual(set(probed), {"off", "cool", "dead"})
        self.assertEqual(finished.healthy, 3)
        self.assertEqual(rows["off"]["status"], "disabled")
        self.assertEqual(rows["cool"]["status"], "cooling_down")
        self.assertEqual(rows["dead"]["status"], "dead")

    async def test_provider_manual_uses_shared_health_job_repository(self):
        """当前出口检测应通过共享任务租约执行并写入统一进度。"""
        cfg = _PoolConfig({"proxy.pool.entries": []})
        provider_calls = 0

        async def provider_probe():
            """返回一次健康的当前出口检测。"""
            nonlocal provider_calls
            provider_calls += 1
            return {"ok": True, "provider": "direct"}

        pool = ManagedProxyPool()
        scheduler = ManagedProxyHealthScheduler(
            pool,
            provider_probe=provider_probe,
        )
        with patch(
            "app.control.proxy.managed_pool.get_config",
            return_value=cfg,
        ), patch(
            "app.control.proxy.managed_health.get_config",
            return_value=cfg,
        ):
            await pool.initialize()
            job = await pool.create_health_job(
                ProxyHealthJobKind.PROVIDER_MANUAL
            )
            claimed = await pool.state_repository.claim_health_job(
                owner=scheduler._owner,
                timestamp_ms=1000,
                lease_ms=60000,
            )
            await scheduler._execute_job(claimed)
            finished = await pool.get_health_job(job.job_id)

        self.assertEqual(provider_calls, 1)
        self.assertEqual(finished.completed, 1)
        self.assertEqual(finished.healthy, 1)
        self.assertEqual(finished.kind, ProxyHealthJobKind.PROVIDER_MANUAL)


if __name__ == "__main__":
    unittest.main()
