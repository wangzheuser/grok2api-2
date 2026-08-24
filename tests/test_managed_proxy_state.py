import asyncio
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from app.control.proxy.managed_state import (
    ProxyBindingCandidate,
    ProxyHealthJobKind,
    ProxyHealthJobStatus,
    ProxyHealthState,
    ProxyProbeOutcome,
    ProxyRuntimeRecord,
    ProxyStateSeed,
)
from app.control.proxy.managed_state_local import LocalManagedProxyStateRepository


class LocalManagedProxyStateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """为每个用例创建两个指向同一 SQLite 文件的 Worker 仓储。"""
        self._temporary = tempfile.TemporaryDirectory()
        path = Path(self._temporary.name) / "accounts.db"
        self.first = LocalManagedProxyStateRepository(path)
        self.second = LocalManagedProxyStateRepository(path)
        await self.first.initialize()
        await self.second.initialize()
        await self.first.sync_entries(
            [ProxyStateSeed("p1", 0), ProxyStateSeed("p2", 0)],
            timestamp_ms=1000,
        )

    async def asyncTearDown(self):
        """释放测试临时目录。"""
        await self.first.close()
        await self.second.close()
        self._temporary.cleanup()

    async def _healthy(self, proxy_id: str) -> ProxyRuntimeRecord:
        """把指定节点原子标记为健康。"""
        runtime = await self.first.get_runtime(proxy_id)
        self.assertIsNotNone(runtime)
        stored = await self.first.compare_and_swap_runtime(
            runtime,
            replace(runtime, health_state=ProxyHealthState.HEALTHY),
        )
        self.assertIsNotNone(stored)
        return stored

    async def test_two_workers_concurrently_reuse_one_account_binding(self):
        """两个 Worker 并发首次绑定时应由账号唯一约束收敛到同一节点。"""
        await self._healthy("p1")
        await self._healthy("p2")
        candidates = [
            ProxyBindingCandidate("p1", 0),
            ProxyBindingCandidate("p2", 0),
        ]

        first, second = await asyncio.gather(
            self.first.acquire_binding("account", candidates, timestamp_ms=2000),
            self.second.acquire_binding("account", candidates, timestamp_ms=2000),
        )

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(first.binding.proxy_id, second.binding.proxy_id)
        self.assertEqual(await self.second.binding_counts(), {"p1": 1})

    async def test_failure_cas_clears_binding_for_other_worker(self):
        """一个 Worker 标记失败并解绑后，另一个 Worker 应立即看见不可调度。"""
        runtime = await self._healthy("p1")
        candidate = [ProxyBindingCandidate("p1", 0)]
        assignment = await self.first.acquire_binding(
            "account",
            candidate,
            timestamp_ms=2000,
        )
        self.assertIsNotNone(assignment)

        stored = await self.first.compare_and_swap_runtime(
            runtime,
            replace(
                runtime,
                health_state=ProxyHealthState.COOLING_DOWN,
                runtime_epoch=runtime.runtime_epoch + 1,
                next_retry_at=5000,
            ),
            clear_bindings=True,
        )

        self.assertIsNotNone(stored)
        self.assertIsNone(
            await self.second.acquire_binding(
                "account",
                candidate,
                timestamp_ms=2001,
            )
        )
        self.assertEqual(await self.second.binding_counts(), {})

    async def test_stale_runtime_version_cannot_overwrite_new_state(self):
        """旧租约对应的 version 更新应被共享仓储拒绝。"""
        original = await self.first.get_runtime("p1")
        current = await self.first.compare_and_swap_runtime(
            original,
            replace(original, health_state=ProxyHealthState.HEALTHY),
        )

        stale = await self.second.compare_and_swap_runtime(
            original,
            replace(original, health_state=ProxyHealthState.DEAD),
        )

        self.assertIsNotNone(current)
        self.assertIsNone(stale)
        visible = await self.second.get_runtime("p1")
        self.assertEqual(visible.health_state, ProxyHealthState.HEALTHY)

    async def test_idle_binding_cleanup_uses_last_used_time(self):
        """绑定清理应只删除超过闲置阈值的账号。"""
        await self._healthy("p1")
        candidate = [ProxyBindingCandidate("p1", 0)]
        await self.first.acquire_binding("old", candidate, timestamp_ms=1000)
        await self.first.acquire_binding("fresh", candidate, timestamp_ms=3000)

        removed = await self.second.cleanup_bindings(cutoff_ms=2000)

        self.assertEqual(removed, 1)
        self.assertEqual(await self.second.binding_counts(), {"p1": 1})

    async def test_health_job_progress_is_idempotent_and_lease_can_resume(self):
        """任务进度应单调递增，租约过期后由另一个 Worker 续跑未完成项。"""
        seeds = [ProxyStateSeed(f"p{index}", 0) for index in range(500)]
        job = await self.first.create_health_job(
            kind=ProxyHealthJobKind.MANUAL_ALL,
            dedupe_key="scope:all",
            items=seeds,
            timestamp_ms=1000,
        )
        claimed = await self.first.claim_health_job(
            owner="worker-a",
            timestamp_ms=1100,
            lease_ms=100,
        )
        blocked = await self.second.claim_health_job(
            owner="worker-b",
            timestamp_ms=1150,
            lease_ms=100,
        )
        resumed = await self.second.claim_health_job(
            owner="worker-b",
            timestamp_ms=1201,
            lease_ms=100,
        )

        self.assertEqual(job.total, 500)
        self.assertEqual(claimed.job_id, job.job_id)
        self.assertIsNone(blocked)
        self.assertEqual(resumed.job_id, job.job_id)
        first_count = await self.second.complete_health_job_item(
            job.job_id,
            proxy_id="p0",
            generation=0,
            outcome=ProxyProbeOutcome.HEALTHY,
            timestamp_ms=1210,
        )
        duplicate = await self.first.complete_health_job_item(
            job.job_id,
            proxy_id="p0",
            generation=0,
            outcome=ProxyProbeOutcome.UNHEALTHY,
            timestamp_ms=1220,
        )
        snapshot = await self.first.get_health_job(job.job_id)

        self.assertTrue(first_count)
        self.assertFalse(duplicate)
        self.assertEqual(snapshot.status, ProxyHealthJobStatus.RUNNING)
        self.assertEqual(snapshot.completed, 1)
        self.assertEqual(snapshot.healthy, 1)
        self.assertEqual(len(await self.first.pending_health_job_items(job.job_id)), 499)

    async def test_new_bootstrap_revokes_old_health_and_binding_once(self):
        """新 bootstrap 应关闭旧健康租约，活动任务复用不得重复重置。"""
        healthy = await self._healthy("p1")
        await self.first.acquire_binding(
            "account",
            [ProxyBindingCandidate("p1", 0)],
            timestamp_ms=1500,
        )

        first_job = await self.first.create_health_job(
            kind=ProxyHealthJobKind.BOOTSTRAP,
            dedupe_key="scope:all",
            items=[ProxyStateSeed("p1", 0)],
            timestamp_ms=2000,
        )
        reset = await self.second.get_runtime("p1")

        self.assertEqual(reset.health_state, ProxyHealthState.UNKNOWN)
        self.assertEqual(reset.runtime_epoch, healthy.runtime_epoch + 1)
        self.assertEqual(await self.second.binding_counts(), {})

        restored = await self.second.compare_and_swap_runtime(
            reset,
            replace(reset, health_state=ProxyHealthState.HEALTHY),
        )
        reused_job = await self.second.create_health_job(
            kind=ProxyHealthJobKind.BOOTSTRAP,
            dedupe_key="scope:all",
            items=[ProxyStateSeed("p1", 0)],
            timestamp_ms=2100,
        )
        visible = await self.first.get_runtime("p1")

        self.assertEqual(reused_job.job_id, first_job.job_id)
        self.assertEqual(visible.health_state, ProxyHealthState.HEALTHY)
        self.assertEqual(visible.runtime_epoch, restored.runtime_epoch)


if __name__ == "__main__":
    unittest.main()
