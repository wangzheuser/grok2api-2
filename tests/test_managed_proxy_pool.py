import unittest
from dataclasses import replace
from unittest.mock import AsyncMock, patch

from app.control.proxy.managed_pool import (
    ProxyEntry,
    ManagedProxyPool,
    account_key_for_token,
)
from app.control.proxy.managed_state import (
    ProxyHealthState,
    ProxyProbeOutcome,
    InMemoryManagedProxyStateRepository,
)
from app.control.proxy.models import (
    ProxyFeedback,
    ProxyFeedbackKind,
    ProxyLease,
)
from app.platform.errors import UpstreamError


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


async def _fallback_lease_factory(**kwargs):
    """返回包含 override 的本地租约。"""
    return ProxyLease(
        lease_id="lease",
        proxy_url=kwargs.get("proxy_url"),
    )


class ManagedProxyPoolTests(unittest.IsolatedAsyncioTestCase):
    async def _pool(self, cfg):
        """构造并初始化共享内存代理池。"""
        pool = ManagedProxyPool(InMemoryManagedProxyStateRepository())
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await pool.initialize()
        return pool

    async def _mark_all_healthy(self, pool, cfg):
        """把配置中的全部节点通过健康门禁。"""
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            for entry in await pool.entries(include_secret=True):
                await pool.record_health_result(
                    entry.id,
                    generation=entry.generation,
                    outcome=ProxyProbeOutcome.HEALTHY,
                    message="HTTP 200",
                    latency_ms=10,
                )

    async def test_unknown_proxy_is_not_schedulable(self):
        """新节点在首次检测通过前应失败关闭。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            with self.assertRaises(UpstreamError) as caught:
                await pool.acquire(
                    account_key=account_key_for_token("token-a"),
                    lease_factory=_fallback_lease_factory,
                )
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(caught.exception.code, "egress_proxy_unavailable")

    async def test_legacy_console_switch_does_not_change_managed_pool(self):
        """旧 Console 总开关退出运行时后不应影响托管池调度。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        await self._mark_all_healthy(pool, cfg)
        fallback = AsyncMock(
            side_effect=_fallback_lease_factory
        )

        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            lease = await pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=fallback,
            )

        self.assertEqual(lease.proxy_url, "http://proxy1:8080")
        fallback.assert_awaited_once()

    async def test_managed_pool_never_falls_back_to_direct(self):
        """Console 池无健康节点时，即使开启回退也不得产生直连租约。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)

        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            with self.assertRaises(UpstreamError) as caught:
                await pool.acquire(
                    account_key=account_key_for_token("token-a"),
                    lease_factory=_fallback_lease_factory,
                )

        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(caught.exception.code, "egress_proxy_unavailable")

    async def test_managed_pool_never_falls_back_to_legacy_global_proxy(self):
        """托管池无健康节点时应保持失败关闭并忽略旧回退字段。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        fallback = AsyncMock(side_effect=_fallback_lease_factory)

        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            with self.assertRaises(UpstreamError) as caught:
                await pool.acquire(
                    account_key=account_key_for_token("token-a"),
                    lease_factory=fallback,
                )

        self.assertEqual(caught.exception.code, "egress_proxy_unavailable")
        fallback.assert_not_awaited()

    async def test_console_override_works_independently_of_global_mode(self):
        """健康 Console 节点应始终通过 override 进入统一 Session 层。"""
        cfg = _PoolConfig(
            {
                "proxy.egress.mode": "direct",
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        await self._mark_all_healthy(pool, cfg)
        fallback = AsyncMock(side_effect=_fallback_lease_factory)

        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            lease = await pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=fallback,
            )

        self.assertEqual(lease.proxy_url, "http://proxy1:8080")
        self.assertEqual(lease.provider.value, "managed_pool")
        self.assertEqual(
            fallback.await_args.kwargs["proxy_url"],
            "http://proxy1:8080",
        )

    async def test_shared_state_failure_returns_stable_503(self):
        """共享状态读取异常时应失败关闭并返回专用错误码。"""
        class FailingRepository(InMemoryManagedProxyStateRepository):
            async def acquire_binding(self, *args, **kwargs):
                """模拟共享存储在请求阶段不可用。"""
                raise RuntimeError("state backend down")

        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = ManagedProxyPool(FailingRepository())
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await pool.initialize()
            with self.assertRaises(UpstreamError) as caught:
                await pool.acquire(
                    account_key=account_key_for_token("token-a"),
                    lease_factory=_fallback_lease_factory,
                )

        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(
            caught.exception.code,
            "egress_proxy_state_unavailable",
        )

    async def test_static_proxy_uses_shared_account_sticky_binding(self):
        """同一共享仓储中的两个池实例应复用账号绑定。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"},
                    {"id": "p2", "url": "http://proxy2:8080"},
                ],
            }
        )
        repo = InMemoryManagedProxyStateRepository()
        first_pool = ManagedProxyPool(repo)
        second_pool = ManagedProxyPool(repo)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await first_pool.initialize()
            await second_pool.initialize()
            await self._mark_all_healthy(first_pool, cfg)
            first = await first_pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=_fallback_lease_factory,
            )
            second = await second_pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=_fallback_lease_factory,
            )
        self.assertEqual(first.proxy_id, second.proxy_id)
        self.assertEqual(first.proxy_url, second.proxy_url)

    async def test_transport_error_is_visible_to_other_pool_instance(self):
        """一个 Worker 标记失败后另一个 Worker应立即改绑。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.static_cooldown_sec": 60,
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"},
                    {"id": "p2", "url": "http://proxy2:8080"},
                ],
            }
        )
        repo = InMemoryManagedProxyStateRepository()
        first_pool = ManagedProxyPool(repo)
        second_pool = ManagedProxyPool(repo)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await first_pool.initialize()
            await second_pool.initialize()
            await self._mark_all_healthy(first_pool, cfg)
            first = await first_pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=_fallback_lease_factory,
            )
            await first_pool.feedback(
                first,
                ProxyFeedback(
                    kind=ProxyFeedbackKind.TRANSPORT_ERROR,
                    reason="connect failed",
                ),
            )
            second = await second_pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=_fallback_lease_factory,
            )
        self.assertNotEqual(first.proxy_id, second.proxy_id)

    async def test_account_errors_do_not_poison_managed_proxy_health(self):
        """账号 401、限流和上游业务错误不应改变代理健康状态。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        await self._mark_all_healthy(pool, cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            lease = await pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=_fallback_lease_factory,
            )
            for feedback in (
                ProxyFeedback(
                    kind=ProxyFeedbackKind.UNAUTHORIZED,
                    status_code=401,
                ),
                ProxyFeedback(
                    kind=ProxyFeedbackKind.RATE_LIMITED,
                    status_code=429,
                ),
                ProxyFeedback(
                    kind=ProxyFeedbackKind.UPSTREAM_5XX,
                    status_code=503,
                ),
            ):
                await pool.feedback(lease, feedback)
            runtime = await pool.state_repository.get_runtime("p1")

        self.assertEqual(runtime.health_state, ProxyHealthState.HEALTHY)
        self.assertEqual(runtime.failure_count, 0)

    async def test_dynamic_template_infers_password_placeholder(self):
        """密码中的 time 占位符也应自动推断为动态模板。"""
        entry = ProxyEntry(
            url="http://proxy:8080",
            username="user",
            password="pass-{time}",
        )
        self.assertEqual(entry.inferred_mode().value, "dynamic_template")

    async def test_inconclusive_probe_does_not_restore_unknown_proxy(self):
        """429/5xx 等不确定结果不应绕过严格健康门禁。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await pool.record_health_result(
                "p1",
                generation=0,
                outcome=ProxyProbeOutcome.INCONCLUSIVE,
                message="HTTP 429",
                latency_ms=10,
            )
            runtime = await pool.state_repository.get_runtime("p1")
        self.assertEqual(runtime.health_state, ProxyHealthState.UNKNOWN)

    async def test_health_failure_does_not_increment_request_failure_count(self):
        """主动检测失败应冷却节点但不混入请求失败计数。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.static_cooldown_sec": 60,
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await pool.record_health_result(
                "p1",
                generation=0,
                outcome=ProxyProbeOutcome.UNHEALTHY,
                message="connect failed",
                latency_ms=123,
            )
            runtime = await pool.state_repository.get_runtime("p1")
        self.assertEqual(runtime.health_state, ProxyHealthState.COOLING_DOWN)
        self.assertEqual(runtime.failure_count, 0)
        self.assertEqual(runtime.health_failure_count, 1)

    async def test_expired_cooldown_becomes_unknown_not_healthy(self):
        """冷却到期后节点必须回到 unknown 并等待重新探测。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.static_cooldown_sec": 1,
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        await self._mark_all_healthy(pool, cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            lease = await pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=_fallback_lease_factory,
            )
            await pool.mark_failure(lease, "connect failed")
            runtime = await pool.state_repository.get_runtime("p1")
            expired = await pool.state_repository.compare_and_swap_runtime(
                runtime,
                replace(runtime, next_retry_at=1),
            )
            self.assertIsNotNone(expired)
            snapshot = await pool.snapshot()

        self.assertEqual(snapshot["items"][0]["status"], "unknown")

    async def test_dead_proxy_requires_reset_before_recovery(self):
        """HTTP 407 进入 dead 后，普通健康结果不得直接恢复节点。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await pool.record_health_result(
                "p1",
                generation=0,
                outcome=ProxyProbeOutcome.UNHEALTHY,
                message="HTTP 407",
                latency_ms=10,
                status_code=407,
            )
            await pool.record_health_result(
                "p1",
                generation=0,
                outcome=ProxyProbeOutcome.HEALTHY,
                message="HTTP 200",
                latency_ms=10,
            )
            runtime = await pool.state_repository.get_runtime("p1")
            reset = await pool.reset_entry("p1")
            reset_runtime = await pool.state_repository.get_runtime("p1")
            await pool.record_health_result(
                "p1",
                generation=0,
                outcome=ProxyProbeOutcome.HEALTHY,
                message="HTTP 200",
                latency_ms=10,
            )
            recovered = await pool.state_repository.get_runtime("p1")

        self.assertEqual(runtime.health_state, ProxyHealthState.DEAD)
        self.assertTrue(reset)
        self.assertEqual(reset_runtime.health_state, ProxyHealthState.UNKNOWN)
        self.assertEqual(recovered.health_state, ProxyHealthState.HEALTHY)

    async def test_active_cooldown_cannot_be_recovered_early(self):
        """冷却未到期时，即使探测返回 200 也应继续保持冷却。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.static_cooldown_sec": 60,
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"}
                ],
            }
        )
        pool = await self._pool(cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await pool.record_health_result(
                "p1",
                generation=0,
                outcome=ProxyProbeOutcome.UNHEALTHY,
                message="HTTP 403",
                latency_ms=10,
            )
            await pool.record_health_result(
                "p1",
                generation=0,
                outcome=ProxyProbeOutcome.HEALTHY,
                message="HTTP 200",
                latency_ms=10,
            )
            runtime = await pool.state_repository.get_runtime("p1")

        self.assertEqual(runtime.health_state, ProxyHealthState.COOLING_DOWN)

    async def test_runtime_error_and_snapshot_never_expose_proxy_password(self):
        """健康错误与管理快照中都不应出现代理明文密码。"""
        secret = "proxy-secret"
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {
                        "id": "p1",
                        "url": "http://proxy1:8080",
                        "username": "user",
                        "password": secret,
                    }
                ],
            }
        )
        pool = await self._pool(cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            await pool.record_health_result(
                "p1",
                generation=0,
                outcome=ProxyProbeOutcome.UNHEALTHY,
                message=f"connect http://user:{secret}@proxy1:8080 failed",
                latency_ms=10,
            )
            snapshot = await pool.snapshot()

        self.assertNotIn(secret, repr(snapshot))
        self.assertIn("***", snapshot["items"][0]["last_error"])

    async def test_duplicate_endpoint_updates_existing_entry(self):
        """相同端点和用户名再次导入时应更新而不是追加。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {
                        "id": "p1",
                        "url": "http://proxy1:8080",
                        "username": "user",
                        "password": "old",
                    }
                ]
            }
        )
        pool = await self._pool(cfg)
        incoming = ProxyEntry(
            url="http://proxy1:8080",
            username="user",
            password="new",
        )
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg), patch.object(
            pool,
            "replace_entries",
            new=AsyncMock(),
        ) as replace_entries:
            result = await pool.add_entries([incoming])
        self.assertEqual(result.added, 0)
        self.assertEqual(result.updated, 1)
        self.assertEqual(result.entries[0].id, "p1")
        self.assertEqual(result.entries[0].generation, 1)
        replace_entries.assert_awaited_once()

    async def test_batch_enable_updates_changed_entries_in_one_write(self):
        """批量启用应只更新禁用节点，并且只持久化一次。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {
                        "id": "p1",
                        "url": "http://proxy1:8080",
                        "password": "secret",
                        "enabled": False,
                    },
                    {
                        "id": "p2",
                        "url": "http://proxy2:8080",
                        "enabled": True,
                    },
                ]
            }
        )
        pool = await self._pool(cfg)
        with patch(
            "app.control.proxy.managed_pool.get_config",
            return_value=cfg,
        ), patch.object(
            pool,
            "replace_entries",
            new=AsyncMock(),
        ) as replace_entries:
            result = await pool.set_entries_enabled(
                ["p1", "p2", "p1"],
                True,
            )

        self.assertEqual(result.changed, 1)
        self.assertEqual(result.unchanged, 1)
        self.assertEqual(result.entries[0].id, "p1")
        self.assertEqual(result.entries[0].generation, 1)
        self.assertEqual(result.entries[0].password, "secret")
        replace_entries.assert_awaited_once()

    async def test_batch_delete_validates_all_ids_before_write(self):
        """批量删除混入未知 ID 时不得产生部分配置变更。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"},
                    {"id": "p2", "url": "http://proxy2:8080"},
                ]
            }
        )
        pool = await self._pool(cfg)
        with patch(
            "app.control.proxy.managed_pool.get_config",
            return_value=cfg,
        ), patch.object(
            pool,
            "replace_entries",
            new=AsyncMock(),
        ) as replace_entries:
            with self.assertRaises(KeyError):
                await pool.remove_entries(["p1", "missing"])
            deleted = await pool.remove_entries(["p1", "p1"])

        self.assertEqual(deleted, 1)
        remaining = replace_entries.await_args.args[0]
        self.assertEqual([entry.id for entry in remaining], ["p2"])
        self.assertEqual(replace_entries.await_count, 1)

    async def test_batch_reset_preserves_unselected_runtime_and_clears_binding(self):
        """批量重置应沿用单节点语义且不影响未选节点。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"},
                    {"id": "p2", "url": "http://proxy2:8080"},
                ],
            }
        )
        pool = await self._pool(cfg)
        await self._mark_all_healthy(pool, cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            lease = await pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=_fallback_lease_factory,
            )
            reset = await pool.reset_entries([lease.proxy_id])
            reset_runtime = await pool.state_repository.get_runtime(lease.proxy_id)
            other_id = "p2" if lease.proxy_id == "p1" else "p1"
            other_runtime = await pool.state_repository.get_runtime(other_id)
            counts = await pool.state_repository.binding_counts()

        self.assertEqual([entry.id for entry in reset], [lease.proxy_id])
        self.assertEqual(reset_runtime.health_state, ProxyHealthState.UNKNOWN)
        self.assertEqual(other_runtime.health_state, ProxyHealthState.HEALTHY)
        self.assertEqual(counts, {})

    async def test_batch_clear_bindings_only_affects_selected_nodes(self):
        """批量解绑应保留未选代理上的账号绑定。"""
        cfg = _PoolConfig(
            {
                "proxy.pool.entries": [
                    {"id": "p1", "url": "http://proxy1:8080"},
                    {"id": "p2", "url": "http://proxy2:8080"},
                ],
            }
        )
        pool = await self._pool(cfg)
        await self._mark_all_healthy(pool, cfg)
        with patch("app.control.proxy.managed_pool.get_config", return_value=cfg):
            first = await pool.acquire(
                account_key=account_key_for_token("token-a"),
                lease_factory=_fallback_lease_factory,
            )
            second = await pool.acquire(
                account_key=account_key_for_token("token-b"),
                lease_factory=_fallback_lease_factory,
            )
            cleared = await pool.clear_entry_bindings([first.proxy_id])
            counts = await pool.state_repository.binding_counts()

        self.assertNotEqual(first.proxy_id, second.proxy_id)
        self.assertEqual(cleared, 1)
        self.assertEqual(counts, {second.proxy_id: 1})

    def test_account_key_is_stable_and_secret_free(self):
        """账号绑定键应稳定且不包含原 token。"""
        first = account_key_for_token("sso=secret")
        second = account_key_for_token("secret")
        self.assertEqual(first, second)
        self.assertNotIn("secret", first)


if __name__ == "__main__":
    unittest.main()
