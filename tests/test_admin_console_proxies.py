import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.control.proxy.console_pool import ConsoleProxyEntry
from app.platform.errors import ValidationError
from app.products.web.admin.proxies import (
    ProxyBatchEnabledRequest,
    ProxyBatchRequest,
    ProxySettingsRequest,
    delete_selected_console_proxies,
    set_selected_console_proxies_enabled,
    test_selected_console_proxies as enqueue_selected_console_proxy_test,
    update_console_proxy_settings,
)


class AdminConsoleProxyBatchTests(unittest.IsolatedAsyncioTestCase):
    """Console 代理批量管理接口测试。"""

    def test_batch_request_deduplicates_ids_in_input_order(self):
        """批量请求应按首次出现顺序清理重复 ID。"""
        request = ProxyBatchRequest(proxy_ids=[" p2 ", "p1", "p2"])

        self.assertEqual(request.proxy_ids, ["p2", "p1"])

    async def test_batch_test_enqueues_one_manual_selection_job(self):
        """批量测试应把所选条目放入同一个共享任务。"""
        entries = [
            ConsoleProxyEntry(id="p1", url="http://proxy1:8080"),
            ConsoleProxyEntry(id="p2", url="http://proxy2:8080"),
        ]
        pool = AsyncMock()
        pool.selected_entries.return_value = entries
        scheduler = AsyncMock()
        scheduler.enqueue.return_value = SimpleNamespace(job_id="job-1")
        request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(console_proxy_health_scheduler=scheduler)
            )
        )

        with patch(
            "app.products.web.admin.proxies.get_console_proxy_pool",
            new=AsyncMock(return_value=pool),
        ):
            result = await enqueue_selected_console_proxy_test(
                ProxyBatchRequest(proxy_ids=["p1", "p2"]),
                request,
            )

        self.assertEqual(result["selected"], 2)
        self.assertEqual(result["job_id"], "job-1")
        scheduler.enqueue.assert_awaited_once()
        self.assertEqual(scheduler.enqueue.await_args.kwargs["entries"], entries)

    async def test_unknown_batch_id_maps_to_stable_validation_error(self):
        """未知 ID 应在任何删除动作前映射为稳定字段错误。"""
        pool = AsyncMock()
        pool.selected_entries.side_effect = KeyError(("missing",))

        with patch(
            "app.products.web.admin.proxies.get_console_proxy_pool",
            new=AsyncMock(return_value=pool),
        ), self.assertRaises(ValidationError) as caught:
            await delete_selected_console_proxies(
                ProxyBatchRequest(proxy_ids=["missing"])
            )

        self.assertEqual(caught.exception.param, "proxy_ids")
        self.assertEqual(caught.exception.code, "proxy_not_found")
        pool.remove_entries.assert_not_awaited()

    async def test_batch_enable_returns_changed_counts_and_one_job(self):
        """批量启用应返回变更计数并只创建一个增量任务。"""
        entry = ConsoleProxyEntry(id="p1", url="http://proxy1:8080")
        pool = AsyncMock()
        pool.selected_entries.return_value = [entry]
        pool.set_entries_enabled.return_value = SimpleNamespace(
            entries=(entry,),
            changed=1,
            unchanged=1,
        )
        scheduler = AsyncMock()
        scheduler.enqueue.return_value = SimpleNamespace(job_id="job-2")
        request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(console_proxy_health_scheduler=scheduler)
            )
        )

        with patch(
            "app.products.web.admin.proxies.get_console_proxy_pool",
            new=AsyncMock(return_value=pool),
        ):
            result = await set_selected_console_proxies_enabled(
                ProxyBatchEnabledRequest(
                    proxy_ids=["p1", "p2"],
                    enabled=True,
                ),
                request,
            )

        self.assertEqual(result["changed"], 1)
        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["job_id"], "job-2")
        scheduler.enqueue.assert_awaited_once()

    async def test_settings_enable_auto_disables_stale_direct_fallback(self):
        """代理池设置入口应持久化服务端规范化后的回退值。"""

        class _Config:
            """提供设置接口测试所需的最小可变配置快照。"""

            def __init__(self):
                self.current = {
                    "proxy": {"egress": {"mode": "direct"}},
                    "console": {
                        "proxy_pool": {
                            "enabled": False,
                            "fallback_to_global_proxy": True,
                        }
                    },
                }
                self.saved = None

            def raw(self):
                """返回当前完整配置。"""
                return self.current

            def get_bool(self, path, default=False):
                """按点路径读取布尔配置。"""
                value = self.current
                for part in path.split("."):
                    if not isinstance(value, dict) or part not in value:
                        return default
                    value = value[part]
                return bool(value)

            async def update(self, patch_value):
                """记录补丁并模拟配置后端合并。"""
                self.saved = patch_value
                self.current["console"]["proxy_pool"].update(
                    patch_value["console"]["proxy_pool"]
                )

            async def load(self):
                """模拟重新加载配置快照。"""

        fake_config = _Config()
        pool = AsyncMock()
        scheduler = AsyncMock()
        scheduler.enqueue.return_value = SimpleNamespace(job_id="job-bootstrap")
        request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(console_proxy_health_scheduler=scheduler)
            )
        )
        with patch("app.products.web.admin.config", fake_config), patch(
            "app.products.web.admin.proxies.config",
            fake_config,
        ), patch(
            "app.products.web.admin.proxies.get_console_proxy_pool",
            new=AsyncMock(return_value=pool),
        ):
            result = await update_console_proxy_settings(
                ProxySettingsRequest(enabled=True),
                request,
            )

        self.assertTrue(result["enabled"])
        self.assertFalse(result["fallback_to_global_proxy"])
        self.assertTrue(result["fallback_auto_disabled"])
        self.assertFalse(
            fake_config.saved["console"]["proxy_pool"][
                "fallback_to_global_proxy"
            ]
        )
        self.assertEqual(result["job_id"], "job-bootstrap")


if __name__ == "__main__":
    unittest.main()
