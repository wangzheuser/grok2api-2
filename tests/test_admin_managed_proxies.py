import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.control.proxy.managed_pool import ProxyEntry
from app.platform.errors import ValidationError
from app.products.web.admin.proxies import (
    ProxyBatchEnabledRequest,
    ProxyBatchRequest,
    ProxySettingsRequest,
    delete_selected_managed_proxies,
    set_selected_managed_proxies_enabled,
    test_selected_managed_proxies as enqueue_selected_managed_proxy_test,
    update_proxy_settings,
)


class _SettingsConfig:
    """提供设置接口测试所需的最小可变配置快照。"""

    def __init__(self, *, entries=None):
        self.current = {
            "proxy": {
                "egress": {
                    "mode": "direct",
                    "skip_ssl_verify": False,
                },
                "resin": {"url_template": ""},
                "pool": {"entries": entries or []},
                "health": {"concurrency": 20},
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

    def get_str(self, path, default=""):
        """按点路径读取字符串配置。"""
        value = self.current
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                return default
            value = value[part]
        return str(value)

    async def update(self, patch_value):
        """记录补丁并模拟配置后端合并。"""
        self.saved = patch_value
        for section, section_value in patch_value["proxy"].items():
            self.current["proxy"].setdefault(section, {}).update(
                section_value
            )

    async def load(self):
        """模拟重新加载配置快照。"""


class AdminManagedProxyBatchTests(unittest.IsolatedAsyncioTestCase):
    """托管代理批量管理接口测试。"""

    def test_batch_request_deduplicates_ids_in_input_order(self):
        """批量请求应按首次出现顺序清理重复 ID。"""
        request = ProxyBatchRequest(proxy_ids=[" p2 ", "p1", "p2"])

        self.assertEqual(request.proxy_ids, ["p2", "p1"])

    async def test_batch_test_enqueues_one_manual_selection_job(self):
        """批量测试应把所选条目放入同一个共享任务。"""
        entries = [
            ProxyEntry(id="p1", url="http://proxy1:8080"),
            ProxyEntry(id="p2", url="http://proxy2:8080"),
        ]
        pool = AsyncMock()
        pool.selected_entries.return_value = entries
        scheduler = AsyncMock()
        scheduler.enqueue.return_value = SimpleNamespace(job_id="job-1")
        request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(proxy_health_scheduler=scheduler)
            )
        )

        with patch(
            "app.products.web.admin.proxies.get_managed_proxy_pool",
            new=AsyncMock(return_value=pool),
        ):
            result = await enqueue_selected_managed_proxy_test(
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
            "app.products.web.admin.proxies.get_managed_proxy_pool",
            new=AsyncMock(return_value=pool),
        ), self.assertRaises(ValidationError) as caught:
            await delete_selected_managed_proxies(
                ProxyBatchRequest(proxy_ids=["missing"])
            )

        self.assertEqual(caught.exception.param, "proxy_ids")
        self.assertEqual(caught.exception.code, "proxy_not_found")
        pool.remove_entries.assert_not_awaited()

    async def test_batch_enable_returns_changed_counts_and_one_job(self):
        """批量启用应返回变更计数并只创建一个增量任务。"""
        entry = ProxyEntry(id="p1", url="http://proxy1:8080")
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
                state=SimpleNamespace(proxy_health_scheduler=scheduler)
            )
        )

        with patch(
            "app.products.web.admin.proxies.get_managed_proxy_pool",
            new=AsyncMock(return_value=pool),
        ):
            result = await set_selected_managed_proxies_enabled(
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

    async def test_settings_switches_to_managed_pool_and_bootstraps(self):
        """设置入口切入托管池后应热加载并创建一次引导任务。"""
        fake_config = _SettingsConfig(
            entries=[{"id": "p1", "url": "http://proxy1:8080"}],
        )
        pool = AsyncMock()
        service = AsyncMock()
        scheduler = AsyncMock()
        scheduler.enqueue.return_value = SimpleNamespace(job_id="job-bootstrap")
        request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(proxy_health_scheduler=scheduler)
            )
        )
        with patch("app.products.web.admin.config", fake_config), patch(
            "app.products.web.admin.proxies.config",
            fake_config,
        ), patch(
            "app.products.web.admin.proxies.get_managed_proxy_pool",
            new=AsyncMock(return_value=pool),
        ), patch(
            "app.products.web.admin.proxies.get_proxy_service",
            new=AsyncMock(return_value=service),
        ):
            result = await update_proxy_settings(
                ProxySettingsRequest(mode="managed_pool"),
                request,
            )

        self.assertEqual(result["mode"], "managed_pool")
        self.assertEqual(
            fake_config.saved["proxy"]["egress"]["mode"],
            "managed_pool",
        )
        self.assertEqual(result["job_id"], "job-bootstrap")
        service.reload_config.assert_awaited_once_with(
            load_managed_pool=True,
        )

    async def test_resin_settings_save_skips_managed_pool_reload(self):
        """保存 Resin 模板时不应等待无关的托管池共享状态。"""
        fake_config = _SettingsConfig()
        service = AsyncMock()
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
        template = "http://node.{uuid}:test-token@172.17.0.1:9200"

        with patch("app.products.web.admin.config", fake_config), patch(
            "app.products.web.admin.proxies.config",
            fake_config,
        ), patch(
            "app.products.web.admin.proxies.get_proxy_service",
            new=AsyncMock(return_value=service),
        ):
            result = await update_proxy_settings(
                ProxySettingsRequest(
                    mode="resin",
                    resin_url_template=template,
                ),
                request,
            )

        self.assertEqual(result["mode"], "resin")
        self.assertEqual(
            fake_config.saved["proxy"]["resin"]["url_template"],
            template,
        )
        self.assertEqual(
            result["resin_url_template"],
            "http://node.{uuid}:***@172.17.0.1:9200",
        )
        self.assertIsNone(result["job_id"])
        service.reload_config.assert_awaited_once_with(
            load_managed_pool=False,
        )


if __name__ == "__main__":
    unittest.main()
