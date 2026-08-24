import unittest
from unittest.mock import patch

from app.control.proxy.clearance import ProxyClearanceManager
from app.control.proxy.models import (
    ClearanceBundle,
    ClearanceBundleState,
    ProxyFeedback,
    ProxyFeedbackKind,
    ProxyProvider,
    ProxyScope,
    RequestKind,
)
from app.platform.runtime.clock import now_ms


class _ClearanceConfig:
    """提供 clearance 管理器使用的最小配置接口。"""

    def __init__(self, *, limit=2048, template=""):
        self.limit = limit
        self.template = template

    def get_str(self, key, default=""):
        """读取字符串配置。"""
        values = {
            "proxy.clearance.mode": "manual",
            "proxy.egress.mode": "resin",
            "proxy.resin.url_template": self.template,
        }
        return values.get(key, default)

    def get_int(self, key, default=0):
        """读取整数配置。"""
        if key == "proxy.clearance.max_cached_bundles":
            return self.limit
        if key == "proxy.clearance.refresh_interval":
            return 3600
        return default


class _ManualProvider:
    """生成带刷新时间的确定性手工 bundle。"""

    def build_bundle(self, *, affinity_key, clearance_host):
        """按出口身份和目标主机构造测试 bundle。"""
        return ClearanceBundle(
            bundle_id=f"{affinity_key}@{clearance_host}",
            cf_cookies=f"cookie-{clearance_host}",
            affinity_key=affinity_key,
            clearance_host=clearance_host,
            last_refresh_at=now_ms(),
        )


class ProxyClearanceTests(unittest.IsolatedAsyncioTestCase):
    async def _lease(self, manager, affinity, origin):
        """获取一个测试租约。"""
        return await manager.acquire_lease(
            proxy_url="http://proxy.test:8080",
            affinity_key=affinity,
            provider=ProxyProvider.RESIN,
            account_key="account",
            scope=ProxyScope.APP,
            kind=RequestKind.HTTP,
            clearance_origin=origin,
        )

    async def test_bundle_isolated_by_affinity_and_origin(self):
        """clearance 应同时按提供者、账号出口身份和 origin 隔离。"""
        manager = ProxyClearanceManager()
        manager._manual = _ManualProvider()
        cfg = _ClearanceConfig()

        with patch("app.control.proxy.clearance.get_config", return_value=cfg):
            first = await self._lease(manager, "resin:a", "https://grok.com")
            second = await self._lease(manager, "resin:a", "https://console.x.ai")
            third = await self._lease(manager, "resin:b", "https://grok.com")

        self.assertEqual(len(manager.bundles), 3)
        self.assertNotEqual(first.cf_cookies, second.cf_cookies)
        self.assertEqual(first.cf_cookies, third.cf_cookies)

    async def test_challenge_invalidates_only_matching_bundle(self):
        """挑战反馈只应使当前出口身份和目标主机的 bundle 失效。"""
        manager = ProxyClearanceManager()
        manager._manual = _ManualProvider()
        cfg = _ClearanceConfig()

        with patch("app.control.proxy.clearance.get_config", return_value=cfg):
            challenged = await self._lease(manager, "resin:a", "https://grok.com")
            await self._lease(manager, "resin:b", "https://grok.com")
            await manager.feedback(
                challenged,
                ProxyFeedback(kind=ProxyFeedbackKind.CHALLENGE),
            )

        states = {
            key[1]: bundle.state for key, bundle in manager.bundles.items()
        }
        self.assertEqual(states["resin:a"], ClearanceBundleState.INVALID)
        self.assertEqual(states["resin:b"], ClearanceBundleState.VALID)

    async def test_lru_limit_and_template_change_invalidation(self):
        """缓存应执行 LRU 上限，Resin 模板变化后旧 bundle 应失效。"""
        manager = ProxyClearanceManager()
        manager._manual = _ManualProvider()
        cfg = _ClearanceConfig(limit=2, template="https://a.{uuid}:x@proxy:8443")

        with patch("app.control.proxy.clearance.get_config", return_value=cfg):
            await self._lease(manager, "resin:a", "https://grok.com")
            await self._lease(manager, "resin:b", "https://grok.com")
            await self._lease(manager, "resin:c", "https://grok.com")
            self.assertEqual(len(manager.bundles), 2)
            cfg.template = "https://b.{uuid}:x@proxy:8443"
            await manager.load()

        self.assertTrue(
            all(
                bundle.state == ClearanceBundleState.INVALID
                for bundle in manager.bundles.values()
            )
        )

    async def test_managed_generation_change_evicts_old_bundles(self):
        """托管节点 generation 或 epoch 变化后应立即清理旧身份 bundle。"""
        manager = ProxyClearanceManager()
        manager._manual = _ManualProvider()
        cfg = _ClearanceConfig()

        with patch("app.control.proxy.clearance.get_config", return_value=cfg):
            for affinity in ("managed:p1:1:0", "managed:p1:2:0"):
                await manager.acquire_lease(
                    proxy_url="http://proxy.test:8080",
                    affinity_key=affinity,
                    provider=ProxyProvider.MANAGED_POOL,
                    account_key="account",
                    scope=ProxyScope.APP,
                    kind=RequestKind.HTTP,
                    clearance_origin="https://grok.com",
                )

        self.assertEqual(
            {key[1] for key in manager.bundles},
            {"managed:p1:2:0"},
        )


if __name__ == "__main__":
    unittest.main()
