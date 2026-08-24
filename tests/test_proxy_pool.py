import unittest
from unittest.mock import AsyncMock, patch

from app.control.proxy.managed_pool import account_key_for_token
from app.control.proxy.models import (
    EgressMode,
    ProxyLease,
    ProxyProvider,
    ProxyRequestContext,
    ProxyScope,
    RequestKind,
)
from app.control.proxy.service import (
    HEALTH_ACCOUNT_KEY,
    DirectProxyProvider,
    ResinProxyProvider,
    ProxyService,
    render_resin_proxy_url,
    resin_uuid_for_account,
)


class _ProxyConfig:
    """为代理提供者暴露最小配置快照接口。"""

    def __init__(self, data):
        self._data = data

    def get_str(self, key, default=""):
        """读取测试配置字符串。"""
        node = self._data
        for part in key.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return str(node)

    def raw(self):
        """返回完整测试配置。"""
        return self._data


class UnifiedProxyProviderTests(unittest.IsolatedAsyncioTestCase):
    def test_resin_uuid_is_stable_and_token_is_not_exposed(self):
        """相同归一化账号应得到同一 UUID，UUID 中不出现原始 token。"""
        plain = account_key_for_token("secret-token")
        prefixed = account_key_for_token("sso=secret-token")

        first = resin_uuid_for_account(plain)
        second = resin_uuid_for_account(prefixed)

        self.assertEqual(first, second)
        self.assertNotIn("secret-token", first)
        self.assertEqual(resin_uuid_for_account(HEALTH_ACCOUNT_KEY), resin_uuid_for_account(HEALTH_ACCOUNT_KEY))

    def test_resin_template_replaces_every_uuid_consistently(self):
        """同一模板中的全部 UUID 占位符应使用同一账号身份。"""
        rendered = render_resin_proxy_url(
            "https://node.{uuid}:token@proxy.test:8443/path/{uuid}",
            "account-key",
        )
        expected = resin_uuid_for_account("account-key")

        self.assertEqual(rendered.count(expected), 2)
        self.assertNotIn("{uuid}", rendered)

    async def test_direct_provider_returns_direct_lease(self):
        """直连提供者应通过统一 clearance 门面生成无代理租约。"""
        clearance = AsyncMock()
        clearance.acquire_lease.return_value = ProxyLease(lease_id="direct")
        context = ProxyRequestContext(
            account_key="account",
            origin="https://grok.com",
            scope=ProxyScope.APP,
            kind=RequestKind.HTTP,
        )

        lease = await DirectProxyProvider(clearance).acquire(context)

        self.assertFalse(lease.has_proxy)
        clearance.acquire_lease.assert_awaited_once_with(
            proxy_url=None,
            affinity_key="direct",
            provider=ProxyProvider.DIRECT,
            account_key="account",
            scope=ProxyScope.APP,
            kind=RequestKind.HTTP,
            clearance_origin="https://grok.com",
        )

    async def test_resin_provider_uses_account_affinity(self):
        """Resin 提供者应渲染稳定 UUID 并只返回 Resin 租约。"""
        config = _ProxyConfig(
            {
                "proxy": {
                    "egress": {"mode": "resin"},
                    "resin": {
                        "url_template": "https://node.{uuid}:token@proxy.test:8443"
                    },
                    "pool": {"entries": []},
                    "health": {"concurrency": 20},
                }
            }
        )
        clearance = AsyncMock()
        clearance.acquire_lease.side_effect = lambda **kwargs: ProxyLease(
            lease_id="resin",
            proxy_url=kwargs["proxy_url"],
            provider=kwargs["provider"],
            affinity_key=kwargs["affinity_key"],
            origin=kwargs["clearance_origin"],
        )
        context = ProxyRequestContext(
            account_key="account",
            origin="https://console.x.ai",
            scope=ProxyScope.APP,
            kind=RequestKind.WEBSOCKET,
        )

        provider = ResinProxyProvider(clearance)
        with patch("app.control.proxy.service.get_config", return_value=config):
            lease = await provider.acquire(context)
            cross_scope = await provider.acquire(
                ProxyRequestContext(
                    account_key="account",
                    origin="https://assets.grok.com",
                    scope=ProxyScope.ASSET,
                    kind=RequestKind.HTTP,
                )
            )
            other_account = await provider.acquire(
                ProxyRequestContext(
                    account_key="other-account",
                    origin="https://grok.com",
                )
            )

        expected_uuid = resin_uuid_for_account("account")
        self.assertEqual(lease.provider, ProxyProvider.RESIN)
        self.assertEqual(lease.affinity_key, f"resin:{expected_uuid}")
        self.assertIn(expected_uuid, lease.proxy_url)
        self.assertEqual(lease.origin, "https://console.x.ai")
        self.assertEqual(cross_scope.affinity_key, lease.affinity_key)
        self.assertNotEqual(other_account.affinity_key, lease.affinity_key)

    async def test_derived_origin_keeps_same_provider_identity(self):
        """复合子步骤应只派生 origin clearance，不重新选择出口。"""
        clearance = AsyncMock()
        clearance.acquire_lease.return_value = ProxyLease(
            lease_id="derived",
            proxy_url="https://node.account:token@proxy.test:8443",
            provider=ProxyProvider.RESIN,
            affinity_key="resin:stable",
            origin="https://assets.grok.com",
        )
        pool = AsyncMock()
        service = ProxyService(pool, clearance)
        parent = ProxyLease(
            lease_id="parent",
            proxy_url="https://node.account:token@proxy.test:8443",
            provider=ProxyProvider.RESIN,
            affinity_key="resin:stable",
            account_key="account",
            proxy_id="resin-gateway",
            proxy_mode="uuid_template",
            generation=7,
            runtime_epoch=2,
            origin="https://grok.com",
        )

        derived = await service.derive(
            parent,
            origin="https://assets.grok.com",
            scope=ProxyScope.ASSET,
            kind=RequestKind.HTTP,
        )

        self.assertEqual(derived.proxy_url, parent.proxy_url)
        self.assertEqual(derived.affinity_key, parent.affinity_key)
        self.assertEqual(derived.proxy_id, parent.proxy_id)
        self.assertEqual(derived.generation, parent.generation)
        self.assertEqual(derived.runtime_epoch, parent.runtime_epoch)
        clearance.acquire_lease.assert_awaited_once_with(
            proxy_url=parent.proxy_url,
            affinity_key=parent.affinity_key,
            provider=parent.provider,
            account_key=parent.account_key,
            scope=ProxyScope.ASSET,
            kind=RequestKind.HTTP,
            clearance_origin="https://assets.grok.com",
        )

    async def test_resin_hot_reload_does_not_load_managed_pool(self):
        """Resin 热切换只加载路由与 clearance，不同步托管池仓储。"""
        config = _ProxyConfig(
            {
                "proxy": {
                    "egress": {"mode": "resin"},
                    "resin": {
                        "url_template": (
                            "http://node.{uuid}:token@172.17.0.1:9200"
                        )
                    },
                    "pool": {"entries": []},
                    "health": {"concurrency": 20},
                }
            }
        )
        pool = AsyncMock()
        clearance = AsyncMock()
        service = ProxyService(pool, clearance)

        with patch("app.control.proxy.service.get_config", return_value=config):
            await service.reload_config(load_managed_pool=False)

        self.assertEqual(service.mode, EgressMode.RESIN)
        pool.load.assert_not_awaited()
        clearance.load.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
