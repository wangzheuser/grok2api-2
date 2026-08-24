import unittest
from unittest.mock import AsyncMock, patch

from app.control.proxy.models import ProxyLease, ProxyScope, RequestKind
from app.dataplane.reverse.transport.assets import (
    delete_asset,
    download_asset,
    list_assets,
)


class _AssetConfig:
    def get_float(self, key, default=0.0):
        """返回测试默认超时。"""
        return default


class AssetProxyRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_asset_calls_use_same_account_scoped_proxy_service(self):
        """资源操作应共用账号身份，只用 origin 区分 clearance 上下文。"""
        runtime = AsyncMock()
        runtime.acquire.return_value = ProxyLease(lease_id="lease")
        runtime.feedback.return_value = None

        async def stream():
            """返回一段本地二进制流。"""
            yield b"data"

        def test_config(key=None, default=None):
            """同时兼容快照对象和快捷键两种配置读取方式。"""
            return _AssetConfig() if key is None else default

        with patch(
            "app.dataplane.reverse.transport.assets.get_config",
            side_effect=test_config,
        ), patch(
            "app.dataplane.reverse.transport.assets.get_proxy_runtime",
            return_value=runtime,
        ), patch(
            "app.dataplane.reverse.transport.assets.get_json",
            new=AsyncMock(return_value={}),
        ), patch(
            "app.dataplane.reverse.transport.assets.delete_json",
            new=AsyncMock(return_value={}),
        ), patch(
            "app.dataplane.reverse.transport.assets.get_bytes_stream",
            new=AsyncMock(return_value=stream()),
        ):
            await list_assets("token")
            await delete_asset("token", "asset-id")
            await download_asset("token", "https://assets.grok.com/file.png")

        calls = runtime.acquire.await_args_list
        self.assertEqual(len(calls), 3)
        self.assertEqual(
            calls[0].kwargs,
            {
                "token": "token",
                "scope": ProxyScope.ASSET,
                "kind": RequestKind.HTTP,
                "clearance_origin": "https://grok.com",
            },
        )
        self.assertEqual(
            calls[1].kwargs,
            {
                "token": "token",
                "scope": ProxyScope.ASSET,
                "kind": RequestKind.HTTP,
                "clearance_origin": "https://grok.com",
            },
        )
        self.assertEqual(
            calls[2].kwargs,
            {
                "scope": ProxyScope.ASSET,
                "kind": RequestKind.HTTP,
                "token": "token",
                "clearance_origin": "https://assets.grok.com",
            },
        )

    async def test_composite_asset_steps_reuse_supplied_lease(self):
        """复合流程传入父租约后，各资源子步骤不应再次申请出口。"""
        runtime = AsyncMock()
        runtime.feedback.return_value = None
        shared_lease = ProxyLease(lease_id="shared")
        runtime.derive.return_value = shared_lease

        async def stream():
            """返回一段本地二进制流。"""
            yield b"data"

        def test_config(key=None, default=None):
            """同时兼容快照对象和快捷键两种配置读取方式。"""
            return _AssetConfig() if key is None else default

        with patch(
            "app.dataplane.reverse.transport.assets.get_config",
            side_effect=test_config,
        ), patch(
            "app.dataplane.reverse.transport.assets.get_proxy_runtime",
            return_value=runtime,
        ), patch(
            "app.dataplane.reverse.transport.assets.get_json",
            new=AsyncMock(return_value={}),
        ), patch(
            "app.dataplane.reverse.transport.assets.delete_json",
            new=AsyncMock(return_value={}),
        ), patch(
            "app.dataplane.reverse.transport.assets.get_bytes_stream",
            new=AsyncMock(return_value=stream()),
        ):
            await list_assets("token", lease=shared_lease)
            await delete_asset("token", "asset-id", lease=shared_lease)
            await download_asset(
                "token",
                "https://assets.grok.com/file.png",
                lease=shared_lease,
            )

        runtime.acquire.assert_not_awaited()
        self.assertEqual(runtime.derive.await_count, 3)


if __name__ == "__main__":
    unittest.main()
