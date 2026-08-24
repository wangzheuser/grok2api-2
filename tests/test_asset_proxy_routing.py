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
    async def test_only_binary_download_requests_resource_pool(self):
        """下载二进制应标记 resource，列表与删除仍使用基础出口。"""
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
            {"scope": ProxyScope.ASSET, "kind": RequestKind.HTTP},
        )
        self.assertEqual(
            calls[1].kwargs,
            {"scope": ProxyScope.ASSET, "kind": RequestKind.HTTP},
        )
        self.assertEqual(
            calls[2].kwargs,
            {
                "scope": ProxyScope.ASSET,
                "kind": RequestKind.HTTP,
                "resource": True,
            },
        )


if __name__ == "__main__":
    unittest.main()
