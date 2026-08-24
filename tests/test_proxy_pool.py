import unittest
from unittest.mock import patch

from app.control.proxy import ProxyDirectory
from app.control.proxy.models import ProxyFeedback, ProxyFeedbackKind
from app.platform.errors import UpstreamError


class _ProxyConfig:
    def __init__(self, values):
        self.values = values

    def get_str(self, key, default=""):
        return str(self.values.get(key, default))

    def get_list(self, key, default=None):
        return self.values.get(key, default or [])

    def get_int(self, key, default=0):
        return int(self.values.get(key, default))


class ProxyDirectoryPoolTests(unittest.IsolatedAsyncioTestCase):
    def _config(self, strategy: str) -> _ProxyConfig:
        """构造包含基础池和资源池的测试配置。"""
        return _ProxyConfig(
            {
                "proxy.egress.mode": "proxy_pool",
                "proxy.egress.rotation_strategy": strategy,
                "proxy.egress.proxy_pool": [
                    "http://base-1:8080",
                    "http://base-2:8080",
                ],
                "proxy.egress.resource_proxy_pool": [
                    "http://resource-1:8080",
                    "http://resource-2:8080",
                ],
                "proxy.clearance.mode": "none",
            }
        )

    async def test_round_robin_rotates_each_pool_independently(self):
        """逐请求轮询时，基础流量和资源流量应分别维护游标。"""
        directory = ProxyDirectory()
        with patch(
            "app.control.proxy.get_config",
            return_value=self._config("round_robin"),
        ):
            await directory.load()
            base = [
                await directory.acquire(),
                await directory.acquire(),
                await directory.acquire(),
            ]
            resources = [
                await directory.acquire(resource=True),
                await directory.acquire(resource=True),
                await directory.acquire(resource=True),
            ]

        self.assertEqual(
            [lease.proxy_url for lease in base],
            ["http://base-1:8080", "http://base-2:8080", "http://base-1:8080"],
        )
        self.assertEqual(
            [lease.proxy_url for lease in resources],
            [
                "http://resource-1:8080",
                "http://resource-2:8080",
                "http://resource-1:8080",
            ],
        )
        self.assertEqual(base[0].proxy_pool, "global")
        self.assertEqual(resources[0].proxy_pool, "global_resource")

    async def test_sticky_failover_rotates_only_failed_pool(self):
        """粘性策略应只推进发生失败的代理池。"""
        directory = ProxyDirectory()
        with patch(
            "app.control.proxy.get_config",
            return_value=self._config("sticky_failover"),
        ):
            await directory.load()
            first = await directory.acquire()
            same = await directory.acquire()
            first_resource = await directory.acquire(resource=True)
            await directory.feedback(
                first,
                ProxyFeedback(kind=ProxyFeedbackKind.TRANSPORT_ERROR),
            )
            # 同一旧节点的并发失败反馈不应再次跳过新节点。
            await directory.feedback(
                same,
                ProxyFeedback(kind=ProxyFeedbackKind.TRANSPORT_ERROR),
            )
            next_base = await directory.acquire()
            same_resource = await directory.acquire(resource=True)

        self.assertEqual(first.proxy_url, same.proxy_url)
        self.assertEqual(next_base.proxy_url, "http://base-2:8080")
        self.assertEqual(first_resource.proxy_url, same_resource.proxy_url)

    async def test_invalid_runtime_config_fails_closed(self):
        """手工写入的空固定代理配置应在请求阶段返回稳定 503。"""
        directory = ProxyDirectory()
        cfg = _ProxyConfig(
            {
                "proxy.egress.mode": "single_proxy",
                "proxy.egress.proxy_url": "",
                "proxy.clearance.mode": "none",
            }
        )
        with patch("app.control.proxy.get_config", return_value=cfg):
            await directory.load()
            with self.assertRaises(UpstreamError) as caught:
                await directory.acquire()

        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(caught.exception.code, "egress_proxy_unavailable")

    async def test_explicit_override_bypasses_invalid_global_config(self):
        """Console 专用 override 不应被全局 direct 或脏配置阻止。"""
        directory = ProxyDirectory()
        cfg = _ProxyConfig(
            {
                "proxy.egress.mode": "proxy_pool",
                "proxy.egress.proxy_pool": [],
                "proxy.clearance.mode": "none",
            }
        )
        with patch("app.control.proxy.get_config", return_value=cfg):
            await directory.load()
            lease = await directory.acquire(
                proxy_url_override="http://console:8080"
            )

        self.assertEqual(lease.proxy_url, "http://console:8080")


if __name__ == "__main__":
    unittest.main()
