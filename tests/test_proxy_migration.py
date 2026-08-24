import unittest

from app.control.proxy.migration import (
    PROXY_SCHEMA_VERSION,
    _proxy_overrides_need_migration,
    build_proxy_migration_patch,
)


class _LegacyConfig:
    """提供迁移函数使用的点路径读取接口。"""

    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        """读取任意测试值。"""
        return self.values.get(key, default)

    def get_str(self, key, default=""):
        """读取字符串测试值。"""
        return str(self.values.get(key, default))

    def get_bool(self, key, default=False):
        """读取布尔测试值。"""
        return bool(self.values.get(key, default))

    def get_int(self, key, default=0):
        """读取整数测试值。"""
        return int(self.values.get(key, default))

    def get_float(self, key, default=0.0):
        """读取浮点测试值。"""
        return float(self.values.get(key, default))

    def get_list(self, key, default=None):
        """读取列表测试值。"""
        return list(self.values.get(key, default or []))


class ProxyMigrationTests(unittest.TestCase):
    def test_enabled_console_inventory_becomes_managed_pool(self):
        """旧 Console 池开启时应保留节点身份并导入独有全局节点。"""
        patch = build_proxy_migration_patch(
            _LegacyConfig(
                {
                    "proxy.egress.mode": "single_proxy",
                    "proxy.egress.proxy_url": "http://global.test:8080",
                    "console.proxy_pool.enabled": True,
                    "console.proxy_pool.entries": [
                        {
                            "id": "stable-console-id",
                            "url": "http://managed.test:8080",
                            "generation": 7,
                            "enabled": True,
                        }
                    ],
                }
            )
        )

        proxy = patch["proxy"]
        entries = proxy["pool"]["entries"]
        self.assertEqual(proxy["schema_version"], PROXY_SCHEMA_VERSION)
        self.assertEqual(proxy["egress"]["mode"], "managed_pool")
        self.assertEqual(entries[0]["id"], "stable-console-id")
        self.assertEqual(entries[0]["generation"], 7)
        self.assertFalse(entries[1]["enabled"])

    def test_lower_priority_duplicate_does_not_disable_console_node(self):
        """全局或资源池重复端点不应覆盖 Console 节点的启用状态。"""
        patch = build_proxy_migration_patch(
            _LegacyConfig(
                {
                    "proxy.egress.mode": "single_proxy",
                    "proxy.egress.proxy_url": "http://same.test:8080",
                    "proxy.egress.resource_proxy_url": "http://same.test:8080",
                    "console.proxy_pool.enabled": True,
                    "console.proxy_pool.entries": [
                        {
                            "id": "stable-console-id",
                            "url": "http://same.test:8080",
                            "generation": 7,
                            "enabled": True,
                        }
                    ],
                }
            )
        )

        entries = patch["proxy"]["pool"]["entries"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["id"], "stable-console-id")
        self.assertTrue(entries[0]["enabled"])

    def test_global_proxy_wins_activation_but_keeps_structured_id(self):
        """重复全局端点应启用旧结构化节点并保留其稳定 ID。"""
        patch = build_proxy_migration_patch(
            _LegacyConfig(
                {
                    "proxy.egress.mode": "single_proxy",
                    "proxy.egress.proxy_url": "http://user:secret@same.test:8080",
                    "console.proxy_pool.enabled": False,
                    "console.proxy_pool.entries": [
                        {
                            "id": "stable-id",
                            "url": "http://same.test:8080",
                            "username": "user",
                            "password": "secret",
                            "generation": 3,
                            "enabled": False,
                        },
                        {
                            "id": "inventory-id",
                            "url": "http://inventory.test:8080",
                            "enabled": True,
                        },
                    ],
                }
            )
        )

        entries = patch["proxy"]["pool"]["entries"]
        by_id = {entry["id"]: entry for entry in entries}
        self.assertEqual(len(entries), 2)
        self.assertTrue(by_id["stable-id"]["enabled"])
        self.assertEqual(by_id["stable-id"]["generation"], 3)
        self.assertFalse(by_id["inventory-id"]["enabled"])

    def test_direct_mode_retains_managed_inventory(self):
        """旧直连状态应继续直连并原样保留托管库存。"""
        patch = build_proxy_migration_patch(
            _LegacyConfig(
                {
                    "proxy.egress.mode": "direct",
                    "console.proxy_pool.enabled": False,
                    "console.proxy_pool.entries": [
                        {
                            "id": "p1",
                            "url": "http://inventory.test:8080",
                            "enabled": True,
                        }
                    ],
                }
            )
        )

        self.assertEqual(patch["proxy"]["egress"]["mode"], "direct")
        self.assertEqual(patch["proxy"]["pool"]["entries"][0]["id"], "p1")

    def test_existing_schema_v2_shape_is_preserved(self):
        """已使用新模式或 Resin 模板时迁移只补版本号。"""
        patch = build_proxy_migration_patch(
            _LegacyConfig(
                {
                    "proxy.egress.mode": "resin",
                    "proxy.resin.url_template": (
                        "https://node.{uuid}:token@proxy.test:8443"
                    ),
                    "proxy.pool.entries": [
                        {"id": "inventory", "url": "http://proxy.test:8080"}
                    ],
                }
            )
        )

        self.assertEqual(
            patch,
            {"proxy": {"schema_version": PROXY_SCHEMA_VERSION}},
        )

    def test_persisted_override_detection_is_idempotent(self):
        """迁移检测只处理旧结构或未标版的新结构。"""
        self.assertFalse(_proxy_overrides_need_migration({}))
        self.assertFalse(
            _proxy_overrides_need_migration(
                {"proxy": {"schema_version": PROXY_SCHEMA_VERSION}}
            )
        )
        self.assertTrue(
            _proxy_overrides_need_migration(
                {"proxy": {"egress": {"mode": "single_proxy"}}}
            )
        )
        self.assertTrue(
            _proxy_overrides_need_migration(
                {"console": {"proxy_pool": {"enabled": True}}}
            )
        )
        self.assertTrue(
            _proxy_overrides_need_migration(
                {"proxy": {"resin": {"url_template": ""}}}
            )
        )


if __name__ == "__main__":
    unittest.main()
