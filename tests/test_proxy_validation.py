import unittest

from app.control.proxy.validation import (
    ProxyConfigIssue,
    validate_resin_url_template,
    validate_unified_proxy_config,
)


class ProxyValidationTests(unittest.TestCase):
    def test_three_modes_are_mutually_exclusive(self):
        """统一配置一次只接受一个受支持的出口模式。"""
        direct = validate_unified_proxy_config(
            {"proxy": {"egress": {"mode": "direct"}}}
        )
        managed = validate_unified_proxy_config(
            {
                "proxy": {
                    "egress": {"mode": "managed_pool"},
                    "pool": {
                        "entries": [{"url": "http://proxy.test:8080"}]
                    },
                }
            }
        )
        resin = validate_unified_proxy_config(
            {
                "proxy": {
                    "egress": {"mode": "resin"},
                    "resin": {
                        "url_template": "https://node.{uuid}:token@proxy.test:8443"
                    },
                }
            }
        )

        self.assertEqual(direct.mode, "direct")
        self.assertEqual(managed.enabled_pool_entries, 1)
        self.assertIn("{uuid}", resin.resin_url_template)

    def test_invalid_mode_and_empty_managed_pool_are_rejected(self):
        """无效模式与空托管池应返回稳定字段错误。"""
        fixtures = [
            (
                {"proxy": {"egress": {"mode": "invalid"}}},
                "proxy.egress.mode",
                "invalid_proxy_mode",
            ),
            (
                {"proxy": {"egress": {"mode": "managed_pool"}}},
                "proxy.pool.entries",
                "proxy_pool_required",
            ),
        ]
        for value, path, code in fixtures:
            with self.subTest(code=code), self.assertRaises(ProxyConfigIssue) as caught:
                validate_unified_proxy_config(value)
            self.assertEqual(caught.exception.path, path)
            self.assertEqual(caught.exception.code, code)

    def test_resin_template_accepts_http_and_https(self):
        """Resin v1 应接受 HTTP/HTTPS 正向代理模板。"""
        for template in (
            "http://node.{uuid}:token@proxy.test:8080",
            "https://node.{uuid}:token@proxy.test:8443/path/{uuid}",
        ):
            with self.subTest(template=template):
                self.assertEqual(validate_resin_url_template(template), template)

    def test_resin_template_rejects_missing_or_unknown_placeholder(self):
        """缺少 UUID 或出现 time 等未知占位符时应定位 Resin 字段。"""
        fixtures = [
            ("https://node:token@proxy.test:8443", "resin_uuid_placeholder_required"),
            (
                "https://node.{uuid}.{time}:token@proxy.test:8443",
                "invalid_resin_proxy_placeholder",
            ),
        ]
        for template, code in fixtures:
            with self.subTest(code=code), self.assertRaises(ProxyConfigIssue) as caught:
                validate_resin_url_template(template)
            self.assertEqual(caught.exception.path, "proxy.resin.url_template")
            self.assertEqual(caught.exception.code, code)

    def test_resin_template_rejects_protocol_and_invalid_port(self):
        """Resin 模板应拒绝 SOCKS 协议和非法端口。"""
        for template in (
            "socks5://node.{uuid}:token@proxy.test:1080",
            "https://node.{uuid}:token@proxy.test:70000",
        ):
            with self.subTest(template=template), self.assertRaises(ProxyConfigIssue) as caught:
                validate_resin_url_template(template)
            self.assertEqual(caught.exception.code, "invalid_resin_proxy_url")

    def test_resin_template_requires_complete_forward_proxy_credentials(self):
        """Resin 正向代理模板必须提供用户名和密码。"""
        for template in (
            "https://proxy.test:8443/path/{uuid}",
            "https://node.{uuid}@proxy.test:8443",
            "https://node.{uuid}:token@proxy.test:8443/{broken",
        ):
            with self.subTest(template=template), self.assertRaises(ProxyConfigIssue) as caught:
                validate_resin_url_template(template)
            self.assertEqual(caught.exception.path, "proxy.resin.url_template")

    def test_health_concurrency_range_is_enforced(self):
        """健康检查并发数必须位于管理端约定范围。"""
        for value in (0, 101, "invalid"):
            with self.subTest(value=value), self.assertRaises(ProxyConfigIssue):
                validate_unified_proxy_config(
                    {
                        "proxy": {
                            "egress": {"mode": "direct"},
                            "health": {"concurrency": value},
                        }
                    }
                )


if __name__ == "__main__":
    unittest.main()
