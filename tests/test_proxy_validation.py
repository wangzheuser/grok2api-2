import unittest

from app.control.proxy.validation import (
    ProxyConfigIssue,
    normalize_proxy_list,
    validate_effective_proxy_config,
    validate_egress_config,
)


class ProxyValidationTests(unittest.TestCase):
    def test_supported_proxy_urls_and_deduplication(self):
        """代理列表应兼容目标协议并按原顺序去重。"""
        value = (
            "http://one:8080\nhttps://two:8443,"
            "socks4://three:1080\nsocks4a://four:1080\n"
            "socks5://five:1080\nsocks5h://six:1080\nhttp://one:8080"
        )

        result = normalize_proxy_list(value, path="proxy.egress.proxy_pool")

        self.assertEqual(len(result), 6)
        self.assertEqual(result[0], "http://one:8080")
        self.assertEqual(result[-1], "socks5h://six:1080")

    def test_invalid_mode_pool_strategy_and_scheme_are_rejected(self):
        """无效模式、空池、策略和协议应返回稳定错误码。"""
        fixtures = [
            ({"mode": "invalid"}, "invalid_proxy_mode"),
            ({"mode": "proxy_pool", "proxy_pool": []}, "proxy_pool_required"),
            (
                {"mode": "direct", "rotation_strategy": "invalid"},
                "invalid_proxy_rotation_strategy",
            ),
            (
                {"mode": "single_proxy", "proxy_url": "ftp://host:21"},
                "invalid_proxy_url",
            ),
        ]
        for value, code in fixtures:
            with self.subTest(code=code), self.assertRaises(ProxyConfigIssue) as caught:
                validate_egress_config(value)
            self.assertEqual(caught.exception.code, code)

    def test_console_fallback_requires_real_global_proxy(self):
        """Console 开启回退时不应接受全局 direct。"""
        with self.assertRaises(ProxyConfigIssue) as caught:
            validate_effective_proxy_config(
                {
                    "proxy": {"egress": {"mode": "direct"}},
                    "console": {
                        "proxy_pool": {
                            "enabled": True,
                            "fallback_to_global_proxy": True,
                        }
                    },
                }
            )

        self.assertEqual(caught.exception.code, "invalid_console_proxy_fallback")
        self.assertEqual(
            caught.exception.path,
            "console.proxy_pool.fallback_to_global_proxy",
        )

    def test_console_health_concurrency_range_is_enforced(self):
        """健康检查并发数必须位于管理端约定范围。"""
        for value in (0, 101, "invalid"):
            with self.subTest(value=value), self.assertRaises(ProxyConfigIssue):
                validate_effective_proxy_config(
                    {
                        "proxy": {"egress": {"mode": "direct"}},
                        "console": {
                            "proxy_pool": {"health_check_concurrency": value}
                        },
                    }
                )

    def test_resource_pool_may_be_empty_and_falls_back_at_runtime(self):
        """资源出口允许留空，基础池仍需保持有效。"""
        result = validate_egress_config(
            {
                "mode": "proxy_pool",
                "proxy_pool": ["http://base:8080"],
                "resource_proxy_pool": [],
            }
        )

        self.assertTrue(result.has_proxy)
        self.assertEqual(result.resource_proxy_pool, ())


if __name__ == "__main__":
    unittest.main()
