import unittest
from unittest.mock import patch

from app.platform.config.snapshot import ConfigSnapshot
from app.products.web.admin import (
    _normalize_console_proxy_fallback_patch,
    _public_config_snapshot,
    _sanitize_proxy_config,
    _validate_effective_proxy_patch,
)
from app.platform.errors import ValidationError


class ConfigProxyListTests(unittest.TestCase):
    def test_get_list_accepts_newlines_and_commas(self):
        """列表读取应兼容后台多行文本和旧逗号格式。"""
        snapshot = ConfigSnapshot()
        snapshot._data = {
            "proxy": {
                "egress": {
                    "proxy_pool": "http://one:8080\nhttp://two:8080,http://three:8080"
                }
            }
        }

        self.assertEqual(
            snapshot.get_list("proxy.egress.proxy_pool"),
            [
                "http://one:8080",
                "http://two:8080",
                "http://three:8080",
            ],
        )

    def test_admin_proxy_pool_is_saved_as_deduplicated_array(self):
        """后台提交的多行代理应保存为去重后的数组。"""
        payload = {
            "proxy": {
                "egress": {
                    "proxy_pool": (
                        "http://one:8080\n"
                        "socks5://two:1080\n"
                        "http://one:8080"
                    )
                }
            }
        }

        sanitized = _sanitize_proxy_config(payload)

        self.assertEqual(
            sanitized["proxy"]["egress"]["proxy_pool"],
            ["http://one:8080", "socks5://two:1080"],
        )

    def test_admin_config_snapshot_redacts_all_proxy_passwords(self):
        """通用配置接口不应返回全局或 Console 代理明文密码。"""
        raw = {
            "proxy": {
                "egress": {
                    "proxy_url": "http://user:global-secret@one:8080",
                    "proxy_pool": ["socks5://user:pool-secret@two:1080"],
                }
            },
            "console": {
                "proxy_pool": {
                    "entries": [
                        {
                            "id": "p1",
                            "url": "http://user:url-secret@three:8080",
                            "username": "user",
                            "password": "entry-secret",
                        }
                    ]
                }
            },
        }

        with patch("app.products.web.admin.config.raw", return_value=raw):
            public = _public_config_snapshot()

        serialized = repr(public)
        for secret in (
            "global-secret",
            "pool-secret",
            "url-secret",
            "entry-secret",
        ):
            self.assertNotIn(secret, serialized)
        self.assertNotIn(
            "password",
            public["console"]["proxy_pool"]["entries"][0],
        )

    def test_admin_save_restores_redacted_global_proxy_passwords(self):
        """编辑脱敏后的固定代理或代理池时应保留原有密码。"""
        current = {
            "proxy": {
                "egress": {
                    "proxy_url": "http://user:fixed-secret@one:8080",
                    "proxy_pool": [
                        "socks5://pool-user:pool-secret@two:1080"
                    ],
                }
            }
        }
        payload = {
            "proxy": {
                "egress": {
                    "proxy_url": "http://user:***@one:8080",
                    "proxy_pool": "socks5://pool-user:***@two:1080",
                }
            }
        }

        with patch("app.products.web.admin.config.raw", return_value=current):
            sanitized = _sanitize_proxy_config(payload)

        self.assertEqual(
            sanitized["proxy"]["egress"]["proxy_url"],
            "http://user:fixed-secret@one:8080",
        )
        self.assertEqual(
            sanitized["proxy"]["egress"]["proxy_pool"],
            ["socks5://pool-user:pool-secret@two:1080"],
        )

    def test_admin_save_rejects_masked_password_for_changed_endpoint(self):
        """掩码 URL 修改到新端点时应要求提交真实凭据。"""
        current = {
            "proxy": {
                "egress": {
                    "proxy_url": "http://user:secret@one:8080",
                }
            }
        }
        payload = {
            "proxy": {
                "egress": {
                    "proxy_url": "http://user:***@changed:8080",
                }
            }
        }

        with patch("app.products.web.admin.config.raw", return_value=current):
            with self.assertRaises(ValidationError) as caught:
                _sanitize_proxy_config(payload)

        self.assertEqual(caught.exception.status, 400)
        self.assertEqual(caught.exception.param, "proxy.egress.proxy_url")
        self.assertEqual(caught.exception.code, "masked_proxy_secret_mismatch")

    def test_effective_validation_deep_merges_partial_patch(self):
        """局部管理端补丁应与当前完整配置合并后再校验。"""
        current = {
            "proxy": {
                "egress": {
                    "mode": "single_proxy",
                    "proxy_url": "http://configured:8080",
                    "rotation_strategy": "sticky_failover",
                }
            },
            "console": {"proxy_pool": {"enabled": False}},
        }

        with patch("app.products.web.admin.config.raw", return_value=current):
            _validate_effective_proxy_patch(
                {"proxy": {"egress": {"rotation_strategy": "round_robin"}}}
            )

    def test_effective_validation_reports_stable_field_error(self):
        """最终固定代理为空时应返回 HTTP 400 对应的字段和稳定错误码。"""
        current = {
            "proxy": {"egress": {"mode": "direct"}},
            "console": {"proxy_pool": {"enabled": False}},
        }
        with patch("app.products.web.admin.config.raw", return_value=current):
            with self.assertRaises(ValidationError) as caught:
                _validate_effective_proxy_patch(
                    {"proxy": {"egress": {"mode": "single_proxy"}}}
                )

        self.assertEqual(caught.exception.status, 400)
        self.assertEqual(caught.exception.param, "proxy.egress.proxy_url")
        self.assertEqual(caught.exception.code, "proxy_url_required")

    def test_enabling_console_pool_auto_disables_stale_direct_fallback(self):
        """启用 Console 池时应自动清理 direct 模式下的遗留回退。"""
        current = {
            "proxy": {"egress": {"mode": "direct"}},
            "console": {
                "proxy_pool": {
                    "enabled": False,
                    "fallback_to_global_proxy": True,
                }
            },
        }
        with patch("app.products.web.admin.config.raw", return_value=current):
            normalized, changed = _normalize_console_proxy_fallback_patch(
                {"console": {"proxy_pool": {"enabled": True}}}
            )
            _validate_effective_proxy_patch(normalized)

        self.assertTrue(changed)
        self.assertFalse(
            normalized["console"]["proxy_pool"]["fallback_to_global_proxy"]
        )

    def test_explicit_direct_fallback_enable_is_normalized_off(self):
        """无全局代理时显式开启回退应返回关闭后的补丁。"""
        current = {
            "proxy": {"egress": {"mode": "direct"}},
            "console": {"proxy_pool": {"enabled": False}},
        }
        with patch("app.products.web.admin.config.raw", return_value=current):
            normalized, changed = _normalize_console_proxy_fallback_patch(
                {
                    "console": {
                        "proxy_pool": {"fallback_to_global_proxy": True}
                    }
                }
            )

        self.assertTrue(changed)
        self.assertFalse(
            normalized["console"]["proxy_pool"]["fallback_to_global_proxy"]
        )

    def test_configured_global_proxy_preserves_console_fallback(self):
        """有效固定代理存在时不得自动关闭 Console 回退。"""
        current = {
            "proxy": {
                "egress": {
                    "mode": "single_proxy",
                    "proxy_url": "http://global:8080",
                }
            },
            "console": {"proxy_pool": {"enabled": False}},
        }
        with patch("app.products.web.admin.config.raw", return_value=current):
            normalized, changed = _normalize_console_proxy_fallback_patch(
                {
                    "console": {
                        "proxy_pool": {
                            "enabled": True,
                            "fallback_to_global_proxy": True,
                        }
                    }
                }
            )

        self.assertFalse(changed)
        self.assertTrue(
            normalized["console"]["proxy_pool"]["fallback_to_global_proxy"]
        )

    def test_switching_active_fallback_to_direct_auto_disables_it(self):
        """全局代理切到 direct 时应同步关闭正在生效的 Console 回退。"""
        current = {
            "proxy": {
                "egress": {
                    "mode": "single_proxy",
                    "proxy_url": "http://global:8080",
                }
            },
            "console": {
                "proxy_pool": {
                    "enabled": True,
                    "fallback_to_global_proxy": True,
                }
            },
        }
        with patch("app.products.web.admin.config.raw", return_value=current):
            normalized, changed = _normalize_console_proxy_fallback_patch(
                {"proxy": {"egress": {"mode": "direct"}}}
            )

        self.assertTrue(changed)
        self.assertFalse(
            normalized["console"]["proxy_pool"]["fallback_to_global_proxy"]
        )

    def test_unrelated_patch_does_not_rewrite_legacy_fallback(self):
        """无关配置补丁不应顺带改写既有 Console 回退字段。"""
        current = {
            "proxy": {"egress": {"mode": "direct"}},
            "console": {
                "proxy_pool": {
                    "enabled": True,
                    "fallback_to_global_proxy": True,
                }
            },
            "app": {"timeout": 30},
        }
        patch_value = {"app": {"timeout": 45}}
        with patch("app.products.web.admin.config.raw", return_value=current):
            normalized, changed = _normalize_console_proxy_fallback_patch(
                patch_value
            )

        self.assertFalse(changed)
        self.assertEqual(normalized, patch_value)


if __name__ == "__main__":
    unittest.main()
