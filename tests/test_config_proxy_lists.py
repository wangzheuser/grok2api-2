import unittest
from unittest.mock import patch

from app.platform.errors import ValidationError
from app.products.web.admin import (
    _public_config_snapshot,
    _sanitize_proxy_config,
    _validate_effective_proxy_patch,
)


class UnifiedProxyConfigTests(unittest.TestCase):
    def test_admin_snapshot_redacts_resin_and_omits_managed_inventory(self):
        """通用配置快照应脱敏 Resin，并由专用接口承载托管库存。"""
        raw = {
            "proxy": {
                "egress": {"mode": "resin"},
                "resin": {
                    "url_template": "https://node.{uuid}:resin-secret@proxy.test:8443"
                },
                "pool": {
                    "entries": [
                        {
                            "id": "p1",
                            "url": "http://user:url-secret@managed.test:8080",
                            "username": "user",
                            "password": "entry-secret",
                        }
                    ]
                },
            }
        }

        with patch("app.products.web.admin.config.raw", return_value=raw):
            public = _public_config_snapshot()

        serialized = repr(public)
        for secret in ("resin-secret", "url-secret", "entry-secret"):
            self.assertNotIn(secret, serialized)
        self.assertIn("***", public["proxy"]["resin"]["url_template"])
        self.assertNotIn("entries", public["proxy"]["pool"])

    def test_admin_save_restores_masked_resin_password(self):
        """未修改的 Resin 脱敏模板应恢复当前真实凭据。"""
        current = {
            "proxy": {
                "resin": {
                    "url_template": "https://node.{uuid}:secret@proxy.test:8443"
                }
            }
        }
        payload = {
            "proxy": {
                "resin": {
                    "url_template": "https://node.{uuid}:***@proxy.test:8443"
                }
            }
        }

        with patch("app.products.web.admin.config.raw", return_value=current):
            sanitized = _sanitize_proxy_config(payload)

        self.assertEqual(
            sanitized["proxy"]["resin"]["url_template"],
            "https://node.{uuid}:secret@proxy.test:8443",
        )

    def test_admin_save_rejects_masked_resin_password_for_changed_endpoint(self):
        """掩码模板修改端点后应要求提交对应真实凭据。"""
        current = {
            "proxy": {
                "resin": {
                    "url_template": "https://node.{uuid}:secret@proxy.test:8443"
                }
            }
        }
        payload = {
            "proxy": {
                "resin": {
                    "url_template": "https://node.{uuid}:***@changed.test:8443"
                }
            }
        }

        with patch("app.products.web.admin.config.raw", return_value=current):
            with self.assertRaises(ValidationError) as caught:
                _sanitize_proxy_config(payload)

        self.assertEqual(caught.exception.param, "proxy.resin.url_template")
        self.assertEqual(caught.exception.code, "masked_proxy_secret_mismatch")

    def test_effective_validation_deep_merges_partial_patch(self):
        """局部补丁应与当前完整配置合并后再校验。"""
        current = {
            "proxy": {
                "egress": {"mode": "resin", "skip_ssl_verify": False},
                "resin": {
                    "url_template": "https://node.{uuid}:secret@proxy.test:8443"
                },
                "health": {"concurrency": 20},
            }
        }

        with patch("app.products.web.admin.config.raw", return_value=current):
            _validate_effective_proxy_patch(
                {"proxy": {"egress": {"skip_ssl_verify": True}}}
            )

    def test_effective_validation_reports_resin_field_error(self):
        """切换 Resin 且模板缺失时应返回稳定字段错误。"""
        current = {"proxy": {"egress": {"mode": "direct"}}}
        with patch("app.products.web.admin.config.raw", return_value=current):
            with self.assertRaises(ValidationError) as caught:
                _validate_effective_proxy_patch(
                    {"proxy": {"egress": {"mode": "resin"}}}
                )

        self.assertEqual(caught.exception.status, 400)
        self.assertEqual(caught.exception.param, "proxy.resin.url_template")
        self.assertEqual(caught.exception.code, "resin_proxy_url_required")

    def test_unrelated_patch_does_not_change_proxy_config(self):
        """无关补丁应在统一配置保持有效时通过校验。"""
        current = {
            "proxy": {"egress": {"mode": "direct"}},
            "app": {"timeout": 30},
        }
        with patch("app.products.web.admin.config.raw", return_value=current):
            _validate_effective_proxy_patch({"app": {"timeout": 45}})

    def test_admin_snapshot_hides_legacy_proxy_groups_after_migration(self):
        """旧双轨字段可留在物理配置，但不应继续进入管理端读取路径。"""
        raw = {
            "proxy": {
                "egress": {
                    "mode": "direct",
                    "proxy_url": "http://legacy.test:8080",
                    "resource_proxy_pool": ["http://asset.test:8080"],
                },
                "pool": {"entries": []},
                "resin": {"url_template": ""},
            },
            "console": {
                "fallback": {"enabled": False},
                "proxy_pool": {"enabled": True},
            },
        }

        with patch("app.products.web.admin.config.raw", return_value=raw):
            public = _public_config_snapshot()

        self.assertNotIn("proxy_url", public["proxy"]["egress"])
        self.assertNotIn("resource_proxy_pool", public["proxy"]["egress"])
        self.assertNotIn("proxy_pool", public["console"])
        self.assertIn("fallback", public["console"])


if __name__ == "__main__":
    unittest.main()
