import unittest
from unittest.mock import patch

from app.control.proxy.console_pool import ConsoleProxyEntry, ConsoleProxyPool, account_key_for_token
from app.control.proxy.models import ProxyFeedback, ProxyFeedbackKind, ProxyLease
from app.platform.runtime.ids import next_hex


class _PoolConfig:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)

    def get_bool(self, key, default=False):
        value = self.values.get(key, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def get_int(self, key, default=0):
        return int(self.values.get(key, default))

    def get_float(self, key, default=0.0):
        return float(self.values.get(key, default))


async def _fallback_lease_factory(**kwargs):
    proxy_url = kwargs.pop("proxy_url_override", None) or "http://global:8080"
    return ProxyLease(lease_id=next_hex(), proxy_url=proxy_url, **kwargs)


class ConsoleProxyPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_static_proxy_uses_account_sticky_binding(self):
        cfg = _PoolConfig({
            "console.proxy_pool.enabled": True,
            "console.proxy_pool.entries": [
                {"id": "p1", "url": "http://proxy1:8080", "mode": "static", "enabled": True},
                {"id": "p2", "url": "http://proxy2:8080", "mode": "static", "enabled": True},
            ],
        })
        pool = ConsoleProxyPool()
        with patch("app.control.proxy.console_pool.get_config", return_value=cfg):
            first = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            second = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            third = await pool.acquire(token="token-b", fallback_lease_factory=_fallback_lease_factory)

        self.assertEqual(first.proxy_id, second.proxy_id)
        self.assertNotEqual(first.proxy_id, third.proxy_id)
        self.assertEqual(first.proxy_pool, "console")
        self.assertEqual(first.account_key, account_key_for_token("token-a"))

    async def test_dynamic_template_rerenders_per_request_but_keeps_proxy_id(self):
        cfg = _PoolConfig({
            "console.proxy_pool.enabled": True,
            "console.proxy_pool.entries": [
                {"id": "dyn", "url": "http://proxy-{time}.example.com:8080", "enabled": True},
            ],
        })
        pool = ConsoleProxyPool()
        with patch("app.control.proxy.console_pool.get_config", return_value=cfg), patch(
            "app.control.proxy.console_pool.now_ms", side_effect=[1000, 2000]
        ):
            first = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            second = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)

        self.assertEqual(first.proxy_id, "dyn")
        self.assertEqual(second.proxy_id, "dyn")
        self.assertIn("1000", first.proxy_url)
        self.assertIn("2000", second.proxy_url)

    async def test_model_transient_rate_limit_feedback_does_not_mark_proxy_failed(self):
        cfg = _PoolConfig({
            "console.proxy_pool.enabled": True,
            "console.proxy_pool.entries": [{"id": "p1", "url": "http://proxy1:8080", "enabled": True}],
        })
        pool = ConsoleProxyPool()
        with patch("app.control.proxy.console_pool.get_config", return_value=cfg):
            lease = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            await pool.feedback(
                lease,
                ProxyFeedback(kind=ProxyFeedbackKind.SUCCESS, status_code=429, reason="model_transient_rate_limit"),
            )
            again = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            snap = await pool.snapshot()

        self.assertEqual(again.proxy_id, lease.proxy_id)
        self.assertEqual(snap["items"][0]["failure_count"], 0)
        self.assertEqual(snap["items"][0]["bound_account_count"], 1)

    async def test_transport_error_marks_failed_and_rebinds_account(self):
        cfg = _PoolConfig({
            "console.proxy_pool.enabled": True,
            "console.proxy_pool.static_cooldown_sec": 60,
            "console.proxy_pool.entries": [
                {"id": "p1", "url": "http://proxy1:8080", "enabled": True},
                {"id": "p2", "url": "http://proxy2:8080", "enabled": True},
            ],
        })
        pool = ConsoleProxyPool()
        with patch("app.control.proxy.console_pool.get_config", return_value=cfg):
            lease = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            await pool.feedback(lease, ProxyFeedback(kind=ProxyFeedbackKind.TRANSPORT_ERROR, reason="connect failed"))
            rebound = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            snap = await pool.snapshot()

        self.assertEqual(lease.proxy_id, "p1")
        self.assertEqual(rebound.proxy_id, "p2")
        self.assertEqual(snap["items"][0]["failure_count"], 1)
        self.assertEqual(snap["items"][0]["status"], "cooling_down")

    def test_password_placeholder_does_not_infer_dynamic_mode(self):
        entry = ConsoleProxyEntry(url="http://proxy.example.com:8080", username="user", password="pass-{time}")
        self.assertEqual(entry.inferred_mode().value, "static")


class ConsoleProxyPoolSoftFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_challenge_is_soft_failure_until_threshold(self):
        cfg = _PoolConfig({
            "console.proxy_pool.enabled": True,
            "console.proxy_pool.challenge_failure_threshold": 2,
            "console.proxy_pool.entries": [
                {"id": "p1", "url": "http://proxy1:8080", "enabled": True},
                {"id": "p2", "url": "http://proxy2:8080", "enabled": True},
            ],
        })
        pool = ConsoleProxyPool()
        with patch("app.control.proxy.console_pool.get_config", return_value=cfg):
            lease = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            await pool.feedback(lease, ProxyFeedback(kind=ProxyFeedbackKind.CHALLENGE, status_code=403))
            still = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)
            await pool.feedback(still, ProxyFeedback(kind=ProxyFeedbackKind.CHALLENGE, status_code=403))
            rebound = await pool.acquire(token="token-a", fallback_lease_factory=_fallback_lease_factory)

        self.assertEqual(still.proxy_id, "p1")
        self.assertEqual(rebound.proxy_id, "p2")


if __name__ == "__main__":
    unittest.main()
