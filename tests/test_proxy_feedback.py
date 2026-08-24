import unittest

from app.control.proxy.feedback import feedback_for_upstream_error
from app.control.proxy.models import ProxyFeedbackKind


class ProxyFeedbackTests(unittest.TestCase):
    def test_only_cloudflare_403_invalidates_clearance(self):
        """普通业务 403 与 Cloudflare challenge 应进入不同反馈分支。"""
        business = feedback_for_upstream_error(
            status_code=403,
            body='{"error":"account blocked"}',
        )
        challenge = feedback_for_upstream_error(
            status_code=403,
            body="Just a moment... Cloudflare",
        )

        self.assertEqual(business.kind, ProxyFeedbackKind.FORBIDDEN)
        self.assertEqual(challenge.kind, ProxyFeedbackKind.CHALLENGE)

    def test_connection_error_code_overrides_http_wrapper_status(self):
        """连接异常即使包装成 502 也应反馈为出口传输故障。"""
        feedback = feedback_for_upstream_error(
            status_code=502,
            code="egress_transport_error",
        )

        self.assertEqual(feedback.kind, ProxyFeedbackKind.TRANSPORT_ERROR)


if __name__ == "__main__":
    unittest.main()
