"""把 UpstreamError 统一映射为代理反馈。"""

from app.control.proxy.feedback import feedback_for_upstream_error
from app.platform.errors import UpstreamError
from app.control.proxy.models import ProxyFeedback


def upstream_feedback(exc: UpstreamError) -> ProxyFeedback:
    """返回保留出口错误码和 Cloudflare 语义的代理反馈。"""
    return feedback_for_upstream_error(
        status_code=exc.status,
        body=str(exc.details.get("body", "")),
        code=exc.code,
        reason=exc.code,
    )


__all__ = ["upstream_feedback"]
