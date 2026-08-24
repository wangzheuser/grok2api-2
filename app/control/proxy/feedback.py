"""Classify upstream HTTP responses into proxy feedback categories."""

from .models import ProxyFeedback, ProxyFeedbackKind


def classify_status_code(status_code: int) -> ProxyFeedbackKind:
    """按 HTTP 状态返回不含响应正文推断的基础反馈分类。"""
    if status_code == 200:
        return ProxyFeedbackKind.SUCCESS
    if status_code == 401:
        return ProxyFeedbackKind.UNAUTHORIZED
    if status_code == 403:
        return ProxyFeedbackKind.FORBIDDEN
    if status_code == 429:
        return ProxyFeedbackKind.RATE_LIMITED
    if status_code >= 500:
        return ProxyFeedbackKind.UPSTREAM_5XX
    return ProxyFeedbackKind.FORBIDDEN


def is_cloudflare_challenge(body: str) -> bool:
    """识别 Cloudflare challenge 的常见响应正文标记。"""
    normalized = str(body or "").lower()
    return any(
        marker in normalized
        for marker in (
            "cloudflare",
            "cf-challenge",
            "cf_chl_",
            "cf_clearance",
            "just a moment",
            "attention required",
        )
    )


def feedback_for_upstream_error(
    *,
    status_code: int | None,
    body: str = "",
    code: str = "",
    reason: str = "",
) -> ProxyFeedback:
    """把上游异常映射为代理反馈，区分出口连接与业务错误。"""
    status = int(status_code or 0)
    if code in {"egress_proxy_unavailable", "egress_transport_error"} or status == 407:
        kind = ProxyFeedbackKind.TRANSPORT_ERROR
    elif status == 403 and is_cloudflare_challenge(body):
        kind = ProxyFeedbackKind.CHALLENGE
    elif status:
        kind = classify_status_code(status)
    else:
        kind = ProxyFeedbackKind.TRANSPORT_ERROR
    return ProxyFeedback(
        kind=kind,
        status_code=status or None,
        reason=reason,
    )


def build_feedback(
    status_code: int,
    *,
    is_cloudflare: bool = False,
    reason: str = "",
    retry_after_ms: int | None = None,
) -> ProxyFeedback:
    """Build a ``ProxyFeedback`` from an HTTP response status code."""
    kind = classify_status_code(status_code)
    if is_cloudflare and status_code == 403:
        kind = ProxyFeedbackKind.CHALLENGE
    return ProxyFeedback(
        kind           = kind,
        status_code    = status_code,
        reason         = reason,
        retry_after_ms = retry_after_ms,
    )


__all__ = [
    "build_feedback",
    "classify_status_code",
    "feedback_for_upstream_error",
    "is_cloudflare_challenge",
]
