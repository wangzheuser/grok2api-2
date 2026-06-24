"""Runtime guard for console model transient rate limits and fallback rules."""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable

from app.control.model import registry as model_registry
from app.platform.errors import UpstreamError
from app.platform.logging.logger import logger
from app.platform.runtime.clock import now_s

from .xai_console_chat import (
    console_model_for,
    console_payload_effort,
    stream_console_chat,
)

MODEL_TRANSIENT_RATE_LIMIT_CODE = "model_transient_rate_limit"


@dataclass(frozen=True, slots=True)
class ConsoleFallbackRule:
    """One configured fallback mapping from public source pattern to target."""

    source_pattern: str
    target_model: str


@dataclass(frozen=True, slots=True)
class ConsoleGuardDecision:
    """Resolved model decision before one upstream console attempt."""

    requested_model: str
    effective_model: str
    fallback_applied: bool
    fallback_reason: str
    circuit_open: bool
    upstream_model: str
    effort: str


@dataclass(slots=True)
class _CircuitState:
    failures: int = 0
    opened_until: int = 0


_circuit_states: dict[tuple[str, str], _CircuitState] = {}


def is_model_transient_rate_limit(exc: BaseException | None) -> bool:
    """Return whether *exc* is an upstream model-level transient 429."""
    return bool(exc and getattr(exc, "code", "") == MODEL_TRANSIENT_RATE_LIMIT_CODE)


async def stream_console_chat_guarded(
    *,
    token: str,
    requested_model: str,
    reasoning_effort: str | None,
    cfg: Any,
    timeout_s: float,
    build_payload: Callable[[str], dict[str, Any]],
    stream_func: Callable[..., AsyncGenerator[tuple[str, str], None]] | None = None,
) -> AsyncGenerator[tuple[str, str], None]:
    """Stream console events with circuit-breaker and one-shot fallback."""
    fallback_used = False
    streamer = stream_func or stream_console_chat

    while True:
        decision = resolve_console_decision(
            requested_model=requested_model,
            reasoning_effort=reasoning_effort,
            cfg=cfg,
            fallback_used=fallback_used,
        )
        if should_fast_fail_without_fallback(decision, cfg):
            raise model_transient_error(
                "Console model circuit is open",
                model=requested_model,
                reasoning_effort=reasoning_effort,
            )

        if decision.fallback_applied:
            fallback_used = True
            logger.warning(
                "console fallback selected: requested_model={} effective_model={} reason={}",
                requested_model,
                decision.effective_model,
                decision.fallback_reason,
            )

        try:
            payload = build_payload(decision.effective_model)
            async for event_type, data in streamer(
                token,
                payload,
                timeout_s=timeout_s,
            ):
                yield event_type, data
            record_console_success(
                decision.effective_model,
                cfg,
                reasoning_effort=reasoning_effort,
            )
            return
        except UpstreamError as exc:
            if not is_model_transient_rate_limit(exc):
                raise

            record_console_transient_failure(
                decision.effective_model,
                cfg,
                reasoning_effort=reasoning_effort,
            )
            fallback_model = None
            if not fallback_used:
                fallback_model = configured_fallback_for(requested_model, cfg)
            if not fallback_model:
                raise

            fallback_used = True
            logger.warning(
                "console fallback selected: requested_model={} effective_model={} reason={}",
                requested_model,
                fallback_model,
                "model_transient_rate_limit",
            )

            try:
                payload = build_payload(fallback_model)
                async for event_type, data in streamer(
                    token,
                    payload,
                    timeout_s=timeout_s,
                ):
                    yield event_type, data
                record_console_success(
                    fallback_model,
                    cfg,
                    reasoning_effort=reasoning_effort,
                )
                return
            except UpstreamError as fallback_exc:
                if is_model_transient_rate_limit(fallback_exc):
                    record_console_transient_failure(
                        fallback_model,
                        cfg,
                        reasoning_effort=reasoning_effort,
                    )
                raise


def parse_fallback_rules(raw: Any) -> list[ConsoleFallbackRule]:
    """Parse fallback rules from textarea/list config values."""
    lines: list[str] = []
    if isinstance(raw, str):
        lines = raw.splitlines()
    elif isinstance(raw, list):
        lines = [str(item) for item in raw]
    elif raw is None:
        lines = []
    else:
        lines = str(raw).splitlines()

    rules: list[ConsoleFallbackRule] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=>" not in stripped:
            raise ValueError(f"invalid fallback rule: {stripped}")
        source, target = [part.strip() for part in stripped.split("=>", 1)]
        if not source or not target:
            raise ValueError(f"invalid fallback rule: {stripped}")
        _validate_rule_source(source)
        _validate_fallback_target(target)
        rules.append(ConsoleFallbackRule(source_pattern=source, target_model=target))
    return rules


def validate_fallback_rules(raw: Any) -> None:
    """Validate fallback rules and raise ValueError on the first error."""
    parse_fallback_rules(raw)


def configured_fallback_for(model: str, cfg: Any) -> str | None:
    """Return the configured fallback model for *model*, if enabled and matched."""
    if not _cfg_bool(cfg, "console.fallback.enabled", False):
        return None
    for rule in parse_fallback_rules(_cfg_get(cfg, "console.fallback.rules", "")):
        if fnmatch.fnmatchcase(model, rule.source_pattern):
            return rule.target_model
    return None


def resolve_console_decision(
    *,
    requested_model: str,
    reasoning_effort: str | None,
    cfg: Any,
    fallback_used: bool = False,
) -> ConsoleGuardDecision:
    """Resolve the effective model before an upstream console call."""
    upstream_model = console_model_for(requested_model)
    effort = console_payload_effort(requested_model, reasoning_effort)
    circuit_open = is_circuit_open(upstream_model, effort, cfg)
    fallback_model = None
    fallback_reason = ""

    if circuit_open and not fallback_used:
        fallback_model = configured_fallback_for(requested_model, cfg)
        fallback_reason = "circuit_open" if fallback_model else ""

    if fallback_model:
        effective_model = fallback_model
        upstream_model = console_model_for(effective_model)
        effort = console_payload_effort(effective_model, reasoning_effort)
        return ConsoleGuardDecision(
            requested_model=requested_model,
            effective_model=effective_model,
            fallback_applied=True,
            fallback_reason=fallback_reason,
            circuit_open=circuit_open,
            upstream_model=upstream_model,
            effort=effort,
        )

    return ConsoleGuardDecision(
        requested_model=requested_model,
        effective_model=requested_model,
        fallback_applied=False,
        fallback_reason=fallback_reason,
        circuit_open=circuit_open,
        upstream_model=upstream_model,
        effort=effort,
    )


def should_fast_fail_without_fallback(decision: ConsoleGuardDecision, cfg: Any) -> bool:
    """Return whether an open circuit without fallback should fail immediately."""
    return (
        decision.circuit_open
        and not decision.fallback_applied
        and _cfg_bool(cfg, "console.rate_limit.fast_fail_without_fallback", True)
    )


def record_console_success(model: str, cfg: Any, *, reasoning_effort: str | None = None) -> None:
    """Clear circuit failures for a successful console upstream call."""
    if not _cfg_bool(cfg, "console.rate_limit.breaker_enabled", True):
        return
    key = _circuit_key(model, reasoning_effort)
    state = _circuit_states.get(key)
    if state is None:
        return
    state.failures = 0
    state.opened_until = 0


def record_console_transient_failure(
    model: str,
    cfg: Any,
    *,
    reasoning_effort: str | None = None,
) -> bool:
    """Record a model transient 429 and return whether the circuit is open."""
    if not _cfg_bool(cfg, "console.rate_limit.breaker_enabled", True):
        return False

    upstream_model = console_model_for(model)
    effort = console_payload_effort(model, reasoning_effort)
    key = (upstream_model, effort)
    state = _circuit_states.setdefault(key, _CircuitState())
    state.failures += 1

    threshold = max(1, _cfg_int(cfg, "console.rate_limit.breaker_threshold", 2))
    ttl = max(1, _cfg_int(cfg, "console.rate_limit.breaker_ttl_sec", 180))
    if state.failures >= threshold:
        state.opened_until = now_s() + ttl
        logger.warning(
            "console model circuit opened: model={} upstream_model={} effort={} failures={} ttl_sec={}",
            model, upstream_model, effort, state.failures, ttl,
        )
    return state.opened_until > now_s()


def model_transient_error(
    message: str,
    *,
    model: str,
    reasoning_effort: str | None = None,
    status: int = 429,
    body: str = "",
) -> UpstreamError:
    """Build a standard model transient rate-limit error."""
    return UpstreamError(
        message,
        status=status,
        body=body,
        code=MODEL_TRANSIENT_RATE_LIMIT_CODE,
        details={
            "body_class": "model_transient_rate_limit",
            "requested_model": model,
            "upstream_model": console_model_for(model),
            "effort": console_payload_effort(model, reasoning_effort),
        },
    )


def reset_console_guard_state() -> None:
    """Clear in-memory circuit state, intended for tests."""
    _circuit_states.clear()


def _circuit_key(model: str, reasoning_effort: str | None = None) -> tuple[str, str]:
    return console_model_for(model), console_payload_effort(model, reasoning_effort)


def is_circuit_open(upstream_model: str, effort: str, cfg: Any) -> bool:
    """Return whether the upstream model/effort circuit is currently open."""
    if not _cfg_bool(cfg, "console.rate_limit.breaker_enabled", True):
        return False
    state = _circuit_states.get((upstream_model, effort))
    if state is None or state.opened_until <= 0:
        return False
    if state.opened_until <= now_s():
        state.opened_until = 0
        return False
    return True


def fallback_target_ids() -> set[str]:
    """Return model IDs that are valid fallback targets."""
    return {
        spec.model_name
        for spec in model_registry.list_enabled()
        if spec.is_console_chat()
    }


def _validate_rule_source(source: str) -> None:
    if source.strip() != source or not source:
        raise ValueError("fallback source pattern cannot be empty")


def _validate_fallback_target(target: str) -> None:
    spec = model_registry.get(target)
    if spec is None or not spec.enabled:
        raise ValueError(f"fallback target model not found: {target}")
    if not spec.is_console_chat():
        raise ValueError(f"fallback target must be a console model: {target}")


def _cfg_get(cfg: Any, key: str, default: Any = None) -> Any:
    getter = getattr(cfg, "get", None)
    if callable(getter):
        return getter(key, default)
    return default


def _cfg_bool(cfg: Any, key: str, default: bool = False) -> bool:
    getter = getattr(cfg, "get_bool", None)
    if callable(getter):
        return bool(getter(key, default))
    value = _cfg_get(cfg, key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _cfg_int(cfg: Any, key: str, default: int = 0) -> int:
    getter = getattr(cfg, "get_int", None)
    if callable(getter):
        return int(getter(key, default))
    try:
        return int(_cfg_get(cfg, key, default))
    except (TypeError, ValueError):
        return default


__all__ = [
    "MODEL_TRANSIENT_RATE_LIMIT_CODE",
    "ConsoleFallbackRule",
    "ConsoleGuardDecision",
    "configured_fallback_for",
    "fallback_target_ids",
    "is_model_transient_rate_limit",
    "model_transient_error",
    "parse_fallback_rules",
    "record_console_success",
    "record_console_transient_failure",
    "reset_console_guard_state",
    "resolve_console_decision",
    "should_fast_fail_without_fallback",
    "stream_console_chat_guarded",
    "validate_fallback_rules",
]
