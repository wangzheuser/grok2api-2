"""Model catalog freshness and unlisted LLM passthrough tests."""

from __future__ import annotations

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.control.model import registry as model_registry
from app.control.model.enums import Capability, ModeId, Tier
from app.control.model.spec import ModelSpec
from app.dataplane.reverse.protocol.console_model_guard import stream_console_chat_guarded
from app.dataplane.reverse.protocol.xai_console_chat import build_console_payload
from app.platform.errors import UpstreamError, ValidationError
from app.products.anthropic.router import MessagesRequest, messages_endpoint
from app.products.openai.router import (
    _validate_chat,
    chat_completions_endpoint,
    image_edits,
    image_generations,
    list_models,
    responses_endpoint,
    videos_create,
)
from app.products.openai.chat import _configured_retry_codes
from app.products.openai.schemas import (
    ChatCompletionRequest,
    ImageGenerationRequest,
    ResponsesCreateRequest,
)
from app.products.web.admin.models import _model_payload


LATEST_MODEL_IDS = [
    "grok-4.5",
    "grok-4.5-low",
    "grok-4.5-medium",
    "grok-4.5-high",
    "grok-4.6",
    "grok-4.6-low",
    "grok-4.6-medium",
    "grok-4.6-high",
    "grok-4.6-xhigh",
]


class ModelCatalogTests(unittest.TestCase):
    """Verify the registered Grok 4.5/4.6 catalog."""

    def test_latest_model_ids_are_registered_once_in_order(self) -> None:
        """Keep the latest model family complete, unique, and stable."""
        ids = [spec.model_name for spec in model_registry.MODELS]
        positions = [ids.index(model_id) for model_id in LATEST_MODEL_IDS]

        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(positions, sorted(positions))
        for model_id in LATEST_MODEL_IDS:
            spec = model_registry.get(model_id)
            self.assertIsNotNone(spec)
            self.assertEqual(spec.mode_id, ModeId.CONSOLE)
            self.assertEqual(spec.tier, Tier.BASIC)
            self.assertEqual(spec.capability, Capability.CONSOLE_CHAT)
            self.assertTrue(spec.enabled)

    def test_latest_model_payloads_map_model_and_effort(self) -> None:
        """Map public effort aliases to the verified upstream base IDs."""
        cases = {
            "grok-4.5": ("grok-4.5", "medium"),
            "grok-4.5-low": ("grok-4.5", "low"),
            "grok-4.5-medium": ("grok-4.5", "medium"),
            "grok-4.5-high": ("grok-4.5", "high"),
            "grok-4.6": ("grok-4.6", "medium"),
            "grok-4.6-low": ("grok-4.6", "low"),
            "grok-4.6-medium": ("grok-4.6", "medium"),
            "grok-4.6-high": ("grok-4.6", "high"),
            "grok-4.6-xhigh": ("grok-4.6", "xhigh"),
        }

        for public_id, (upstream_id, effort) in cases.items():
            with self.subTest(public_id=public_id):
                payload = build_console_payload(
                    messages=[{"role": "user", "content": "hi"}],
                    model=public_id,
                )
                self.assertEqual(payload["model"], upstream_id)
                self.assertEqual(payload["reasoning"], {"effort": effort})
                self.assertNotIn("tools", payload)

    def test_unlisted_model_uses_transient_console_spec(self) -> None:
        """Resolve an unlisted ID without adding it to the public catalog."""
        model_id = "grok-future-preview"
        spec = model_registry.resolve_llm(model_id)

        self.assertEqual(spec.model_name, model_id)
        self.assertEqual(spec.public_name, model_id)
        self.assertEqual(spec.mode_id, ModeId.CONSOLE)
        self.assertEqual(spec.tier, Tier.BASIC)
        self.assertEqual(spec.capability, Capability.CONSOLE_CHAT)
        self.assertIsNone(model_registry.get(model_id))
        self.assertNotIn(model_id, [item.model_name for item in model_registry.list_enabled()])

    def test_unlisted_model_id_reaches_console_payload_unchanged(self) -> None:
        """Keep the caller's unlisted ID unchanged in the upstream payload."""
        model_id = "grok-future-preview"
        payload = build_console_payload(
            messages=[{"role": "user", "content": "hi"}],
            model=model_id,
        )

        self.assertEqual(payload["model"], model_id)

    def test_registered_model_keeps_original_spec(self) -> None:
        """Return the canonical object for registered models."""
        self.assertIs(
            model_registry.resolve_llm("grok-4.6"),
            model_registry.get("grok-4.6"),
        )

    def test_passthrough_rejects_blank_or_padded_ids(self) -> None:
        """Reject IDs that cannot be forwarded verbatim."""
        for model_id in ("", " ", " grok-4.6", "grok-4.6 "):
            with self.subTest(model_id=model_id), self.assertRaises(ValueError):
                model_registry.resolve_llm(model_id)

    def test_registered_disabled_model_is_not_reclassified(self) -> None:
        """Prevent passthrough from bypassing a registered disabled model."""
        disabled = ModelSpec(
            "grok-disabled",
            ModeId.CONSOLE,
            Tier.BASIC,
            Capability.CONSOLE_CHAT,
            False,
            "Disabled",
        )
        with patch.dict(model_registry._BY_NAME, {disabled.model_name: disabled}):
            req = ChatCompletionRequest(
                model=disabled.model_name,
                messages=[{"role": "user", "content": "hi"}],
            )
            with self.assertRaises(ValidationError):
                _validate_chat(req)

    def test_public_models_include_latest_but_not_transient_ids(self) -> None:
        """List registered available models without leaking transient IDs."""
        transient_id = "grok-future-preview"
        model_registry.resolve_llm(transient_id)
        request = SimpleNamespace()

        async def run():
            """Call the public catalog with an available basic pool."""
            with patch(
                "app.products.openai.router._available_pools",
                new=AsyncMock(return_value=frozenset({"basic"})),
            ):
                return await list_models(request)

        response = asyncio.run(run())
        ids = [item["id"] for item in json.loads(response.body)["data"]]
        self.assertTrue(set(LATEST_MODEL_IDS).issubset(ids))
        self.assertNotIn(transient_id, ids)

    def test_admin_payload_marks_latest_model_available_and_fallback(self) -> None:
        """Expose the latest model's Console and runtime metadata."""
        spec = model_registry.resolve("grok-4.6-xhigh")
        payload = _model_payload(
            spec,
            pools=frozenset({"basic"}),
            fallback_targets={spec.model_name},
            created=123,
        )

        self.assertTrue(payload["available"])
        self.assertTrue(payload["is_console"])
        self.assertTrue(payload["fallback_target"])
        self.assertEqual(payload["console_model"], "grok-4.6")
        self.assertEqual(payload["fixed_effort"], "xhigh")

    def test_default_retry_codes_switch_accounts_on_console_403(self) -> None:
        """Retry another account when one Console identity is rejected."""
        self.assertIn(403, _configured_retry_codes({}))


class LlmPassthroughEndpointTests(unittest.TestCase):
    """Verify all text inference surfaces accept unlisted model IDs."""

    def test_chat_completions_forwards_unlisted_model(self) -> None:
        """Forward an unlisted Chat Completions model unchanged."""
        req = ChatCompletionRequest(
            model="grok-future-chat",
            messages=[{"role": "user", "content": "hi"}],
            stream=False,
        )

        async def run():
            """Invoke the endpoint with the upstream service mocked."""
            with patch(
                "app.products.openai.router.chat_completions",
                new=AsyncMock(return_value={"id": "chatcmpl-test"}),
            ) as mocked:
                response = await chat_completions_endpoint(req)
                return response, mocked.await_args.kwargs

        response, kwargs = asyncio.run(run())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(kwargs["model"], req.model)

    def test_responses_forwards_unlisted_model(self) -> None:
        """Forward an unlisted Responses model unchanged."""
        req = ResponsesCreateRequest(
            model="grok-future-responses",
            input="hi",
            stream=False,
        )

        async def run():
            """Invoke the endpoint with the Responses service mocked."""
            with patch(
                "app.products.openai.responses.create",
                new=AsyncMock(return_value={"id": "resp-test"}),
            ) as mocked:
                response = await responses_endpoint(req)
                return response, mocked.await_args.kwargs

        response, kwargs = asyncio.run(run())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(kwargs["model"], req.model)

    def test_anthropic_messages_forwards_unlisted_model(self) -> None:
        """Forward an unlisted Anthropic Messages model unchanged."""
        req = MessagesRequest(
            model="grok-future-anthropic",
            messages=[{"role": "user", "content": "hi"}],
            stream=False,
        )

        async def run():
            """Invoke the endpoint with the Messages service mocked."""
            with patch(
                "app.products.anthropic.messages.create",
                new=AsyncMock(return_value={"id": "msg-test"}),
            ) as mocked:
                response = await messages_endpoint(req)
                return response, mocked.await_args.kwargs

        response, kwargs = asyncio.run(run())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(kwargs["model"], req.model)

    def test_text_endpoints_reject_blank_model_ids(self) -> None:
        """Keep local parameter errors for blank model IDs."""
        chat_req = ChatCompletionRequest(
            model=" ",
            messages=[{"role": "user", "content": "hi"}],
        )
        responses_req = ResponsesCreateRequest(model=" ", input="hi")
        messages_req = MessagesRequest(
            model=" ",
            messages=[{"role": "user", "content": "hi"}],
        )

        with self.assertRaises(ValidationError):
            _validate_chat(chat_req)
        with self.assertRaises(ValidationError):
            asyncio.run(responses_endpoint(responses_req))
        with self.assertRaises(ValidationError):
            asyncio.run(messages_endpoint(messages_req))

    def test_media_endpoints_still_reject_unlisted_models(self) -> None:
        """Keep passthrough limited to text inference surfaces."""
        with self.assertRaises(ValidationError):
            asyncio.run(
                image_generations(
                    ImageGenerationRequest(model="grok-future-media", prompt="x")
                )
            )
        with self.assertRaises(ValidationError):
            asyncio.run(
                image_edits(
                    model="grok-future-media",
                    prompt="x",
                    image=[],
                )
            )
        with self.assertRaises(ValidationError):
            asyncio.run(videos_create(model="grok-future-media", prompt="x"))

    def test_upstream_errors_remain_unchanged_for_passthrough(self) -> None:
        """Preserve upstream status codes instead of returning model_not_found."""

        async def run(status: int) -> None:
            """Consume one guarded stream that raises an upstream error."""
            expected = UpstreamError("upstream rejected model", status=status)

            async def fail_stream(*_args, **_kwargs):
                """Raise the exact upstream error supplied by the test."""
                if False:
                    yield "", ""
                raise expected

            try:
                async for _ in stream_console_chat_guarded(
                    token="token",
                    requested_model="grok-future-preview",
                    reasoning_effort=None,
                    cfg={},
                    timeout_s=1,
                    build_payload=lambda model: {"model": model},
                    stream_func=fail_stream,
                ):
                    pass
            except UpstreamError as exc:
                self.assertIs(exc, expected)
                self.assertEqual(exc.status, status)
            else:
                self.fail("expected UpstreamError")

        for status in (400, 403, 404, 429, 500):
            with self.subTest(status=status):
                asyncio.run(run(status))


if __name__ == "__main__":
    unittest.main()
