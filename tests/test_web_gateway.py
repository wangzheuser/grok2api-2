from types import SimpleNamespace
import unittest
from unittest.mock import patch

import aiohttp
import orjson

from app.control.model.enums import ModeId
from app.control.proxy.models import ProxyLease
from app.dataplane.reverse.protocol.xai_chat import StreamAdapter
from app.dataplane.reverse.transport import web_gateway
from app.dataplane.reverse.transport.web_gateway import (
    gateway_endpoint,
    gateway_headers,
    gateway_session,
    gateway_turn_events,
    parse_session_user_id,
    resolve_gateway_user_id,
    stream_gateway_chat,
)
from app.platform.errors import UpstreamError


_USER_ID = "123e4567-e89b-12d3-a456-426614174000"


def _lease(*, with_user_id: bool = True) -> ProxyLease:
    """构造 Gateway 测试租约。"""
    cookies = "cf_clearance=clearance; __cf_bm=bm"
    if with_user_id:
        cookies += f"; x-userid={_USER_ID}"
    return ProxyLease(
        lease_id="lease-1",
        proxy_url="http://proxy.example:8080",
        cf_cookies=cookies,
        user_agent="Mozilla/5.0 Chrome/136.0.0.0",
    )


def _frame(event: dict, *, session_id: str = "") -> str:
    """序列化测试用 Gateway envelope。"""
    value = {"event": event}
    if session_id:
        value["session_id"] = session_id
    return orjson.dumps(value).decode()


class _FakeWS:
    def __init__(self, frames: list[str]) -> None:
        self.frames = frames
        self.sent: list[dict] = []

    def __aiter__(self):
        """返回异步消息迭代器。"""
        return self._iterate()

    async def _iterate(self):
        """按顺序产生文本 WebSocket frame。"""
        for frame in self.frames:
            yield SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=frame)

    async def send_str(self, value: str) -> None:
        """记录客户端发出的 JSON frame。"""
        self.sent.append(orjson.loads(value))

    def exception(self):
        """模拟无底层 WebSocket 异常。"""
        return None


class _FakeConnection:
    def __init__(self, ws: _FakeWS) -> None:
        self.ws = ws
        self.closed = False

    async def __aenter__(self) -> _FakeWS:
        """返回伪 WebSocket。"""
        return self.ws

    async def __aexit__(self, *_args) -> None:
        """记录连接上下文已关闭。"""
        self.closed = True


class GatewayProtocolBuilderTests(unittest.TestCase):
    def test_endpoint_session_and_headers_follow_mgw_contract(self):
        """endpoint、会话能力和握手 Cookie 均符合 MGW 协议。"""
        endpoint, origin = gateway_endpoint("https://grok.com/base", _USER_ID)
        session = gateway_session(ModeId.EXPERT)
        headers = gateway_headers("token", _USER_ID, origin, _lease())

        self.assertEqual(endpoint, f"wss://grok.com/ws/mgw/?uid={_USER_ID}")
        self.assertEqual(origin, "https://grok.com")
        self.assertEqual(session["model"], "expert")
        self.assertEqual(
            session["x_grok"]["protocol_capabilities"],
            ["conversation_attached", "custom_methods_v1"],
        )
        self.assertTrue(session["x_grok"]["use_chunk"])
        self.assertIn(f"x-userid={_USER_ID}", headers["Cookie"])
        self.assertEqual(headers["Cookie"].lower().count("x-userid="), 1)
        self.assertNotIn("Authorization", headers)
        self.assertNotIn("DPoP", headers)

    def test_turn_events_include_file_mentions_and_attachment_ids(self):
        """文件 ID 同时出现在 mention、item 和事件附件字段。"""
        item_event, response_event = gateway_turn_events(
            "conversation-1",
            "hello",
            ["file-1", "file-2"],
        )
        item = item_event["event"]["item"]
        chunks = item["x_grok"]["input_chunks"]

        self.assertEqual(item_event["session_id"], "conversation-1")
        self.assertEqual(item["file_attachment_ids"], ["file-1", "file-2"])
        self.assertEqual(item_event["event"]["file_attachment_ids"], ["file-1", "file-2"])
        self.assertEqual(chunks[0]["mention"]["target"]["file_mention"]["file_id"], "file-1")
        self.assertEqual(chunks[-1]["text"]["text"], "hello")
        self.assertEqual(response_event["event"]["type"], "response.create")

    def test_session_identity_parsing_rejects_blocked_state(self):
        """Session identity 支持嵌套 userId，并优先拒绝 blocked 状态。"""
        body = orjson.dumps(
            {"status": "authenticated", "session": {"userId": _USER_ID}}
        )
        self.assertEqual(parse_session_user_id(body), _USER_ID)
        with self.assertRaises(UpstreamError) as captured:
            parse_session_user_id(
                orjson.dumps(
                    {
                        "status": "blocked",
                        "session": {"userId": _USER_ID},
                    }
                )
            )
        self.assertEqual(captured.exception.status, 401)


class GatewayIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        """清理全局身份缓存，避免用例相互影响。"""
        async with web_gateway._identity_lock:
            web_gateway._identity_cache.clear()

    async def test_missing_cookie_identity_is_fetched_through_same_lease(self):
        """Cookie 无 x-userid 时使用相同代理租约调用 Session 接口。"""
        seen: dict = {}
        response = SimpleNamespace(
            status_code=200,
            content=orjson.dumps(
                {"status": "authenticated", "user": {"id": _USER_ID}}
            ),
        )

        class _FakeSession:
            def __init__(self, *, lease) -> None:
                seen["lease"] = lease

            async def __aenter__(self):
                """返回伪 HTTP 会话。"""
                return self

            async def __aexit__(self, *_args):
                """退出伪 HTTP 会话。"""
                return None

            async def get(self, url, *, headers, timeout):
                """记录 Session 请求参数并返回身份响应。"""
                seen.update(url=url, headers=headers, timeout=timeout)
                return response

        lease = _lease(with_user_id=False)
        with patch("app.dataplane.reverse.transport.web_gateway.ResettableSession", _FakeSession):
            user_id = await resolve_gateway_user_id("token-without-userid", lease)

        self.assertEqual(user_id, _USER_ID)
        self.assertIs(seen["lease"], lease)
        self.assertEqual(seen["url"], "https://grok.com/api/auth/session")
        self.assertIn("sso=token-without-userid", seen["headers"]["Cookie"])


class GatewayTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_state_machine_sends_turn_after_created_and_attached(self):
        """握手两阶段完成后发送用户 item 与 response.create，并输出全部事件。"""
        frames = [
            _frame({"type": "session.created"}, session_id="conversation-1"),
            _frame(
                {
                    "type": "conversation.attached",
                    "conversation": {"id": "conversation-1"},
                }
            ),
            _frame(
                {
                    "type": "response.chunk",
                    "chunk": {
                        "text": {
                            "text": "hello",
                            "channel": "CHANNEL_ASSISTANT_RESPONSE",
                        }
                    },
                }
            ),
            _frame(
                {
                    "type": "response.done",
                    "response": {"id": "response-1", "status": "completed"},
                }
            ),
        ]
        ws = _FakeWS(frames)
        connection = _FakeConnection(ws)
        captured: dict = {}

        async def _connect(_self, url, headers=None, timeout=None, ws_kwargs=None, **kwargs):
            """记录握手参数并返回伪连接。"""
            captured.update(
                url=url,
                headers=headers,
                timeout=timeout,
                ws_kwargs=ws_kwargs,
                kwargs=kwargs,
            )
            return connection

        with patch(
            "app.dataplane.reverse.transport.web_gateway.WebSocketClient.connect",
            _connect,
        ):
            output = [
                value
                async for value in stream_gateway_chat(
                    token="token",
                    mode_id=ModeId.FAST,
                    prompt="question",
                    attachments=["file-1"],
                    lease=_lease(),
                    timeout_s=2,
                )
            ]

        self.assertEqual(output, frames)
        self.assertTrue(connection.closed)
        self.assertEqual(captured["url"], f"wss://grok.com/ws/mgw/?uid={_USER_ID}")
        self.assertEqual(captured["ws_kwargs"]["max_msg_size"], 16 * 1024 * 1024)
        self.assertEqual([item["event"]["type"] for item in ws.sent], [
            "session.create",
            "conversation.item.create",
            "response.create",
        ])
        item = ws.sent[1]["event"]["item"]
        self.assertEqual(item["x_grok"]["input_chunks"][-1]["text"]["text"], "question")

    async def test_inconsistent_conversation_id_fails_closed(self):
        """session 与 conversation ID 不一致时在发送用户消息前终止。"""
        ws = _FakeWS(
            [
                _frame({"type": "session.created"}, session_id="conversation-1"),
                _frame(
                    {
                        "type": "conversation.attached",
                        "conversation": {"id": "conversation-2"},
                    }
                ),
            ]
        )
        connection = _FakeConnection(ws)

        async def _connect(*_args, **_kwargs):
            """返回会话 ID 冲突的伪连接。"""
            return connection

        with patch(
            "app.dataplane.reverse.transport.web_gateway.WebSocketClient.connect",
            _connect,
        ):
            with self.assertRaises(UpstreamError):
                _ = [
                    value
                    async for value in stream_gateway_chat(
                        token="token",
                        mode_id=ModeId.FAST,
                        prompt="question",
                        attachments=[],
                        lease=_lease(),
                        timeout_s=2,
                    )
                ]
        self.assertEqual([item["event"]["type"] for item in ws.sent], ["session.create"])

    async def test_openai_chat_stream_uses_gateway_transport(self):
        """产品层 _stream_chat 直接委托 MGW，不再构造旧 REST SSE 请求。"""
        from app.products.openai import chat as openai_chat

        lease = _lease()
        captured: dict = {}

        class _Proxy:
            async def acquire(self, **_kwargs):
                """返回固定 Web 租约。"""
                return lease

        class _Config:
            def get_str(self, _key, default=""):
                """返回测试默认字符串配置。"""
                return default

        async def _proxy_runtime():
            """返回伪代理运行时。"""
            return _Proxy()

        async def _gateway(**kwargs):
            """记录产品层委托参数并返回一个原始 Gateway frame。"""
            captured.update(kwargs)
            yield _frame({"type": "response.done", "response": {"status": "completed"}})

        with (
            patch.object(openai_chat, "get_proxy_runtime", _proxy_runtime),
            patch.object(openai_chat, "get_config", return_value=_Config()),
            patch.object(openai_chat, "stream_gateway_chat", _gateway),
        ):
            output = [
                value
                async for value in openai_chat._stream_chat(
                    token="token",
                    mode_id=ModeId.FAST,
                    message="question",
                    files=[],
                    timeout_s=12,
                )
            ]

        self.assertEqual(len(output), 1)
        self.assertIs(captured["lease"], lease)
        self.assertEqual(captured["prompt"], "question")
        self.assertEqual(captured["mode_id"], ModeId.FAST)
        self.assertEqual(captured["timeout_s"], 12)


class GatewayStreamAdapterTests(unittest.TestCase):
    def test_gateway_frames_produce_text_reasoning_sources_citations_and_image(self):
        """MGW 专用 chunk 被适配为现有正文、思考、信源、引用和图片状态。"""
        adapter = StreamAdapter()
        frames = [
            {"type": "conversation.attached", "conversation": {"id": "conversation-1"}},
            {
                "type": "response.chunk",
                "chunk": {
                    "tool_usage_card": {
                        "tool_usage_card_id": "tool-1",
                        "web_search": {"args": {"query": "grok gateway"}},
                    }
                },
            },
            {
                "type": "response.chunk",
                "chunk": {
                    "tool_result": {
                        "tool_call_id": "tool-1",
                        "web_search": {
                            "webpages": [
                                {"url": "https://example.com/a", "title": "Example A"}
                            ]
                        },
                    }
                },
            },
            {
                "type": "response.chunk",
                "chunk": {
                    "text": {
                        "text": "answer",
                        "channel": "CHANNEL_ASSISTANT_RESPONSE",
                    }
                },
            },
            {
                "type": "response.chunk",
                "chunk": {"render_citation": {"url": "https://example.com/a"}},
            },
            {
                "type": "response.chunk",
                "chunk": {
                    "text": {"text": "thought", "channel": "CHANNEL_ANALYSIS"}
                },
            },
            {
                "type": "response.grok.output",
                "output": {
                    "card_attachment": {
                        "image_chunk": {
                            "progress": 100,
                            "moderated": False,
                            "imageUrl": "/generated/image.png",
                            "imageUuid": "image-1",
                        }
                    }
                },
            },
            {
                "type": "response.done",
                "response": {"id": "response-1", "status": "completed"},
            },
        ]
        events = []
        for event in frames:
            events.extend(adapter.feed(_frame(event)))

        text = "".join(item.content for item in events if item.kind == "text")
        thinking = "".join(item.content for item in events if item.kind == "thinking")
        annotations = [item.annotation_data for item in events if item.kind == "annotation"]
        self.assertEqual(adapter.conversation_id, "conversation-1")
        self.assertEqual(adapter.parent_response_id, "response-1")
        self.assertEqual(text, "answer [[1]](https://example.com/a)")
        self.assertIn("web_search: grok gateway", thinking)
        self.assertIn("thought", thinking)
        self.assertEqual(annotations[0]["title"], "Example A")
        self.assertEqual(annotations[0]["start_index"], len("answer"))
        self.assertEqual(adapter.search_sources_list()[0]["url"], "https://example.com/a")
        self.assertEqual(
            adapter.image_urls,
            [("https://assets.grok.com/generated/image.png", "image-1")],
        )
        self.assertEqual(events[-1].kind, "soft_stop")

    def test_gateway_error_classification_preserves_retry_status(self):
        """anti-bot 与 usage limit 分别映射为 403 和 429。"""
        adapter = StreamAdapter()
        with self.assertRaises(UpstreamError) as anti_bot:
            adapter.feed(
                _frame(
                    {
                        "type": "error",
                        "error": {"code": 7, "message": "anti-bot rejected"},
                    }
                )
            )
        self.assertEqual(anti_bot.exception.status, 403)

        with self.assertRaises(UpstreamError) as usage:
            adapter.feed(
                _frame(
                    {
                        "type": "response.grok.output",
                        "output": {
                            "stream_error": {"message": "Usage limit reached"}
                        },
                    }
                )
            )
        self.assertEqual(usage.exception.status, 429)


if __name__ == "__main__":
    unittest.main()
