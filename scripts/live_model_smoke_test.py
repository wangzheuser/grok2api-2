#!/usr/bin/env python3
"""
线上模型冒烟脚本：发现 /v1/models 中暴露的模型，并做最小实调验证。

能力覆盖：
- 文本 / Console 模型：POST /v1/chat/completions
- 图片生成：POST /v1/images/generations
- 图片编辑：POST /v1/images/edits
- 视频：POST /v1/videos + GET /v1/videos/{id}

输出目标：
- 标准输出打印摘要和 Markdown 结果表
- 可选写出 JSON 报告，便于留档或二次处理
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Final
from urllib.parse import urljoin

import aiohttp

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.control.model.registry import MODELS, get as get_model_spec  # noqa: E402

PNG_1X1_BLUE: Final[bytes] = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wn5s0sAAAAASUVORK5CYII="
)
CHAT_PROMPT: Final[str] = "只回复 OK"
IMAGE_PROMPT: Final[str] = "a red dot on white background"
IMAGE_EDIT_PROMPT: Final[str] = "turn it blue"
VIDEO_PROMPT: Final[str] = "a static white ball on black background"


@dataclass(slots=True)
class ProbeResult:
    """一条模型验证记录。"""

    model: str
    listed_in_models: bool
    capability: str
    http_status: int | None
    latency_ms: int | None
    result: str
    error_summary: str
    conclusion: str


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""

    parser = argparse.ArgumentParser(description="Smoke-test Grok2API live models.")
    parser.add_argument(
        "--base-url",
        required=True,
        help="目标 OpenAI 兼容端点；支持传 https://host/v1 或 https://host。",
    )
    parser.add_argument(
        "--api-key",
        default="",
        help="Bearer API Key；留空时不附带 Authorization 头。",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=90.0,
        help="单次 HTTP 请求超时秒数，默认 90。",
    )
    parser.add_argument(
        "--console-delay",
        type=float,
        default=1.5,
        help="Console 模型之间的节流秒数，默认 1.5。",
    )
    parser.add_argument(
        "--json-output",
        default="",
        help="可选：将完整结果写入 JSON 文件。",
    )
    return parser.parse_args()


def normalize_base_url(raw_base_url: str) -> str:
    """规范化 base URL，确保最终路径落在 /v1/ 下。"""

    base = raw_base_url.strip().rstrip("/")
    if base.endswith("/v1"):
        return f"{base}/"
    return f"{base}/v1/"


def capability_label(spec: Any | None) -> str:
    """把模型 spec 转成脚本输出用的能力标签。"""

    if spec is None:
        return "unknown"
    if spec.is_image_edit():
        return "image_edit"
    if spec.is_image():
        return "image_generation"
    if spec.is_video():
        return "video"
    if spec.is_console_chat():
        return "console_chat"
    if spec.is_chat():
        return "chat"
    return "unknown"


def extract_error_summary(payload: Any, body_text: str) -> str:
    """尽量从 JSON 错误体中抽出可读摘要。"""

    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = str(error.get("message") or "").strip()
            code = str(error.get("code") or "").strip()
            detail = " ".join(part for part in (message, f"[{code}]" if code else "") if part)
            if detail:
                return detail
        detail = str(payload.get("detail") or "").strip()
        if detail:
            return detail
    body = body_text.strip().replace("\n", " ")
    return body[:200]


def extract_chat_content(payload: Any) -> str:
    """抽取非流式 chat completion 的首条输出。"""

    if not isinstance(payload, dict):
        return ""
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0]
    if not isinstance(first, dict):
        return ""
    message = first.get("message")
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            text_parts = [
                str(item.get("text", "")).strip()
                for item in content
                if isinstance(item, dict) and item.get("type") in {"text", "output_text"}
            ]
            return " ".join(part for part in text_parts if part)
    text = first.get("text")
    if isinstance(text, str):
        return text.strip()
    return ""


def extract_image_reference(payload: Any) -> str:
    """抽取图片返回中的任意可用引用。"""

    if not isinstance(payload, dict):
        return ""
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        return ""
    for item in data:
        if not isinstance(item, dict):
            continue
        for key in ("url", "b64_json", "revised_prompt"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def format_http_status(status: int | None) -> str:
    """格式化状态码列。"""

    return "-" if status is None else str(status)


def format_latency(latency_ms: int | None) -> str:
    """格式化耗时列。"""

    return "-" if latency_ms is None else str(latency_ms)


def render_markdown_table(results: list[ProbeResult]) -> str:
    """渲染 Markdown 表格。"""

    headers = [
        "model",
        "listed_in_models",
        "capability",
        "http_status",
        "latency_ms",
        "result",
        "error_summary",
        "conclusion",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for item in results:
        row = [
            item.model,
            "yes" if item.listed_in_models else "no",
            item.capability,
            format_http_status(item.http_status),
            format_latency(item.latency_ms),
            item.result,
            item.error_summary.replace("|", "\\|"),
            item.conclusion,
        ]
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


class LiveModelSmokeTester:
    """执行 live endpoint 模型发现与最小实调。"""

    def __init__(self, *, base_url: str, api_key: str, timeout_s: float, console_delay_s: float):
        self.base_url = normalize_base_url(base_url)
        self.api_key = api_key.strip()
        self.timeout_s = timeout_s
        self.console_delay_s = console_delay_s
        self.console_quota_blocked = False

    def build_headers(self) -> dict[str, str]:
        """构建鉴权头。"""

        if not self.api_key:
            return {}
        return {"Authorization": f"Bearer {self.api_key}"}

    async def run(self) -> tuple[list[str], list[ProbeResult]]:
        """执行完整冒烟流程。"""

        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            listed_models = await self.discover_models(session)
            results = await self.probe_models(session, listed_models)
            return listed_models, results

    async def discover_models(self, session: aiohttp.ClientSession) -> list[str]:
        """拉取 /v1/models 中当前暴露的模型列表。"""

        status, _, payload, body_text = await self.request_json(
            session,
            "GET",
            "models",
        )
        if status in {401, 403}:
            summary = extract_error_summary(payload, body_text) or "Authentication failed."
            raise RuntimeError(f"/v1/models 鉴权失败：HTTP {status} {summary}")
        if status < 200 or status >= 300:
            summary = extract_error_summary(payload, body_text) or "Unexpected discovery error."
            raise RuntimeError(f"/v1/models 请求失败：HTTP {status} {summary}")
        if not isinstance(payload, dict):
            raise RuntimeError("/v1/models 返回不是 JSON object")
        items = payload.get("data")
        if not isinstance(items, list):
            raise RuntimeError("/v1/models 缺少 data 列表")
        models: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            model_id = item.get("id")
            if isinstance(model_id, str) and model_id.strip():
                models.append(model_id.strip())
        return list(dict.fromkeys(models))

    async def probe_models(
        self,
        session: aiohttp.ClientSession,
        listed_models: list[str],
    ) -> list[ProbeResult]:
        """按注册表顺序输出结果；仅对 live list 中的模型发起验证。"""

        listed_set = set(listed_models)
        ordered_models = [spec.model_name for spec in MODELS]
        registered_set = set(ordered_models)
        extras = [model for model in listed_models if model not in registered_set]
        report_order = ordered_models + extras
        results: list[ProbeResult] = []

        for index, model_name in enumerate(report_order):
            spec = get_model_spec(model_name)
            capability = capability_label(spec)
            listed = model_name in listed_set

            if not listed:
                results.append(
                    ProbeResult(
                        model=model_name,
                        listed_in_models=False,
                        capability=capability,
                        http_status=None,
                        latency_ms=None,
                        result="not_tested",
                        error_summary="",
                        conclusion="未暴露",
                    )
                )
                continue

            if self.console_quota_blocked and capability == "console_chat":
                results.append(
                    ProbeResult(
                        model=model_name,
                        listed_in_models=True,
                        capability=capability,
                        http_status=429,
                        latency_ms=None,
                        result="blocked",
                        error_summary="前序 Console 模型已触发 429，后续按配额阻塞处理",
                        conclusion="配额/风控阻塞",
                    )
                )
                continue

            result = await self.probe_single_model(session, model_name, capability)
            results.append(result)

            # Console 模型单独做轻微节流，避免把共享配额打满。
            if capability == "console_chat" and index < len(report_order) - 1 and not self.console_quota_blocked:
                await asyncio.sleep(self.console_delay_s)

        return results

    async def probe_single_model(
        self,
        session: aiohttp.ClientSession,
        model_name: str,
        capability: str,
    ) -> ProbeResult:
        """按模型能力选择对应的最小验证动作。"""

        if capability in {"chat", "console_chat", "unknown"}:
            result = await self.probe_chat_model(session, model_name, capability)
        elif capability == "image_generation":
            result = await self.probe_image_generation_model(session, model_name, capability)
        elif capability == "image_edit":
            result = await self.probe_image_edit_model(session, model_name, capability)
        elif capability == "video":
            result = await self.probe_video_model(session, model_name, capability)
        else:
            result = ProbeResult(
                model=model_name,
                listed_in_models=True,
                capability=capability,
                http_status=None,
                latency_ms=None,
                result="skipped",
                error_summary=f"Unsupported capability: {capability}",
                conclusion="未实现验证逻辑",
            )

        if capability == "console_chat" and result.http_status == 429:
            self.console_quota_blocked = True
        return result

    async def probe_chat_model(
        self,
        session: aiohttp.ClientSession,
        model_name: str,
        capability: str,
    ) -> ProbeResult:
        """验证文本类模型。"""

        payload = {
            "model": model_name,
            "stream": False,
            "max_tokens": 8,
            "messages": [{"role": "user", "content": CHAT_PROMPT}],
        }
        status, latency_ms, body, raw_text = await self.request_json(
            session,
            "POST",
            "chat/completions",
            json_body=payload,
        )
        if 200 <= status < 300:
            content = extract_chat_content(body)
            if content:
                return ProbeResult(
                    model=model_name,
                    listed_in_models=True,
                    capability=capability,
                    http_status=status,
                    latency_ms=latency_ms,
                    result="ok",
                    error_summary="",
                    conclusion="真实可用",
                )
            return ProbeResult(
                model=model_name,
                listed_in_models=True,
                capability=capability,
                http_status=status,
                latency_ms=latency_ms,
                result="bad_response",
                error_summary="响应成功但未提取到 assistant 内容",
                conclusion="伪可用",
            )
        return self.failed_result(model_name, capability, status, latency_ms, body, raw_text)

    async def probe_image_generation_model(
        self,
        session: aiohttp.ClientSession,
        model_name: str,
        capability: str,
    ) -> ProbeResult:
        """验证图片生成模型。"""

        payload = {
            "model": model_name,
            "prompt": IMAGE_PROMPT,
            "n": 1,
            "size": "1024x1024",
            "response_format": "url",
        }
        status, latency_ms, body, raw_text = await self.request_json(
            session,
            "POST",
            "images/generations",
            json_body=payload,
        )
        if 200 <= status < 300 and extract_image_reference(body):
            return ProbeResult(
                model=model_name,
                listed_in_models=True,
                capability=capability,
                http_status=status,
                latency_ms=latency_ms,
                result="ok",
                error_summary="",
                conclusion="真实可用",
            )
        if 200 <= status < 300:
            return ProbeResult(
                model=model_name,
                listed_in_models=True,
                capability=capability,
                http_status=status,
                latency_ms=latency_ms,
                result="bad_response",
                error_summary="响应成功但未提取到图片引用",
                conclusion="伪可用",
            )
        return self.failed_result(model_name, capability, status, latency_ms, body, raw_text)

    async def probe_image_edit_model(
        self,
        session: aiohttp.ClientSession,
        model_name: str,
        capability: str,
    ) -> ProbeResult:
        """验证图片编辑模型。"""

        form = aiohttp.FormData()
        form.add_field("model", model_name)
        form.add_field("prompt", IMAGE_EDIT_PROMPT)
        form.add_field("n", "1")
        form.add_field("size", "1024x1024")
        form.add_field("response_format", "url")
        form.add_field(
            "image[]",
            PNG_1X1_BLUE,
            filename="pixel.png",
            content_type="image/png",
        )
        status, latency_ms, body, raw_text = await self.request_json(
            session,
            "POST",
            "images/edits",
            form_data=form,
        )
        if 200 <= status < 300 and extract_image_reference(body):
            return ProbeResult(
                model=model_name,
                listed_in_models=True,
                capability=capability,
                http_status=status,
                latency_ms=latency_ms,
                result="ok",
                error_summary="",
                conclusion="真实可用",
            )
        if 200 <= status < 300:
            return ProbeResult(
                model=model_name,
                listed_in_models=True,
                capability=capability,
                http_status=status,
                latency_ms=latency_ms,
                result="bad_response",
                error_summary="响应成功但未提取到图片引用",
                conclusion="伪可用",
            )
        return self.failed_result(model_name, capability, status, latency_ms, body, raw_text)

    async def probe_video_model(
        self,
        session: aiohttp.ClientSession,
        model_name: str,
        capability: str,
    ) -> ProbeResult:
        """验证视频创建与首次查询链路。"""

        form = aiohttp.FormData()
        form.add_field("model", model_name)
        form.add_field("prompt", VIDEO_PROMPT)
        form.add_field("seconds", "6")
        form.add_field("size", "720x1280")

        create_status, create_latency_ms, create_body, create_text = await self.request_json(
            session,
            "POST",
            "videos",
            form_data=form,
        )
        if create_status < 200 or create_status >= 300:
            return self.failed_result(
                model_name,
                capability,
                create_status,
                create_latency_ms,
                create_body,
                create_text,
            )
        video_id = ""
        if isinstance(create_body, dict):
            raw_id = create_body.get("id")
            if isinstance(raw_id, str):
                video_id = raw_id.strip()
        if not video_id:
            return ProbeResult(
                model=model_name,
                listed_in_models=True,
                capability=capability,
                http_status=create_status,
                latency_ms=create_latency_ms,
                result="bad_response",
                error_summary="视频创建成功但返回缺少 id",
                conclusion="伪可用",
            )

        retrieve_status, retrieve_latency_ms, retrieve_body, retrieve_text = await self.request_json(
            session,
            "GET",
            f"videos/{video_id}",
        )
        total_latency_ms = (create_latency_ms or 0) + (retrieve_latency_ms or 0)
        if 200 <= retrieve_status < 300 and isinstance(retrieve_body, dict):
            status_value = str(retrieve_body.get("status") or "").strip()
            if status_value:
                return ProbeResult(
                    model=model_name,
                    listed_in_models=True,
                    capability=capability,
                    http_status=retrieve_status,
                    latency_ms=total_latency_ms,
                    result="ok",
                    error_summary="",
                    conclusion="真实可用",
                )
            return ProbeResult(
                model=model_name,
                listed_in_models=True,
                capability=capability,
                http_status=retrieve_status,
                latency_ms=total_latency_ms,
                result="bad_response",
                error_summary="视频查询成功但返回缺少 status",
                conclusion="伪可用",
            )
        return self.failed_result(
            model_name,
            capability,
            retrieve_status,
            total_latency_ms,
            retrieve_body,
            retrieve_text,
        )

    def failed_result(
        self,
        model_name: str,
        capability: str,
        status: int,
        latency_ms: int | None,
        payload: Any,
        body_text: str,
    ) -> ProbeResult:
        """统一生成失败记录。"""

        summary = extract_error_summary(payload, body_text)
        conclusion = "配额/风控阻塞" if status == 429 else "伪可用"
        result = "blocked" if status == 429 else "failed"
        return ProbeResult(
            model=model_name,
            listed_in_models=True,
            capability=capability,
            http_status=status,
            latency_ms=latency_ms,
            result=result,
            error_summary=summary,
            conclusion=conclusion,
        )

    async def request_json(
        self,
        session: aiohttp.ClientSession,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        form_data: aiohttp.FormData | None = None,
    ) -> tuple[int, int, Any, str]:
        """发起一次请求，并尽量把响应解析成 JSON。"""

        url = urljoin(self.base_url, path)
        headers = self.build_headers()
        start = time.perf_counter()
        try:
            async with session.request(
                method,
                url,
                headers=headers,
                json=json_body,
                data=form_data,
            ) as response:
                body_text = await response.text()
                latency_ms = int((time.perf_counter() - start) * 1000)
                try:
                    payload = json.loads(body_text)
                except json.JSONDecodeError:
                    payload = None
                return response.status, latency_ms, payload, body_text
        except asyncio.TimeoutError as exc:
            raise RuntimeError(f"请求超时：{method} {url}") from exc
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"请求失败：{method} {url} {exc}") from exc


def summarize_results(results: list[ProbeResult]) -> dict[str, int]:
    """统计各类结论数量。"""

    summary = {
        "真实可用": 0,
        "伪可用": 0,
        "未暴露": 0,
        "配额/风控阻塞": 0,
        "其他": 0,
    }
    for item in results:
        if item.conclusion in summary:
            summary[item.conclusion] += 1
        else:
            summary["其他"] += 1
    return summary


def write_json_report(
    output_path: str,
    *,
    base_url: str,
    listed_models: list[str],
    results: list[ProbeResult],
) -> None:
    """把结果落到 JSON 文件。"""

    payload = {
        "base_url": normalize_base_url(base_url).rstrip("/"),
        "listed_models_count": len(listed_models),
        "listed_models": listed_models,
        "summary": summarize_results(results),
        "results": [asdict(item) for item in results],
    }
    Path(output_path).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


async def async_main(args: argparse.Namespace) -> int:
    """异步主流程。"""

    tester = LiveModelSmokeTester(
        base_url=args.base_url,
        api_key=args.api_key,
        timeout_s=args.timeout,
        console_delay_s=args.console_delay,
    )
    listed_models, results = await tester.run()
    summary = summarize_results(results)

    print(f"Base URL: {normalize_base_url(args.base_url).rstrip('/')}")
    print(f"Listed models: {len(listed_models)}")
    print(
        "Summary: "
        + ", ".join(f"{key}={value}" for key, value in summary.items() if value)
    )
    print()
    print(render_markdown_table(results))

    if args.json_output:
        write_json_report(
            args.json_output,
            base_url=args.base_url,
            listed_models=listed_models,
            results=results,
        )
        print()
        print(f"JSON report written to: {args.json_output}")
    return 0


def main() -> int:
    """同步入口。"""

    args = parse_args()
    try:
        return asyncio.run(async_main(args))
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
