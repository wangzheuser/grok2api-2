#!/usr/bin/env python3
"""
模型可用性基准脚本：发现 /v1/models 中暴露的模型，并对每个模型做多轮实调统计。

能力覆盖：
- 文本 / Console / 未识别模型：POST /v1/chat/completions
- 图片生成模型：POST /v1/images/generations

输出目标：
- 标准输出打印每次测试明细和每模型汇总表
- 可选写出 JSON 报告，便于留档或二次处理
"""

from __future__ import annotations

import argparse
import json
import socket
import ssl
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from http.client import HTTPConnection, HTTPSConnection, HTTPException
from urllib.parse import urljoin, urlparse

CHAT_PROMPT = "只回复 OK"
IMAGE_PROMPT = "a red dot on white background"
CACHE_PATH = Path.home() / ".grok2api_model_benchmark.json"
CAPABILITY_ORDER = [
    "grok-4.3-high",
    "grok-4.3-medium",
    "grok-4.3-low",
    "grok-4.3-console",
    "grok-4.20-multi-agent-xhigh",
    "grok-4.20-multi-agent-high",
    "grok-4.20-multi-agent-medium",
    "grok-4.20-multi-agent-low",
    "grok-4.20-multi-agent-console",
    "grok-4.20-0309-reasoning-console",
    "grok-4.20-0309-console",
    "grok-4.20-0309-non-reasoning-console",
    "grok-build-console",
]
CAPABILITY_RANK = {model: index for index, model in enumerate(CAPABILITY_ORDER)}
CAPABILITY_ZH = {
    "chat": "文本对话",
    "console_chat": "Console文本对话",
    "image_generation": "图片生成",
}


@dataclass
class AttemptResult:
    """单次模型调用结果。"""

    model: str
    capability: str
    attempt: int
    success: bool
    http_status: int | None
    latency_ms: int | None
    error_summary: str
    response_preview: str


@dataclass
class ModelSummary:
    """单个模型的多轮测试汇总。"""

    model: str
    capability: str
    attempts: int
    success_count: int
    failure_count: int
    success_rate: float
    avg_latency_ms: int | None
    min_latency_ms: int | None
    max_latency_ms: int | None
    p95_latency_ms: int | None


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""

    parser = argparse.ArgumentParser(description="Benchmark Grok2API model availability.")
    parser.add_argument(
        "--base-url",
        default="",
        help="目标 OpenAI 兼容端点；支持传 https://host/v1 或 https://host。未传则交互式输入。",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Bearer API Key；未传则交互式明文输入，留空时不附带 Authorization 头。",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=None,
        help="每个模型测试次数，范围 1~10；未传则交互式输入，默认 1。",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=90.0,
        help="单次 HTTP 请求超时秒数，默认 90。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="每次请求后的等待秒数，默认 1.0。",
    )
    parser.add_argument(
        "--models",
        default="",
        help="可选：逗号分隔指定模型列表；默认测试 /v1/models 返回的全部模型。",
    )
    parser.add_argument(
        "--json-output",
        default="",
        help="可选：将完整结果写入 JSON 文件。",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="不读取也不保存交互式 Base URL / API Key 缓存。",
    )
    return parser.parse_args()


def load_cached_credentials() -> dict[str, str]:
    """读取上次缓存的 Base URL 和 API Key。"""

    if not CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        "base_url": str(payload.get("base_url") or "").strip(),
        "api_key": str(payload.get("api_key") or "").strip(),
    }


def save_cached_credentials(base_url: str, api_key: str) -> None:
    """保存本次使用的 Base URL 和 API Key，供下次默认使用。"""

    payload = {
        "base_url": base_url.strip(),
        "api_key": api_key.strip(),
    }
    CACHE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    try:
        CACHE_PATH.chmod(0o600)
    except OSError:
        # chmod 失败不影响脚本主流程。
        pass


def prompt_with_default(label: str, default: str, *, required: bool) -> str:
    """读取交互式输入，空输入时使用默认值。"""

    while True:
        suffix = f" [{default}]" if default else ""
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default:
            return default
        if not required:
            return ""
        print(f"{label} 不能为空。")


def prompt_int_with_default(label: str, default: int, *, min_value: int, max_value: int) -> int:
    """读取交互式整数输入，空输入时使用默认值。"""

    while True:
        raw_value = input(f"{label} [{default}]: ").strip()
        if not raw_value:
            return default
        try:
            value = int(raw_value)
        except ValueError:
            print(f"{label} 必须是整数。")
            continue
        if min_value <= value <= max_value:
            return value
        print(f"{label} 必须在 {min_value}~{max_value} 之间。")


def prompt_missing_args(args: argparse.Namespace) -> None:
    """补齐需要交互式输入的参数，并缓存本次使用值。"""

    cached = {} if args.no_cache else load_cached_credentials()
    if not args.base_url.strip():
        args.base_url = prompt_with_default(
            "Base URL",
            cached.get("base_url", ""),
            required=True,
        )
    if args.api_key is None:
        args.api_key = prompt_with_default(
            "API Key",
            cached.get("api_key", ""),
            required=False,
        )
    if args.runs is None:
        args.runs = prompt_int_with_default(
            "测试次数",
            1,
            min_value=1,
            max_value=10,
        )
    if not args.no_cache:
        save_cached_credentials(args.base_url, args.api_key or "")


def validate_args(args: argparse.Namespace) -> None:
    """校验基础参数，避免无意义请求。"""

    if args.runs is None:
        args.runs = 1
    if not 1 <= args.runs <= 10:
        raise ValueError("--runs 必须在 1~10 之间")
    if args.timeout <= 0:
        raise ValueError("--timeout 必须大于 0")
    if args.delay < 0:
        raise ValueError("--delay 不能小于 0")


def normalize_base_url(raw_base_url: str) -> str:
    """规范化 base URL，确保最终路径落在 /v1/ 下。"""

    base = raw_base_url.strip().rstrip("/")
    if base.endswith("/v1"):
        return f"{base}/"
    return f"{base}/v1/"


def parse_model_filter(raw_models: str) -> list[str]:
    """解析逗号分隔模型过滤列表。"""

    models = [item.strip() for item in raw_models.split(",") if item.strip()]
    return list(dict.fromkeys(models))


def capability_label(model_name: str) -> str:
    """按模型名推断测试能力，避免脚本依赖项目运行时。"""

    lowered = model_name.lower()
    if "image" in lowered or "imagine" in lowered:
        return "image_generation"
    if "console" in lowered or "grok-build" in lowered or "multi-agent" in lowered or "grok-4.3" in lowered:
        return "console_chat"
    return "chat"


def extract_error_summary(payload: Any, body_text: str) -> str:
    """尽量从 JSON 错误体中抽出可读摘要。"""

    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = str(error.get("message") or "").strip()
            code = str(error.get("code") or "").strip()
            if message and code:
                return f"{message} [{code}]"
            if message:
                return message
        detail = str(payload.get("detail") or "").strip()
        if detail:
            return detail
    return body_text.strip().replace("\n", " ")[:200]


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
            parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") in {"text", "output_text"}:
                    text = str(item.get("text") or "").strip()
                    if text:
                        parts.append(text)
            return " ".join(parts)
    text = first.get("text")
    if isinstance(text, str):
        return text.strip()
    return ""


def extract_image_reference(payload: Any) -> str:
    """抽取图片生成返回中的任意可用引用。"""

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


def preview_text(value: str, limit: int = 120) -> str:
    """生成适合表格展示的响应预览。"""

    normalized = value.strip().replace("\n", " ")
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def percentile_95(values: list[int]) -> int | None:
    """计算整数耗时列表的 P95。"""

    if not values:
        return None
    if len(values) == 1:
        return values[0]
    sorted_values = sorted(values)
    index = int((len(sorted_values) - 1) * 0.95)
    return sorted_values[index]


def markdown_escape(value: Any) -> str:
    """转义 Markdown 表格单元格。"""

    return str(value if value is not None else "-").replace("|", "\\|").replace("\n", " ")


def capability_zh(capability: str) -> str:
    """把能力类型转成中文展示文案。"""

    return CAPABILITY_ZH.get(capability, capability)


def capability_level(model_name: str) -> str:
    """生成模型推荐等级说明。"""

    rank = CAPABILITY_RANK.get(model_name)
    if rank is None:
        return "未知推荐等级"
    return f"第 {rank + 1} 推荐档"


def sort_usable_summaries(summaries: list[ModelSummary]) -> list[ModelSummary]:
    """筛选真实可用模型，并按固定推荐优先级排序。"""

    usable = [item for item in summaries if item.success_rate == 100.0]
    original_order = {item.model: index for index, item in enumerate(summaries)}
    return sorted(
        usable,
        key=lambda item: (
            CAPABILITY_RANK.get(item.model, len(CAPABILITY_ORDER) + original_order[item.model]),
            original_order[item.model],
        ),
    )


class ModelAvailabilityBenchmark:
    """执行模型发现和多轮可用性测试。"""

    def __init__(self, *, base_url: str, api_key: str, runs: int, timeout_s: float, delay_s: float):
        self.base_url = normalize_base_url(base_url)
        self.api_key = api_key.strip()
        self.runs = runs
        self.timeout_s = timeout_s
        self.delay_s = delay_s

    def build_headers(self) -> dict[str, str]:
        """构建鉴权头。"""

        if not self.api_key:
            return {}
        return {"Authorization": f"Bearer {self.api_key}"}

    def run(self, selected_models: list[str]) -> tuple[list[str], list[AttemptResult], list[ModelSummary]]:
        """执行完整测试流程。"""

        discovered_models = self.discover_models()
        target_models = selected_models or discovered_models
        attempts = self.probe_models(target_models)
        return discovered_models, attempts, summarize_attempts(attempts)

    def discover_models(self) -> list[str]:
        """拉取 /v1/models 中当前暴露的模型列表。"""

        status, _, payload, body_text = self.request_json("GET", "models")
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

    def probe_models(self, models: list[str]) -> list[AttemptResult]:
        """串行测试每个模型，保证每个模型完整运行指定次数。"""

        results: list[AttemptResult] = []
        for model_name in models:
            capability = capability_label(model_name)
            print(f"正在测试 {model_name}（{capability_zh(capability)}）...", flush=True)
            for attempt in range(1, self.runs + 1):
                result = self.probe_once(model_name, capability, attempt)
                results.append(result)
                status = result.http_status if result.http_status is not None else "-"
                latency = result.latency_ms if result.latency_ms is not None else "-"
                print(
                    f"  第 {attempt} 次：{'成功' if result.success else '失败'} "
                    f"HTTP状态={status} 耗时(ms)={latency} {result.error_summary}",
                    flush=True,
                )
                if self.delay_s > 0:
                    time.sleep(self.delay_s)
        return results

    def probe_once(
        self,
        model_name: str,
        capability: str,
        attempt: int,
    ) -> AttemptResult:
        """按模型能力执行一次最小可用性验证。"""

        if capability == "image_generation":
            return self.probe_image_generation_once(model_name, capability, attempt)
        return self.probe_chat_once(model_name, capability, attempt)

    def probe_chat_once(
        self,
        model_name: str,
        capability: str,
        attempt: int,
    ) -> AttemptResult:
        """验证一次文本类模型。"""

        payload = {
            "model": model_name,
            "stream": False,
            "max_tokens": 8,
            "messages": [{"role": "user", "content": CHAT_PROMPT}],
        }
        status, latency_ms, body, raw_text = self.request_json(
            "POST",
            "chat/completions",
            json_body=payload,
        )
        content = extract_chat_content(body)
        success = 200 <= status < 300 and bool(content)
        return AttemptResult(
            model=model_name,
            capability=capability,
            attempt=attempt,
            success=success,
            http_status=status,
            latency_ms=latency_ms,
            error_summary="" if success else extract_error_summary(body, raw_text) or "未提取到 assistant 内容",
            response_preview=preview_text(content if content else raw_text),
        )

    def probe_image_generation_once(
        self,
        model_name: str,
        capability: str,
        attempt: int,
    ) -> AttemptResult:
        """验证一次图片生成模型。"""

        payload = {
            "model": model_name,
            "prompt": IMAGE_PROMPT,
            "n": 1,
            "size": "1024x1024",
            "response_format": "url",
        }
        status, latency_ms, body, raw_text = self.request_json(
            "POST",
            "images/generations",
            json_body=payload,
        )
        image_reference = extract_image_reference(body)
        success = 200 <= status < 300 and bool(image_reference)
        return AttemptResult(
            model=model_name,
            capability=capability,
            attempt=attempt,
            success=success,
            http_status=status,
            latency_ms=latency_ms,
            error_summary="" if success else extract_error_summary(body, raw_text) or "未提取到图片引用",
            response_preview=preview_text(image_reference if image_reference else raw_text),
        )

    def request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> tuple[int, int, Any, str]:
        """发起一次请求，并尽量把响应解析成 JSON。"""

        url = urljoin(self.base_url, path)
        headers = self.build_headers()
        body_bytes = b""
        if json_body is not None:
            body_bytes = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            headers = {**headers, "Content-Type": "application/json"}
        headers = {**headers, "Accept": "application/json"}

        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return 0, 0, None, f"不支持的 URL scheme：{parsed.scheme}"
        path_with_query = parsed.path or "/"
        if parsed.query:
            path_with_query = f"{path_with_query}?{parsed.query}"

        connection_cls = HTTPSConnection if parsed.scheme == "https" else HTTPConnection
        port = parsed.port
        start = time.perf_counter()
        conn = None
        try:
            conn = connection_cls(parsed.hostname, port=port, timeout=self.timeout_s)
            conn.request(method, path_with_query, body=body_bytes if body_bytes else None, headers=headers)
            response = conn.getresponse()
            raw_body = response.read()
            latency_ms = int((time.perf_counter() - start) * 1000)
            body_text = raw_body.decode("utf-8", errors="replace")
            try:
                payload = json.loads(body_text)
            except json.JSONDecodeError:
                payload = None
            return response.status, latency_ms, payload, body_text
        except TimeoutError:
            latency_ms = int((time.perf_counter() - start) * 1000)
            return 0, latency_ms, None, "请求超时"
        except (HTTPException, OSError, socket.timeout, ssl.SSLError) as exc:
            latency_ms = int((time.perf_counter() - start) * 1000)
            return 0, latency_ms, None, f"请求失败：{exc}"
        finally:
            if conn is not None:
                conn.close()


def summarize_attempts(attempts: list[AttemptResult]) -> list[ModelSummary]:
    """按模型聚合单次测试结果。"""

    grouped: dict[str, list[AttemptResult]] = {}
    for item in attempts:
        grouped.setdefault(item.model, []).append(item)

    summaries: list[ModelSummary] = []
    for model, items in grouped.items():
        success_count = sum(1 for item in items if item.success)
        latencies = [item.latency_ms for item in items if item.latency_ms is not None]
        summaries.append(
            ModelSummary(
                model=model,
                capability=items[0].capability,
                attempts=len(items),
                success_count=success_count,
                failure_count=len(items) - success_count,
                success_rate=round(success_count / len(items) * 100, 2) if items else 0.0,
                avg_latency_ms=round(statistics.mean(latencies)) if latencies else None,
                min_latency_ms=min(latencies) if latencies else None,
                max_latency_ms=max(latencies) if latencies else None,
                p95_latency_ms=percentile_95(latencies),
            )
        )
    return summaries


def render_attempt_table(attempts: list[AttemptResult]) -> str:
    """渲染每次测试明细表。"""

    headers = ["模型ID", "能力类型", "第几次", "是否成功", "HTTP状态", "耗时(ms)", "错误摘要", "响应预览"]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for item in attempts:
        row = [
            item.model,
            capability_zh(item.capability),
            item.attempt,
            "是" if item.success else "否",
            item.http_status,
            item.latency_ms,
            item.error_summary,
            item.response_preview,
        ]
        lines.append("| " + " | ".join(markdown_escape(value) for value in row) + " |")
    return "\n".join(lines)


def render_summary_table(summaries: list[ModelSummary]) -> str:
    """渲染每模型汇总表。"""

    headers = [
        "模型ID",
        "能力类型",
        "测试次数",
        "成功次数",
        "失败次数",
        "成功率(%)",
        "平均耗时(ms)",
        "最小耗时(ms)",
        "最大耗时(ms)",
        "P95耗时(ms)",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for item in summaries:
        row = [
            item.model,
            capability_zh(item.capability),
            item.attempts,
            item.success_count,
            item.failure_count,
            item.success_rate,
            item.avg_latency_ms,
            item.min_latency_ms,
            item.max_latency_ms,
            item.p95_latency_ms,
        ]
        lines.append("| " + " | ".join(markdown_escape(value) for value in row) + " |")
    return "\n".join(lines)


def render_usable_models_table(summaries: list[ModelSummary]) -> str:
    """渲染真实可用模型推荐优先级表。"""

    usable = sort_usable_summaries(summaries)
    headers = ["排序", "模型ID", "推荐等级", "成功率(%)", "平均耗时(ms)", "P95耗时(ms)"]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    if not usable:
        lines.append("| - | 暂无真实可用模型 | - | - | - | - |")
        return "\n".join(lines)
    for index, item in enumerate(usable, start=1):
        row = [
            index,
            item.model,
            capability_level(item.model),
            item.success_rate,
            item.avg_latency_ms,
            item.p95_latency_ms,
        ]
        lines.append("| " + " | ".join(markdown_escape(value) for value in row) + " |")
    return "\n".join(lines)


def write_json_report(
    output_path: str,
    *,
    base_url: str,
    runs: int,
    discovered_models: list[str],
    attempts: list[AttemptResult],
    summaries: list[ModelSummary],
) -> None:
    """把完整结果落到 JSON 文件，不写入 API Key。"""

    usable_models = sort_usable_summaries(summaries)
    payload = {
        "base_url": normalize_base_url(base_url).rstrip("/"),
        "runs": runs,
        "discovered_models_count": len(discovered_models),
        "discovered_models": discovered_models,
        "summaries": [asdict(item) for item in summaries],
        "attempts": [asdict(item) for item in attempts],
        "usable_models_by_capability": [
            {
                "rank": index,
                "model": item.model,
                "capability_level": capability_level(item.model),
                "success_rate": item.success_rate,
                "avg_latency_ms": item.avg_latency_ms,
                "p95_latency_ms": item.p95_latency_ms,
            }
            for index, item in enumerate(usable_models, start=1)
        ],
    }
    Path(output_path).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def run_main(args: argparse.Namespace) -> int:
    """主流程。"""

    selected_models = parse_model_filter(args.models)
    benchmark = ModelAvailabilityBenchmark(
        base_url=args.base_url,
        api_key=args.api_key or "",
        runs=args.runs,
        timeout_s=args.timeout,
        delay_s=args.delay,
    )
    discovered_models, attempts, summaries = benchmark.run(selected_models)

    print()
    print(f"服务地址：{normalize_base_url(args.base_url).rstrip('/')}")
    print(f"发现模型数：{len(discovered_models)}")
    print(f"已测试模型数：{len(summaries)}")
    print(f"每个模型测试次数：{args.runs}")
    print()
    print("## 真实可用模型（按推荐优先级排序）")
    print(render_usable_models_table(summaries))
    print()
    print("## 汇总结果")
    print(render_summary_table(summaries))
    print()
    print("## 单次测试明细")
    print(render_attempt_table(attempts))

    if args.json_output:
        write_json_report(
            args.json_output,
            base_url=args.base_url,
            runs=args.runs,
            discovered_models=discovered_models,
            attempts=attempts,
            summaries=summaries,
        )
        print()
        print(f"JSON报告已写入：{args.json_output}")
    return 0


def main() -> int:
    """同步入口。"""

    args = parse_args()
    try:
        prompt_missing_args(args)
        validate_args(args)
        return run_main(args)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
