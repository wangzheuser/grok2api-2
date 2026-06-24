#!/usr/bin/env python3
# ruff: noqa: E402
"""Console 429 A/B probe for account/proxy/root-cause diagnosis.

该脚本直接调用 console.x.ai 上游，不经过本服务 HTTP 入口，用于区分：
- 同账号换代理是否改善：IP / 出口维度；
- 同代理换账号是否改善：账号 / SSO token 维度；
- 同账号同代理是否间歇成功：上游模型容量 / 瞬态波动。

报告中只保存 token / proxy 的短哈希和标签，不保存明文凭据。
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import orjson

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.control.account.enums import AccountStatus
from app.control.account.backends.factory import create_repository
from app.dataplane.proxy.adapters.headers import build_console_headers
from app.dataplane.proxy.adapters.profile import resolve_proxy_profile
from app.dataplane.proxy.adapters.session import normalize_proxy_url
from app.dataplane.reverse.protocol.xai_console_chat import (
    build_console_payload,
    classify_console_line,
)
from app.platform.config.snapshot import config, get_config

DEFAULT_MODELS = (
    "grok-4.3-console",
    "grok-4.3-low",
    "grok-4.3-medium",
    "grok-4.3-high",
)
CONSOLE_URL = "https://console.x.ai/v1/responses"
PROMPT = "只回复 OK"


@dataclass(slots=True)
class AccountSample:
    """脱敏账号样本。"""

    index: int
    token: str
    token_hash: str
    pool: str
    console_remaining: int | None
    usage_success: int
    usage_fail: int


@dataclass(slots=True)
class ProxySample:
    """代理样本，报告中仅暴露 label/hash。"""

    label: str
    proxy_url: str | None
    proxy_hash: str
    source: str

    @property
    def has_proxy(self) -> bool:
        """Return whether this sample uses an outbound proxy."""
        return bool(self.proxy_url)


@dataclass(slots=True)
class ProbeAttempt:
    """单次 A/B 调用结果。"""

    model: str
    account_index: int
    token_hash: str
    proxy_label: str
    proxy_hash: str
    proxy_source: str
    has_proxy: bool
    run: int
    http_status: int | None
    success: bool
    latency_ms: int
    body_class: str
    body_hash: str
    body_preview: str
    output_preview: str
    error: str


def short_hash(value: str | None) -> str:
    """Return a stable short hash for sensitive correlation fields."""
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8", "ignore")).hexdigest()[:12]


def sanitize_preview(text: str, *, limit: int = 240) -> str:
    """Return a compact preview with obvious token-like values redacted."""
    import re

    if not text:
        return ""
    compact = " ".join(text.replace("\r", " ").replace("\n", " ").split())
    compact = re.sub(r"(?i)(bearer\s+)[a-z0-9._~+/=-]+", r"\1<redacted>", compact)
    compact = re.sub(r"(?i)(sso(?:-rw)?=)[^;\s]+", r"\1<redacted>", compact)
    compact = re.sub(r"(?i)(cf_clearance=)[^;\s]+", r"\1<redacted>", compact)
    compact = re.sub(r"[A-Za-z0-9_-]{32,}", "<redacted>", compact)
    return compact[:limit]


def classify_body(status: int | None, body: str) -> str:
    """Classify upstream body into a root-cause-oriented bucket."""
    lower = body.lower()
    if status == 200:
        return "ok"
    if "insufficient_model_capacity" in lower or "model capacity" in lower:
        return "upstream_capacity"
    if "monthly_request_count" in lower or "quota" in lower:
        return "account_quota"
    if (
        "suspicious activity" in lower
        or "account throttled" in lower
        or "account_throttled" in lower
    ):
        return "account_throttle"
    if "rate limit" in lower or "too many requests" in lower:
        return "rate_limit"
    if status == 403 or "cloudflare" in lower or "cf_clearance" in lower or "challenge" in lower:
        return "cloudflare_challenge"
    if status == 429:
        return "rate_limit_empty" if not body else "rate_limit_unknown"
    if not body:
        return "empty_body"
    return "unknown"


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description="Probe console.x.ai 429 root cause by account/proxy A/B.")
    parser.add_argument("--models", default=",".join(DEFAULT_MODELS), help="逗号分隔模型列表。")
    parser.add_argument("--accounts", type=int, default=6, help="抽样账号数量。")
    parser.add_argument("--runs", type=int, default=2, help="每个 account/proxy/model 组合运行次数。")
    parser.add_argument("--timeout", type=float, default=90.0, help="单请求超时秒数。")
    parser.add_argument("--delay", type=float, default=0.5, help="每次请求后的等待秒数。")
    parser.add_argument("--concurrency", type=int, default=1, help="并发数，默认串行降低干扰。")
    parser.add_argument("--output", default="", help="JSON 报告输出路径。")
    parser.add_argument("--proxies", default="", help="逗号分隔代理 URL。")
    parser.add_argument("--proxy-file", default="", help="代理池 JSON 文件，例如 kiro.rs/config/proxy_pool.json。")
    parser.add_argument("--include-config-proxy", action="store_true", help="加入当前配置中的代理。")
    parser.add_argument("--include-direct", action="store_true", help="加入直连样本。")
    parser.add_argument("--max-proxies", type=int, default=4, help="代理样本上限。")
    parser.add_argument(
        "--account-pool",
        default="basic",
        choices=("basic", "super", "heavy", "any"),
        help="账号池过滤，默认 basic（console 模型主路径）。",
    )
    parser.add_argument(
        "--prefer-console-quota",
        action="store_true",
        help="优先选择 console 剩余额度较高的账号。",
    )
    return parser.parse_args()


def split_csv(raw: str) -> list[str]:
    """Split a comma-separated string and de-duplicate values."""
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return list(dict.fromkeys(values))


def render_dynamic_value(value: str, now_ms: int) -> str:
    """Render kiro.rs-style dynamic proxy placeholders."""
    return value.replace("{time}", str(now_ms))


def proxy_url_with_auth(url: str, username: str | None, password: str | None) -> str:
    """Attach username/password to a proxy URL when credentials are separate."""
    if not username and not password:
        return url
    parts = urlsplit(url)
    if parts.username or "@" in parts.netloc:
        return url
    user = quote(username or "", safe="")
    pwd = quote(password or "", safe="")
    auth = f"{user}:{pwd}@" if password is not None else f"{user}@"
    return urlunsplit((parts.scheme, f"{auth}{parts.netloc}", parts.path, parts.query, parts.fragment))


def load_proxy_file(path: Path) -> list[str]:
    """Load proxy URLs from a kiro.rs-style proxy_pool.json file."""
    if not path.exists():
        return []
    now_ms = int(time.time() * 1000)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        return []

    urls: list[str] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if item.get("enabled") is False:
            continue
        raw_url = str(item.get("url") or "").strip()
        if not raw_url:
            continue
        username = item.get("username")
        password = item.get("password")
        url = render_dynamic_value(raw_url, now_ms)
        user = render_dynamic_value(str(username), now_ms) if username is not None else None
        pwd = render_dynamic_value(str(password), now_ms) if password is not None else None
        urls.append(proxy_url_with_auth(url, user, pwd))
    return urls


def config_proxy_urls() -> list[str]:
    """Read configured proxy URLs from current config, including legacy keys."""
    cfg = get_config()
    values: list[str] = []
    for key in (
        "proxy.egress.proxy_url",
        "proxy.base_proxy_url",
    ):
        value = str(cfg.get(key, "") or "").strip()
        if value:
            values.append(value)
    for key in ("proxy.egress.proxy_pool",):
        values.extend(str(item).strip() for item in cfg.get_list(key, []) if str(item).strip())
    return list(dict.fromkeys(values))


async def load_accounts(limit: int, pool: str, prefer_console_quota: bool) -> list[AccountSample]:
    """Load active account samples from the configured account repository."""
    repo = create_repository()
    await repo.initialize()
    try:
        snapshot = await repo.runtime_snapshot()
    finally:
        await repo.close()

    candidates = []
    for record in snapshot.items:
        if record.status != AccountStatus.ACTIVE or record.is_deleted():
            continue
        if pool != "any" and record.pool != pool:
            continue
        quota = record.quota_set()
        console_window = quota.console
        remaining = console_window.remaining if console_window else None
        candidates.append((record, remaining))

    if prefer_console_quota:
        candidates.sort(
            key=lambda item: (
                -1 if item[1] is None else -int(item[1]),
                item[0].usage_fail_count,
                -item[0].usage_use_count,
            )
        )
    else:
        candidates.sort(key=lambda item: (item[0].usage_fail_count, -item[0].usage_use_count))

    samples: list[AccountSample] = []
    for index, (record, remaining) in enumerate(candidates[:limit]):
        samples.append(
            AccountSample(
                index=index,
                token=record.token,
                token_hash=short_hash(record.token),
                pool=record.pool,
                console_remaining=remaining,
                usage_success=record.usage_use_count,
                usage_fail=record.usage_fail_count,
            )
        )
    return samples


async def build_proxy_samples(args: argparse.Namespace) -> list[ProxySample]:
    """Build proxy samples from direct/config/manual/file sources."""
    await config.load()
    urls: list[tuple[str, str]] = []

    if args.include_direct:
        urls.append(("direct", ""))
    if args.include_config_proxy:
        urls.extend(("config", url) for url in config_proxy_urls())
    urls.extend(("arg", url) for url in split_csv(args.proxies))
    if args.proxy_file:
        urls.extend(("file", url) for url in load_proxy_file(Path(args.proxy_file)))

    dedup: list[tuple[str, str]] = []
    seen: set[str] = set()
    for source, url in urls:
        normalized = normalize_proxy_url(url) if url else ""
        key = normalized or "<direct>"
        if key in seen:
            continue
        seen.add(key)
        dedup.append((source, normalized))

    samples: list[ProxySample] = []
    for idx, (source, url) in enumerate(dedup[: max(1, args.max_proxies)]):
        label = "direct" if not url else f"{source}-{idx}"
        samples.append(
            ProxySample(
                label=label,
                proxy_url=url or None,
                proxy_hash=short_hash(url),
                source=source,
            )
        )
    return samples


def session_kwargs_for_proxy(proxy: ProxySample) -> dict[str, Any]:
    """Build curl_cffi AsyncSession kwargs for one proxy sample."""
    profile = resolve_proxy_profile(None)
    kwargs: dict[str, Any] = {}
    if profile.browser:
        kwargs["impersonate"] = profile.browser
    if proxy.proxy_url:
        if urlsplit(proxy.proxy_url).scheme.lower().startswith("socks"):
            kwargs["proxy"] = proxy.proxy_url
        else:
            kwargs["proxies"] = {"http": proxy.proxy_url, "https": proxy.proxy_url}
    return kwargs


async def probe_once(
    *,
    model: str,
    account: AccountSample,
    proxy: ProxySample,
    run: int,
    timeout: float,
) -> ProbeAttempt:
    """Execute one direct console.x.ai call for a specific account/proxy/model."""
    from curl_cffi.requests import AsyncSession

    payload = build_console_payload(
        messages=[{"role": "user", "content": PROMPT}],
        model=model,
        temperature=0.0,
        top_p=1.0,
        reasoning_effort="low",
        stream=True,
    )
    headers = build_console_headers(account.token)
    body = orjson.dumps(payload)
    started = time.perf_counter()
    status: int | None = None
    body_text = ""
    output_text = ""
    error = ""

    try:
        async with AsyncSession(**session_kwargs_for_proxy(proxy)) as session:
            response = await session.post(
                CONSOLE_URL,
                headers=headers,
                data=body,
                timeout=timeout,
                stream=True,
            )
            status = response.status_code
            if status != 200:
                try:
                    body_text = response.content.decode("utf-8", "replace")
                except Exception:
                    body_text = ""
            else:
                current_event = ""
                async for raw_line in response.aiter_lines():
                    if isinstance(raw_line, bytes):
                        raw_line = raw_line.decode("utf-8", "replace")
                    kind, value = classify_console_line(raw_line)
                    if kind == "event":
                        current_event = value
                    elif kind == "data":
                        if current_event == "response.output_text.delta":
                            try:
                                item = orjson.loads(value)
                                output_text += str(item.get("delta") or "")
                            except Exception:
                                pass
                        elif current_event == "error":
                            body_text = value
                    elif kind == "done":
                        break
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    latency_ms = int((time.perf_counter() - started) * 1000)
    success = status == 200 and bool(output_text.strip())
    raw_for_class = body_text or output_text
    return ProbeAttempt(
        model=model,
        account_index=account.index,
        token_hash=account.token_hash,
        proxy_label=proxy.label,
        proxy_hash=proxy.proxy_hash,
        proxy_source=proxy.source,
        has_proxy=proxy.has_proxy,
        run=run,
        http_status=status,
        success=success,
        latency_ms=latency_ms,
        body_class=classify_body(status, raw_for_class),
        body_hash=short_hash(raw_for_class),
        body_preview=sanitize_preview(body_text),
        output_preview=sanitize_preview(output_text, limit=80),
        error=sanitize_preview(error),
    )


def summarize(attempts: list[ProbeAttempt]) -> dict[str, Any]:
    """Build aggregate summaries and root-cause hints from attempts."""
    by_model: dict[str, dict[str, int]] = {}
    by_proxy: dict[str, dict[str, int]] = {}
    by_account: dict[str, dict[str, int]] = {}
    body_classes: dict[str, int] = {}

    def add(bucket: dict[str, dict[str, int]], key: str, attempt: ProbeAttempt) -> None:
        item = bucket.setdefault(key, {"total": 0, "success": 0, "429": 0, "403": 0, "error": 0})
        item["total"] += 1
        if attempt.success:
            item["success"] += 1
        if attempt.http_status == 429:
            item["429"] += 1
        if attempt.http_status == 403:
            item["403"] += 1
        if attempt.http_status is None:
            item["error"] += 1

    for attempt in attempts:
        add(by_model, attempt.model, attempt)
        add(by_proxy, attempt.proxy_label, attempt)
        add(by_account, attempt.token_hash, attempt)
        body_classes[attempt.body_class] = body_classes.get(attempt.body_class, 0) + 1

    same_account_proxy_variance = 0
    for model in {item.model for item in attempts}:
        for token_hash in {item.token_hash for item in attempts}:
            rows = [item for item in attempts if item.model == model and item.token_hash == token_hash]
            statuses_by_proxy = {
                row.proxy_label: row.http_status
                for row in rows
            }
            if 200 in statuses_by_proxy.values() and 429 in statuses_by_proxy.values():
                same_account_proxy_variance += 1

    same_proxy_account_variance = 0
    for model in {item.model for item in attempts}:
        for proxy_label in {item.proxy_label for item in attempts}:
            rows = [item for item in attempts if item.model == model and item.proxy_label == proxy_label]
            statuses_by_account = {
                row.token_hash: row.http_status
                for row in rows
            }
            if 200 in statuses_by_account.values() and 429 in statuses_by_account.values():
                same_proxy_account_variance += 1

    same_combo_flapping = 0
    for model in {item.model for item in attempts}:
        for token_hash in {item.token_hash for item in attempts}:
            for proxy_label in {item.proxy_label for item in attempts}:
                statuses = {
                    item.http_status
                    for item in attempts
                    if item.model == model
                    and item.token_hash == token_hash
                    and item.proxy_label == proxy_label
                }
                if 200 in statuses and 429 in statuses:
                    same_combo_flapping += 1

    hints: list[str] = []
    if body_classes.get("upstream_capacity", 0):
        hints.append("observed_upstream_capacity_marker")
    if same_account_proxy_variance:
        hints.append("same_account_diff_proxy_changes_429")
    if same_proxy_account_variance:
        hints.append("same_proxy_diff_account_changes_429")
    if same_combo_flapping:
        hints.append("same_account_proxy_flaps")
    if not hints and body_classes.get("rate_limit_empty", 0):
        hints.append("429_without_body_needs_service_log_or_header_correlation")

    return {
        "by_model": by_model,
        "by_proxy": by_proxy,
        "by_account": by_account,
        "body_classes": body_classes,
        "variance": {
            "same_account_proxy_variance": same_account_proxy_variance,
            "same_proxy_account_variance": same_proxy_account_variance,
            "same_combo_flapping": same_combo_flapping,
        },
        "root_cause_hints": hints,
    }


async def async_main() -> int:
    """Run the A/B probe."""
    args = parse_args()
    if args.accounts <= 0:
        raise SystemExit("--accounts 必须大于 0")
    if args.runs <= 0:
        raise SystemExit("--runs 必须大于 0")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency 必须大于 0")

    accounts = await load_accounts(args.accounts, args.account_pool, args.prefer_console_quota)
    proxies = await build_proxy_samples(args)
    models = split_csv(args.models) or list(DEFAULT_MODELS)

    if not accounts:
        raise SystemExit("没有可用账号样本")
    if not proxies:
        raise SystemExit("没有代理样本；请使用 --include-direct、--include-config-proxy、--proxies 或 --proxy-file")

    print(f"账号样本: {len(accounts)}，代理样本: {len(proxies)}，模型: {len(models)}，runs={args.runs}")
    print("代理标签:", ", ".join(f"{p.label}({p.source})" for p in proxies))

    sem = asyncio.Semaphore(args.concurrency)
    attempts: list[ProbeAttempt] = []

    async def run_guarded(model: str, account: AccountSample, proxy: ProxySample, run: int) -> None:
        async with sem:
            attempt = await probe_once(
                model=model,
                account=account,
                proxy=proxy,
                run=run,
                timeout=args.timeout,
            )
            attempts.append(attempt)
            status = attempt.http_status if attempt.http_status is not None else "ERR"
            print(
                f"{model} account={account.index}:{account.token_hash} "
                f"proxy={proxy.label} run={run} status={status} "
                f"class={attempt.body_class} latency={attempt.latency_ms}ms"
            )
            if args.delay > 0:
                await asyncio.sleep(args.delay)

    tasks = [
        run_guarded(model, account, proxy, run)
        for model in models
        for account in accounts
        for proxy in proxies
        for run in range(1, args.runs + 1)
    ]
    for task in tasks:
        await task

    report = {
        "created_at": int(time.time()),
        "models": models,
        "account_samples": [
            {k: v for k, v in asdict(account).items() if k != "token"}
            for account in accounts
        ],
        "proxy_samples": [asdict(proxy) | {"proxy_url": None} for proxy in proxies],
        "attempts": [asdict(attempt) for attempt in attempts],
        "summary": summarize(attempts),
    }

    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        try:
            output.chmod(0o600)
        except OSError:
            pass
        print(f"报告已写入: {output}")

    return 0


def main() -> int:
    """CLI entrypoint."""
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
