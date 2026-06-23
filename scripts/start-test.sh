#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
ROOT_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
BENCHMARK_SCRIPT="$SCRIPT_DIR/model_availability_benchmark.py"

if [ ! -f "$BENCHMARK_SCRIPT" ]; then
  echo "ERROR: 找不到模型可用性测试脚本：$BENCHMARK_SCRIPT" >&2
  exit 1
fi

if [ -n "${PYTHON_BIN:-}" ]; then
  PYTHON="$PYTHON_BIN"
elif [ -x "$ROOT_DIR/.venv/bin/python" ]; then
  PYTHON="$ROOT_DIR/.venv/bin/python"
else
  PYTHON="${PYTHON:-python3}"
fi

has_option() {
  option_name="$1"
  shift

  for arg do
    if [ "$arg" = "$option_name" ] || [ "${arg#"$option_name"=}" != "$arg" ]; then
      return 0
    fi
  done

  return 1
}

BASE_URL_VALUE="${GROK2API_BASE_URL:-${BASE_URL:-}}"
API_KEY_VALUE="${GROK2API_API_KEY:-${API_KEY:-}}"
RUNS_VALUE="${RUNS:-1}"
JSON_OUTPUT_VALUE="${JSON_OUTPUT:-}"

# 默认不缓存 API Key，避免把敏感凭据写入用户目录；需要缓存时设置 START_TEST_CACHE=1。
if [ "${START_TEST_CACHE:-0}" != "1" ] && ! has_option "--no-cache" "$@"; then
  set -- --no-cache "$@"
fi

# 未显式指定时，从环境变量补齐常用参数；其余参数保持透传。
if [ -n "$BASE_URL_VALUE" ] && ! has_option "--base-url" "$@"; then
  set -- --base-url "$BASE_URL_VALUE" "$@"
fi

if [ -n "$API_KEY_VALUE" ] && ! has_option "--api-key" "$@"; then
  set -- --api-key "$API_KEY_VALUE" "$@"
fi

if [ -n "$RUNS_VALUE" ] && ! has_option "--runs" "$@"; then
  set -- --runs "$RUNS_VALUE" "$@"
fi

if [ -n "$JSON_OUTPUT_VALUE" ] && ! has_option "--json-output" "$@"; then
  set -- --json-output "$JSON_OUTPUT_VALUE" "$@"
fi

if ! has_option "-h" "$@" && ! has_option "--help" "$@"; then
  echo "开始执行模型可用性测试..."
  echo "测试脚本：$BENCHMARK_SCRIPT"
fi

exec "$PYTHON" "$BENCHMARK_SCRIPT" "$@"
