#!/usr/bin/env bash
# scripts/spec-check.sh — 统一规范检查入口（harness 版）。
#
# Usage:
#   spec-check                          # 确定性结构规则（changed 作用域，Fail/Warn）
#   spec-check --class semantic         # jev 语义层（需 http://127.0.0.1:8001）
#   spec-check --scope all              # 全仓结构规则
#   spec-check dylint                   # dylint 语法 lint（changed 作用域）
#   spec-check dylint --all             # 全仓 dylint
#
# Extra arguments are passed through to the underlying command. `cargo xtask`
# is defined in harness .cargo/config.toml and dispatches to the rust-specs
# checks crate (structure subcommand) or scripts/lint-dioxus.sh (dylint).
#
# Exit code: 0=no findings, 1=FAIL, 2=env unavailable.
set -euo pipefail

MODE="structure"
CLASS="deterministic"
SCOPE="changed"
BASE="origin/main"
ROOT=""
ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    dylint)
      MODE="dylint"
      shift
      ;;
    --class)
      CLASS="$2"
      shift 2
      ;;
    --scope)
      SCOPE="$2"
      shift 2
      ;;
    --base)
      BASE="$2"
      shift 2
      ;;
    --root)
      ROOT="$2"
      shift 2
      ;;
    --all)
      SCOPE="all"
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$ROOT" ]]; then
  ROOT="$(git rev-parse --show-toplevel)"
fi
cd "$ROOT"

if [[ "$MODE" == "dylint" ]]; then
  exec bash scripts/lint-dioxus.sh "${ARGS[@]}"
else
  exec cargo xtask structure \
    --class "$CLASS" \
    --scope "$SCOPE" \
    --base "$BASE" \
    --root "$ROOT" \
    "${ARGS[@]}"
fi
