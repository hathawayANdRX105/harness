#!/usr/bin/env bash
# 快关：跑 testless 判定受影响的测试（testless 选区 → nextest 收窄跑）。
#
# 用法: just test-fast [base]（默认 origin/master）
#
# 判定契约（与 gateway/ferrite 同族）：
#   ① testless 不可用 / 输出异常      → 出声降级：全仓 nextest
#   ② exit=2 = testless 判 run-all    → 全仓 nextest
#   ③ 选出名单但 0 通过（零命中可疑） → 出声降级：全仓 nextest
#   ④ 名单为空且 git diff 非空        → 全仓 nextest（harness 是 scaffold，
#      暂无多 crate 闭包可算，保守全量）
#
# 判定基准取 merge-base（三点 base...HEAD），不是 base 的当前 tip：
# testless 的 --from 是「<rev> 对工作树」的两方 diff，base tip 会往前走，
# 两方 diff 可能为空 → 快关假绿（ferrite 实测教训，PR #399）。
#
# 失败一律不静默：任何降级都先打印原因，且降级跑的是全量。
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

base="${1:-origin/master}"

# nextest 的 cargo profile：本地默认空（dev = Cranelift，快）；CI 设
# TEST_FAST_PROFILE=ci（LLVM，与合并门同一后端）。
nextest_profile="${TEST_FAST_PROFILE:-}"
if [ -n "$nextest_profile" ]; then
  profile_arg=(--cargo-profile "$nextest_profile")
else
  profile_arg=()
fi

base_rev="$(git merge-base "$base" HEAD 2>/dev/null || true)"
if [ -z "$base_rev" ]; then
  echo "⚠️  git merge-base ${base} HEAD 取不到 → 保留原引用，交由 testless 判定"
  base_rev="$base"
fi

full_run() {
  echo "↩︎ 降级全量：$1"
  echo ">>> cargo nextest run --workspace ${profile_arg[*]}"
  out="$(cargo nextest run --workspace "${profile_arg[@]}" 2>&1)" && rc=0 || rc=$?
  printf '%s\n' "$out"
  # scaffold / 空 workspace：nextest 报 "no tests to run" 是合法空态，不是失败。
  # 只转这个明确信号（带提示），真实测试失败仍传播——不制造假绿。
  if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q 'no tests to run'; then
    echo "✓ workspace 无测试（scaffold）—— 无可跑用例"
    exit 0
  fi
  exit "$rc"
}

# 解析 nextest Summary 的通过数。**先去 ANSI 色**再匹配：CI 设
# CARGO_TERM_COLOR=always 时 grep 永不命中，会把每条 PR 误判成零命中
#（ferrite 壳仓实测教训，run 41297721679）。
passed_count() {
  printf '%s\n' "$1" | sed -E 's/\x1b\[[0-9;]*m//g' \
    | grep -oE '[0-9]+ passed' | tail -1 | grep -oE '[0-9]+' || true
}

# ── ① testless 缺席 ──────────────────────────────────────────────────────────
if ! command -v testless >/dev/null 2>&1; then
  full_run "testless 不可用（安装 testless 并 \`testless index\` 可启用收窄跑）"
  exit $?
fi

err="$(mktemp)"
trap 'rm -f "$err"' EXIT
json="$(testless select --from "$base_rev" 2>"$err")" && rc=0 || rc=$?

# ── ② run-all（影响面 = 全仓）───────────────────────────────────────────────
if [ "$rc" -eq 2 ]; then
  reason="$(printf '%s' "$json" | jq -r '.reason // empty' 2>/dev/null || true)"
  full_run "testless 判定 run-all（vs ${base_rev}）：${reason:-（JSON 未给 reason）}"
  exit $?
fi

# ── ① testless 异常输出 ──────────────────────────────────────────────────────
if [ "$rc" -ne 0 ] || ! printf '%s' "$json" | jq -e . >/dev/null 2>&1; then
  if [ -s "$err" ]; then echo "testless stderr:"; sed 's/^/    /' "$err"; fi
  full_run "testless 输出异常（exit=${rc}），建议 \`testless index\` 重建索引后重试"
  exit $?
fi

names="$(printf '%s\n' "$json" | jq -r '.tests[]? | .name | last' | sort -u | tr '\n' ' ')"

# ── ④ 空选区：harness 是 scaffold，保守全量 ─────────────────────────────────
if [ -z "${names// }" ]; then
  echo "无 testless 选区（vs ${base_rev}）→ 保守全量（harness 暂无多 crate 闭包可算）"
  full_run "无 testless 选区"
  exit $?
fi

# ── 收窄跑：filterset 精确匹配 ───────────────────────────────────────────────
expr=""
for n in $names; do
  [ -n "$expr" ] && expr="$expr | "
  expr="${expr}test(=${n})"
done
echo "testless 选出 $(printf '%s' "$names" | wc -w) 条用例（vs ${base_rev}）"
# --no-tests=pass：命中 0 条时 nextest 默认非零退出；这里靠解析出的 passed
# 数自行判「零命中可疑」（③）。
out="$(cargo nextest run --no-tests=pass -E "$expr" "${profile_arg[@]}" 2>&1)" && rc=0 || rc=$?
printf '%s\n' "$out"
if [ "$rc" -ne 0 ]; then
  echo "✗ 收窄跑失败（filterset 精确匹配用例红）"
  exit 1
fi
passed="$(passed_count "$out")"; passed="${passed:-0}"
if [ "$passed" -eq 0 ]; then
  full_run "零命中可疑：选了 $(printf '%s' "$names" | wc -w) 条但一条没跑，建议 \`testless index\` 重建索引"
  exit $?
fi
echo "✓ 收窄跑通过（passed=${passed}）"
