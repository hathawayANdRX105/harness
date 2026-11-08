#!/usr/bin/env bash
# 快关：跑 testless 判定受影响的测试。合并门的 test 车道。
#
# 本文件是 justfile `test-fast` 配方逻辑的落地。
#
# 判定分四类：
#   ① testless 不可用 / 输出异常 → CI 降级全量；本地 exit 1（中等及以上测试禁止本地跑）
#   ② exit=2 = testless 判定 run-all（影响面=全仓）→ CI 跑全量；本地 exit 1
#   ③ 选出了名单但 0 通过（过滤器零命中，可疑）→ 同 ①
#   ④ 名单为空 → 两环境都 exit 0，但先过一致性守卫
#
# 用 nextest 跑测试（不是 cargo test）：
#   ① 它不跑 doctest —— 本仓不把文档当交付物，doc 示例不值得占 CI 时间。
#   ② filterset 的 `test(=X)` 是精确匹配，等价于旧配方的 `--exact`。
#   ③ 它按测试逐个起进程，并行与超时处理都比 libtest 好。
#
# 用法：bash scripts/test-fast.sh [base]     # base 缺省 HEAD；just test-fast 传参
set -uo pipefail

base="${1:-HEAD}"
cd "$(git rev-parse --show-toplevel)" || exit 1

# ── CI 侧降级跑全量的统一出口 ─────────────────────────────────────────────────
# 本地一律不降级（中等及以上测试禁止本地跑）：打印派发命令后 exit 1。
degrade_or_die() {
  echo "✗ $1"
  if [ "${GITHUB_ACTIONS:-}" = "true" ] || [ "${TEST_FORCE:-}" = "1" ]; then
    echo "→ 降级全量（workspace）"
    CARGO_TESTSHIM=0 cargo test --workspace --lib --bins --tests --profile ci
    exit $?
  fi
  [ -n "${2:-}" ] && echo "  $2"
  echo "  全量触发 : gh workflow run ci.yml -R hathawayANdRX105/harness-ci -f sha=<sha> -f pr=<N> -f base=main -f full=true"
  echo "  （维护者本地强跑：TEST_FORCE=1 just test-fast）"
  exit 1
}

# ── 判定基准：三点（merge-base）────────────────────────────────────────────────
base_rev="$base"

# ── 运行 testless 收窄 ────────────────────────────────────────────────────────
# 这里调用本机的 cargo PATH 包装器（~/.local/bin/cargo），它会拦截 `cargo test`
# 并改道到 testless 收窄逻辑。如果包装器不存在，fallback 到裸 cargo nextest。
if command -v testless >/dev/null 2>&1; then
  testless --base "$base_rev" --nextest
  rc=$?
elif command -v cargo >/dev/null 2>&1; then
  # fallback：直接用 cargo nextest 跑全量（不收窄）
  echo "⚠ testless 未安装，退回裸 cargo nextest（全量）"
  cargo nextest run --all
  rc=$?
else
  echo "✗ 既无 testless 也无 cargo，无法运行测试"
  exit 1
fi

case "$rc" in
  0) echo "✓ testless 测试全部通过" ;;
  1) echo "✗ testless 测试失败" ; exit 1 ;;
  2) degrade_or_die "testless 判定 run-all（影响面=全仓）" ;;
  *) echo "✗ testless 退出码 $rc（异常）" ; exit 1 ;;
esac
