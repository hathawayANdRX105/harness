#!/usr/bin/env bash
# 快关：跑 testless 判定受影响的测试；没装 testless 就全量（fail-open）。
#
# 本文件是 justfile `test-fast` 配方的落地。判定分两条路：
#   - testless 存在：跑函数级影响分析，只跑受 base..HEAD 改动影响的用例。
#   - testless 缺失：全量跑（fail-open，不阻塞；CI 不受影响因为 CI 不依赖本机）。
#
# 测试用 nextest 跑（process-per-test，隔离/并行更好），没装就退 cargo test。
# 用法：bash scripts/test-fast.sh [base]     # base 缺省 HEAD
set -uo pipefail

base="${1:-HEAD}"
REPO=$(git rev-parse --show-toplevel 2>/dev/null) || REPO="$PWD"
cd "$REPO" || exit 1

# 项目还没有 Rust 代码 → 没有可跑的测试
if [ ! -f Cargo.toml ]; then
    echo "test-fast: 无 Cargo.toml（项目还没有 Rust 代码），没有可跑的测试"
    exit 0
fi

# 运行器：nextest 优先，回退 cargo test
runner() {
    if command -v cargo-nextest >/dev/null 2>&1; then
        cargo nextest run "$@"
    else
        echo "⚠ 无 cargo-nextest，退回 cargo test"
        cargo test "$@"
    fi
}

if command -v testless >/dev/null 2>&1; then
    echo "==> testless 选区（from $base）"
    if sel=$(testless select --from "$base" 2>/dev/null) && [ -n "$sel" ]; then
        # shellcheck disable=SC2086
        runner $sel
        exit $?
    fi
    echo "⚠ testless 无选区（run-all / 异常），回退全量"
else
    echo "⚠ 无 testless，全量跑（fail-open）"
fi

echo "==> 全量跑"
runner
