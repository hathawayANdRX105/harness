#!/usr/bin/env bash
# spec-check — rust-specs dylint 门（预编译库加载）。
#
# 为什么不是 cargo dylint --git/--path 直跑：dylint 用 --target-dir 构建库，
# 但本机 cargo fork（huacnlee/cargo）会重定向 target 目录，库落在 dylint
# 不查找的位置。预编译 + DYLINT_LIBRARY_PATH 加载是实测跑通的形式
#（exit=0），见 .agent/rules/testing-ci.md §3。
#
# Usage:
#   spec-check            # 加载预编译库跑 lint 门（缺 nightly/dylint/库 → skip 提示）
#   spec-check build      # 从 rust-specs (重新) 构建预编译库
#
# 环境变量：
#   RUST_SPECS_DIR   rust-specs 检出目录（默认探测 $HOME/projects/rust-specs）
#   DYLINT_LIB_DIR   预编译库目录（默认 <repo>/target/dylint/libraries/<toolchain>-<triple>/release）
set -uo pipefail

REPO=$(git rev-parse --show-toplevel 2>/dev/null) || REPO="$PWD"
cd "$REPO" || exit 1

NIGHTLY="nightly-2026-08-20"

# ── 无 Cargo.toml（项目还没有 Rust 代码）→ 友好 skip ─────────────────────────
if [ ! -f Cargo.toml ]; then
    echo "spec-check: 无 Cargo.toml（项目还没有 Rust 代码），dylint 门暂时 skip"
    exit 0
fi

# ── 工具探测：缺件即 skip（不阻塞流程）─────────────────────────────────────
if ! rustup toolchain list 2>/dev/null | grep -q "$NIGHTLY"; then
    echo "spec-check: 缺 $NIGHTLY（dylint 需要 nightly），门 skip —— 在桌面/CI 补跑"
    exit 0
fi
if ! cargo "+$NIGHTLY" dylint --version >/dev/null 2>&1; then
    echo "spec-check: 缺 cargo-dylint（cargo +$NIGHTLY install cargo-dylint），门 skip"
    exit 0
fi

# ── 本地跑：metadata entry 加载，系统 cargo 规避 fork 重定向 ────────────────
# 本机默认 cargo 是 huacnlee fork（~/.local/bin/cargo），会把 target 目录
# 重定向到 dylint 查找路径之外。PATH=/usr/bin 前缀强制用系统标准 cargo 跑，
# 与 CI 同一入口（cargo dylint --all，规则库从 root Cargo.toml 的
# [workspace.metadata.dylint] tag 拉取）。
rc=0
if PATH="/usr/bin:$PATH" cargo "+$NIGHTLY" dylint --all --workspace; then
    echo "✓ spec-check 通过"
else
    echo "✗ spec-check 有 findings（fix 或在 PR 里说明）"
    rc=1
fi
exit "$rc"
