# Harness — justfile

default: check

# Build check
check:
	cargo check --workspace --all-targets

# Format
fmt:
	cargo fmt --all

# Format check (no file changes)
fmt-check:
	cargo fmt --all --check

# Clippy (same backend as CI: LLVM via --profile ci)
clippy:
	cargo clippy --workspace --all-targets --profile ci -- -D warnings

# Build binary
build:
	cargo build

# Full test run (workspace). Daily entry is `just test-fast`; full runs belong
# to CI, local is fine too — this suite is small.
test:
	cargo nextest run --workspace

# Normative checks (rust-specs dylint, pre-built library loading). Usage:
#   just spec-check          # run the lint gate (skips with a notice when nightly/dylint/libs are missing)
#   just spec-check build    # (re)build the pre-built lint libraries from rust-specs
spec-check *args:
	bash scripts/spec-check.sh {{args}}
verify: fmt-check clippy check gate

# Normative gate: spec-check only (standalone headless crate set,
# no layering/apps/cratebook gates like ferrite's monorepo).
gate: spec-check

# Dev environment health check
dev-check:
	#!/usr/bin/env bash
	echo "== toolchain =="
	rustc --version 2>/dev/null || echo "  rustc missing"
	echo "== optimizations enabled in this shell? =="
	env | grep -E "CARGO_BUILD_RUSTC_WRAPPER|CARGO_TARGET_X86" >/dev/null && echo "  yes" || echo "  no (source scripts/dev-env.sh to enable)"
	echo "== process hygiene (see AGENTS.md) =="
	echo "  1. Long-running services: never 'nohup &' (process group reaped when call ends); use persistent background task + ss"
	echo "  2. Never pkill -f cargo/rustc (T state != dead, kills parallel session builds); readlink /proc/<pid>/cwd first"

# Shellcheck gate (system shellcheck)
lint-shell:
	#!/usr/bin/env bash
	set -euo pipefail
	cd "$(git rev-parse --show-toplevel)" || exit 1
	mapfile -t tracked < <(git ls-files '*.sh' 'bin/*.sh' 'lib/*.sh' | grep -v '^\.wt/')
	shellcheck --format=gcc --severity=style "${tracked[@]}" && echo "✓ shellcheck passed (${#tracked[@]} files)"

# Testless test selection (testless → nextest). Usage: just test-fast [base]
test-fast base="origin/master":
	bash scripts/test-fast.sh "{{base}}"
