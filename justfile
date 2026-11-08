# Harness — justfile

default: check

# Build check
check:
	cargo check

# Format
fmt:
	cargo fmt --all

# Format check (no file changes)
fmt-check:
	cargo fmt --all --check

# Clippy
clippy:
	cargo clippy --all-targets -- -D warnings

# Build binary
build:
	cargo build

# Full test (local refused until the project has code / CI; TEST_FORCE=1 overrides)
test:
	#!/usr/bin/env bash
	set -uo pipefail
	if [ "${GITHUB_ACTIONS:-}" != "true" ] && [ "${TEST_FORCE:-}" != "1" ]; then
		echo "✗ Local full test rejected (project has no test code yet / CI not configured)"
		echo "  Daily validation: just test-fast"
		echo "  Maintainer forced local full: TEST_FORCE=1 just test"
		exit 1
	fi
	exec cargo test --all

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

# Testless test selection (falls back to a full run when testless is absent). Usage: just test-fast [base]
test-fast base="HEAD":
	bash scripts/test-fast.sh "{{base}}"
