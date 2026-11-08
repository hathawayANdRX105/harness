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

# Conditional full test (CI only; local run refused unless TEST_FORCE=1)
test-scoped base:
	bash scripts/test-full.sh "{{base}}"

# Full test (CI only; local run refused unless TEST_FORCE=1)
test:
	#!/usr/bin/env bash
	set -uo pipefail
	if [ "${GITHUB_ACTIONS:-}" != "true" ] && [ "${TEST_FORCE:-}" != "1" ]; then
		echo "✗ Local full test rejected (medium or above tests prohibited locally)"
		echo "  Daily validation: just test-fast"
		echo "  Full trigger: gh workflow run ci.yml -R hathawayANdRX105/harness-ci -f sha=<sha> -f pr=<N> -f base=main -f full=true"
		echo "  (GitHub Actions unrestricted; maintainer forced local full: TEST_FORCE=1 just test)"
		exit 1
	fi
	exec cargo test --all

# Actively dispatch shell-repo CI and watch results (no 5-minute poll wait).
# The private repo has zero workflows; tests run in the public shell repo.
# Immediate path after an agent commit: dispatch ci.yml + gh run watch for the
# shell-ci merge gate; polling is only a fallback.
# Usage: just ci [PR] | just ci --full [PR] | just ci --main <sha>
ci *args:
	bash scripts/ci-trigger.sh {{args}}

# Normative checks (rust-specs; single gate entry). Usage:
#   just spec-check [--class deterministic|advisory|semantic] [--scope changed|all] [--base ]
#   just spec-check dylint [--scope changed|all] [--base ]
spec-check *args:
	bash scripts/spec-check.sh {{args}}
verify: fmt-check clippy check gate

# Normative gate: spec-check only (harness is a standalone headless crate set;
# no layering/apps/cratebook gates like ferrite's monorepo).
gate: spec-check

# Pre-merge pipeline: verify -> cratebook freshness -> audit ledger
premerge: verify

# Dev environment health check
dev-check:
	#!/usr/bin/env bash
	echo "== process hygiene (see AGENTS.md) =="
	echo "  1. Long-running services: never 'nohup &' (process group reaped when call ends); use persistent background task + ss"
	echo "  2. Never pkill -f cargo/rustc (T state != dead, kills parallel session builds); readlink /proc/<pid>/cwd first"

# Shellcheck gate (system shellcheck, falls back to bun x shellcheck)
lint-shell:
	#!/usr/bin/env bash
	set -euo pipefail
	cd "$(git rev-parse --show-toplevel)" || exit 1
	mapfile -t tracked < <(git ls-files '*.sh' 'bin/*.sh' 'lib/*.sh' | grep -v '^\.wt/')
	shellcheck --format=gcc --severity=style "${tracked[@]}" && echo "✓ shellcheck passed (${#tracked[@]} files)"

# Testless test selection (function-level impact analysis). Usage: just test-fast [main]
test-fast base="HEAD":
	bash scripts/test-fast.sh "{{base}}"
