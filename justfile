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

# Product entries → combinations → domains/pages; dev entries stay isolated.
# The policy source is scripts/layering-policy.toml.
layering-check:
	python3 scripts/check-layering.py

# Application capability contract: each app under arch/ declares its capabilities and
# the crates implementing them; this checks the declaration against the real build
# conservative ordinary dependency closure from Cargo declarations.
apps-check:
	python3 scripts/arch-apps.py

# Normative gate: spec-check + cratebook freshness + layering policy + capability contract
gate: spec-check cratebook-check layering-check apps-check

# Pre-merge pipeline: verify -> cratebook freshness -> audit ledger
premerge: verify cratebook-check audit-check

# Audit ledger: generate / check / panel
audit-generate:
	python3 scripts/merge-audit.py generate --worktree

audit-check:
	python3 scripts/merge-audit.py check --worktree

audit-html:
	python3 scripts/merge-audit.py html --worktree

# Cratebook: regenerate docs and lock file
cratebook:
	python3 scripts/cratebook.py

# Cratebook freshness check
cratebook-check:
	python3 scripts/cratebook.py --check

# ---------- worktree cleanup ----------
# List: just wt-candidates | clean: just wt-clean | dry run: just wt-clean-dry | targets: just wt-targets-clean
wt-candidates:
	bash scripts/wt-clean.sh candidates

wt-clean:
	bash scripts/wt-clean.sh clean

wt-clean-dry:
	bash scripts/wt-clean.sh clean --dry-run

# Compile diagnosis (read-only)
build-doctor:
	bash scripts/build-doctor.sh

# List orphan builds (cwd deleted)
build-orphans:
	bash scripts/build-orphans.sh

# Kill orphan builds
build-orphans-kill:
	bash scripts/build-orphans.sh --kill

# Clean all .wt/*/target and root target
wt-targets-clean:
	#!/usr/bin/env bash
	set -u; total=0
	for t in .wt/*/target target; do
	  [ -d "$t" ] || continue
	  sz=$(du -sh "$t" | cut -f1); n=0
	  find "$t" -mindepth 1 -maxdepth 1 -exec sh -c 'gio trash "$1" 2>/dev/null && true' _ {} \; 2>/dev/null
	  gio trash "$t" 2>/dev/null && { echo "  ✓ trashed $t ($sz)"; total=$((total+1)); }
	done
	[ "$total" -eq 0 ] && echo "no cleanable target"

# Dev environment health check
dev-check:
	#!/usr/bin/env bash
	echo "== orphan build check (cargo/rustc/sccache with deleted cwd) =="
	bash scripts/build-orphans.sh --brief || true
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

# wasm32 compile gate (testless-gated). Usage: just wasm-check [main]
wasm-check base="origin/main":
	scripts/wasm-check.sh "{{base}}"
