# Testing & CI

**When to read this**: when writing a test, deciding what to run locally, reading a gate or CI result, or suspecting that "CI is green but did not actually verify anything".

**What this solves**: how tests are written and run here, what the local gates check, and how this repo's CI can pass without asserting anything.

Read order: §1–§2 before writing a test, §3 when a gate blocks you, §4 when you watch CI, §5 when CI surprises you.

## 1. Writing tests

- Tests live in each crate's standard `tests/` directory (`crates/harness-*/tests/`); unit tests inside `src/` (`#[cfg(test)]` modules) are not the convention here — follow the existing layout of the crate you are touching. Assert **real behavior** — never decorate with `assert!(true)`.
- Every test carries an English comment stating its **purpose**, the **scenarios it covers**, and the **acceptance criterion** it asserts.
- **A test that cannot reach its dependency fails; it does not return early.** A test that prints "skipping" and `return`s is recorded as passed — that is a false green, and it is a defect in the test, not a graceful fallback. If the environment is genuinely optional for that test, say so in the file header and make the case explicit.
- Tests that share a mutable resource (a port, a temp dir root, a mock server) must be serialized: nextest runs **one process per test**, so a `static Mutex` cannot serialize two test binaries against the same resource. Add them to a `test-groups` entry in `.config/nextest.toml` (the config file path is fixed by nextest: `workspace-root/.config/nextest.toml`) and validate with `cargo nextest show-config test-groups`.
- The toolchain is stable (harness targets `rust-version = "1.99"`, edition 2024). Do not pin nightly for daily work; the only nightly use is the rust-specs dylint gate (see §3).

## 2. What to run locally

| Scenario | Command | Notes |
|---|---|---|
| Affected tests only | `just test-fast [base]` or `bash scripts/test-fast.sh` | Requires `testless`; when absent it falls back to a full run. Exits loudly on anomalies — never a silent skip. |
| Verify compilation | `cargo check -p <crate>` | Wrap in `.githooks/heavy` on multi-session machines. |
| Debug a single failing case | `cargo test -p <crate> -- <test name>` | Debug only, not acceptance; must finish in ~3 seconds. |
| Full check suite | `just verify` | fmt + clippy + check + spec gate. The local "everything" entry. |
| Full test runs | Prefer CI | The repo's own `.github/workflows/ci.yml` runs lint + workspace tests on every push/PR. Avoid `cargo test --workspace` locally on small machines; if needed, scope per crate. |

**Do not rely on the `~/.local/bin/cargo` PATH shim** present on some machines: a
cargo fork in `~/.cargo/bin/` shadows it, and CI does not have it at all. Invoke
`testless` explicitly from scripts (as `scripts/test-fast.sh` does), never by
expecting `cargo test` to be rerouted.

## 3. Normative gates (rust-specs)

- Normative lints come from rust-specs (https://github.com/hathawayANdRX105/rust-specs), loaded as **pre-built dylint libraries**:
  1. Build the lint crates once in the rust-specs checkout: `cargo +nightly-2026-08-20 build --release -p ban_unwrap` (repeat per needed lint crate).
  2. Place each `.so` as `lib<name>@<toolchain>.so` in a directory (e.g. `target/dylint/libraries/nightly-2026-08-20-x86_64-unknown-linux-gnu/release/`).
  3. Load and run: `DYLINT_LIBRARY_PATH=<dir> cargo +nightly-2026-08-20 dylint --manifest-path Cargo.toml --lib <name>`.
- Why not `--git` / `--path` directly: dylint passes `--target-dir` when building libraries, but on machines whose cargo redirects target directories (e.g. the huacnlee/cargo fork), artifacts land where dylint does not look. The pre-built + `DYLINT_LIBRARY_PATH` path is the verified form; a wrapper script should automate build → rename → load once adopted.
- Requires a nightly toolchain + `cargo-dylint`. On machines without nightly (termux), the gate **skips with a notice** — run it on desktop or CI before treating a change as clean.
- **Read gate output completely**: `head`, `tail`, and `grep -v` swallow later findings. When a report says "N checks passed", confirm N covered the part you changed.
- **A gate finding is a work item, not a verdict**: read the rule text, fix the root cause, and record one disposition per finding (`rule ID → root cause → fix (file:line)`). A blocked commit or push is fixed at the root cause, never bypassed with `--no-verify`.

## 4. CI

- CI is the repo's own `.github/workflows/ci.yml`: `lint` job (fmt --check + clippy `-D warnings`) and `test` job (`cargo test --workspace`), both on stable via `dtolnay/rust-toolchain@stable`, with `Swatinem/rust-cache` and shared cache keys.
- A PR merges only when CI is green. When CI fails, pull the logs (`gh run view <id> --log-failed`) and fix on the same PR.
- `gh run view --log-failed` returns empty while the run is not `completed` — check run state before concluding there are no logs.

## 5. Ways CI can be falsely green

1. **Feature-gated tests**: a test file with `#![cfg(feature = "xxx")]` is compiled and executed but outputs `running 0 tests`, because CI never enables that feature. Fix: make the test run without the feature, or add `--features` for that package in the workflow; always note the feature requirement in the test file header.
2. **Resource-gated tests that return early**: a test that cannot reach a required service prints a skip message and returns — recorded as passed. The mock LSP integration tests need python3; confirm the service is actually up rather than trusting the skip message.
3. **`#[ignore]` tests**: CI never passes `--ignored`, so ignored tests never run at all.

## 6. Hard constraints

- Run `just verify` (fmt + clippy + gates) before committing; never bypass a gate with `--no-verify`.
- When CI fails, fix on the same PR — do not open another PR.
- Never hand-edit `.config/nextest.toml`'s path or move it into `.cargo/`: nextest discovers it only at `workspace-root/.config/nextest.toml`.
