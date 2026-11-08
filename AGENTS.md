# harness conventions

harness is a standalone headless Rust project, currently at the scaffolding
stage (no `Cargo.toml` / crate code yet — this repo carries the toolchain,
rules, and scripts the project will be built on). This file carries hard
constraints and the order of operations; expanded procedures live under
`.agent/rules/` — look them up when needed instead of reading them end to end.

| Scenario                                                | Read                         |
| ------------------------------------------------------- | ---------------------------- |
| Local dev environment, build/test optimizations, termux | `.agent/rules/dev-env.md`    |
| Rust public API / code style / investigation and review | `.agent/rules/rust-code.md`  |
| Writing / running tests, CI policy                      | `.agent/rules/testing-ci.md` |

## Common entry points

| Purpose                                         | Command                 | Notes                                                                                         |
| ----------------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------- |
| Build check                                     | `just check`            | `cargo check` — needs a Cargo.toml once code lands                                            |
| Affected tests (testless), falls back to full   | `just test-fast [base]` | `bash scripts/test-fast.sh`                                                                   |
| Full check suite (fmt + clippy + check + gates) | `just verify`           | local "everything" entry                                                                      |
| Rust syntax lint (rust-specs dylint)            | `just spec-check`       | first time: `just spec-check build`; skips with a notice when nightly/dylint/libs are missing |
| Enable per-machine optimizations                | `. scripts/dev-env.sh`  | sccache / mold / cargo fork; probe-based, see `.agent/rules/dev-env.md`                       |
| Environment checkup                             | `just dev-check`        | toolchain + enabled optimizations + process hygiene                                           |
| Shellcheck                                      | `just lint-shell`       | tracked shell scripts                                                                         |

## 1. Understand the project first

The project is empty of code so far; when you start it, read in this order:

1. Root `Cargo.toml` `[workspace.lints]` (once created) and `clippy.toml` —
   the lints this repo enforces. Skim before writing code so you do not run
   into them.
2. `.agent/rules/` — the rule bodies; follow the navigation table above
   instead of reading everything.

The intended layout is a flat set of focused crates under `crates/`. Find the
owning crate before writing code; cross-layer logic belongs in the owning
crate, not assembled at the call site.

## 2. Prepare the development environment

### One task, one worktree

- Never run `git worktree add` from inside a worktree — it nests.
- Push the branch immediately after creating it (`git push -u origin <type>/<slug>`).
- Give subagents **global absolute paths** (`/home/<user>/projects/harness/.wt/<name>/`); never let them guess relative paths.

### Cargo configuration: shared baseline + probe-based optimizations

`.cargo/config.toml` holds only the **shared baseline** (jobs ceiling). The
optional optimizations — `sccache` (compile cache), `mold` (linker) — are
**never hardcoded** there: machines without those tools would fail to start
cargo. Instead they are injected per machine by sourcing the probe script
`scripts/dev-env.sh`, which exports env vars (env > project config in cargo's
precedence). Desktops enable them; termux phones probe, find nothing, and
silently run plain cargo — same files, no fork. Read the full story in
`.agent/rules/dev-env.md` and the header comment of `scripts/dev-env.sh`.

### Toolchain

- `rust-toolchain.toml` pins **stable** — that is what CI and daily builds
  use (edition 2024, resolver 3).
- Nightly is only for the rust-specs dylint gate. rustup's `+<toolchain>`
  command-line argument overrides `rust-toolchain.toml`, so run it as
  `cargo +nightly-2026-08-20 dylint ...` without changing the pin (the
  spec-check script already does this). On machines without nightly (termux),
  the gate skips with a notice.
- If the `huacnlee/cargo` fork is installed, it replaces `~/.cargo/bin/cargo`
  and shares one target directory across all worktrees, self-limited in size.

## 3. Write Rust code

Follow `.agent/rules/rust-code.md`. Highlights:

- Comments — including doc comments — are written in **English**; explain why, not what.
- Investigate before writing: LSP (`hover` / `goToDefinition` / `findReferences`) before touching a public symbol; list every caller, migrate them in the same change — no dual-path compatibility.
- Preferred syntax forms: plain `if` / `else` for two-way choices, `match` for three-or-more dispatch, one iter chain for collections, Option/Result combinators over hand-written match, named functions over closures, nesting within 3 levels.
- One function does one thing: past roughly 80 lines, split by responsibility.
- `unwrap` / `expect` stay out of production paths; propagate `Result`.

## 4. Test

Follow `.agent/rules/testing-ci.md`. Highlights:

- Tests live under each crate's `tests/` directory; assert **real behavior** — never `assert!(true)`.
- Every test carries an English comment stating purpose, scenarios covered, and acceptance criterion.
- Test runner is nextest (process-per-test) when installed, else `cargo test`; testless narrows to affected tests when installed, else a full run — both fallbacks are fail-open (see `scripts/test-fast.sh`).
- **GitHub PR CI is the primary test path — prefer it, never "finish locally".** PR CI runs the committed tree's lint + nextest and is the merge gate; fix failures on the same PR. Local loops exist for fast feedback on small scopes only — they never stand in for CI and never authorize a push. (The CI is not set up yet on this branch; until it is, `just verify` + `just test-fast` carry it.)
- **When you do run locally, use `just test-fast`, not bare `cargo test`** — testless narrows to what the diff can actually break, while `cargo test` re-runs whole crates. Every fallback is loud and declared.

## 5. Wrap up

### Processes and resources

- Stop the long-lived processes you started and confirm the ports are released; wait for background job results before closing the session.
- Keep only what the maintainer explicitly asked to keep; release resources instead of competing for the user's CPU and memory.

### Worktrees and branches

- Clean up only what **this session** created; never touch worktrees, branches, or processes other sessions are using.
- Check three things before removing anything: the work is merged/pushed, the tree has no uncommitted changes, and the directory belongs to the branch being cleaned. If any fails, state what blocks the cleanup instead of forcing it.
- Order: `git worktree remove <dir>` → `git branch -d <branch>`.
- `rm -rf .wt/`, `rm -rf .wt/*`, and `git clean` are strictly forbidden.

### Commits and identity

- Conventional commits: title in English; one commit does one thing — no unrelated changes, formatting noise, or generated files mixed in.
- The commit identity is the maintainer account; never override it with `git -c user.name=…`, and never add a `Co-authored-by:` bot trailer.
- Run `just verify` before committing — do not wait for a push failure to find out.
- While the project is early-stage and single-maintainer, work may be
  committed and pushed to `main` directly (no PR required); switch to the
  PR flow when the maintainer says so.

### Pull requests

- When PRs are in use: title in plain English (conventional-commit style); the body describes what, why, and how to test. When CI fails, pull the logs and fix on the same PR.

## 6. Special cautions

### Deleting things

- Confirm a file is really waste (generated output, merged temporary files) before deleting — not merely "looks unused".
- Delete recoverably (`gio trash`); for `rm -rf`, overwriting writes, or wiping databases, explain the impact and wait for confirmation.

### Secrets and irreversible changes

- Credentials, tokens, private keys: never printed to output, never committed; examples use placeholders (`<API_KEY>`).
- Do not dump whole config files or environments (they may contain secrets); read only the fields you need.

### Long jobs and pointless reads

- Wrap compiles, tests, and installs in `.githooks/heavy`; batches expected to run long go to CI.
- Never feed a whole repository or a huge file to a review tool in one shot (rate limits + noise): review per module and feed only the diff of this change.
- Find things with the LSP or search first — do not read large directories file by file.

### Gate findings are work items, not verdicts

1. Read the rule text first; record pending adjudications in the delivery record — do not guess and do not ignore.
2. Fix the root cause: make the constraint hold instead of making the check stop firing.
3. Gaming fixes are violations whether or not the gate turns green: editing rule files, `--no-verify`, `#[allow(...)]` suppression, `assert!(true)` in tests.
4. Leave one written disposition per finding: `rule ID → root cause → fix (file:line)`.

### Limits of your authority

- AGENTS.md and `.agent/rules/` are the rules of this repo; when a rule conflicts with reality, escalate to the maintainer — never rewrite a rule yourself.
- Do not create issues or restructure epics unless the maintainer explicitly asks for it.
