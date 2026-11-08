# harness conventions

harness is a standalone headless Rust project. This file carries hard constraints
and the order of operations; expanded procedures live under `.agent/rules/` —
look them up when needed instead of reading them end to end.

## 1. Understand the project first

Read these first (five minutes to a global picture):

1. Root `README.md` crate manifest — the **single source of truth** for module
   responsibilities.
2. Root `Cargo.toml` `[workspace.lints.clippy]` — the idiom lints this repo denies;
   skim before writing code so you do not run into them.
3. Root `checks.toml` — spec policy: which paths each rule covers, severities, the
   semantic-layer endpoint.
4. `.agent/rules/` — the rule bodies; follow the navigation table above instead of
   reading everything.

## Common entry points

| Purpose                                         | Command                                                                                               |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Environment checkup                             | `just dev-check`                                                                                      |
| Local tests (the only local test entry)         | `just test-fast`                                                                                      |
| Full check suite (fmt + clippy + check + gates) | `just verify`                                                                                         |
| Spec gates                                      | `just spec-check`                                                                                     |
| Rust syntax lint (rust-specs dylint)            | `cargo +nightly-2026-08-20 dylint --path ~/projects/rust-specs --pattern 'lints/*' --all --workspace` |
| Worktree cleanup                                | `just wt-candidates` → `just wt-clean`                                                                |

## 2. Prepare the development environment

### One task, one worktree

- Never run `git worktree add` from inside a worktree — it nests.
- Push the branch immediately after creating it (`git push -u origin <type>/<slug>`).
- Give subagents **global absolute paths** (`/home/<user>/projects/harness/.wt/<name>/`); never let them guess relative paths.

```bash
cd ~/projects/harness
git worktree add .wt/<name> -b <type>/<slug>     # type: feat/fix/chore/refactor/epic/release
```

### Cargo configuration is already in place

`.cargo/config.toml` sets `jobs=6` and `rustc-wrapper=sccache`; the dev profile turns
incremental compilation off and trims debuginfo. The settings apply to `.wt/*` worktrees
automatically through cargo's upward search.

- Do not `export RUSTC_WRAPPER` or change `jobs` in a session — the repo config is the
  single source.
- With incremental off, same-tree small edits recompile at crate level — expected
  behavior, not a regression; sccache carries cold builds, branch switches, and
  CI-style rebuilds.

### Cranelift codegen backend (host dev builds)

The workspace pins `nightly-2026-08-20` (`rust-toolchain.toml`) and selects the
Cranelift backend per profile in `.cargo/config.toml`: `dev` (host builds, tests)
compiles with Cranelift — roughly 2x faster codegen than LLVM; `ci` and everything else
stays on LLVM.

- Never set `[profile.dev] codegen-backend` in `Cargo.toml`: the manifest form needs
  `cargo-features = ["codegen-backend"]`, which breaks any stable cargo parsing the
  manifest. The `.cargo/config.toml` form is the only supported one.

### `cargo test` transparent rerouting (machine-level)

本机 `~/.local/bin/cargo` 是一个 PATH 包装器（机器配置，**不在仓库里**，CI 与
其他机器不受影响）：在 harness 仓内（含 `.wt/*`）执行 `cargo test` 时，它会
改道到 `scripts/test-fast.sh` 的 testless 收窄跑，其余命令一律原样转发。

- 裸 `cargo test` → 完整 `just test-fast` 语义。
- `cargo test -p X` → `TEST_FAST_SCOPE_PKGS` 收窄模式；交集为空 → 回落原命令。
- 透传不改道的形状：`--` 之后的用例过滤器、`--doc`、`--no-run`、`--test <name>` 等。
- 失败一律 fail-open：包装器任何拿不准的情况都直接执行真实 cargo。
- 关闭/卸载：`CARGO_TESTSHIM=0`（临时）或删除 `~/.local/bin/cargo`.

## 3. Write Rust code

### Project preferences (hard)

- Comments — including doc comments — are written in **English**; keep them true to the code and explain why, not what.
- Function names are verb-noun and self-explanatory (`parse_channel_config`, not `do_config`); type names state their role.
- Unit, integration and contract tests live under `tests/`, never inside `src/`.
- Shared loop/protocol code must not acquire coding host implementations through its Cargo dependency closure.
- Do not add dead fields or dead code, and never add `#[allow(dead_code)]` to silence a
  warning — delete it or write down why it must stay.

### Syntax norms by construct

Each construct has one preferred form; when a rewrite would need one of the listed
escapes, keep the original form instead. The only `for` escapes: a body needing `?`,
`break`, `continue`, `return`, or `.await`, or a loop folding several accumulators whose
update order matters.

- **if-else**: a two-way value choice is a plain `if` / `else` expression
  (`if cond { A } else { B }`). No `else` after an early return, no `if-else` chains
  past two branches, nesting within 3 levels.
- **Conditional rendering in rsx**: a two-way `if` / `else` child is the plain form
  inside `rsx!` as well; `match` / `while` / `loop` stay out of `rsx!`.
- **match**: the dispatch tool — use it for enums with three or more arms, guarded arms
  (`Some(x) if c`), or patterns binding several parts; keep arms exhaustive and avoid a
  bare wildcard when the enum can grow.
- **iter chain**: building or transforming collections is one chain — `map` / `filter` /
  `filter_map` / `flat_map` finished by `collect` or `for_each`; an accumulator loop
  becomes `xs.into_iter().map(f).collect()`; three same-shape push loops merge with
  `.chain(..)`.
- **Option**: two-way dispatch goes through combinators — `.map` / `.map_or` /
  `.and_then` / `.filter` / `.unwrap_or`; both `match { Some(..) => .., _ => .. }` and
  `if let Some(..) { .. } else { .. }` count as hand-written combinators.
- **Result**: propagate with `?` first, then `.map` / `.and_then` for the payload,
  `.map_err` for context, `.map_or` / `.unwrap_or` for defaults; a `match` that only
  re-wraps variants (`Ok(v) => Ok(f(v))`) is a hand-written combinator.
- **rsx**: keep `rsx!` element tags clean and readable — write as little Rust syntax
  inside as possible; `if` / `match` / `while` / `loop` stay out, `for` is allowed for
  lists with a stable `key`, and non-trivial expressions move into named functions or
  computed values above the `rsx!` block.
- **closures**: `|x| expr` for a single expression; when a closure captures many
  variables or its body grows past one expression, that is a signal to refactor into a
  named function call instead; prefer borrowing captured state over cloning into the
  closure.

### Reuse first, then split

- Before writing, look for a same-shape implementation and an installed dependency
  (code graph / LSP / search): if an existing tool solves it, do not write a new one.
- One function does one thing: past roughly 80 lines, split it by responsibility into
  well-named functions; push cross-layer logic down into the owning crate instead of
  assembling it at the call site.
- When an interface changes, migrate every caller in the same change — no dual-path
  compatibility.

### Use the LSP to inspect interfaces

| Question                                                    | Operation                                                                        |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------- |
| What type is this symbol, what does its signature look like | `hover`                                                                          |
| Where is it implemented, how is it used                     | `goToDefinition`                                                                 |
| Who breaks if I change it                                   | `findReferences` (mandatory before touching a public symbol)                     |
| Who calls this function in the repo                         | `findReferences`; repo-wide: `codegraph callers <symbol>` after `codegraph sync` |

### Validate syntax with rust-specs

Syntax rules run from the repo being checked (this repo). The rule list with good/bad
examples per rule: `~/projects/rust-specs/README.md` (Rules table) and
`lints/<name>/README.md`.

```bash
# syntax rules — run from the repo being checked
cargo +nightly-2026-08-20 dylint --path ~/projects/rust-specs --pattern 'lints/*' --all --workspace
cargo +nightly-2026-08-20 dylint --no-deps --path ~/projects/rust-specs --pattern 'lints/*' -p <crate>
```

## 4. Test

### Division of labor: CI runs the heavy work, you run the light work

- **CI is the main battlefield**: a PR may merge only when its `shell-ci` status is green;
  the full suite runs automatically after every main push (`shell-ci-full`, asynchronous,
  does not block merges).
- **Agent 提交后必须主动派发壳仓 CI：`just ci`**。私有仓零 workflow，测试全在公开壳仓跑；
  `just ci` 立刻派发 `ci.yml -R ferrite-ci` + `gh run watch` 实时盯 `shell-ci`，跑完自动
  把结果回填到 PR comment。**这是唯一触发路径**——没有轮询兜底，agent 漏跑则 PR 无
  `shell-ci` 无法合并，所以提交后必须跑。
- Run locally: `just test-fast` (the only local test entry; testless selects affected cases
  from the diff and exits loudly on anomalies or zero hits — it never skips silently).
- Also cheap locally: `cargo check -p <crate>`, `cargo clippy -p <crate> --all-targets --
-D warnings`, `cargo fmt --check`, and single-case debugging that finishes in about three
  seconds.
- Forbidden locally: whole-package tests, a full test binary, e2e, `just test`, and
  `just test-scoped` / `scripts/test-full.sh` (conditional full runs are CI-only; `just test`
  refuses and prints the dispatch command). For a full run via the shell repo: `just ci
--full <PR>`.

### How tests are written here

- All tests live in the `tests/` directory — no `#[cfg(test)]`, `#[test]`, or `mod ..._tests;`
  is allowed inside `src/` or alongside source files. Unit tests, integration tests, and
  contract tests all go under `tests/`; root `tests/` (package `tests-http`) is the single
  place for them; assert **real behavior** — never decorate with `assert!(true)`.
- Every test carries a comment stating its purpose, the scenario it covers, and the
  acceptance condition being asserted — in English, above the test function.
- Fix bugs by reproducing first, then verify the reproduction path no longer triggers; when a
  test fails, decide whether the implementation or the test is wrong — never bend the test to
  the implementation.
- For anything you could not verify (missing runtime, credentials, hardware), write “not
  verified + why”; never report “did not run” as “passed”.

### When CI fails

Look at the concrete job with `gh pr checks <PR>` and fix it on the **same PR** — failed
checks are not an excuse to open another PR.

## 5. Wrap up

### Processes and resources

- Stop the long-lived processes you started (dev servers, watchers, background jobs) and
  confirm the ports are released; wait for background job results before closing the session.
- Keep only what the maintainer explicitly asked to keep; release resources instead of
  competing with the user's CPU and memory.

### Worktrees and branches

- Clean up only what **this session** created; never touch worktrees, branches, or processes
  other sessions are using.
- Check three things before removing anything: the PR is merged, the tree has no uncommitted
  changes, and the directory belongs to the branch being cleaned. If any fails, state what
  blocks the cleanup instead of forcing it.
- Order: `git worktree remove <dir>` → `git branch -d <branch>` → delete the remote branch.
  `-d` fails while the worktree still exists, so remove first.
- `rm -rf .wt/`, `rm -rf .wt/*`, and `git clean` are strictly forbidden — they wipe other
  sessions' worktrees too.
- For batch cleanup use the repo scripts: `just wt-candidates` → `just wt-clean`.

### Residue in the working tree

- `git status --porcelain` must come back empty: probe files, `.tmp/`, `*.log`, and debug
  artifacts are deleted or committed deliberately (with a written reason for keeping them).
- `todo/` is a local task-brief directory (gitignored) and never gets committed.
- Recycle build artifacts with `just wt-targets-clean`; it cleans **all** `.wt/*/target`
  plus the root `target`, so run it only when other sessions are idle.

### Commits and identity

- Conventional commits: title in English, body may be Chinese; one commit does one thing —
  no unrelated changes, formatting noise, or generated files mixed in.
- The commit identity is the maintainer account `hathawayANdRX105`; never override it with
  `git -c user.name=…`, and never add a `Co-authored-by:` bot trailer.
- Run `just verify` before committing — do not wait for a push failure to find out.

### Pull requests

- All work goes through a PR — never commit or push directly to main.
- Title in plain English (conventional-commit style), with at least one type label (`bug` /
  `enhancement` / `feature` / `documentation` / `chore` / `refactor` / `tests` / `epic`);
  the body follows `.github/PULL_REQUEST_TEMPLATE.md` — section headings in English with
  Chinese content.
- Validate before creating — run from the rust-specs repo: `cd ~/projects/rust-specs && cargo
xtask check github-pr --title "<PR title>" --body-file <body file>`; do not wait for `gh pr
create` to reject it.
- Review findings are fixed on the **same PR**; after opening or updating, watch `gh pr
checks` and repair failures immediately instead of waiting to be asked.

### Merging

- Squash only: `gh pr merge <N> --squash --delete-branch --body "Agent 🤖 - Merge: <reason>"`.
- `--merge` and `--rebase` are forbidden (including `-m` / `-r`), and so is a local `git merge
<branch>` pushed to main — merging goes through a PR.

### Issues

- Issues go through the YAML forms in `.github/ISSUE_TEMPLATE/` (`bug` / `feature` / `task`):
  the form name and type label are English; the title and body are written in Chinese; one
  issue describes one main outcome; no unredacted agent output, keys, or user data goes in.
- Validate title and body before `gh issue create` — run from the rust-specs repo: `cargo
xtask check github-issue --title "<issue title>" --body-file <body file>`.
- Creating issues is otherwise forbidden by default (only when the maintainer explicitly asks,
  and report first); findings are fixed on the PR instead of a new issue.
- Closing an issue needs the maintainer's instruction, and its Done when checklist must be
  fully checked first.

### Delivery record

The PR body's `## Delivery record` section lists one line per finding:
`rule ID → root cause → fix (file:line)`. Rejections list `rule ID + reason + basis` for the
maintainer to judge. WARN is handled with the same care as FAIL.

## 6. Special cautions

### Deleting things

- Confirm a file is really waste (generated output, merged temporary files) before deleting —
  not merely “looks unused”.
- Delete recoverably (`gio trash`); for `rm -rf`, overwriting writes, or wiping databases,
  explain the impact and wait for confirmation.
- If the target is someone else's artifact, a file whose purpose you cannot explain, or a
  gitignored file you did not create — stop and ask.

### Secrets and irreversible changes

- Credentials, tokens, private keys: never printed to output, never committed, never pasted
  into a PR body; examples use placeholders (`<API_KEY>`, `127.0.0.1`).
- Do not dump whole config files or environments (they may contain secrets); read only the
  fields you need.
- System-level configuration, fonts, global environment, and dotfiles stay untouched by
  default — ask before changing; installing packages, changing PATH, or installing systemd
  units needs confirmation first.
- Database migrations, config-format changes, and major dependency upgrades must be reversible
  — confirm the rollback path first.

### Long jobs and pointless reads

- Wrap compiles, tests, and installs in `.githooks/heavy`; batches expected to run long go to
  CI — do not grind them locally.
- Never feed a whole repository or a huge file to a review tool in one shot (rate limits +
  noise): review per module and feed only the diff of this change.
- Read gate output completely: `head`, `tail`, and `grep -v` swallow later findings; when a
  report says “N checks passed”, confirm N covered the part you changed.
- Find things with the code graph, the LSP, or search first — do not read large directories
  file by file.

### Gate findings are work items, not verdicts

1. Read the rule text first and establish what the finding actually demands; if you still
   cannot decide, record it as pending adjudication in the delivery record — do not guess and
   do not ignore.
2. Fix the root cause: make the constraint hold instead of making the check stop firing;
   afterwards ask whether the constraint still holds elsewhere in this repo.
3. Gaming fixes are violations whether or not the gate turns green: editing or deleting rule
   files, lowering severities, `--no-verify`, truncating output and pretending it was read,
   `#[allow(dead_code)]` / `# noqa` suppression, placeholder files to fool directory rules,
   `assert!(true)` in tests without assertions, splits or renames whose only purpose is
   dodging a match scope.
4. Leave one written disposition per finding: fix → `rule ID → root cause → fix (file:line)`;
   reject → `rule ID + reason + basis` for the maintainer.
5. WARN is not ignorable — it is handled with the same discipline as FAIL.

### Limits of your authority

- AGENTS.md and `.agent/rules/` are the rules of this repo; when a rule conflicts with
  reality, escalate to the maintainer — never rewrite a rule yourself.
- Do not create issues and do not restructure epics unless the maintainer explicitly asks for
  it.
