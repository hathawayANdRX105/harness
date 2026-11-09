# Local Development Environment

**When to read this**: when starting work in this repo, when a build feels slow or always cold, when configuring a new machine (desktop or termux phone), or when processes/worktrees pile up.

**What this solves**: harness's dev optimizations are optional and probe-based. This rule tells you where they live, and the multi-session process hygiene that applies regardless of machine.

## 1. Toolchain optimizations (read the skill first)

The probe script is `scripts/dev-env.sh` — read its header comment for why each probe is written the way it is. Summary of the model:

- `.cargo/config.toml` holds the shared baseline: the `jobs` ceiling plus the **profile-scoped Cranelift codegen backend** (a workspace build contract, not a machine detail). Never hardcode `sccache` / `mold` into it — machines lacking the tool then fail to start cargo.
- Per-machine optimizations (sccache, mold) are injected by sourcing `scripts/dev-env.sh`, which probes and exports env vars (env > project config in cargo's precedence). Desktop enables them; termux probes find nothing and silently run plain cargo.
- Command-path choices (nextest vs `cargo test`, testless vs full run, dylint gate vs skip) happen at run time via `command -v` in justfile/scripts.

Before building or testing, verify what this machine enables:

```sh
HARNESS_DEV_ENV_VERBOSE=1 . scripts/dev-env.sh
```

## 2. Process hygiene (multi-session)

- Run long-lived services (dev servers, watchers) as **session-persistent background jobs** — never `nohup ... &` (the process group is reaped when the tool call ends, so the service silently dies).
- `pkill -f cargo|rustc` is forbidden — those may be other sessions' builds. Identify processes via `readlink /proc/<pid>/cwd` first; resume T-state builds with `kill -CONT` only after establishing ownership.
- Stop builds inside a worktree **before** `git worktree remove`; orphaned builds (cwd `(deleted)`) slow down every later session.
- Wrap CPU-heavy commands (compile / test / install) in `.githooks/heavy <cmd>`. On machines with systemd it applies a cgroup CPU quota derived from the core count — never hard-code a CPUQuota number; on machines without systemd (termux) it runs the command directly.

## 3. Worktrees

- One task, one worktree: `git worktree add .wt/<name> -b <type>/<slug>`; never run `git worktree add` from inside a worktree (it nests).
- Push the branch immediately after creating it (`git push -u origin <type>/<slug>`).
- Never `rm -rf .wt/` or `git clean` — they wipe other sessions' worktrees too.

## 4. Toolchain notes

- **nightly-2026-08-20 + Cranelift**: `rust-toolchain.toml` pins this nightly (Cranelift needs the unstable `codegen-backend` gate). The dev profile compiles with Cranelift (`.cargo/config.toml`), the ci profile stays on LLVM — the backend CI validates and that ships. The cargo fork (`huacnlee/cargo`), if installed, replaces `~/.cargo/bin/cargo` and shares one target directory across all worktrees with a self-limited size.
- nextest config, if any test-group serialization is needed, lives at `.config/nextest.toml` — that path is fixed by nextest, not movable into `.cargo/`.
