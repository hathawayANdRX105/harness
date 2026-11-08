# Rust Code Conventions

**When to read this**: when writing Rust code, reviewing code, or changing an existing symbol.

**What this solves**: the writing rules for Rust in this repo — how to investigate before changing, how to name and shape types, which syntax form to use, when to abstract, and how to write comments. Each rule says what the repo expects, not what Rust permits.

Read order: §1 before you touch anything, §2–§3 while you write, §4–§5 as you finish, §6 when you review.

## 1. Investigate before writing

- **Look before you write**: find an existing Rust method, a same-shape implementation, or an installed dependency first. Use the LSP (`hover` / `goToDefinition` / `findReferences`) to inspect a symbol's signature, implementation, and callers; use `codegraph callers <symbol>` for repo-wide reach if the index is built (`codegraph sync`; index lives in `.codegraph/`, gitignored).
- **Before changing a public symbol, list every caller** (`findReferences` / `codegraph callers`). Changing an interface migrates every caller in the same change — no dual-path compatibility.
- **Push logic down**: cross-layer logic belongs in the owning crate (`crates/harness-*`), not assembled at the call site. harness is a flat workspace of focused crates; keep each crate's responsibility coherent.
- **One function does one thing**: past roughly 80 lines, split it by responsibility into well-named functions.

## 2. Naming and type design

- **Conversion methods follow `as_` / `to_` / `into_`**: `as_*` is free and borrowed → borrowed (`str::as_bytes`); `to_*` costs something or allocates, borrowed → owned (`str::to_string`); `into_*` consumes `self` (`String::into_bytes`).
- **Getters do not use the `get_` prefix**: `fn first(&self) -> &First`, not `get_first`. Reserve `get` for the rare single-obvious-target case (e.g. `Cell::get`).
- **Prefer enums and newtypes over `bool` / `Option` arguments**: `Widget::new(Small, Round)` is clearer than `Widget::new(true, false)`. Use newtypes to distinguish values sharing one underlying type.
- **Implement common traits eagerly**: `Clone`, `PartialEq`, `Eq`, `Hash`, `Debug`, `Display`, `Default` (and `Copy` when cheap) as the type's semantics allow; every public type implements `Debug`.

## 3. Preferred syntax forms

Each construct has one preferred form. When a rewrite would need one of the listed escapes, keep the original form instead.

- **if / if-else**: four shapes, each with its own home.
  - *Early return* (guard clause, goto-style): `if cond { return ...; }` — the plain `if` block.
  - *Binary value choice*: a plain `if` / `else` expression — `if cond { A } else { B }`. This is the default form for any two-way choice, in plain code and inside view construction. Do **not** route it through a helper — wrapping a two-branch choice in a call adds a name to look up without making the branch easier to read.
  - *Multiple branches / enum dispatch*: `match`.
  - Avoid `else` after an early return, and multi-branch `if-else` chains; `if let` / `match` on Option/Result is replaced by the combinators below.
- **match**: the dispatch tool — enums with three or more arms, guarded arms (`Some(x) if c`), or patterns binding several parts; keep arms exhaustive and avoid a bare wildcard when the enum can grow.
- **iter chain**: building or transforming collections is one chain — `map` / `filter` / `filter_map` / `flat_map` finished by `collect` or `for_each`; merge same-shape push loops with `.chain(..)`. Prefer chaining over hand-rolled loops.
- **Option**: two-way dispatch goes through combinators — `.map` / `.map_or` / `.and_then` / `.filter` / `.unwrap_or`.
- **Result**: propagate with `?` first, then `.map` / `.and_then` for the payload, `.map_err` for context, `.map_or` / `.unwrap_or` for defaults.
- **View construction (ratatui/REPL output)**: keep view-building code readable — non-trivial expressions move into named helper functions above the view block, exactly like plain code; conditional widget trees follow the same `if` / `else` and `match` preferences.
- **closures**: **reduce closure use** — prefer named functions. A closure is acceptable only as a tiny inline adapter (`|x| expr`). When a closure captures many variables or its body grows past one expression, refactor into a named function call.
- **nesting**: keep nesting within **3 levels**; at 4 levels or more, extract into functions or restructure with early returns / combinators.

## 4. Reuse and abstraction

- **Abstract at the third occurrence** (Rule of Three): duplicating twice is cheaper than a premature abstraction. Prefer duplication over the wrong abstraction — when an abstraction starts accumulating parameters and conditional paths to serve different callers, it is likely wrong; inline it back into the callers.
- **Abstract when behavior or data actually repeats** — identical behavior, overlapping data, or the same states/constants in multiple places:

  | What repeats | Abstract into |
  |---|---|
  | the same logic | a function |
  | shared behavior across types | a trait |
  | repeated data or state | a struct or an enum |
  | repeated constants | a `const` or enum variants |
  | one value with distinct meanings | a newtype |

- Keep the abstraction small: a trait with one implementor and no second caller is usually a premature generalization.

## 5. Comments

- **Goal, details, cautions**: comments state the implementation goal and the details / edge cases to watch for. No framework or task boilerplate, no meaningless numbered design content.
- **File headers**: every source file starts with a comment describing what the file contains and what functionality is implemented here — not a bullet list of items, but an explanation of what code and features live in this file.
- **Test comments**: test code must carry a comment stating the test purpose, the covered scenarios, and the acceptance criteria.
- Comments explain **why**, not what — the code already says what it does.
- Prefer line comments (`//`) over block comments (`/* ... */`); put a comment on its own line when possible; write complete sentences; keep comments true to the code — when the code changes, update the comment.
- Comments are written in **English**.

## 6. Hard constraints

- Writing macros requires maintainer approval — do not introduce `macro_rules!` or proc macros on your own. (harness already defines proc-macro crates in its own workspace; new ones still need approval.)
- Nesting stays within 3 levels; 4+ levels must be refactored.
- A two-way value choice is a plain `if` / `else` expression, never a helper call.
- Closures are minimized in favor of named functions.
- `unwrap` / `expect` are denied in production paths by `[workspace.lints.clippy]` in the root `Cargo.toml` (tests are exempt via `clippy.toml`) — propagate `Result` instead.
- Never feed the whole repository to a review tool in one shot — batch per module and cover only this change's diff.
