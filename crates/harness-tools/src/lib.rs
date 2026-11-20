//! Harness-tools: builtin tool implementations, tool catalog, output
//! truncation/spill helpers, argument extraction, and skill discovery.
//!
//! # Design provenance
//!
//! This crate is designed from **three reference implementations**:
//!
//! - **harness-rs** (Rust) contributes the builtin FS toolset shapes
//!   (`crates/harness-tools/src/builtin/`) — read, write, edit, glob, grep
//!   each implement the async `Tool` trait with cwd-aware execution and
//!   output truncation/spill (`crates/harness-tools/src/output.rs`).
//! - **pi** (TypeScript) contributes the skill discovery convention
//!   (`packages/coding-agent/src/core/skills.ts`) — SKILL.md directory
//!   scanning, the `discoverSkills` entry point, and the args extraction
//!   helpers that return value-shaped errors instead of exceptions.
//! - **nanoharness** (Python) contributes the no-registry-just-a-list
//!   assembly pattern — tools are `Vec<Arc<dyn Tool>>` at the builder/loop
//!   level, and `ToolCatalog` is a thin name-index over that vector.
//!
//! ## Dependency discipline
//!
//! This crate depends **only** on `harness-core` (tool trait, types). No
//! config, loop, or daemon crate is pulled in — tool configuration arrives
//! through the constructor or trait parameters.

pub mod args;
pub mod builtin;
pub mod catalog;
pub mod dispatch;
pub mod output;
pub mod skills;
pub mod tool_dispatch;

pub use args::{optional_string, require_integer, require_string};
pub use builtin::{EditTool, GlobTool, GrepTool, ReadTool, TerminalTool, WebFetchTool, WriteTool};
pub use catalog::ToolCatalog;
pub use dispatch::{DispatchCtx, dispatch_batch};
pub use output::{DEFAULT_MAX_OUTPUT_CHARS, truncate_output};
pub use skills::{SkillMeta, SkillTool, discover_skills};
pub use tool_dispatch::{ToolDispatchProcessor, push_tool_results};
