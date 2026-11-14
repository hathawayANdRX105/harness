//! Harness builder: turn a declarative [`HarnessSpec`] into an assembled
//! runtime by explicit injection.
//!
//! The crate has four stages, each usable on its own:
//!
//! | Stage | Entry point | What it does |
//! |---|---|---|
//! | Declare | [`HarnessSpec`] (serde) | Describes tools, prompt sections, skills, hooks, MCP servers, models and a compaction policy. Validates without any config. |
//! | Analyze | [`analyze`] | Resolves the units' `provides` / `requires` / `conflicts`, rejects missing/ambiguous providers, conflicts and cycles, and derives a deterministic install order. |
//! | Explain | [`HarnessSpec::explain`] / [`explain`] | Dry-run text rendering of everything above — no processes, no filesystem. |
//! | Assemble | [`HarnessBuilder::build`] | Produces an [`AssembledRuntime`]: model, tools, prompt ledger, hooks, MCP pool, skills, config surface, and [`harness_loop::LoopDeps`] for the scheduler. |
//!
//! # Design provenance
//!
//! NanoHarness's `HarnessBuilder` (`refs/nanoharness/nanoharness/profiles/`)
//! is the reference: `build` validates a profile spec, `_analyze` resolves the
//! extension manifest's provides/requires/conflicts into a topological install
//! order, services are bound by name, and `explain` is a dry run. `harness-rs`'s
//! `AgentLoop` builder contributes the fluent injection surface
//! (`with_tool` / `with_system` / `with_compact_policy`). This crate is the
//! Rust reading of both: a static, serde-first spec and an explicit builder —
//! not a dynamic extension manager and not a god-object composition root.
//!
//! # Explicit injection, no coding fallback
//!
//! A capability that is not written in the spec is not assembled. The builder
//! never installs a default tool set, never picks a default model, never
//! substitutes an allow-all policy and never falls back to the default
//! compaction ladder. Missing injections are named errors:
//! [`BuildError::MissingModel`], [`BuildError::MissingPolicy`],
//! [`BuildError::MissingEventSink`], [`BuildError::MissingCwd`],
//! [`BuildError::MissingExternalTool`].
//!
//! # Config mapping
//!
//! Mapping the application config onto harness types lives here (this crate is
//! the seam): [`ConfigBindings::from_config`] maps `[models]` aliases to
//! [`harness_model::AliasRegistry`], `[budget]` to [`harness_core::run::RunLimits`]
//! and [`harness_context::Budget`], and `[mcp.servers]` to
//! [`harness_mcp::McpServerConfig`]. A spec MCP server marked `from_config`
//! takes its endpoint from those bindings by name.
//!
//! # Dependency discipline
//!
//! The builder depends on the contract and component crates it assembles
//! (`harness-core`, `harness-config`, `harness-model`, `harness-mcp`,
//! `harness-tools`, `harness-loop`, `harness-context`) and deliberately not on
//! the scheduler, session, transport or daemon: it produces a runtime those
//! layers consume, it does not drive them.

mod analyze;
mod assemble;
mod error;
mod explain;
mod spec;

pub use analyze::{Analysis, analyze};
pub use assemble::{
    AssembledRuntime, CompactionPlan, ConfigBindings, HarnessBuilder, McpPool, McpServerTools,
    alias_registry_from_models, budget_from_config, compaction_stages_from_spec,
    mcp_server_config_from_entry, mcp_server_specs, mcp_tool_specs, model_info_from_spec,
    prompt_sections, run_limits_from_config, skill_specs_from_roots,
};
pub use error::{AnalyzeError, BuildError, SpecError};
pub use explain::explain;
pub use spec::{
    BUILTIN_TOOLS, CompactionPolicySpec, CompactionStageSpec, HarnessSpec, HookKind, HookSpec,
    McpServerSpec, ModelSpec, PromptSectionSpec, SkillSpec, ToolKind, ToolSpec, UnitManifest,
};
