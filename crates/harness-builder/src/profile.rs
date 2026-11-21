//! A capability preset: what one agent may use.
//!
//! The preset is the declarative half of assembly — the host (or, later, a
//! file) states which tools, skills, prompt sections, hooks, MCP servers,
//! models and compaction policy an agent runs with, and the builder wires
//! those declarations into the runtime objects it injects. It is **data**:
//! serde round-trippable, no behavior, no validation machinery — mistakes
//! surface at build time, when a declared entry cannot be resolved against
//! what the host injected.

use serde::{Deserialize, Serialize};

/// The preset itself: one named capability set for an agent run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessProfile {
    /// Human-readable name of this preset (logs, debug output).
    pub name: String,
    #[serde(default)]
    pub tools: Vec<ToolEntry>,
    #[serde(default)]
    pub prompt: Vec<PromptSection>,
    #[serde(default)]
    pub skills: Vec<SkillRef>,
    #[serde(default)]
    pub hooks: Vec<HookEntry>,
    #[serde(default)]
    pub mcp_servers: Vec<McpServerEntry>,
    #[serde(default)]
    pub models: Vec<ModelEntry>,
    /// Handle of the model the runtime must run; `build` rejects an injected
    /// model whose `info().handle` differs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub compaction: CompactionPolicy,
}

impl HarnessProfile {
    /// An empty preset with just a name — the starting point for composing
    /// one.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tools: Vec::new(),
            prompt: Vec::new(),
            skills: Vec::new(),
            hooks: Vec::new(),
            mcp_servers: Vec::new(),
            models: Vec::new(),
            default_model: None,
            compaction: CompactionPolicy::default(),
        }
    }
}

/// How a tool entry resolves to an installed tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    Builtin,
    Mcp,
    External,
}

/// One tool the agent may use. The `name` is the model-facing tool name for
/// `builtin` / `external`; for `mcp` it is only a label and `server` selects
/// the server whose tools are installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolEntry {
    pub name: String,
    pub kind: ToolKind,
    /// MCP server name (kind `mcp` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    /// Optional remote-tool filter for kind `mcp`: install only the server
    /// tool with this remote name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
}

impl ToolEntry {
    /// A built-in harness-tools entry by name.
    pub fn builtin(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: ToolKind::Builtin,
            server: None,
            remote: None,
        }
    }

    /// A host-injected tool: the builder resolves the name against the
    /// tools registered via `HarnessBuilder::with_tool`.
    pub fn external(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: ToolKind::External,
            server: None,
            remote: None,
        }
    }
}

/// One named section of the system-prompt ledger. Sections are assembled into
/// a single `harness_core::message::Message::System` whose map is ordered by
/// section name (the ledger contract), so declaration order is not preserved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptSection {
    pub name: String,
    pub content: String,
}

/// One skill the agent may activate: `path` is the skills root directory
/// scanned for `<name>/SKILL.md` folders (`harness_tools::discover_skills`),
/// and `name` selects the skill to install from that root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillRef {
    pub name: String,
    pub path: String,
}

/// The behaviors a hook can encode declaratively. Each kind maps to exactly
/// one runtime seam, so a preset may declare at most one hook per kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HookKind {
    /// Refuse calls to the named tools before execution (the
    /// `before_tool_call` seam).
    DenyTools { tools: Vec<String> },
    /// End the run cleanly after the first tool batch executes (the
    /// `should_terminate_tool_batch` seam).
    TerminateAfterToolBatch,
    /// Append a user message to the transcript before each model call —
    /// materializes as an `InstructionProcessor` node in the prompt-side
    /// chain.
    InjectInstruction { text: String },
}

impl HookKind {
    /// Which seam this kind occupies — the key used to reject two hooks
    /// competing for the same seam at build time.
    pub fn seam(&self) -> &'static str {
        match self {
            HookKind::DenyTools { .. } => "before_tool_call",
            HookKind::TerminateAfterToolBatch => "should_terminate_tool_batch",
            HookKind::InjectInstruction { .. } => "prompt_chain",
        }
    }
}

/// One declared hook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookEntry {
    pub name: String,
    pub kind: HookKind,
}

/// One declared MCP server. `from_config` marks the server as config-backed:
/// its endpoint fields are resolved by name from the builder's bound
/// `harness_config::McpConfig` at build time instead of standing alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerEntry {
    pub name: String,
    /// Command to spawn (stdio transport).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables injected into the server process.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    /// Server URL (HTTP transport).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_timeout_ms: Option<u64>,
    #[serde(default)]
    pub from_config: bool,
}

/// One model catalog entry — the identity an injected model adapter is
/// checked against, not the adapter itself (adapters are injected, never
/// defaulted).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEntry {
    pub handle: String,
    pub provider: String,
    pub model: String,
    pub context_window: u32,
    #[serde(default = "default_true")]
    pub supports_tool_use: bool,
    #[serde(default = "default_true")]
    pub supports_streaming: bool,
}

const fn default_true() -> bool {
    true
}

/// The compaction policy: an explicit list of stages. Empty means no
/// compaction is configured — there is deliberately no default ladder.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionPolicy {
    #[serde(default)]
    pub stages: Vec<CompactionStage>,
}

/// One compaction stage, mirroring `harness_context::CompactionStage`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionStage {
    /// Budget ratio that activates this stage (0.0 – 1.0).
    pub threshold: f64,
    /// Target ratio after compaction.
    pub target_ratio: f64,
    pub label: String,
}

/// The builtin tool names `harness-tools` provides, in the order the builder
/// recognizes them.
pub const BUILTIN_TOOLS: [&str; 7] = [
    "read", "write", "edit", "glob", "grep", "terminal", "webfetch",
];
