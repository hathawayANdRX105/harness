//! The declarative assembly declaration: `HarnessSpec` and its section types.
//!
//! A spec describes everything a runtime is made of — tools, prompt sections,
//! skills, hooks, MCP servers, models and a compaction policy — as plain
//! serde data, so a spec round-trips through TOML/JSON and can be validated
//! **without any config file or builder**. The builder (#10) consumes the
//! validated spec; nothing here reaches for a runtime.
//!
//! # Design provenance
//!
//! The spec is the Rust reading of NanoHarness's pydantic profiles
//! (`refs/nanoharness/nanoharness/profiles/models.py`) plus the
//! `harness-rs` `AgentLoop` builder knobs (`with_tool` / `with_system` /
//! `with_compact_policy`). The three differences are deliberate:
//!
//! - the capability manifest (`provides` / `requires` / `conflicts`) lives on
//!   **each installable unit** instead of a separate extension registry, so
//!   dependency analysis is a pure function of the spec (`analyze.rs`);
//! - every unit is explicitly declared — there is no implicit "default tool
//!   set" or "default prompt", so a spec with an empty `tools` list is a
//!   valid spec that assembles no tools;
//! - the compaction policy lists concrete stages or is empty (no hidden
//!   default ladder): a host that wants the five-stage ladder writes it out.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::SpecError;

/// The capability manifest of one installable unit. `provides` names the
/// capabilities this unit makes available to other units, `requires` names
/// capabilities that must exist somewhere in the spec (analysis fails when
/// none is provided), and `conflicts` names capabilities whose presence makes
/// this unit invalid (analysis fails when a provider exists).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnitManifest {
    #[serde(default)]
    pub provides: Vec<String>,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub conflicts: Vec<String>,
}

/// The full declarative description of one assembled runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessSpec {
    /// Human-readable name of this assembly (logs, explain output).
    pub name: String,
    #[serde(default)]
    pub tools: Vec<ToolSpec>,
    #[serde(default)]
    pub prompt_sections: Vec<PromptSectionSpec>,
    #[serde(default)]
    pub skills: Vec<SkillSpec>,
    #[serde(default)]
    pub hooks: Vec<HookSpec>,
    #[serde(default)]
    pub mcp_servers: Vec<McpServerSpec>,
    #[serde(default)]
    pub models: Vec<ModelSpec>,
    /// Handle of the model the runtime must run; `build` rejects an injected
    /// model whose `info().handle` differs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default)]
    pub compaction: CompactionPolicySpec,
}

impl HarnessSpec {
    /// An empty spec with just a name — the starting point for composing one.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tools: Vec::new(),
            prompt_sections: Vec::new(),
            skills: Vec::new(),
            hooks: Vec::new(),
            mcp_servers: Vec::new(),
            models: Vec::new(),
            default_model: None,
            compaction: CompactionPolicySpec::default(),
        }
    }

    /// Validate the spec standalone — no config, no builder, no filesystem.
    ///
    /// Checks structural invariants (unique unit ids, non-empty names, MCP
    /// tool references point at a declared server, builtin tool names are
    /// known, hook kinds are distinct per loop seam, model and compaction
    /// fields are sane) and that every capability name is non-empty.
    ///
    /// # Errors
    ///
    /// Returns the first `SpecError` found; dependency *relations* (missing
    /// providers, conflicts, cycles) are checked by [`crate::analyze`], not
    /// here.
    pub fn validate(&self) -> Result<(), SpecError> {
        if self.name.trim().is_empty() {
            return Err(SpecError::EmptySpecName);
        }
        self.validate_unique_units()?;
        self.validate_tools()?;
        self.validate_mcp_servers()?;
        self.validate_prompt_sections()?;
        self.validate_skills()?;
        self.validate_hooks()?;
        self.validate_models()?;
        self.validate_compaction()
    }

    /// Dry-run introspection: a deterministic text rendering of what this
    /// spec would assemble, including validation and dependency results.
    ///
    /// Never spawns a process, connects a server or touches the filesystem —
    /// it only reads the spec and runs the pure analysis.
    pub fn explain(&self) -> String {
        crate::explain::explain(self)
    }

    fn validate_unique_units(&self) -> Result<(), SpecError> {
        let mut seen = HashSet::new();
        let ids = self
            .tools
            .iter()
            .map(ToolSpec::unit_id)
            .chain(self.prompt_sections.iter().map(PromptSectionSpec::unit_id))
            .chain(self.skills.iter().map(SkillSpec::unit_id))
            .chain(self.hooks.iter().map(HookSpec::unit_id))
            .chain(self.mcp_servers.iter().map(McpServerSpec::unit_id));
        for id in ids {
            if !seen.insert(id.clone()) {
                return Err(SpecError::DuplicateUnit { id });
            }
        }
        Ok(())
    }

    fn validate_tools(&self) -> Result<(), SpecError> {
        let servers: HashSet<&str> = self
            .mcp_servers
            .iter()
            .map(|server| server.name.as_str())
            .collect();
        for tool in &self.tools {
            if tool.name.trim().is_empty() {
                return Err(SpecError::EmptyToolName);
            }
            validate_manifest(&tool.unit_id(), &tool.manifest)?;
            match tool.kind {
                ToolKind::Builtin => {
                    if tool.server.is_some() || tool.remote.is_some() {
                        return Err(SpecError::UnexpectedMcpFields {
                            tool: tool.name.clone(),
                        });
                    }
                    if !is_builtin_tool(&tool.name) {
                        return Err(SpecError::UnknownBuiltin {
                            name: tool.name.clone(),
                        });
                    }
                }
                ToolKind::Mcp => {
                    let Some(server) = tool.server.as_deref() else {
                        return Err(SpecError::MissingMcpServer {
                            tool: tool.name.clone(),
                        });
                    };
                    if server.trim().is_empty() {
                        return Err(SpecError::MissingMcpServer {
                            tool: tool.name.clone(),
                        });
                    }
                    if !servers.contains(server) {
                        return Err(SpecError::UnknownMcpServer {
                            tool: tool.name.clone(),
                            server: server.to_string(),
                        });
                    }
                    if tool
                        .remote
                        .as_deref()
                        .is_some_and(|remote| remote.trim().is_empty())
                    {
                        return Err(SpecError::EmptyRemoteTool {
                            tool: tool.name.clone(),
                        });
                    }
                }
                ToolKind::External => {
                    if tool.server.is_some() || tool.remote.is_some() {
                        return Err(SpecError::UnexpectedMcpFields {
                            tool: tool.name.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_mcp_servers(&self) -> Result<(), SpecError> {
        for server in &self.mcp_servers {
            if server.name.trim().is_empty() {
                return Err(SpecError::EmptyMcpServerName);
            }
            validate_manifest(&server.unit_id(), &server.manifest)?;
            if server
                .command
                .as_deref()
                .is_some_and(|command| command.trim().is_empty())
            {
                return Err(SpecError::EmptyMcpCommand {
                    server: server.name.clone(),
                });
            }
            if server
                .url
                .as_deref()
                .is_some_and(|url| url.trim().is_empty())
            {
                return Err(SpecError::EmptyMcpUrl {
                    server: server.name.clone(),
                });
            }
            // A config-backed server may defer its endpoint to the bound
            // application config; a spec-declared server must stand alone.
            if !server.from_config && server.command.is_none() && server.url.is_none() {
                return Err(SpecError::MissingMcpEndpoint {
                    server: server.name.clone(),
                });
            }
            if server.env.keys().any(|key| key.trim().is_empty()) {
                return Err(SpecError::EmptyEnvKey {
                    server: server.name.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate_prompt_sections(&self) -> Result<(), SpecError> {
        for section in &self.prompt_sections {
            if section.name.trim().is_empty() {
                return Err(SpecError::EmptyPromptSection);
            }
            validate_manifest(&section.unit_id(), &section.manifest)?;
            if section.content.trim().is_empty() {
                return Err(SpecError::EmptyPromptContent {
                    name: section.name.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate_skills(&self) -> Result<(), SpecError> {
        for skill in &self.skills {
            if skill.name.trim().is_empty() || skill.path.trim().is_empty() {
                return Err(SpecError::EmptySkill);
            }
            validate_manifest(&skill.unit_id(), &skill.manifest)?;
        }
        Ok(())
    }

    fn validate_hooks(&self) -> Result<(), SpecError> {
        let mut seams = HashSet::new();
        for hook in &self.hooks {
            if hook.name.trim().is_empty() {
                return Err(SpecError::EmptyHookName);
            }
            validate_manifest(&hook.unit_id(), &hook.manifest)?;
            if !seams.insert(hook.kind.seam()) {
                return Err(SpecError::DuplicateHookKind {
                    seam: hook.kind.seam(),
                });
            }
            match &hook.kind {
                HookKind::DenyTools { tools } => {
                    if tools.is_empty() || tools.iter().any(|tool| tool.trim().is_empty()) {
                        return Err(SpecError::EmptyHookTools {
                            hook: hook.name.clone(),
                        });
                    }
                }
                HookKind::TerminateAfterToolBatch => {}
                HookKind::InjectInstruction { text } => {
                    if text.trim().is_empty() {
                        return Err(SpecError::EmptyInstruction {
                            hook: hook.name.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_models(&self) -> Result<(), SpecError> {
        let mut handles = HashSet::new();
        for model in &self.models {
            if model.handle.trim().is_empty() {
                return Err(SpecError::EmptyModelField {
                    handle: model.handle.clone(),
                    field: "handle",
                });
            }
            if model.provider.trim().is_empty() {
                return Err(SpecError::EmptyModelField {
                    handle: model.handle.clone(),
                    field: "provider",
                });
            }
            if model.model.trim().is_empty() {
                return Err(SpecError::EmptyModelField {
                    handle: model.handle.clone(),
                    field: "model",
                });
            }
            if model.context_window == 0 {
                return Err(SpecError::ZeroContextWindow {
                    handle: model.handle.clone(),
                });
            }
            if !handles.insert(model.handle.clone()) {
                return Err(SpecError::DuplicateModel {
                    handle: model.handle.clone(),
                });
            }
        }
        if let Some(default) = &self.default_model {
            if default.trim().is_empty() {
                return Err(SpecError::EmptyDefaultModel);
            }
            if !handles.contains(default) {
                return Err(SpecError::UnknownDefaultModel {
                    handle: default.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate_compaction(&self) -> Result<(), SpecError> {
        let mut previous: Option<f64> = None;
        for stage in &self.compaction.stages {
            if stage.label.trim().is_empty() {
                return Err(SpecError::EmptyStageLabel);
            }
            if !(stage.threshold > 0.0 && stage.threshold <= 1.0) {
                return Err(SpecError::InvalidThreshold {
                    label: stage.label.clone(),
                    threshold: stage.threshold,
                });
            }
            if !(stage.target_ratio >= 0.0 && stage.target_ratio <= stage.threshold) {
                return Err(SpecError::InvalidTarget {
                    label: stage.label.clone(),
                    target: stage.target_ratio,
                });
            }
            if let Some(previous_threshold) = previous
                && stage.threshold <= previous_threshold
            {
                return Err(SpecError::UnorderedStages {
                    label: stage.label.clone(),
                });
            }
            previous = Some(stage.threshold);
        }
        Ok(())
    }
}

/// How a declared tool is realized at assembly time. `Builtin` maps to a
/// `harness-tools` builtin by name; `Mcp` pulls the server's advertised tools;
/// `External` resolves against tools injected into the builder by the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ToolKind {
    Builtin,
    Mcp,
    External,
}

/// One declared tool. The `name` is the unit id and, for `builtin` /
/// `external` kinds, the model-facing tool name; for `mcp` it is only the
/// unit label and `server` selects the server whose tools are installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSpec {
    pub name: String,
    pub kind: ToolKind,
    /// MCP server name (kind `mcp` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    /// Optional remote-tool filter for kind `mcp`: install only the server
    /// tool with this remote name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    #[serde(default)]
    pub manifest: UnitManifest,
}

impl ToolSpec {
    /// The unit id this tool contributes to the install order.
    pub fn unit_id(&self) -> String {
        format!("tool:{}", self.name)
    }
}

/// One named section of the system-prompt ledger. Sections are assembled into
/// a single `harness_core::message::Message::System` whose map is ordered by
/// section name (the ledger contract), so declaration order is not preserved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptSectionSpec {
    pub name: String,
    pub content: String,
    #[serde(default)]
    pub manifest: UnitManifest,
}

impl PromptSectionSpec {
    /// The unit id this prompt section contributes to the install order.
    pub fn unit_id(&self) -> String {
        format!("prompt:{}", self.name)
    }
}

/// One declared skill: `path` is the skills root directory scanned for
/// `<name>/SKILL.md` folders (`harness_tools::discover_skills`), and `name`
/// selects the skill to install from that root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillSpec {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub manifest: UnitManifest,
}

impl SkillSpec {
    /// The unit id this skill contributes to the install order.
    pub fn unit_id(&self) -> String {
        format!("skill:{}", self.name)
    }
}

/// The behaviors a hook can encode declaratively. Each kind maps to exactly
/// one `harness_loop::LoopHooks` seam, so a spec may declare at most one hook
/// per kind (validation enforces it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HookKind {
    /// Refuse calls to the named tools before execution (the
    /// `before_tool_call` seam).
    DenyTools { tools: Vec<String> },
    /// End the run cleanly after the first tool batch executes (the
    /// `should_terminate_tool_batch` seam).
    TerminateAfterToolBatch,
    /// Append a user message to the transcript before each model call (the
    /// `prepare_request` seam).
    InjectInstruction { text: String },
}

impl HookKind {
    /// Which `LoopHooks` seam this kind occupies — the key used to reject
    /// two hooks competing for the same seam.
    pub fn seam(&self) -> &'static str {
        match self {
            HookKind::DenyTools { .. } => "before_tool_call",
            HookKind::TerminateAfterToolBatch => "should_terminate_tool_batch",
            HookKind::InjectInstruction { .. } => "prepare_request",
        }
    }
}

/// One declared hook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookSpec {
    pub name: String,
    pub kind: HookKind,
    #[serde(default)]
    pub manifest: UnitManifest,
}

impl HookSpec {
    /// The unit id this hook contributes to the install order.
    pub fn unit_id(&self) -> String {
        format!("hook:{}", self.name)
    }
}

/// One declared MCP server. `from_config` marks the server as config-backed:
/// its endpoint fields are resolved by name from the builder's bound
/// `harness_config::McpConfig` at build time instead of standing alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerSpec {
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
    #[serde(default)]
    pub manifest: UnitManifest,
}

impl McpServerSpec {
    /// The unit id this server contributes to the install order.
    pub fn unit_id(&self) -> String {
        format!("mcp:{}", self.name)
    }
}

/// One declared model catalog entry — the identity an injected model adapter
/// is validated against, not the adapter itself (adapters are injected, never
/// defaulted).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelSpec {
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
pub struct CompactionPolicySpec {
    #[serde(default)]
    pub stages: Vec<CompactionStageSpec>,
}

/// One compaction stage, mirroring `harness_context::CompactionStage`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionStageSpec {
    /// Budget ratio that activates this stage (0.0 – 1.0).
    pub threshold: f64,
    /// Target ratio after compaction.
    pub target_ratio: f64,
    pub label: String,
}

/// The builtin tool names `harness-tools` provides, in the order the builder
/// recognizes them.
pub const BUILTIN_TOOLS: [&str; 5] = ["read", "write", "edit", "glob", "grep"];

fn is_builtin_tool(name: &str) -> bool {
    BUILTIN_TOOLS.contains(&name)
}

fn validate_manifest(unit: &str, manifest: &UnitManifest) -> Result<(), SpecError> {
    manifest
        .provides
        .iter()
        .chain(&manifest.requires)
        .chain(&manifest.conflicts)
        .try_for_each(|capability| {
            if capability.trim().is_empty() {
                Err(SpecError::EmptyCapability {
                    unit: unit.to_string(),
                })
            } else {
                Ok(())
            }
        })
}
