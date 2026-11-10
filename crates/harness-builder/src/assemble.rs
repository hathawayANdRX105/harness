//! Runtime assembly: map config sections onto harness types, then build the
//! assembled runtime from a validated spec by explicit injection.
//!
//! # Design provenance
//!
//! - NanoHarness's `HarnessBuilder.build`
//!   (`refs/nanoharness/nanoharness/profiles/builder.py:162`) — validate the
//!   spec, resolve the manifest, bind services, return one assembled object.
//!   The Rust reading replaces its dynamic `ExtensionManager` with explicit
//!   `Arc<dyn ...>` injection and a static install order.
//! - `harness-rs`'s `AgentLoop` builder (`with_tool` / `with_system` /
//!   `with_compact_policy`) — the fluent, explicit knob surface. Here the
//!   knobs are typed once and the resulting `LoopDeps` is what the loop
//!   consumes.
//! - `harness-config` owns the config schema; this crate is the **mapping
//!   layer** between it and `harness-mcp` / `harness-model` /
//!   `harness-context` types.
//!
//! # No defaults
//!
//! The builder never invents a model, a tool policy, an event sink or a tool
//! set. A spec that declares nothing assembles nothing; a spec that declares
//! something the host did not inject fails with a named error. That is the
//! "explicit injection, no coding fallback" rule this crate exists to enforce.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use harness_config::{BudgetConfig, Config, McpServerEntry, ModelsConfig};
use harness_context::{Budget, CompactionStage};
use harness_core::EventSink;
use harness_core::message::Message;
use harness_core::model::{Model, ModelInfo};
use harness_core::policy::{ApprovalBroker, ToolPolicy};
use harness_core::run::RunLimits;
use harness_core::tool::{Tool, ToolRequest};
use harness_loop::{LoopDeps, LoopHooks, ToolExecutionMode};
use harness_mcp::{Mcp, McpServerConfig, ToolMeta};
use harness_model::AliasRegistry;
use harness_tools::{
    EditTool, GlobTool, GrepTool, ReadTool, SkillMeta, WriteTool, discover_skills,
};

use crate::BuildError;
use crate::analyze;
use crate::spec::{
    CompactionPolicySpec, HarnessSpec, HookKind, McpServerSpec, ModelSpec, PromptSectionSpec,
    SkillSpec, ToolKind, ToolSpec,
};

// ---------------------------------------------------------------------------
// Config -> harness type mapping
// ---------------------------------------------------------------------------

/// Run limits mapped from the application's `[budget]` section.
pub fn run_limits_from_config(config: &BudgetConfig) -> RunLimits {
    RunLimits {
        max_turns: config.max_turns,
        max_input_tokens: config.max_input_tokens,
        max_output_tokens: config.max_output_tokens,
    }
}

/// A fresh budget for one run: nothing consumed yet, `max_input_tokens`
/// available. The compactor updates `used_tokens` as the ledger grows.
pub fn budget_from_config(config: &BudgetConfig) -> Budget {
    Budget {
        used_tokens: 0,
        available_tokens: u64::from(config.max_input_tokens),
    }
}

/// Model aliases mapped from `[models]`. Config documents first-match-wins;
/// the registry is a map, so the first declaration of a name is inserted and
/// later duplicates are ignored.
pub fn alias_registry_from_models(models: &ModelsConfig) -> AliasRegistry {
    let mut registry = AliasRegistry::new();
    let mut seen = HashSet::new();
    for alias in &models.aliases {
        if seen.insert(alias.name.clone()) {
            registry.insert(alias.name.clone(), alias.model_id.clone());
        }
    }
    registry
}

/// One `[mcp.servers]` entry mapped to the MCP client's config type.
///
/// The config stores only the **name** of the environment variable holding a
/// server secret; the value is read here, at assembly time, and never
/// persisted. A configured-but-unset variable is a hard error — silently
/// spawning a server without its secret produces a confusing handshake
/// failure later.
///
/// # Errors
///
/// Returns [`BuildError::MissingEnvVar`] when `env_var` names an unset
/// variable.
pub fn mcp_server_config_from_entry(entry: &McpServerEntry) -> Result<McpServerConfig, BuildError> {
    let mut env = HashMap::new();
    if let Some(variable) = &entry.env_var {
        let value = std::env::var(variable).map_err(|_| BuildError::MissingEnvVar {
            server: entry.name.clone(),
            name: variable.clone(),
        })?;
        env.insert(variable.clone(), value);
    }
    Ok(McpServerConfig {
        name: entry.name.clone(),
        command: Some(entry.command.clone()),
        args: entry.args.clone(),
        env,
        url: None,
        tool_call_timeout_ms: None,
    })
}

/// A spec model declaration mapped to the core catalog type.
pub fn model_info_from_spec(spec: &ModelSpec) -> ModelInfo {
    ModelInfo {
        handle: spec.handle.clone(),
        provider: spec.provider.clone(),
        model: spec.model.clone(),
        context_window: spec.context_window,
        supports_tool_use: spec.supports_tool_use,
        supports_streaming: spec.supports_streaming,
    }
}

/// Spec compaction stages mapped to `harness-context` stages.
pub fn compaction_stages_from_spec(policy: &CompactionPolicySpec) -> Vec<CompactionStage> {
    policy
        .stages
        .iter()
        .map(|stage| CompactionStage {
            threshold: stage.threshold,
            target_ratio: stage.target_ratio,
            label: stage.label.clone(),
        })
        .collect()
}

/// Everything the builder takes from the application config, mapped once.
#[derive(Debug, Clone)]
pub struct ConfigBindings {
    pub aliases: AliasRegistry,
    pub limits: RunLimits,
    pub budget: Budget,
    pub mcp_servers: Vec<McpServerConfig>,
}

impl ConfigBindings {
    /// Map every builder-relevant config section.
    ///
    /// # Errors
    ///
    /// Forwards [`mcp_server_config_from_entry`] failures (a configured but
    /// unset secret variable).
    pub fn from_config(config: &Config) -> Result<Self, BuildError> {
        let mcp_servers = config
            .mcp
            .servers
            .iter()
            .map(mcp_server_config_from_entry)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            aliases: alias_registry_from_models(&config.models),
            limits: run_limits_from_config(&config.budget),
            budget: budget_from_config(&config.budget),
            mcp_servers,
        })
    }

    /// Look up a mapped MCP server by its config name.
    pub fn mcp_server(&self, name: &str) -> Option<&McpServerConfig> {
        self.mcp_servers.iter().find(|server| server.name == name)
    }
}

impl Default for ConfigBindings {
    fn default() -> Self {
        let limits = RunLimits::default();
        Self {
            aliases: AliasRegistry::new(),
            limits,
            budget: Budget {
                used_tokens: 0,
                available_tokens: u64::from(limits.max_input_tokens),
            },
            mcp_servers: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Assembled runtime
// ---------------------------------------------------------------------------

/// One MCP server installed by the builder: the names it advertised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerTools {
    pub name: String,
    pub tools: Vec<String>,
}

/// The MCP half of the assembled runtime. The live transports are shared into
/// the assembled `McpProxyTool` instances; this records which servers were
/// connected and what each advertised, for introspection and shutdown logs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct McpPool {
    pub servers: Vec<McpServerTools>,
}

impl McpPool {
    pub fn len(&self) -> usize {
        self.servers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.servers.is_empty()
    }
}

/// Compaction configuration for the host: the explicit stage ladder and the
/// budget it runs against. The loop itself does not consume a compactor yet,
/// so this travels with the runtime for the scheduler/host to wire.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionPlan {
    pub stages: Vec<CompactionStage>,
    pub budget: Budget,
}

/// One assembled runtime — everything the loop and its host need, produced
/// from a spec plus explicit injections.
///
/// The scheduler (#11) consumes this by calling [`AssembledRuntime::loop_deps`]
/// or [`AssembledRuntime::into_loop_deps`]; the remaining fields (prompt,
/// skills, models, aliases, limits, budget, compaction, MCP pool) are the
/// session/config surface it carries alongside the loop dependencies.
pub struct AssembledRuntime {
    pub spec_name: String,
    /// Unit ids in the order dependency analysis resolved them.
    pub install_order: Vec<String>,
    pub model: Arc<dyn Model>,
    /// The prompt ledger, `None` when the spec declares no prompt sections.
    pub prompt: Option<Message>,
    pub tools: Vec<Arc<dyn Tool>>,
    pub mcp_pool: McpPool,
    pub hooks: LoopHooks,
    pub skills: Vec<SkillMeta>,
    pub models: Vec<ModelInfo>,
    pub aliases: AliasRegistry,
    pub limits: RunLimits,
    pub budget: Budget,
    pub compaction: Option<CompactionPlan>,
    pub policy: Arc<dyn ToolPolicy>,
    pub approval_broker: Option<Arc<dyn ApprovalBroker>>,
    pub event_sink: Arc<dyn EventSink>,
    pub tool_mode: ToolExecutionMode,
}

impl AssembledRuntime {
    /// The loop dependencies, cloned out of this runtime. The runtime stays
    /// usable (prompt, skills, config surface).
    pub fn loop_deps(&self) -> LoopDeps {
        LoopDeps {
            model: Arc::clone(&self.model),
            tools: self.tools.clone(),
            event_sink: Arc::clone(&self.event_sink),
            policy: Arc::clone(&self.policy),
            approval_broker: self.approval_broker.clone(),
            hooks: self.hooks.clone(),
            tool_mode: self.tool_mode,
        }
    }

    /// Consume the runtime into loop dependencies, moving the shared handles
    /// instead of cloning them.
    pub fn into_loop_deps(self) -> LoopDeps {
        LoopDeps {
            model: self.model,
            tools: self.tools,
            event_sink: self.event_sink,
            policy: self.policy,
            approval_broker: self.approval_broker,
            hooks: self.hooks,
            tool_mode: self.tool_mode,
        }
    }
}

impl fmt::Debug for AssembledRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tool_names: Vec<&str> = self.tools.iter().map(|tool| tool.name()).collect();
        f.debug_struct("AssembledRuntime")
            .field("spec_name", &self.spec_name)
            .field("install_order", &self.install_order)
            .field("model", self.model.info())
            .field("tools", &tool_names)
            .field("prompt", &self.prompt.as_ref().map(prompt_section_names))
            .field("mcp_pool", &self.mcp_pool)
            .field("hooks", &self.hooks)
            .field("skills", &self.skills)
            .field("models", &self.models)
            .field("aliases", &self.aliases)
            .field("limits", &self.limits)
            .field("budget", &self.budget)
            .field("compaction", &self.compaction)
            .field("tool_mode", &self.tool_mode)
            .finish_non_exhaustive()
    }
}

fn prompt_section_names(message: &Message) -> Vec<String> {
    match message {
        Message::System { sections } => sections.keys().cloned().collect(),
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builds an [`AssembledRuntime`] from a [`HarnessSpec`].
///
/// Every capability the spec declares must be supplied explicitly: the model
/// adapter, the tool policy and the event sink are required injections; tools
/// of kind `external` must be registered with [`HarnessBuilder::with_tool`];
/// builtin tools need a working directory via [`HarnessBuilder::with_cwd`].
/// Nothing is defaulted — see the module docs.
pub struct HarnessBuilder {
    spec: HarnessSpec,
    model: Option<Arc<dyn Model>>,
    policy: Option<Arc<dyn ToolPolicy>>,
    approval_broker: Option<Arc<dyn ApprovalBroker>>,
    event_sink: Option<Arc<dyn EventSink>>,
    external_tools: HashMap<String, Arc<dyn Tool>>,
    cwd: Option<PathBuf>,
    config: Option<ConfigBindings>,
    tool_mode: ToolExecutionMode,
}

impl HarnessBuilder {
    /// Start a builder for a spec.
    pub fn new(spec: HarnessSpec) -> Self {
        Self {
            spec,
            model: None,
            policy: None,
            approval_broker: None,
            event_sink: None,
            external_tools: HashMap::new(),
            cwd: None,
            config: None,
            tool_mode: ToolExecutionMode::default(),
        }
    }

    /// Inject the model adapter. Required — the builder never constructs one.
    pub fn with_model(mut self, model: Arc<dyn Model>) -> Self {
        self.model = Some(model);
        self
    }

    /// Inject the tool policy. Required — there is no implicit allow-all.
    pub fn with_policy(mut self, policy: Arc<dyn ToolPolicy>) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Inject an optional approval broker for policy decisions that ask for
    /// human approval.
    pub fn with_approval_broker(mut self, broker: Arc<dyn ApprovalBroker>) -> Self {
        self.approval_broker = Some(broker);
        self
    }

    /// Inject the event sink. Required — the loop's event log is not optional.
    pub fn with_event_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.event_sink = Some(sink);
        self
    }

    /// Register one externally implemented tool. The spec references it by
    /// its `name()`.
    pub fn with_tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.external_tools.insert(tool.name().to_string(), tool);
        self
    }

    /// Set the working directory for builtin tools. Required when the spec
    /// declares a builtin tool.
    pub fn with_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Bind the mapped application config (aliases, limits, budget, MCP
    /// server endpoints for config-backed spec servers).
    pub fn with_config_bindings(mut self, config: ConfigBindings) -> Self {
        self.config = Some(config);
        self
    }

    /// Map and bind an application [`Config`] in one step.
    ///
    /// # Errors
    ///
    /// Forwards [`ConfigBindings::from_config`] failures.
    pub fn with_config(mut self, config: &Config) -> Result<Self, BuildError> {
        self.config = Some(ConfigBindings::from_config(config)?);
        Ok(self)
    }

    /// How tools are dispatched within a turn (parallel by default, matching
    /// the loop's own default).
    pub fn with_tool_mode(mut self, mode: ToolExecutionMode) -> Self {
        self.tool_mode = mode;
        self
    }

    /// Validate, analyze, and assemble.
    ///
    /// # Errors
    ///
    /// [`BuildError::Spec`] / [`BuildError::Analyze`] for spec and dependency
    /// problems; [`BuildError::MissingModel`], [`BuildError::MissingPolicy`],
    /// [`BuildError::MissingEventSink`], [`BuildError::MissingCwd`] and
    /// [`BuildError::MissingExternalTool`] for missing explicit injections;
    /// [`BuildError::Mcp`] / [`BuildError::SkillNotFound`] for assembly
    /// failures.
    pub fn build(self) -> Result<AssembledRuntime, BuildError> {
        self.spec.validate()?;
        let analysis = analyze::analyze(&self.spec)?;
        let model = self.model.ok_or(BuildError::MissingModel)?;
        if let Some(expected) = &self.spec.default_model {
            let actual = model.info().handle.as_str();
            if actual != expected.as_str() {
                return Err(BuildError::ModelMismatch {
                    expected: expected.clone(),
                    actual: actual.to_string(),
                });
            }
        }
        let policy = self.policy.ok_or(BuildError::MissingPolicy)?;
        let event_sink = self.event_sink.ok_or(BuildError::MissingEventSink)?;
        let config = self.config.unwrap_or_default();

        let tools = assemble_tools(
            &self.spec,
            &analysis.install_order,
            &self.external_tools,
            self.cwd.as_deref(),
            &config,
        )?;
        let skills = assemble_skills(&self.spec.skills)?;
        let prompt = assemble_prompt(&self.spec.prompt_sections);
        let hooks = assemble_hooks(&self.spec, &prompt);
        let models: Vec<ModelInfo> = self.spec.models.iter().map(model_info_from_spec).collect();
        let compaction_stages = compaction_stages_from_spec(&self.spec.compaction);
        let compaction = (!compaction_stages.is_empty()).then_some(CompactionPlan {
            stages: compaction_stages,
            budget: config.budget,
        });

        Ok(AssembledRuntime {
            spec_name: self.spec.name,
            install_order: analysis.install_order,
            model,
            prompt,
            tools: tools.tools,
            mcp_pool: tools.pool,
            hooks,
            skills,
            models,
            aliases: config.aliases,
            limits: config.limits,
            budget: config.budget,
            compaction,
            policy,
            approval_broker: self.approval_broker,
            event_sink,
            tool_mode: self.tool_mode,
        })
    }
}

impl fmt::Debug for HarnessBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HarnessBuilder")
            .field("spec", &self.spec.name)
            .field("has_model", &self.model.is_some())
            .field("has_policy", &self.policy.is_some())
            .field("has_event_sink", &self.event_sink.is_some())
            .field(
                "external_tools",
                &self.external_tools.keys().collect::<Vec<_>>(),
            )
            .field("cwd", &self.cwd)
            .field("has_config", &self.config.is_some())
            .field("tool_mode", &self.tool_mode)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tool assembly
// ---------------------------------------------------------------------------

struct ToolAssembly {
    tools: Vec<Arc<dyn Tool>>,
    pool: McpPool,
}

struct ToolContext<'a, 's> {
    external: &'a HashMap<String, Arc<dyn Tool>>,
    cwd: Option<&'a Path>,
    config: &'a ConfigBindings,
    servers: &'a HashMap<&'s str, &'s McpServerSpec>,
}

/// One spawned MCP server: the metadata it advertised and the tools built
/// from it (each tool shares the server's transport).
struct SpawnedServer {
    metas: Vec<ToolMeta>,
    tools: Vec<Arc<dyn Tool>>,
}

fn assemble_tools(
    spec: &HarnessSpec,
    install_order: &[String],
    external: &HashMap<String, Arc<dyn Tool>>,
    cwd: Option<&Path>,
    config: &ConfigBindings,
) -> Result<ToolAssembly, BuildError> {
    let by_name: HashMap<&str, &ToolSpec> = spec
        .tools
        .iter()
        .map(|tool| (tool.name.as_str(), tool))
        .collect();
    let servers: HashMap<&str, &McpServerSpec> = spec
        .mcp_servers
        .iter()
        .map(|server| (server.name.as_str(), server))
        .collect();
    let context = ToolContext {
        external,
        cwd,
        config,
        servers: &servers,
    };

    let mut assembly = ToolAssembly {
        tools: Vec::new(),
        pool: McpPool::default(),
    };
    let mut spawned: HashMap<String, SpawnedServer> = HashMap::new();
    let mut installed_names = HashSet::new();
    for unit in install_order {
        let Some(name) = unit.strip_prefix("tool:") else {
            continue;
        };
        let tool = by_name
            .get(name)
            .copied()
            .ok_or_else(|| BuildError::UnknownToolUnit { unit: unit.clone() })?;
        let installed = assemble_one_tool(tool, &context, &mut spawned, &mut assembly.pool)?;
        for tool in installed {
            if !installed_names.insert(tool.name().to_string()) {
                return Err(BuildError::DuplicateTool {
                    name: tool.name().to_string(),
                });
            }
            assembly.tools.push(tool);
        }
    }
    Ok(assembly)
}

fn assemble_one_tool(
    tool: &ToolSpec,
    context: &ToolContext<'_, '_>,
    spawned: &mut HashMap<String, SpawnedServer>,
    pool: &mut McpPool,
) -> Result<Vec<Arc<dyn Tool>>, BuildError> {
    match tool.kind {
        ToolKind::Builtin => Ok(vec![builtin_tool(&tool.name, context.cwd)?]),
        ToolKind::External => {
            let injected = context.external.get(&tool.name).cloned().ok_or_else(|| {
                BuildError::MissingExternalTool {
                    name: tool.name.clone(),
                }
            })?;
            Ok(vec![injected])
        }
        ToolKind::Mcp => assemble_mcp_tool(tool, context, spawned, pool),
    }
}

fn builtin_tool(name: &str, cwd: Option<&Path>) -> Result<Arc<dyn Tool>, BuildError> {
    let cwd = cwd.ok_or(BuildError::MissingCwd)?.to_path_buf();
    match name {
        "read" => Ok(Arc::new(ReadTool::new(cwd))),
        "write" => Ok(Arc::new(WriteTool::new(cwd))),
        "edit" => Ok(Arc::new(EditTool::new(cwd))),
        "glob" => Ok(Arc::new(GlobTool::new(cwd))),
        "grep" => Ok(Arc::new(GrepTool::new(cwd))),
        other => Err(BuildError::UnknownBuiltin {
            name: other.to_string(),
        }),
    }
}

fn assemble_mcp_tool(
    tool: &ToolSpec,
    context: &ToolContext<'_, '_>,
    spawned: &mut HashMap<String, SpawnedServer>,
    pool: &mut McpPool,
) -> Result<Vec<Arc<dyn Tool>>, BuildError> {
    let server_name = tool
        .server
        .as_deref()
        .ok_or_else(|| BuildError::MissingMcpServer {
            tool: tool.name.clone(),
        })?;
    let connection = match spawned.entry(server_name.to_string()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            let server = context.servers.get(server_name).copied().ok_or_else(|| {
                BuildError::UnknownMcpServer {
                    tool: tool.name.clone(),
                    server: server_name.to_string(),
                }
            })?;
            let config = resolve_server_config(server, context.config)?;
            let connection = spawn_mcp_server(&config)?;
            pool.servers.push(McpServerTools {
                name: server_name.to_string(),
                tools: connection
                    .metas
                    .iter()
                    .map(|meta| meta.name.clone())
                    .collect(),
            });
            entry.insert(connection)
        }
    };

    let selected: Vec<Arc<dyn Tool>> = match &tool.remote {
        Some(remote) => {
            let wanted: HashSet<&str> = connection
                .metas
                .iter()
                .filter(|meta| &meta.remote == remote)
                .map(|meta| meta.name.as_str())
                .collect();
            connection
                .tools
                .iter()
                .filter(|candidate| wanted.contains(candidate.name()))
                .cloned()
                .collect()
        }
        None => connection.tools.clone(),
    };
    if selected.is_empty() {
        return Err(match &tool.remote {
            Some(remote) => BuildError::McpToolNotFound {
                server: server_name.to_string(),
                tool: remote.clone(),
            },
            None => BuildError::McpServerHasNoTools {
                server: server_name.to_string(),
            },
        });
    }
    Ok(selected)
}

fn resolve_server_config(
    server: &McpServerSpec,
    config: &ConfigBindings,
) -> Result<McpServerConfig, BuildError> {
    if server.from_config {
        let base =
            config
                .mcp_server(&server.name)
                .ok_or_else(|| BuildError::ConfigServerNotFound {
                    server: server.name.clone(),
                })?;
        return Ok(merge_server_config(base.clone(), server));
    }
    Ok(server_config_from_spec(server))
}

fn server_config_from_spec(server: &McpServerSpec) -> McpServerConfig {
    McpServerConfig {
        name: server.name.clone(),
        command: server.command.clone(),
        args: server.args.clone(),
        env: server
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        url: server.url.clone(),
        tool_call_timeout_ms: server.tool_call_timeout_ms,
    }
}

/// Merge a config-backed server with its spec declaration: the spec wins on
/// every field it sets, the config fills the rest.
fn merge_server_config(mut base: McpServerConfig, spec: &McpServerSpec) -> McpServerConfig {
    base.name = spec.name.clone();
    base.command = spec.command.clone().or(base.command);
    if !spec.args.is_empty() {
        base.args = spec.args.clone();
    }
    base.env.extend(
        spec.env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    base.url = spec.url.clone().or(base.url);
    base.tool_call_timeout_ms = spec.tool_call_timeout_ms.or(base.tool_call_timeout_ms);
    base
}

fn spawn_mcp_server(config: &McpServerConfig) -> Result<SpawnedServer, BuildError> {
    let signal = AtomicBool::new(false);
    let connected = match (&config.command, &config.url) {
        (Some(_), _) => Mcp::spawn(config, &signal),
        (None, Some(_)) => Mcp::connect_http(config, &signal),
        (None, None) => {
            return Err(BuildError::MissingMcpEndpoint {
                server: config.name.clone(),
            });
        }
    };
    let mcp = connected.map_err(|source| BuildError::Mcp {
        server: config.name.clone(),
        source,
    })?;
    let metas = mcp.list_tools().to_vec();
    let tools = mcp.into_tools();
    Ok(SpawnedServer { metas, tools })
}

// ---------------------------------------------------------------------------
// Prompt, hooks and skills
// ---------------------------------------------------------------------------

fn assemble_prompt(sections: &[PromptSectionSpec]) -> Option<Message> {
    if sections.is_empty() {
        return None;
    }
    let ledger: BTreeMap<String, Option<String>> = sections
        .iter()
        .map(|section| (section.name.clone(), Some(section.content.clone())))
        .collect();
    Some(Message::System { sections: ledger })
}

fn assemble_skills(specs: &[SkillSpec]) -> Result<Vec<SkillMeta>, BuildError> {
    specs.iter().map(resolve_skill).collect()
}

fn resolve_skill(spec: &SkillSpec) -> Result<SkillMeta, BuildError> {
    discover_skills(Path::new(&spec.path))
        .into_iter()
        .find(|skill| skill.name == spec.name)
        .ok_or_else(|| BuildError::SkillNotFound {
            name: spec.name.clone(),
            path: spec.path.clone(),
        })
}

type PrepareRequestFn = Arc<dyn Fn(Vec<Message>) -> Vec<Message> + Send + Sync>;
type BeforeToolCallFn = Arc<dyn Fn(&ToolRequest) -> Result<(), String> + Send + Sync>;

fn assemble_hooks(spec: &HarnessSpec, prompt: &Option<Message>) -> LoopHooks {
    let mut hooks = LoopHooks::default();

    // The prompt ledger and a declared instruction both ride the one
    // `prepare_request` seam, so they are composed in a fixed order: the
    // prompt first (it must lead the transcript), then the instruction.
    let prompt_hook = prompt.clone().map(prompt_prepare_hook);
    let instruction_hook = spec.hooks.iter().find_map(|hook| match &hook.kind {
        HookKind::InjectInstruction { text } => Some(instruction_prepare_hook(text)),
        _ => None,
    });
    hooks.prepare_request = compose_prepare_hooks(prompt_hook, instruction_hook);

    for hook in &spec.hooks {
        match &hook.kind {
            HookKind::DenyTools { tools } => {
                hooks.before_tool_call = Some(deny_tools_hook(&hook.name, tools));
            }
            HookKind::TerminateAfterToolBatch => {
                hooks.should_terminate_tool_batch = Some(Arc::new(|_, _| true));
            }
            HookKind::InjectInstruction { .. } => {}
        }
    }
    hooks
}

fn prompt_prepare_hook(prompt: Message) -> PrepareRequestFn {
    Arc::new(move |mut messages| {
        if messages.first() != Some(&prompt) {
            messages.insert(0, prompt.clone());
        }
        messages
    })
}

fn instruction_prepare_hook(text: &str) -> PrepareRequestFn {
    let instruction = Message::user_text(text);
    Arc::new(move |mut messages| {
        if !messages.contains(&instruction) {
            messages.push(instruction.clone());
        }
        messages
    })
}

fn compose_prepare_hooks(
    first: Option<PrepareRequestFn>,
    second: Option<PrepareRequestFn>,
) -> Option<PrepareRequestFn> {
    match (first, second) {
        (None, None) => None,
        (Some(hook), None) | (None, Some(hook)) => Some(hook),
        (Some(first), Some(second)) => Some(Arc::new(move |messages| second(first(messages)))),
    }
}

fn deny_tools_hook(hook_name: &str, tools: &[String]) -> BeforeToolCallFn {
    let denied: HashSet<String> = tools.iter().cloned().collect();
    let hook_name = hook_name.to_string();
    Arc::new(move |request| {
        if denied.contains(&request.name) {
            Err(format!(
                "tool `{}` is denied by hook `{hook_name}`",
                request.name
            ))
        } else {
            Ok(())
        }
    })
}
