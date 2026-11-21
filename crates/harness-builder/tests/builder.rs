//! Integration tests for the harness builder.
//!
//! Every test is self-contained: mock `Model` / `Tool` / `ToolPolicy` /
//! `EventSink` implementations replace the network and the daemon, and a
//! `tempfile::TempDir` supplies the working directory for builtin tools.
//!
//! # Coverage map
//!
//! | Test | Scenario | Acceptance |
//! |---|---|---|
//! | `profile_builds_and_runs_minimal_runtime` | minimal profile (read + prompt + mock model) | build assembles it, the loop completes one turn |
//! | `config_bindings_map_into_runtime` | config models/budget/mcp sections | every mapped field reaches the runtime |
//! | `mcp_entry_maps_to_server_config` | config MCP entry, with and without env var | fields map; unset secret is a named error |
//! | `external_tool_requires_injection` | profile declares an external tool | build fails without `with_tool` |
//! | `external_tool_injection_installs_tool` | host injects the declared tool | the injected tool is installed |
//! | `model_is_never_defaulted` | no model injected | build fails with `MissingModel` |
//! | `default_model_must_match_injected_model` | handle mismatch | build fails with `ModelMismatch` |
//! | `declared_skills_are_discovered` | a real `SKILL.md` under a temp root | the named skill is assembled |
//! | `declared_skills_register_the_skill_tool` | profile declares a skill | `skill` tool registered, skills metadata unchanged |
//! | `skill_tool_must_not_shadow_a_declared_tool` | external tool named `skill` plus a declared skill | build fails with `DuplicateTool` |
//! | `hooks_compose_prompt_and_instruction` | prompt + instruction + deny hooks | one composed prepare hook and one deny hook |
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use harness_builder::{
    BuildError, ConfigBindings, HarnessBuilder, HarnessProfile, HookEntry, HookKind, ModelEntry,
    PromptSection, SkillRef, ToolEntry, ToolKind, mcp_server_config_from_entry,
    mcp_server_entries_from_config, mcp_tool_entries, prompt_sections, skill_refs_from_roots,
};
use harness_config::{
    AgentsConfig, BudgetConfig, CURRENT_CONFIG_VERSION, Config, McpConfig, McpServerEntry,
    ModelAlias, ModelsConfig, PromptConfig, RegistryConfig, SchedulerConfig, SessionConfig,
    UpstreamConfig,
};
use harness_core::event::{EventSink, RunEvent, RunEventKind};
use harness_core::message::Message;
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
use harness_core::tool::{Tool, ToolError, ToolRequest, ToolResult, ToolRisk, ToolSchema};
use harness_core::topology::{RunSeams, Turn};
use harness_core::{LoopHooks, RunControl, StopReason};
use harness_loop::run;

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

/// A model that records every request and returns pre-recorded turns FIFO.
struct MockModel {
    info: ModelInfo,
    turns: Mutex<VecDeque<Vec<ModelEvent>>>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl MockModel {
    fn new(turns: Vec<Vec<ModelEvent>>) -> Self {
        Self {
            info: ModelInfo {
                handle: "mock".into(),
                provider: "mock".into(),
                model: "mock-1".into(),
                context_window: 8192,
                supports_tool_use: true,
                supports_streaming: true,
            },
            turns: Mutex::new(turns.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn with_handle(mut self, handle: &str) -> Self {
        self.info.handle = handle.to_string();
        self
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests
            .lock()
            .unwrap_or_else(|error| panic!("request log lock poisoned: {error}"))
            .clone()
    }
}

#[async_trait::async_trait]
impl Model for MockModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, request: &ModelRequest) -> ModelStream<'_> {
        self.requests
            .lock()
            .unwrap_or_else(|error| panic!("request log lock poisoned: {error}"))
            .push(request.clone());
        let events = self
            .turns
            .lock()
            .unwrap_or_else(|error| panic!("turn queue lock poisoned: {error}"))
            .pop_front()
            .unwrap_or_else(|| panic!("mock model turn queue exhausted"));
        Box::pin(futures::stream::iter(events))
    }
}

/// An externally implemented tool injected through `with_tool`.
struct FixedTool {
    name: String,
}

impl FixedTool {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Tool for FixedTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: self.name.clone(),
            description: format!("fixed tool {}", self.name),
            input: serde_json::json!({}),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::ReadOnly
    }

    async fn invoke(
        &self,
        _args: serde_json::Value,
        _cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            content: "ok".into(),
            is_error: false,
            trace: None,
        })
    }
}

/// A policy that allows every call — injected explicitly by each build test.
struct AllowAllPolicy;

impl ToolPolicy for AllowAllPolicy {
    fn decide(
        &self,
        _stage: PolicyStage,
        _request: &ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
        PolicyDecision {
            outcome: PolicyOutcome::Allow,
            reason: "test policy allows all".into(),
            source: "test".into(),
            context_injection: None,
            output_suffix: None,
        }
    }
}

/// Collects emitted run events for assertions.
#[derive(Default)]
struct TestSink {
    events: Mutex<Vec<RunEvent>>,
}

impl EventSink for TestSink {
    fn emit(&self, event: RunEvent) {
        self.events
            .lock()
            .unwrap_or_else(|error| panic!("test sink lock poisoned: {error}"))
            .push(event);
    }
}

impl TestSink {
    fn events(&self) -> Vec<RunEvent> {
        self.events
            .lock()
            .unwrap_or_else(|error| panic!("test sink lock poisoned: {error}"))
            .clone()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn external_tool(name: &str) -> ToolEntry {
    ToolEntry {
        name: name.into(),
        kind: ToolKind::External,
        server: None,
        remote: None,
    }
}

fn builtin_tool(name: &str) -> ToolEntry {
    ToolEntry {
        name: name.into(),
        kind: ToolKind::Builtin,
        server: None,
        remote: None,
    }
}

fn mock_model_entry() -> ModelEntry {
    ModelEntry {
        handle: "mock".into(),
        provider: "mock".into(),
        model: "mock-1".into(),
        context_window: 8192,
        supports_tool_use: true,
        supports_streaming: true,
    }
}

fn builder_for(profile: HarnessProfile, model: Arc<dyn Model>) -> HarnessBuilder {
    HarnessBuilder::new(profile)
        .with_model(model)
        .with_policy(Arc::new(AllowAllPolicy))
        .with_event_sink(Arc::new(TestSink::default()))
}

fn make_ctx() -> RunContext {
    RunContext {
        run_id: RunId("run-1".into()),
        session_id: SessionId("session-1".into()),
        task: Task {
            description: "builder test".into(),
            source: None,
            deadline: None,
        },
        limits: RunLimits::default(),
        started_at_ms: 0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Purpose: the acceptance path — a minimal profile is built and run through
/// one loop turn.
/// Scenario: one builtin `read` tool, one prompt section, one declared model,
/// a mock model that answers without tool calls.
/// Acceptance: `build` assembles the read tool and the prompt ledger; the
/// loop's first request carries the system ledger and the read schema; the
/// run completes.
#[tokio::test]
async fn profile_builds_and_runs_minimal_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;

    let mut profile = HarnessProfile::new("minimal");
    profile.tools.push(builtin_tool("read"));
    profile.prompt.push(PromptSection {
        name: "system".into(),
        content: "You are a test harness.".into(),
    });
    profile.models.push(mock_model_entry());
    profile.default_model = Some("mock".into());

    let model = Arc::new(MockModel::new(vec![vec![
        ModelEvent::TextDelta("hello".into()),
        ModelEvent::Stop(StopReason::EndTurn),
    ]]));
    let sink = Arc::new(TestSink::default());
    let runtime = HarnessBuilder::new(profile)
        .with_model(model.clone())
        .with_policy(Arc::new(AllowAllPolicy))
        .with_event_sink(sink.clone())
        .with_cwd(dir.path().to_path_buf())
        .build()?;

    assert_eq!(runtime.tools.len(), 1, "exactly one tool is assembled");
    assert_eq!(
        runtime.tools[0].name(),
        "read",
        "the builtin read tool is installed"
    );
    assert!(runtime.prompt.is_some(), "the prompt ledger is assembled");
    assert!(
        !runtime.prompt_chain.is_empty(),
        "the prompt rides the prompt-side topology"
    );
    assert!(
        runtime.mcp_pool.is_empty(),
        "no MCP server was declared, so none is connected"
    );

    let deps = Arc::new(runtime.into_loop_deps());
    let reason = run(make_ctx(), deps, RunControl::new()).await;
    assert_eq!(reason, RunStopReason::Completed, "one-turn run completes");

    let requests = model.requests();
    assert_eq!(requests.len(), 1, "the loop made exactly one model call");
    let first = &requests[0];
    assert!(
        matches!(
            first.messages.first(),
            Some(Message::System { sections }) if sections.contains_key("system")
        ),
        "the prompt ledger leads the transcript"
    );
    assert!(
        first.tools.iter().any(|schema| schema.name == "read"),
        "the read schema is advertised to the model"
    );
    assert!(
        sink.events()
            .iter()
            .any(|event| matches!(event.kind, RunEventKind::RunFinished { .. })),
        "the loop emitted RunFinished"
    );
    Ok(())
}

/// Purpose: every builder-relevant config section reaches the assembled
/// runtime through `ConfigBindings`.
/// Scenario: config declares one alias, a budget, and one MCP server.
/// Acceptance: limits, budget, aliases and the mapped MCP server match the
/// config, and the runtime carries the same limits/budget/aliases.
#[test]
fn config_bindings_map_into_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config {
        version: CURRENT_CONFIG_VERSION,
        upstream: UpstreamConfig {
            base_url: "https://example.invalid/v1".into(),
            api_key_var: "HARNESS_API_KEY".into(),
        },
        models: ModelsConfig {
            aliases: vec![ModelAlias {
                name: "fast".into(),
                model_id: "gpt-4o-mini".into(),
                upstream: None,
            }],
        },
        session: SessionConfig::default(),
        budget: BudgetConfig {
            max_input_tokens: 1234,
            max_output_tokens: 321,
            max_turns: 7,
        },
        mcp: McpConfig {
            servers: vec![McpServerEntry {
                name: "local".into(),
                command: "npx".into(),
                args: vec!["-y".into(), "server".into()],
                env_var: None,
            }],
        },
        registry: RegistryConfig::default(),
        scheduler: SchedulerConfig::default(),
        agents: AgentsConfig::default(),
        prompt: PromptConfig::default(),
    };

    let bindings = ConfigBindings::from_config(&config)?;
    assert_eq!(
        bindings.limits,
        RunLimits {
            max_turns: 7,
            max_input_tokens: 1234,
            max_output_tokens: 321,
        },
        "budget limits map field by field"
    );
    assert_eq!(
        bindings.budget.available_tokens, 1234,
        "available budget comes from max_input_tokens"
    );
    assert_eq!(
        bindings.aliases.resolve("fast"),
        "gpt-4o-mini",
        "model aliases map into the registry"
    );
    let Some(server) = bindings.mcp_server("local") else {
        panic!("the local MCP server must be mapped");
    };
    assert_eq!(server.command.as_deref(), Some("npx"), "MCP command maps");
    assert_eq!(
        server.args,
        vec!["-y".to_string(), "server".to_string()],
        "MCP args map"
    );
    assert!(server.env.is_empty(), "no env var is configured");

    let mut spec = HarnessProfile::new("configured");
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());
    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .with_config_bindings(bindings)
        .build()?;
    assert_eq!(
        runtime.limits.max_turns, 7,
        "runtime limits come from config"
    );
    assert_eq!(
        runtime.budget.available_tokens, 1234,
        "runtime budget comes from config"
    );
    assert_eq!(
        runtime.aliases.resolve("fast"),
        "gpt-4o-mini",
        "runtime aliases come from config"
    );
    Ok(())
}

/// Purpose: a config MCP entry maps to the MCP client config type, and a
/// configured-but-unset secret variable is a named error rather than a
/// silently degraded server.
/// Scenario: one entry without `env_var`, one entry naming an unset variable.
/// Acceptance: name/command/args/url/env map; the second returns
/// `MissingEnvVar` naming the server and variable.
#[test]
fn mcp_entry_maps_to_server_config() -> Result<(), Box<dyn std::error::Error>> {
    let entry = McpServerEntry {
        name: "local".into(),
        command: "npx".into(),
        args: vec!["-y".into(), "server".into()],
        env_var: None,
    };
    let mapped = mcp_server_config_from_entry(&entry)?;
    assert_eq!(mapped.name, "local", "server name maps");
    assert_eq!(mapped.command.as_deref(), Some("npx"), "command maps");
    assert_eq!(
        mapped.args,
        vec!["-y".to_string(), "server".to_string()],
        "args map"
    );
    assert!(mapped.env.is_empty(), "no secret is injected");
    assert_eq!(mapped.url, None, "config entries are stdio-only");
    assert_eq!(mapped.tool_call_timeout_ms, None, "no timeout override");

    let secret = McpServerEntry {
        name: "secret".into(),
        command: "run".into(),
        args: Vec::new(),
        env_var: Some("HARNESS_BUILDER_TEST_UNSET_SECRET".into()),
    };
    let error = mcp_server_config_from_entry(&secret).err();
    assert!(
        matches!(
            error,
            Some(BuildError::MissingEnvVar { ref server, ref name })
                if server == "secret" && name == "HARNESS_BUILDER_TEST_UNSET_SECRET"
        ),
        "an unset secret is a named build error: {error:?}"
    );
    Ok(())
}

/// Purpose: an external tool is installed only when the host injects it.
/// Scenario: the spec declares `custom` but the builder registers nothing.
/// Acceptance: `build` returns `MissingExternalTool { name: "custom" }`.
#[test]
fn external_tool_requires_injection() {
    let mut spec = HarnessProfile::new("external");
    spec.tools.push(external_tool("custom"));

    let error = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .build()
        .err();
    assert!(
        matches!(error, Some(BuildError::MissingExternalTool { ref name }) if name == "custom"),
        "the missing external tool is named: {error:?}"
    );
}
/// Purpose: tools install in profile declaration order — the catalog's
/// `names()` promises a stable order for prefix caching, and a HashMap
/// iteration would randomize it across daemon starts.
/// Acceptance: `runtime.tools` follows declaration order in both.
#[test]
fn tools_install_in_declaration_order() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;

    let mut first = HarnessProfile::new("order-a");
    first.tools.push(builtin_tool("glob"));
    first.tools.push(builtin_tool("read"));
    first.models.push(mock_model_entry());
    first.default_model = Some("mock".into());
    let runtime = builder_for(first, Arc::new(MockModel::new(Vec::new())))
        .with_cwd(dir.path().to_path_buf())
        .build()?;
    let names: Vec<&str> = runtime.tools.iter().map(|tool| tool.name()).collect();
    assert_eq!(names, vec!["glob", "read"], "declaration order is kept");

    let mut second = HarnessProfile::new("order-b");
    second.tools.push(builtin_tool("read"));
    second.tools.push(builtin_tool("glob"));
    second.models.push(mock_model_entry());
    second.default_model = Some("mock".into());
    let runtime = builder_for(second, Arc::new(MockModel::new(Vec::new())))
        .with_cwd(dir.path().to_path_buf())
        .build()?;
    let names: Vec<&str> = runtime.tools.iter().map(|tool| tool.name()).collect();
    assert_eq!(names, vec!["read", "glob"], "reverse input reverses output");
    Ok(())
}

/// Purpose: a profile that declares the same tool name twice must fail at
/// build time, not silently keep one entry.
/// Scenario: two builtin `read` entries.
/// Acceptance: `build` fails with `DuplicateTool`.
#[test]
fn duplicate_tool_entry_fails_build() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;

    let mut spec = HarnessProfile::new("dup-tool");
    spec.tools.push(builtin_tool("read"));
    spec.tools.push(builtin_tool("read"));
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let error = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .with_cwd(dir.path().to_path_buf())
        .build()
        .err();
    assert!(
        matches!(error, Some(BuildError::DuplicateTool { ref name }) if name == "read"),
        "the duplicate tool entry is rejected: {error:?}"
    );
    Ok(())
}

/// Purpose: at most one hook may occupy a loop seam; two competing hooks
/// fail at build time instead of the earlier one being silently overwritten.
/// Scenario: two `DenyTools` hooks on the `before_tool_call` seam.
/// Acceptance: `build` fails with `DuplicateHookKind` naming the seam.
#[test]
fn duplicate_hook_seam_fails_build() {
    let mut spec = HarnessProfile::new("dup-hook");
    spec.hooks.push(HookEntry {
        name: "first".into(),
        kind: HookKind::DenyTools {
            tools: vec!["rm".into()],
        },
    });
    spec.hooks.push(HookEntry {
        name: "second".into(),
        kind: HookKind::DenyTools {
            tools: vec!["sudo".into()],
        },
    });
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let error = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .build()
        .err();
    assert!(
        matches!(error, Some(BuildError::DuplicateHookKind { seam }) if seam == "before_tool_call"),
        "two hooks on one seam are rejected: {error:?}"
    );
}
/// Acceptance: `build` succeeds and the runtime carries the injected tool.
#[test]
fn external_tool_injection_installs_tool() -> Result<(), Box<dyn std::error::Error>> {
    let mut spec = HarnessProfile::new("injected");
    spec.tools.push(external_tool("custom"));
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .with_tool(Arc::new(FixedTool::new("custom")))
        .build()?;
    assert_eq!(runtime.tools.len(), 1, "the injected tool is installed");
    assert_eq!(
        runtime.tools[0].name(),
        "custom",
        "the injected tool is used"
    );
    Ok(())
}

/// Purpose: the builder never invents a model adapter.
/// Scenario: policy and event sink injected, no model.
/// Acceptance: `build` returns `MissingModel`.
#[test]
fn model_is_never_defaulted() {
    let spec = HarnessProfile::new("no-model");
    let error = HarnessBuilder::new(spec)
        .with_policy(Arc::new(AllowAllPolicy))
        .with_event_sink(Arc::new(TestSink::default()))
        .build()
        .err();
    assert!(
        matches!(error, Some(BuildError::MissingModel)),
        "a model must be injected: {error:?}"
    );
}

/// Purpose: `default_model` is a contract with the injected adapter.
/// Scenario: the spec declares `mock` but the injected model reports `other`.
/// Acceptance: `build` returns `ModelMismatch` naming both handles.
#[test]
fn default_model_must_match_injected_model() {
    let mut spec = HarnessProfile::new("mismatch");
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let model = Arc::new(MockModel::new(Vec::new()).with_handle("other"));
    let error = builder_for(spec, model).build().err();
    assert!(
        matches!(
            error,
            Some(BuildError::ModelMismatch {
                ref expected,
                ref actual,
            }) if expected == "mock" && actual == "other"
        ),
        "the handle mismatch is reported: {error:?}"
    );
}

/// Purpose: a declared skill is resolved from its skills root and installed.
/// Scenario: a temp root contains `greet/SKILL.md`.
/// Acceptance: the runtime carries exactly that skill and its description.
#[test]
fn declared_skills_are_discovered() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let skill_dir = dir.path().join("greet");
    std::fs::create_dir_all(&skill_dir)?;
    std::fs::write(skill_dir.join("SKILL.md"), "# Greet\nA greeting skill.\n")?;

    let mut spec = HarnessProfile::new("skills");
    spec.skills.push(SkillRef {
        name: "greet".into(),
        path: dir.path().to_string_lossy().into_owned(),
    });
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new()))).build()?;
    assert_eq!(runtime.skills.len(), 1, "one skill is assembled");
    assert_eq!(
        runtime.skills[0].name, "greet",
        "the skill name is resolved"
    );
    assert_eq!(
        runtime.skills[0].description, "A greeting skill.",
        "the description comes from SKILL.md"
    );
    Ok(())
}
/// Purpose: a spec that declares skills also registers the `skill` tool.
/// Scenario: a temp root holds demo/SKILL.md; the spec declares the demo
/// skill and builds with a mock model, policy, and sink.
/// Acceptance: `runtime.tools` contains exactly one tool named `skill`, and
/// `runtime.skills` still carries exactly the declared skill metadata.
#[test]
fn declared_skills_register_the_skill_tool() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let skill_dir = dir.path().join("demo");
    std::fs::create_dir_all(&skill_dir)?;
    std::fs::write(skill_dir.join("SKILL.md"), "# Demo\n\nA demo skill.\n")?;

    let mut spec = HarnessProfile::new("skill-activation");
    spec.skills.push(SkillRef {
        name: "demo".into(),
        path: dir.path().to_string_lossy().into_owned(),
    });
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new()))).build()?;

    let skill_tools: Vec<_> = runtime
        .tools
        .iter()
        .filter(|tool| tool.name() == "skill")
        .collect();
    assert_eq!(
        skill_tools.len(),
        1,
        "exactly one `skill` tool is registered: {:?}",
        runtime
            .tools
            .iter()
            .map(|tool| tool.name())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        runtime.skills.len(),
        1,
        "the skills metadata list is unchanged"
    );
    assert_eq!(runtime.skills[0].name, "demo", "the declared skill is kept");
    Ok(())
}

/// Purpose: the auto-registered `skill` tool must not shadow a declared tool.
/// Scenario: the spec declares an external tool named `skill` (host-injected)
/// and declares a skill; the name is taken.
/// Acceptance: `build` fails with `BuildError::DuplicateTool { name: "skill" }`.
#[test]
fn skill_tool_must_not_shadow_a_declared_tool() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let skill_dir = dir.path().join("demo");
    std::fs::create_dir_all(&skill_dir)?;
    std::fs::write(skill_dir.join("SKILL.md"), "# Demo\n\nA demo skill.\n")?;

    let mut spec = HarnessProfile::new("skill-collision");
    spec.tools.push(external_tool("skill"));
    spec.skills.push(SkillRef {
        name: "demo".into(),
        path: dir.path().to_string_lossy().into_owned(),
    });
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let error = HarnessBuilder::new(spec)
        .with_model(Arc::new(MockModel::new(Vec::new())))
        .with_policy(Arc::new(AllowAllPolicy))
        .with_event_sink(Arc::new(TestSink::default()))
        .with_tool(Arc::new(FixedTool::new("skill")))
        .build()
        .err()
        .ok_or_else(|| "build must fail on the `skill` tool collision".to_string())?;
    assert!(
        matches!(error, BuildError::DuplicateTool { ref name } if name == "skill"),
        "expected DuplicateTool for `skill`, got {error:?}"
    );
    Ok(())
}

/// Purpose: prompt injection and a declared instruction share the single
/// `prepare_request` seam, and a deny hook blocks only its listed tools.
/// Scenario: one prompt section, one `InjectInstruction` hook, one
/// `DenyTools` hook.
/// Acceptance: the prompt topology's one pass puts the system ledger first
/// and the instruction last; the deny hook rejects `rm` and allows `read`.
#[tokio::test]
async fn hooks_compose_prompt_and_instruction() -> Result<(), Box<dyn std::error::Error>> {
    let mut spec = HarnessProfile::new("hooks");
    spec.prompt.push(PromptSection {
        name: "system".into(),
        content: "system prompt".into(),
    });
    spec.hooks.push(HookEntry {
        name: "steer".into(),
        kind: HookKind::InjectInstruction {
            text: "follow the rules".into(),
        },
    });
    spec.hooks.push(HookEntry {
        name: "guard".into(),
        kind: HookKind::DenyTools {
            tools: vec!["rm".into()],
        },
    });
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new()))).build()?;

    assert!(
        !runtime.prompt_chain.is_empty(),
        "the prompt topology carries the system ledger and instruction nodes"
    );

    // Drive one machining pass and read the ledger back out — the order the
    // model will see: system ledger first, instruction after.
    let mut turn = Turn::new(
        Arc::new(MockModel::new(Vec::new())),
        RunControl::new(),
        make_ctx(),
        Vec::new(),
        RunSeams {
            policy: Arc::new(AllowAllPolicy),
            approval_broker: None,
            hooks: LoopHooks::default(),
            event_sink: Arc::new(TestSink::default()),
            transcript_sink: None,
        },
    );
    runtime.prompt_chain.step(&mut turn).await;
    let messages = turn.ledger;
    assert!(
        matches!(
            messages.first(),
            Some(Message::System { sections })
                if sections.get("system") == Some(&Some("system prompt".to_string()))
        ),
        "the prompt ledger is inserted first"
    );
    assert!(
        messages
            .iter()
            .any(|message| *message == Message::user_text("follow the rules")),
        "the instruction is appended"
    );

    let deny = runtime
        .hooks
        .before_tool_call
        .as_ref()
        .ok_or_else(|| std::io::Error::other("deny hook missing"))?;
    let denied = ToolRequest {
        call_id: "call-1".into(),
        name: "rm".into(),
        args: serde_json::json!({}),
        run_id: RunId("run-1".into()),
        session_id: SessionId("session-1".into()),
        turn_idx: 0,
    };
    assert!(deny(&denied).is_err(), "the denied tool is blocked");
    let allowed = ToolRequest {
        name: "read".into(),
        ..denied
    };
    assert!(deny(&allowed).is_ok(), "other tools pass the hook");
    Ok(())
}

// ---------------------------------------------------------------------------
// Config → spec mapping
// ---------------------------------------------------------------------------

/// Purpose: config prompt sections map one-to-one into spec entries.
/// Scenario: two named sections; the mapping is order-preserving.
/// Acceptance: the spec entries carry the config's names and content.
#[test]
fn prompt_sections_map_in_order() {
    let sections = prompt_sections(&[
        harness_config::PromptSection {
            name: "identity".into(),
            content: "You are harness.".into(),
        },
        harness_config::PromptSection {
            name: "rules".into(),
            content: "Be terse.".into(),
        },
    ]);
    assert_eq!(sections.len(), 2);
    assert_eq!(sections[0].name, "identity");
    assert_eq!(sections[0].content, "You are harness.");
    assert_eq!(sections[1].name, "rules");
    assert_eq!(prompt_sections(&[]), Vec::new());
}

/// Purpose: config MCP entries map into server declarations plus one tool
/// unit per server that installs the server's whole tool set.
/// Scenario: one stdio server entry.
/// Acceptance: the server spec defers to config (`from_config`), and the
/// tool spec selects that server with no remote filter.
#[test]
fn mcp_specs_map_from_entries() {
    let entries = vec![McpServerEntry {
        name: "docs".into(),
        command: "npx".into(),
        args: vec!["-y".into(), "docs-server".into()],
        env_var: None,
    }];

    let servers = mcp_server_entries_from_config(&entries);
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].name, "docs");
    assert!(
        servers[0].from_config,
        "resolution stays in config bindings"
    );

    let tools = mcp_tool_entries(&entries);
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].kind, ToolKind::Mcp);
    assert_eq!(tools[0].server.as_deref(), Some("docs"));
    assert_eq!(tools[0].remote, None, "no remote filter installs all tools");
}

/// Purpose: skill roots are scanned and mapped one spec per discovered skill.
/// Scenario: two roots, the second repeating a name from the first.
/// Acceptance: first-root skills win, no duplicate names, and an unreadable
/// root contributes nothing instead of failing startup.
#[test]
fn skill_specs_from_roots_scan_and_dedupe() -> Result<(), Box<dyn std::error::Error>> {
    let first = tempfile::TempDir::new()?;
    for name in ["alpha", "beta"] {
        let dir = first.path().join(name);
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("SKILL.md"), format!("# {name}\nFirst root.\n"))?;
    }
    let second = tempfile::TempDir::new()?;
    let dir = second.path().join("alpha");
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("SKILL.md"), "# alpha\nSecond root.\n")?;
    let missing = tempfile::TempDir::new()?
        .path()
        .join("does-not-exist")
        .display()
        .to_string();

    let specs = skill_refs_from_roots(&[
        first.path().display().to_string(),
        second.path().display().to_string(),
        missing,
    ]);
    let names: Vec<&str> = specs.iter().map(|spec| spec.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["alpha", "beta"],
        "first root wins, dedup applied"
    );
    assert_eq!(specs[0].path, first.path().display().to_string());
    Ok(())
}

/// Purpose: prompt and skill config sections reach the assembled runtime.
/// Scenario: a config with two prompt sections and one skills root holding
/// one skill, built through the full builder.
/// Acceptance: the runtime carries the prompt ledger with both sections, the
/// skill metadata, and the auto-registered `skill` tool.
#[test]
fn config_prompt_and_skills_reach_the_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;
    let skill_dir = dir.path().join("greet");
    std::fs::create_dir_all(&skill_dir)?;
    std::fs::write(skill_dir.join("SKILL.md"), "# Greet\nA greeting skill.\n")?;

    let config = Config {
        version: CURRENT_CONFIG_VERSION,
        upstream: UpstreamConfig {
            base_url: "https://example.invalid/v1".into(),
            api_key_var: "HARNESS_API_KEY".into(),
        },
        models: ModelsConfig::default(),
        session: SessionConfig::default(),
        budget: BudgetConfig::default(),
        mcp: McpConfig::default(),
        registry: RegistryConfig {
            hooks: Vec::new(),
            skills: vec![dir.path().display().to_string()],
        },
        scheduler: SchedulerConfig::default(),
        agents: AgentsConfig::default(),
        prompt: PromptConfig {
            sections: vec![
                harness_config::PromptSection {
                    name: "identity".into(),
                    content: "You are harness.".into(),
                },
                harness_config::PromptSection {
                    name: "rules".into(),
                    content: "Be terse.".into(),
                },
            ],
        },
    };

    let mut spec = HarnessProfile::new("config-wired");
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());
    spec.prompt = prompt_sections(&config.prompt.sections);
    spec.skills = skill_refs_from_roots(&config.registry.skills);

    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .with_cwd(dir.path().to_path_buf())
        .build()?;

    let Some(prompt) = runtime.prompt.as_ref() else {
        panic!("configured prompt sections must assemble a system message");
    };
    let Message::System { sections } = prompt else {
        panic!("prompt should be a system message");
    };
    assert_eq!(
        sections.get("identity").map(|c| c.as_deref()),
        Some(Some("You are harness."))
    );
    assert_eq!(
        sections.get("rules").map(|c| c.as_deref()),
        Some(Some("Be terse."))
    );
    assert_eq!(
        runtime
            .skills
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>(),
        vec!["greet"],
        "the skills root fed the spec"
    );
    assert!(
        runtime.tools.iter().any(|tool| tool.name() == "skill"),
        "declaring skills registers the skill tool"
    );
    Ok(())
}

/// Purpose: the injected compactor survives assembly and reaches the loop
/// dependencies, but only when the spec declares compaction stages — without
/// stages the builder records no plan and drops the compactor.
/// Scenario: the default ladder declared in the spec plus a structural
/// compactor built from the same ladder.
/// Acceptance: `loop_deps().compactor` is `Some`, and the plan's stages match
/// the ladder.
#[test]
fn compactor_injection_reaches_loop_deps() -> Result<(), Box<dyn std::error::Error>> {
    let ladder = harness_context::default_stages();
    let mut spec = HarnessProfile::new("compacted");
    spec.compaction.stages = ladder
        .iter()
        .map(|stage| harness_builder::CompactionStage {
            threshold: stage.threshold,
            target_ratio: stage.target_ratio,
            label: stage.label.clone(),
        })
        .collect();
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let compactor = Arc::new(harness_context::DefaultCompactor::with_stages(
        harness_context::default_stages(),
    ));
    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .with_compactor(Arc::clone(&compactor) as Arc<dyn harness_context::Compactor>)
        .build()?;

    let plan = runtime
        .compaction
        .as_ref()
        .ok_or("a declared ladder must record a compaction plan")?;
    assert_eq!(plan.stages.len(), 5, "the default ladder has five stages");
    assert!(
        plan.compactor.is_some(),
        "the injected compactor is recorded"
    );
    assert!(
        runtime.prompt_chain.names().contains(&"compact"),
        "the compactor rides the prompt chain as the compaction processor"
    );
    Ok(())
}

/// Purpose: without declared stages the builder records no compaction plan
/// even when a compactor was injected — the explicit-injection contract cuts
/// both ways.
/// Scenario: an empty ladder plus an injected compactor.
/// Acceptance: `runtime.compaction` is `None` and `loop_deps().compactor` is
/// `None`.
#[test]
fn compactor_without_stages_is_dropped() -> Result<(), Box<dyn std::error::Error>> {
    let mut spec = HarnessProfile::new("no-ladder");
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let compactor: Arc<dyn harness_context::Compactor> =
        Arc::new(harness_context::DefaultCompactor::new());
    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .with_compactor(compactor)
        .build()?;

    assert!(runtime.compaction.is_none(), "no stages, no plan");
    assert!(runtime.prompt_chain.is_empty(), "no stages, no gate node");
    Ok(())
}
/// Purpose: `with_processor` is the host's extension surface — a custom node
/// rides the prompt topology after the spec's own nodes.
/// Scenario: a spec with one prompt section, plus a host node that appends
/// a marker user message before the model call.
/// Acceptance: the built prompt topology carries the custom node after the
/// system ledger, and one `step` pass leaves both messages in the ledger.
#[tokio::test]
async fn with_processor_rides_the_prompt_chain() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;

    use async_trait::async_trait;
    use harness_core::topology::{Processor, Route};

    struct MarkerNode;

    #[async_trait]
    impl Processor for MarkerNode {
        fn name(&self) -> &'static str {
            "marker"
        }

        async fn process(&self, turn: &mut Turn) -> Route {
            turn.ledger.push(Message::user_text("host node ran"));
            Route::Next
        }
    }

    let mut spec = HarnessProfile::new("custom-node");
    spec.prompt.push(PromptSection {
        name: "system".into(),
        content: "system prompt".into(),
    });
    spec.models.push(mock_model_entry());
    spec.default_model = Some("mock".into());

    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .with_processor(Arc::new(MarkerNode))
        .build()?;

    assert_eq!(
        runtime.prompt_chain.len(),
        2,
        "system ledger node + the host node"
    );
    let mut turn = Turn::new(
        Arc::new(MockModel::new(Vec::new())),
        RunControl::new(),
        make_ctx(),
        Vec::new(),
        RunSeams {
            policy: Arc::new(AllowAllPolicy),
            approval_broker: None,
            hooks: LoopHooks::default(),
            event_sink: Arc::new(TestSink::default()),
            transcript_sink: None,
        },
    );
    runtime.prompt_chain.step(&mut turn).await;
    let messages = turn.ledger;
    assert!(
        matches!(
            messages.first(),
            Some(Message::System { sections }) if sections.contains_key("system")
        ),
        "the spec's system ledger still leads"
    );
    assert!(
        messages
            .iter()
            .any(|message| *message == Message::user_text("host node ran")),
        "the host node ran after the spec's nodes"
    );
    Ok(())
}
