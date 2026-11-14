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
//! | `spec_builds_and_explains_minimal_runtime` | minimal spec (read + prompt + mock model) | explain renders it, build assembles it, the loop completes one turn |
//! | `conflicts_fail_build` | unit conflicts with a provided capability | analysis and build both reject it |
//! | `missing_requirement_fails_build` | unit requires an unprovided capability | analysis and build both reject it |
//! | `dependency_cycles_are_rejected` | two units require each other's capability | analysis reports a cycle |
//! | `install_order_is_topological` | dependent declared before its provider | provider installs first |
//! | `config_bindings_map_into_runtime` | config models/budget/mcp sections | every mapped field reaches the runtime |
//! | `mcp_entry_maps_to_server_config` | config MCP entry, with and without env var | fields map; unset secret is a named error |
//! | `external_tool_requires_injection` | spec declares an external tool | build fails without `with_tool` |
//! | `external_tool_injection_installs_tool` | host injects the declared tool | the injected tool is installed |
//! | `model_is_never_defaulted` | no model injected | build fails with `MissingModel` |
//! | `default_model_must_match_injected_model` | handle mismatch | build fails with `ModelMismatch` |
//! | `declared_skills_are_discovered` | a real `SKILL.md` under a temp root | the named skill is assembled |
//! | `declared_skills_register_the_skill_tool` | spec declares a skill | `skill` tool registered, skills metadata unchanged |
//! | `skill_tool_must_not_shadow_a_declared_tool` | external tool named `skill` plus a declared skill | build fails with `DuplicateTool` |
//! | `hooks_compose_prompt_and_instruction` | prompt + instruction + deny hooks | one composed prepare hook and one deny hook |
//! | `spec_validation_is_standalone` | invalid builtin / duplicate hook seam / bad threshold | `validate()` rejects each without config |

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use harness_builder::{
    AnalyzeError, BuildError, ConfigBindings, HarnessBuilder, HarnessSpec, HookKind, HookSpec,
    ModelSpec, PromptSectionSpec, SkillSpec, SpecError, ToolKind, ToolSpec, UnitManifest, analyze,
    mcp_server_config_from_entry,
};
use harness_config::{
    AgentsConfig, BudgetConfig, CURRENT_CONFIG_VERSION, Config, McpConfig, McpServerEntry,
    ModelAlias, ModelsConfig, RegistryConfig, SchedulerConfig, SessionConfig, UpstreamConfig,
};
use harness_core::event::{EventSink, RunEvent, RunEventKind};
use harness_core::message::Message;
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
use harness_core::tool::{Tool, ToolError, ToolRequest, ToolResult, ToolRisk, ToolSchema};
use harness_core::{RunControl, StopReason};
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

fn manifest(provides: &[&str], requires: &[&str], conflicts: &[&str]) -> UnitManifest {
    UnitManifest {
        provides: provides.iter().map(|name| name.to_string()).collect(),
        requires: requires.iter().map(|name| name.to_string()).collect(),
        conflicts: conflicts.iter().map(|name| name.to_string()).collect(),
    }
}

fn external_tool(name: &str, provides: &[&str], requires: &[&str], conflicts: &[&str]) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        kind: ToolKind::External,
        server: None,
        remote: None,
        manifest: manifest(provides, requires, conflicts),
    }
}

fn builtin_tool(name: &str) -> ToolSpec {
    ToolSpec {
        name: name.into(),
        kind: ToolKind::Builtin,
        server: None,
        remote: None,
        manifest: UnitManifest::default(),
    }
}

fn mock_model_spec() -> ModelSpec {
    ModelSpec {
        handle: "mock".into(),
        provider: "mock".into(),
        model: "mock-1".into(),
        context_window: 8192,
        supports_tool_use: true,
        supports_streaming: true,
    }
}

fn builder_for(spec: HarnessSpec, model: Arc<dyn Model>) -> HarnessBuilder {
    HarnessBuilder::new(spec)
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

/// Purpose: the acceptance path — a minimal spec is validated, explained,
/// built and run through one loop turn.
/// Scenario: one builtin `read` tool, one prompt section, one declared model,
/// a mock model that answers without tool calls.
/// Acceptance: `explain` names the spec/units and reports `validation: ok`;
/// `build` assembles the read tool and the prompt ledger; the loop's first
/// request carries the system ledger and the read schema; the run completes.
#[tokio::test]
async fn spec_builds_and_explains_minimal_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::TempDir::new()?;

    let mut spec = HarnessSpec::new("minimal");
    spec.tools.push(builtin_tool("read"));
    spec.prompt_sections.push(PromptSectionSpec {
        name: "system".into(),
        content: "You are a test harness.".into(),
        manifest: UnitManifest::default(),
    });
    spec.models.push(mock_model_spec());
    spec.default_model = Some("mock".into());

    spec.validate()?;

    let text = spec.explain();
    assert!(
        text.contains("harness spec `minimal`"),
        "explain names the spec: {text}"
    );
    assert!(text.contains("tool:read"), "explain lists the tool: {text}");
    assert!(
        text.contains("prompt:system"),
        "explain lists the prompt section: {text}"
    );
    assert!(
        text.contains("validation: ok"),
        "explain reports validation: {text}"
    );
    assert!(
        text.contains("install order (2 units)"),
        "explain prints the install order: {text}"
    );

    let model = Arc::new(MockModel::new(vec![vec![
        ModelEvent::TextDelta("hello".into()),
        ModelEvent::Stop(StopReason::EndTurn),
    ]]));
    let sink = Arc::new(TestSink::default());
    let runtime = HarnessBuilder::new(spec)
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
        runtime.hooks.prepare_request.is_some(),
        "the prompt rides the prepare_request hook"
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

/// Purpose: a conflict declared by one unit against another unit's capability
/// is a build failure.
/// Scenario: `alpha` provides `net`, `beta` conflicts with `net`.
/// Acceptance: `analyze` returns `Conflict` naming beta/net/alpha, and
/// `build` surfaces it as `BuildError::Analyze`.
#[test]
fn conflicts_fail_build() {
    let mut spec = HarnessSpec::new("conflict");
    spec.tools.push(external_tool("alpha", &["net"], &[], &[]));
    spec.tools.push(external_tool("beta", &[], &[], &["net"]));

    let analysis = analyze(&spec);
    assert!(
        matches!(
            analysis,
            Err(AnalyzeError::Conflict {
                ref unit,
                ref capability,
                ref provider,
            }) if unit == "tool:beta" && capability == "net" && provider == "tool:alpha"
        ),
        "conflict names both units: {analysis:?}"
    );

    let error = HarnessBuilder::new(spec).build().err();
    assert!(
        matches!(
            error,
            Some(BuildError::Analyze(AnalyzeError::Conflict { .. }))
        ),
        "build rejects the conflict: {error:?}"
    );
}

/// Purpose: a required capability with no provider is a build failure.
/// Scenario: a tool requires `missing.cap`; no unit provides it.
/// Acceptance: `analyze` returns `MissingRequirement`, and `build` surfaces
/// it as `BuildError::Analyze`.
#[test]
fn missing_requirement_fails_build() {
    let mut spec = HarnessSpec::new("missing");
    spec.tools
        .push(external_tool("needy", &[], &["missing.cap"], &[]));

    let analysis = analyze(&spec);
    assert!(
        matches!(
            analysis,
            Err(AnalyzeError::MissingRequirement {
                ref unit,
                ref capability,
            }) if unit == "tool:needy" && capability == "missing.cap"
        ),
        "missing provider is named: {analysis:?}"
    );

    let error = HarnessBuilder::new(spec).build().err();
    assert!(
        matches!(
            error,
            Some(BuildError::Analyze(AnalyzeError::MissingRequirement { .. }))
        ),
        "build rejects the missing requirement: {error:?}"
    );
}

/// Purpose: two units requiring each other's capability form a cycle.
/// Scenario: `prompt:a` provides `a.cap` and requires `b.cap`; `prompt:b`
/// provides `b.cap` and requires `a.cap`.
/// Acceptance: `analyze` returns `Cycle` listing both units.
#[test]
fn dependency_cycles_are_rejected() {
    let mut spec = HarnessSpec::new("cycle");
    spec.prompt_sections.push(PromptSectionSpec {
        name: "a".into(),
        content: "a".into(),
        manifest: manifest(&["a.cap"], &["b.cap"], &[]),
    });
    spec.prompt_sections.push(PromptSectionSpec {
        name: "b".into(),
        content: "b".into(),
        manifest: manifest(&["b.cap"], &["a.cap"], &[]),
    });

    let error = analyze(&spec).err();
    assert!(
        matches!(error, Some(AnalyzeError::Cycle { ref units }) if units.len() == 2),
        "cycle lists the blocked units: {error:?}"
    );
}

/// Purpose: the install order is topological, not declaration order.
/// Scenario: a hook that requires `base.prompt` is declared before the prompt
/// section that provides it.
/// Acceptance: the prompt section is ordered before the hook.
#[test]
fn install_order_is_topological() {
    let mut spec = HarnessSpec::new("order");
    spec.hooks.push(HookSpec {
        name: "guard".into(),
        kind: HookKind::TerminateAfterToolBatch,
        manifest: manifest(&[], &["base.prompt"], &[]),
    });
    spec.prompt_sections.push(PromptSectionSpec {
        name: "base".into(),
        content: "base".into(),
        manifest: manifest(&["base.prompt"], &[], &[]),
    });

    let analysis = analyze(&spec).unwrap_or_else(|error| panic!("analysis failed: {error}"));
    assert_eq!(
        analysis.install_order,
        vec!["prompt:base".to_string(), "hook:guard".to_string()],
        "the provider installs before its dependent"
    );
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

    let mut spec = HarnessSpec::new("configured");
    spec.models.push(mock_model_spec());
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
    let mut spec = HarnessSpec::new("external");
    spec.tools.push(external_tool("custom", &[], &[], &[]));

    let error = builder_for(spec, Arc::new(MockModel::new(Vec::new())))
        .build()
        .err();
    assert!(
        matches!(error, Some(BuildError::MissingExternalTool { ref name }) if name == "custom"),
        "the missing external tool is named: {error:?}"
    );
}

/// Purpose: an external tool declared by the spec is installed only when the
/// host injects an implementation with the same name.
/// Scenario: the spec declares `custom`; the builder registers a `FixedTool`
/// named `custom`.
/// Acceptance: `build` succeeds and the runtime carries the injected tool.
#[test]
fn external_tool_injection_installs_tool() -> Result<(), Box<dyn std::error::Error>> {
    let mut spec = HarnessSpec::new("injected");
    spec.tools.push(external_tool("custom", &[], &[], &[]));
    spec.models.push(mock_model_spec());
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
    let spec = HarnessSpec::new("no-model");
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
    let mut spec = HarnessSpec::new("mismatch");
    spec.models.push(mock_model_spec());
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

    let mut spec = HarnessSpec::new("skills");
    spec.skills.push(SkillSpec {
        name: "greet".into(),
        path: dir.path().to_string_lossy().into_owned(),
        manifest: UnitManifest::default(),
    });
    spec.models.push(mock_model_spec());
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

    let mut spec = HarnessSpec::new("skill-activation");
    spec.skills.push(SkillSpec {
        name: "demo".into(),
        path: dir.path().to_string_lossy().into_owned(),
        manifest: UnitManifest::default(),
    });
    spec.models.push(mock_model_spec());
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

    let mut spec = HarnessSpec::new("skill-collision");
    spec.tools.push(external_tool("skill", &[], &[], &[]));
    spec.skills.push(SkillSpec {
        name: "demo".into(),
        path: dir.path().to_string_lossy().into_owned(),
        manifest: UnitManifest::default(),
    });
    spec.models.push(mock_model_spec());
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
/// Acceptance: the composed prepare hook puts the system ledger first and
/// the instruction last; the deny hook rejects `rm` and allows `read`.
#[test]
fn hooks_compose_prompt_and_instruction() -> Result<(), Box<dyn std::error::Error>> {
    let mut spec = HarnessSpec::new("hooks");
    spec.prompt_sections.push(PromptSectionSpec {
        name: "system".into(),
        content: "system prompt".into(),
        manifest: UnitManifest::default(),
    });
    spec.hooks.push(HookSpec {
        name: "steer".into(),
        kind: HookKind::InjectInstruction {
            text: "follow the rules".into(),
        },
        manifest: UnitManifest::default(),
    });
    spec.hooks.push(HookSpec {
        name: "guard".into(),
        kind: HookKind::DenyTools {
            tools: vec!["rm".into()],
        },
        manifest: UnitManifest::default(),
    });
    spec.models.push(mock_model_spec());
    spec.default_model = Some("mock".into());

    let runtime = builder_for(spec, Arc::new(MockModel::new(Vec::new()))).build()?;

    let prepare = runtime
        .hooks
        .prepare_request
        .as_ref()
        .ok_or_else(|| std::io::Error::other("composed prepare hook missing"))?;
    let messages = prepare(Vec::new());
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
        turn: 0,
    };
    assert!(deny(&denied).is_err(), "the denied tool is blocked");
    let allowed = ToolRequest {
        name: "read".into(),
        ..denied
    };
    assert!(deny(&allowed).is_ok(), "other tools pass the hook");
    Ok(())
}

/// Purpose: `validate()` is a standalone structural check — no config and no
/// builder needed.
/// Scenario: an unknown builtin, two hooks on the same loop seam, and a
/// compaction threshold outside (0, 1].
/// Acceptance: each malformed spec is rejected with its own `SpecError`.
#[test]
fn spec_validation_is_standalone() {
    let mut unknown_builtin = HarnessSpec::new("bad-tool");
    unknown_builtin.tools.push(builtin_tool("teleport"));
    assert!(
        matches!(
            unknown_builtin.validate(),
            Err(SpecError::UnknownBuiltin { ref name }) if name == "teleport"
        ),
        "an unknown builtin is rejected"
    );

    let mut duplicate_seam = HarnessSpec::new("bad-hooks");
    duplicate_seam.hooks.push(HookSpec {
        name: "first".into(),
        kind: HookKind::TerminateAfterToolBatch,
        manifest: UnitManifest::default(),
    });
    duplicate_seam.hooks.push(HookSpec {
        name: "second".into(),
        kind: HookKind::TerminateAfterToolBatch,
        manifest: UnitManifest::default(),
    });
    assert!(
        matches!(
            duplicate_seam.validate(),
            Err(SpecError::DuplicateHookKind {
                seam: "should_terminate_tool_batch"
            })
        ),
        "two hooks on one seam are rejected"
    );

    let mut bad_compaction = HarnessSpec::new("bad-compaction");
    bad_compaction
        .compaction
        .stages
        .push(harness_builder::CompactionStageSpec {
            threshold: 1.5,
            target_ratio: 0.5,
            label: "impossible".into(),
        });
    assert!(
        matches!(
            bad_compaction.validate(),
            Err(SpecError::InvalidThreshold { ref label, .. }) if label == "impossible"
        ),
        "an out-of-range threshold is rejected"
    );
}
