//! Integration test: config prompt/skills/MCP sections wire into the daemon
//! runtime spec.
//!
//! Purpose: prove that config ([prompt].sections, [registry].skills,
//! [mcp].servers) reaches [`AssembledRuntime`] through the same mapping
//! `build_runtime` applies in `src/main.rs`.
//!
//! Note: `build_runtime` lives in the binary target (`src/main.rs`),
//! which integration tests cannot import, so this test reproduces its
//! exact spec-mapping steps against [`HarnessBuilder`] — the same
//! `harness_builder` mapping helpers and the same MCP-tool name dedup
//! guard.
//!
//! Scenario: a Config with two prompt sections, one skills root holding a
//! `greet/SKILL.md`, and one MCP server entry pointing at a trivial stdio
//! MCP server script that implements `initialize` and `tools/list`.
//! Acceptance: the runtime's prompt ledger carries both sections, the
//! `skill` tool is registered, and the runtime exposes a
//! `mcp__<server>__<tool>` tool name.

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use harness_builder::{
    HarnessBuilder, HarnessSpec, ToolKind, mcp_server_specs, mcp_tool_specs, prompt_sections,
    skill_specs_from_roots,
};
use harness_config::{
    AgentsConfig, BudgetConfig, CURRENT_CONFIG_VERSION, Config, McpConfig, McpServerEntry,
    ModelsConfig, PromptConfig, PromptSection, RegistryConfig, SchedulerConfig, SessionConfig,
    UpstreamConfig,
};
use harness_core::event::{EventSink, RunEvent};
use harness_core::model::{Model, ModelInfo};
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::tool::ToolRequest;

/// Stub policy: allows every call, injected explicitly by this test.
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
    events: std::sync::Mutex<Vec<RunEvent>>,
}

impl EventSink for TestSink {
    fn emit(&self, event: RunEvent) {
        self.events
            .lock()
            .unwrap_or_else(|error| panic!("test sink lock poisoned: {error}"))
            .push(event);
    }
}

/// A model that reports a fixed handle; `stream` is never reached by `build`.
struct MockModel {
    info: ModelInfo,
}

impl MockModel {
    fn new() -> Self {
        Self {
            info: ModelInfo {
                handle: "mock".into(),
                provider: "mock".into(),
                model: "mock-1".into(),
                context_window: 8192,
                supports_tool_use: true,
                supports_streaming: true,
            },
        }
    }
}

#[async_trait]
impl Model for MockModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(
        &self,
        _request: &harness_core::model::ModelRequest,
    ) -> harness_core::model::ModelStream<'_> {
        Box::pin(futures::stream::empty())
    }
}

/// A trivial stdio MCP server: answers `initialize` and `tools/list`, then
/// never replies to notifications. Exposed to the daemon as a subprocess
/// command so `build` can spawn it and advertise `mcp__trivial__echo`.
const MCP_SERVER_SCRIPT: &str = r#"#!/usr/bin/env python3
import json
import sys

REPLIES = {
    "initialize": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "serverInfo": {"name": "trivial", "version": "1.0.0"},
    },
    "tools/list": {
        "tools": [
            {
                "name": "echo",
                "description": "A trivial echo tool.",
                "inputSchema": {"type": "object", "properties": {}},
            }
        ],
    },
}


def main():
    out = sys.stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        rid = req.get("id")
        method = req.get("method")
        if rid is None:
            # Notification (e.g. notifications/initialized): no reply expected.
            continue
        result = REPLIES.get(method, {})
        out.write(json.dumps({"jsonrpc": "2.0", "id": rid, "result": result}) + "\n")
        out.flush()


main()
"#;

#[test]
fn config_prompt_skills_mcp_wire_into_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("harness-wiring-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;

    // One skill root: greet/SKILL.md.
    let skill_dir = dir.join("greet");
    std::fs::create_dir_all(&skill_dir)?;
    std::fs::write(skill_dir.join("SKILL.md"), "# Greet\nA greeting skill.\n")?;

    // The trivial MCP server script on disk, invoked via python3.
    let server_script = dir.join("trivial_mcp_server.py");
    std::fs::write(&server_script, MCP_SERVER_SCRIPT)?;

    let config = Config {
        version: CURRENT_CONFIG_VERSION,
        upstream: UpstreamConfig {
            base_url: "https://example.invalid/v1".into(),
            api_key_var: "HARNESS_TEST_KEY".into(),
        },
        models: ModelsConfig::default(),
        session: SessionConfig::default(),
        budget: BudgetConfig::default(),
        mcp: McpConfig {
            servers: vec![McpServerEntry {
                name: "trivial".into(),
                command: "python3".into(),
                args: vec![server_script.display().to_string()],
                env_var: None,
            }],
        },
        registry: RegistryConfig {
            hooks: Vec::new(),
            skills: vec![dir.display().to_string()],
        },
        scheduler: SchedulerConfig::default(),
        agents: AgentsConfig::default(),
        prompt: PromptConfig {
            sections: vec![
                PromptSection {
                    name: "identity".into(),
                    content: "You are harness.".into(),
                },
                PromptSection {
                    name: "rules".into(),
                    content: "Be terse.".into(),
                },
            ],
        },
    };

    // ── Mirror `build_runtime`'s spec mapping ──────────────────────────
    let mut spec = HarnessSpec::new("config-wired");
    spec.default_model = Some("mock".into());
    spec.models = vec![harness_builder::ModelSpec {
        handle: "mock".into(),
        provider: "mock".into(),
        model: "mock-1".into(),
        context_window: 128_000,
        supports_tool_use: true,
        supports_streaming: true,
    }];
    spec.tools = harness_builder::BUILTIN_TOOLS
        .iter()
        .map(|name| harness_builder::ToolSpec::builtin(*name))
        .collect();
    spec.prompt_sections = prompt_sections(&config.prompt.sections);
    spec.skills = skill_specs_from_roots(&config.registry.skills);
    spec.mcp_servers = mcp_server_specs(&config.mcp.servers);
    // MCP tool units are namespaced `mcp-<server>`; spec validation catches
    // a genuine `tool:<name>` collision.
    spec.tools.extend(mcp_tool_specs(&config.mcp.servers));

    // Spec-level wiring.
    assert_eq!(spec.prompt_sections.len(), 2, "both prompt sections mapped");
    assert_eq!(spec.skills.len(), 1, "discovered skill mapped");
    assert_eq!(spec.mcp_servers.len(), 1, "MCP server entry mapped");
    assert!(
        spec.tools
            .iter()
            .any(|tool| tool.name == "mcp-trivial" && tool.kind == ToolKind::Mcp),
        "MCP tool unit declared"
    );
    assert!(spec.hooks.is_empty(), "hooks declarative-only, left empty");

    // ── Assemble the runtime ──────────────────────────────────────────
    let runtime = HarnessBuilder::new(spec)
        .with_model(Arc::new(MockModel::new()))
        .with_policy(Arc::new(AllowAllPolicy))
        .with_event_sink(Arc::new(TestSink::default()))
        .with_cwd(dir.clone())
        .with_config(&config)
        .context("mapping the config onto builder bindings")?
        .build()
        .context("assembling the runtime")?;

    // Prompt sections reach the prompt ledger.
    use harness_core::message::Message;
    let Some(prompt) = runtime.prompt.as_ref() else {
        panic!("configured prompt sections must assemble a system message");
    };
    let Message::System { sections } = prompt else {
        panic!("prompt should be a system message");
    };
    assert_eq!(
        sections.get("identity").map(|c| c.as_deref()),
        Some(Some("You are harness.")),
        "identity prompt section present"
    );
    assert_eq!(
        sections.get("rules").map(|c| c.as_deref()),
        Some(Some("Be terse.")),
        "rules prompt section present"
    );

    // Declaring skills registers the `skill` tool.
    assert!(
        runtime.tools.iter().any(|tool| tool.name() == "skill"),
        "declaring skills registers the skill tool"
    );

    // The MCP server's advertised tool is namespaced `mcp__<server>__<tool>`.
    let mcp_names: Vec<&str> = runtime
        .tools
        .iter()
        .map(|tool| tool.name())
        .filter(|name| name.starts_with("mcp__"))
        .collect();
    assert!(
        mcp_names.contains(&"mcp__trivial__echo"),
        "expected mcp__trivial__echo tool, got {:?}",
        mcp_names
    );

    Ok(())
}
