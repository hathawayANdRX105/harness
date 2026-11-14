//! Integration tests for the daemon's `subagent` tool.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use futures::stream::iter as stream_iter;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use harness_config::SubagentProfile;
use harness_core::event::{EventSink, RunEvent, RunEventKind, SubagentStatus};
use harness_core::message::StopReason;
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::run::{RunId, RunLimits, RunStopReason, SessionId};
use harness_core::tool::{Tool, ToolError, ToolRequest, ToolResult, ToolRisk, ToolSchema};

use harness_daemon::subagent::{DEFAULT_SUBAGENT_TOOLS, SubagentTool};

/// A sink that records every event for inspection.
#[derive(Default)]
struct CollectSink {
    events: Mutex<Vec<RunEvent>>,
}

impl EventSink for CollectSink {
    fn emit(&self, event: RunEvent) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }
}

impl CollectSink {
    fn snapshot(&self) -> Vec<RunEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// A tool that returns a fixed result.
struct StubTool {
    name: String,
    result: String,
}

#[async_trait]
impl Tool for StubTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: self.name.clone(),
            description: "stub tool".into(),
            input: json!({"type": "object"}),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::ReadOnly
    }

    async fn invoke(
        &self,
        _args: Value,
        _cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            content: self.result.clone(),
            is_error: false,
            trace: None,
        })
    }
}

/// A model that streams pre-recorded turns, FIFO; when the queue is
/// empty it repeats the last turn forever.
struct ScriptedModel {
    info: ModelInfo,
    turns: Mutex<std::collections::VecDeque<Vec<ModelEvent>>>,
    last: Vec<ModelEvent>,
}

impl ScriptedModel {
    fn new(turns: Vec<Vec<ModelEvent>>) -> Self {
        let last = turns.last().cloned().unwrap_or_default();
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
            last,
        }
    }
}

#[async_trait]
impl Model for ScriptedModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, _req: &ModelRequest) -> ModelStream<'_> {
        let events = self
            .turns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop_front()
            .unwrap_or_else(|| self.last.clone());
        Box::pin(stream_iter(events))
    }
}

/// Allow-every-thing policy.
struct AllowAll;

impl ToolPolicy for AllowAll {
    fn decide(
        &self,
        _stage: PolicyStage,
        _req: &ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
        PolicyDecision {
            outcome: PolicyOutcome::Allow,
            reason: String::new(),
            source: "test-allow-all".into(),
            context_injection: None,
            output_suffix: None,
        }
    }
}

fn parent_limits() -> RunLimits {
    RunLimits {
        max_turns: 50,
        max_input_tokens: 150_000,
        max_output_tokens: 8_000,
    }
}

fn base_catalog() -> std::sync::Arc<harness_daemon::tools::ToolCatalog> {
    std::sync::Arc::new(harness_daemon::tools::ToolCatalog::new(vec![
        std::sync::Arc::new(StubTool {
            name: "read".into(),
            result: "stub content".into(),
        }),
        std::sync::Arc::new(StubTool {
            name: "glob".into(),
            result: "stub content".into(),
        }),
        std::sync::Arc::new(StubTool {
            name: "grep".into(),
            result: "stub content".into(),
        }),
    ]))
}

fn profiles() -> HashMap<String, SubagentProfile> {
    HashMap::from([(
        "scout".into(),
        SubagentProfile {
            name: "scout".into(),
            tools: vec!["read".into(), "glob".into(), "grep".into()],
            max_turns: 5,
        },
    )])
}

/// Invoke the tool as a per-run instance scoped to the given parent:
/// mirrors how `wire::start_run` installs the tool before a run.
async fn invoke(
    tool: &SubagentTool,
    parent_run: &str,
    parent_session: &str,
    limits: RunLimits,
    task: &str,
    profile: &str,
) -> Result<ToolResult, ToolError> {
    let scoped = tool.for_run(
        &RunId(parent_run.into()),
        &SessionId(parent_session.into()),
        limits,
        // Tests let the parent allow every base tool: profile-level
        // restriction is exercised instead.
        base_catalog()
            .names()
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );
    scoped
        .invoke(
            json!({"task": task, "profile": profile}),
            &CancellationToken::new(),
        )
        .await
}

/// Purpose: a completed child reports Completed, captures its final
/// text, and emits SubagentStarted + SubagentReport on the shared sink.
/// Scenario: profile `scout`, child model answers with text and no tool
/// calls.
/// Acceptance: `is_error` is false; the payload names `parent-1--sub0`,
/// profile `scout`, status `completed`, reason `completed`, and carries
/// the child's text; the sink shows `subagent_started` at seq 0 and
/// `subagent_report` last.
#[tokio::test]
async fn completed_child_reports_completed_with_captured_text() {
    let sink = std::sync::Arc::new(CollectSink::default());
    let model: std::sync::Arc<dyn Model> = std::sync::Arc::new(ScriptedModel::new(vec![vec![
        ModelEvent::TextDelta("ok".into()),
        ModelEvent::Stop(StopReason::EndTurn),
    ]]));
    let tool = SubagentTool::new(
        std::sync::Arc::clone(&model),
        profiles(),
        std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
        std::sync::Arc::new(AllowAll),
    );
    tool.set_catalog(base_catalog());

    let result = invoke(
        &tool,
        "parent-1",
        "s-1",
        parent_limits(),
        "summarize",
        "scout",
    )
    .await;

    let result = result.unwrap_or_else(|e| panic!("subagent invoke failed: {e}"));
    assert_eq!(
        result.is_error, false,
        "a completed child is not a tool error"
    );

    let payload: Value =
        serde_json::from_str(&result.content).unwrap_or_else(|e| panic!("payload is JSON: {e}"));
    assert_eq!(payload["child_run_id"], "parent-1--sub0");
    assert_eq!(payload["profile"], "scout");
    assert_eq!(payload["status"], "completed");
    assert_eq!(payload["reason"]["kind"], "completed");
    assert_eq!(payload["text"], "ok");

    let events = sink.snapshot();
    assert!(events.first().is_some(), "at least SubagentStarted emitted");
    let started = &events[0];
    assert_eq!(started.seq, 0);
    assert_eq!(
        started.kind,
        RunEventKind::SubagentStarted {
            child_run_id: RunId("parent-1--sub0".into()),
            profile: "scout".into(),
        }
    );
    assert_eq!(started.session_id, SessionId("s-1--sub".into()));
    let report = events
        .iter()
        .last()
        .unwrap_or_else(|| panic!("no events emitted"));
    assert_eq!(
        report.kind,
        RunEventKind::SubagentReport {
            child_run_id: RunId("parent-1--sub0".into()),
            status: SubagentStatus::Completed,
            reason: RunStopReason::Completed,
        }
    );
    assert!(
        report.seq > started.seq,
        "report lands after the started event"
    );
}

/// Purpose: a child that exhausts its turns reports Blocked.
/// Scenario: profile with `max_turns: 1`; the child model requests a
/// tool every turn.
/// Acceptance: status `blocked`, reason `max_turns`, `is_error` false.
#[tokio::test]
async fn max_turns_child_reports_blocked() {
    let sink = std::sync::Arc::new(CollectSink::default());
    let model: std::sync::Arc<dyn Model> = std::sync::Arc::new(ScriptedModel::new(vec![vec![
        ModelEvent::ToolCallStart {
            id: "c1".into(),
            name: "read".into(),
        },
        ModelEvent::ToolCallArgs {
            id: "c1".into(),
            partial_json: "{}".into(),
        },
        ModelEvent::ToolCallEnd { id: "c1".into() },
        ModelEvent::Stop(StopReason::ToolUse),
    ]]));
    let profile = SubagentProfile {
        name: "scout".into(),
        tools: vec!["read".into()],
        max_turns: 1,
    };
    let mut profiles = HashMap::new();
    profiles.insert("scout".into(), profile);
    let tool = SubagentTool::new(
        std::sync::Arc::clone(&model),
        profiles,
        std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
        std::sync::Arc::new(AllowAll),
    );
    tool.set_catalog(base_catalog());
    let result = invoke(
        &tool,
        "parent-1",
        "s-1",
        parent_limits(),
        "loop forever",
        "scout",
    )
    .await;
    let result = result.unwrap_or_else(|e| panic!("subagent invoke failed: {e}"));
    assert_eq!(result.is_error, false);

    let payload: Value =
        serde_json::from_str(&result.content).unwrap_or_else(|e| panic!("payload is JSON: {e}"));
    assert_eq!(payload["status"], "blocked");
    assert_eq!(payload["reason"]["kind"], "max_turns");
    assert_eq!(payload["child_run_id"], "parent-1--sub0");

    let events = sink.snapshot();
    let report = events
        .iter()
        .last()
        .unwrap_or_else(|| panic!("no events emitted"));
    assert_eq!(
        report.kind,
        RunEventKind::SubagentReport {
            child_run_id: RunId("parent-1--sub0".into()),
            status: SubagentStatus::Blocked,
            reason: RunStopReason::MaxTurns,
        }
    );
}

/// Purpose: a child whose stream errors reports Error.
/// Scenario: child model emits text then `ModelEvent::Error`.
/// Acceptance: status `error`, reason carries the error
/// message, `is_error` false.
#[tokio::test]
async fn error_child_reports_error() {
    let sink = std::sync::Arc::new(CollectSink::default());
    let model: std::sync::Arc<dyn Model> = std::sync::Arc::new(ScriptedModel::new(vec![vec![
        ModelEvent::TextDelta("partial".into()),
        ModelEvent::Error {
            message: "upstream died".into(),
        },
    ]]));
    let tool = SubagentTool::new(
        std::sync::Arc::clone(&model),
        profiles(),
        std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
        std::sync::Arc::new(AllowAll),
    );
    tool.set_catalog(base_catalog());
    let result = invoke(&tool, "parent-1", "s-1", parent_limits(), "fail", "scout").await;
    let result = result.unwrap_or_else(|e| panic!("subagent invoke failed: {e}"));
    assert_eq!(result.is_error, false);

    let payload: Value =
        serde_json::from_str(&result.content).unwrap_or_else(|e| panic!("payload is JSON: {e}"));
    assert_eq!(payload["status"], "error");
    assert_eq!(payload["reason"]["kind"], "error");
    assert_eq!(payload["reason"]["message"], "upstream died");
    assert_eq!(payload["text"], "partial");
}

/// Purpose: an unknown profile is a labeled tool error.
/// Scenario: invoke with a profile name nothing registered.
/// Acceptance: `is_error`, content names the unknown profile and lists
/// available profiles.
#[tokio::test]
async fn unknown_profile_reports_available_profiles() {
    let sink = std::sync::Arc::new(CollectSink::default());
    let tool = SubagentTool::new(
        std::sync::Arc::new(ScriptedModel::new(vec![])),
        profiles(),
        std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
        std::sync::Arc::new(AllowAll),
    );
    tool.set_catalog(base_catalog());
    let err = match invoke(
        &tool,
        "parent-1",
        "s-1",
        parent_limits(),
        "whatever",
        "nope",
    )
    .await
    {
        Ok(value) => panic!("unknown profile must be rejected: {value:?}"),
        Err(err) => err,
    };
    let content = err.to_string();
    assert!(
        content.contains("unknown profile"),
        "names the profile: {content}"
    );
    assert!(
        content.contains("nope"),
        "names the unknown profile: {content}"
    );
    assert!(
        content.contains("scout"),
        "lists available profiles: {content}"
    );
}

/// Purpose: an unknown tool in a profile is a labeled tool error.
/// Scenario: profile lists a tool absent from the base catalog.
/// Acceptance: `is_error`, content names the unknown tool and lists
/// base tool names.
#[tokio::test]
async fn unknown_tool_in_profile_reports_base_tools() {
    let sink = std::sync::Arc::new(CollectSink::default());
    let mut bad = profiles();
    bad.insert(
        "mischief".into(),
        SubagentProfile {
            name: "mischief".into(),
            tools: vec!["nope".into()],
            max_turns: 5,
        },
    );
    let tool = SubagentTool::new(
        std::sync::Arc::new(ScriptedModel::new(vec![])),
        bad,
        std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
        std::sync::Arc::new(AllowAll),
    );
    tool.set_catalog(base_catalog());
    let err = match invoke(
        &tool,
        "parent-1",
        "s-1",
        parent_limits(),
        "whatever",
        "mischief",
    )
    .await
    {
        Ok(value) => panic!("unknown tool in profile must be rejected: {value:?}"),
        Err(err) => err,
    };
    let content = err.to_string();
    assert!(
        content.contains("unknown tool"),
        "names the tool problem: {content}"
    );
    assert!(
        content.contains("nope"),
        "names the unknown tool: {content}"
    );
    for expected in DEFAULT_SUBAGENT_TOOLS {
        assert!(
            content.contains(expected),
            "lists base tool {expected}: {content}"
        );
    }
}

/// Purpose: a profile tool the parent run itself is not allowed to call
/// is rejected before any child run starts — delegation never amplifies
/// the caller's tool authority.
/// Scenario: the parent's active set is only [read, glob, grep] while
/// the profile grants `write`.
/// Acceptance: the invoke is an `InvalidArgs` error naming the withheld
/// tool, and no child run is spawned.
#[tokio::test]
async fn profile_tool_withheld_from_parent_is_rejected() {
    let sink = std::sync::Arc::new(CollectSink::default());
    let mut profiles = profiles();
    profiles.insert(
        "writer".into(),
        SubagentProfile {
            name: "writer".into(),
            tools: vec!["write".into()],
            max_turns: 5,
        },
    );
    let tool = SubagentTool::new(
        std::sync::Arc::new(ScriptedModel::new(vec![])),
        profiles,
        std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
        std::sync::Arc::new(AllowAll),
    );
    tool.set_catalog(base_catalog());

    let scoped = tool.for_run(
        &RunId("parent-1".into()),
        &SessionId("s-1".into()),
        parent_limits(),
        // The parent's session withholds `write`.
        vec!["read".into(), "glob".into(), "grep".into()],
    );
    let err = match scoped
        .invoke(
            json!({"task": "edit things", "profile": "writer"}),
            &CancellationToken::new(),
        )
        .await
    {
        Ok(value) => panic!("a withheld tool must be rejected: {value:?}"),
        Err(err) => err,
    };
    let content = err.to_string();
    assert!(
        content.contains("write"),
        "names the withheld tool: {content}"
    );
    assert!(
        sink.snapshot().is_empty(),
        "no child events may be emitted for a rejected invocation"
    );
}

/// Purpose: child ids derive from the parent as specified.
/// Scenario: two invokes under one parent, then one under a second
/// tool instance.
/// Acceptance: `parent-1--sub0`, `parent-1--sub1` under the first
/// instance; a second instance's child starts back at `parent-2--sub0`
/// (the child counter is per shared tool instance, not per parent);
/// sessions `s-1--sub` / `s-2--sub`.
#[tokio::test]
async fn child_ids_derive_from_parent() {
    let make_tool = |sink: std::sync::Arc<CollectSink>| {
        let model: std::sync::Arc<dyn Model> = std::sync::Arc::new(ScriptedModel::new(vec![vec![
            ModelEvent::TextDelta("x".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]]));
        SubagentTool::new(
            model,
            profiles(),
            sink as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        )
    };

    let sink = std::sync::Arc::new(CollectSink::default());
    let tool = make_tool(std::sync::Arc::clone(&sink));
    tool.set_catalog(base_catalog());
    let r0 = invoke(&tool, "parent-1", "s-1", parent_limits(), "one", "scout")
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let p0: Value = serde_json::from_str(&r0.content).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(p0["child_run_id"], "parent-1--sub0");

    let r1 = invoke(&tool, "parent-1", "s-1", parent_limits(), "two", "scout")
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let p1: Value = serde_json::from_str(&r1.content).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(p1["child_run_id"], "parent-1--sub1");
    assert_eq!(sink.snapshot()[0].session_id, SessionId("s-1--sub".into()));

    let sink2 = std::sync::Arc::new(CollectSink::default());
    let tool2 = make_tool(std::sync::Arc::clone(&sink2));
    tool2.set_catalog(base_catalog());
    let r2 = invoke(&tool2, "parent-2", "s-2", parent_limits(), "three", "scout")
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let p2: Value = serde_json::from_str(&r2.content).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(p2["child_run_id"], "parent-2--sub0");
    assert_eq!(sink2.snapshot()[0].session_id, SessionId("s-2--sub".into()));
}

/// Purpose: an unwired catalog is a labeled error, not a crash.
/// Scenario: invoke before `set_catalog`.
/// Acceptance: `is_error`, content explains the wiring is not ready.
#[tokio::test]
async fn unset_catalog_reports_wiring_not_ready() {
    let sink = std::sync::Arc::new(CollectSink::default());
    let tool = SubagentTool::new(
        std::sync::Arc::new(ScriptedModel::new(vec![])),
        profiles(),
        std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
        std::sync::Arc::new(AllowAll),
    );
    let result = invoke(
        &tool,
        "parent-1",
        "s-1",
        parent_limits(),
        "whatever",
        "scout",
    )
    .await;
    let err = match result {
        Ok(value) => panic!("an unwired catalog must be rejected: {value:?}"),
        Err(err) => err,
    };
    let content = err.to_string();
    assert!(
        content.contains("not ready"),
        "explains the missing catalog: {content}"
    );
}

/// Purpose: an invocation outside a run is a labeled error.
/// Scenario: catalog wired, no parent context anchored.
/// Acceptance: `is_error`, content explains the missing parent context.
#[tokio::test]
async fn no_parent_context_reports_invalid_args() {
    let sink = std::sync::Arc::new(CollectSink::default());
    let tool = SubagentTool::new(
        std::sync::Arc::new(ScriptedModel::new(vec![])),
        profiles(),
        std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
        std::sync::Arc::new(AllowAll),
    );
    tool.set_catalog(base_catalog());

    // The shared instance carries no parent scope: only the per-run
    // instance `for_run` produces can spawn a child.
    let result = tool
        .invoke(
            json!({"task": "whatever", "profile": "scout"}),
            &CancellationToken::new(),
        )
        .await;
    let err = match result {
        Ok(value) => panic!("a scope-less invoke must be rejected: {value:?}"),
        Err(err) => err,
    };
    let content = err.to_string();
    assert!(
        content.contains("outside of a run"),
        "explains the missing parent context: {content}"
    );
}
