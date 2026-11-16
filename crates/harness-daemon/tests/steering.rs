//! End-to-end steering test: a `steer` command queued while a run is busy
//! reaches the next turn boundary and is replayed to the model as a user
//! message.
//!
//! Purpose: prove the transport → scheduler → `RunControl` → loop-steering
//! path works on an active run, and that non-active runs are rejected.
//! Scenario: the scripted model asks for a slow tool on turn 0; while the
//! tool runs, the test sends `steer`; turn 1 must carry the message.
//! Acceptance: the steer command answers `{steered: true}`, the second model
//! request contains the steering text as a user message, and steering an
//! unknown run returns the typed `NoActiveRun` error.

use std::collections::VecDeque;
use std::env;
use std::fs;
use std::process::id;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::iter as stream_iter;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use harness_builder::{HarnessBuilder, HarnessSpec, ModelSpec, ToolSpec};
use harness_core::event::{EventSink, RunEvent};
use harness_core::message::{Content, Message, StopReason};
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::run::RunId;
use harness_core::tool::{Tool, ToolError, ToolRequest, ToolResult, ToolRisk, ToolSchema};
use harness_daemon::tools::ToolCatalog;
use harness_daemon::wire::DaemonHandler;
use harness_scheduler::RunStatus;
use harness_scheduler::Scheduler;
use harness_session::{RecallStore, SessionStore};
use harness_storage::open;
use harness_transport::{CommandHandler, SlashCommand, TransportError};

/// Model that records every request and replays a scripted turn list.
///
/// Turn 0 asks for the `slow` tool; turn 1 answers. The queue repeats the
/// last turn if the loop asks again, so a bug cannot hang the test.
struct SteeringModel {
    info: ModelInfo,
    turns: Mutex<VecDeque<Vec<ModelEvent>>>,
    last: Vec<ModelEvent>,
    requests: Mutex<Vec<Vec<Message>>>,
}

impl SteeringModel {
    fn new(turns: Vec<Vec<ModelEvent>>) -> Self {
        let last = turns.last().cloned().unwrap_or_default();
        Self {
            info: ModelInfo {
                handle: "steer".into(),
                provider: "fake".into(),
                model: "steer-1".into(),
                context_window: 8192,
                supports_tool_use: true,
                supports_streaming: true,
            },
            turns: Mutex::new(turns.into()),
            last,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<Vec<Message>> {
        self.requests
            .lock()
            .unwrap_or_else(|e| panic!("model lock poisoned: {e}"))
            .clone()
    }
}

#[async_trait]
impl Model for SteeringModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, req: &ModelRequest) -> ModelStream<'_> {
        self.requests
            .lock()
            .unwrap_or_else(|e| panic!("model lock poisoned: {e}"))
            .push(req.messages.clone());
        let events = self
            .turns
            .lock()
            .unwrap_or_else(|e| panic!("turn lock poisoned: {e}"))
            .pop_front()
            .unwrap_or_else(|| self.last.clone());
        Box::pin(stream_iter(events))
    }
}

/// A tool that takes long enough for the test to steer mid-run.
struct SlowTool;

#[async_trait]
impl Tool for SlowTool {
    fn name(&self) -> &str {
        "slow"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "slow".into(),
            description: "sleeps briefly".into(),
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
        tokio::time::sleep(Duration::from_millis(400)).await;
        Ok(ToolResult {
            content: "slept".into(),
            is_error: false,
            trace: None,
        })
    }
}

/// Event sink with no observers: the handler's pump only needs a handle.
struct NullSink;

impl EventSink for NullSink {
    fn emit(&self, _event: RunEvent) {}
}

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

/// Concatenate the text content of a user or assistant message.
fn text_of(message: &Message) -> String {
    match message {
        Message::User { content } | Message::Assistant { content } => content
            .iter()
            .filter_map(|part| match part {
                Content::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steer_reaches_the_next_turn_and_rejects_unknown_runs() {
    let dir = env::temp_dir().join(format!("harness-steer-{}", id()));
    fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create temp dir: {e}"));
    let store = Arc::new(
        open(dir.join("sessions.db"))
            .await
            .unwrap_or_else(|e| panic!("open store: {e}")),
    );

    let model = Arc::new(SteeringModel::new(vec![
        vec![
            ModelEvent::ToolCallStart {
                id: "call-1".into(),
                name: "slow".into(),
            },
            ModelEvent::ToolCallArgs {
                id: "call-1".into(),
                partial_json: "{}".into(),
            },
            ModelEvent::ToolCallEnd {
                id: "call-1".into(),
            },
            ModelEvent::Stop(StopReason::ToolUse),
        ],
        vec![
            ModelEvent::TextDelta("done".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ],
    ]));

    let mut spec = HarnessSpec::new("steering-test");
    spec.default_model = Some("steer".into());
    spec.models = vec![ModelSpec {
        handle: "steer".into(),
        provider: "fake".into(),
        model: "steer-1".into(),
        context_window: 8192,
        supports_tool_use: true,
        supports_streaming: true,
    }];
    spec.tools = vec![ToolSpec::external("slow")];
    let runtime = HarnessBuilder::new(spec)
        .with_model(Arc::clone(&model) as Arc<dyn Model>)
        .with_policy(Arc::new(AllowAll))
        .with_event_sink(Arc::new(NullSink))
        .with_cwd(dir.clone())
        .with_tool(Arc::new(SlowTool))
        .build()
        .unwrap_or_else(|e| panic!("build runtime: {e}"));

    let scheduler = Scheduler::new(4, 4);
    let recall: Arc<dyn RecallStore> = store.recall.clone();
    let session_store: Arc<dyn SessionStore> = store.session.clone();
    // The catalog is the per-session tool source: start_run replaces the
    // runtime's tool list with its active subset, so an empty catalog would
    // hide the slow tool and finish the run before the steer arrives.
    let catalog = Arc::new(ToolCatalog::new(runtime.tools.clone()));
    let handler = DaemonHandler::new(scheduler.clone(), session_store, recall, catalog, None);
    handler.set_runtime(runtime);
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(64);
    handler.set_event_tx(event_tx);

    let response = handler
        .handle(SlashCommand::Run {
            task: "call the slow tool once, then answer".into(),
            session_id: Some("steer".into()),
        })
        .unwrap_or_else(|e| panic!("run command failed: {e}"));
    let run_id = response["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("run response missing run_id: {response}"))
        .to_string();

    // Let the run reach the tool call, then steer while it sleeps.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let steered = handler
        .handle(SlashCommand::Steer {
            run_id: run_id.clone(),
            message: "prefer the short path".into(),
        })
        .unwrap_or_else(|e| panic!("steer command failed: {e}"));
    assert_eq!(steered["steered"], true, "an active run accepts steering");

    for _ in 0..500 {
        if matches!(
            scheduler.status(&RunId(run_id.clone())),
            Some(RunStatus::Finished(_))
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let requests = model.requests();
    assert_eq!(requests.len(), 2, "tool turn plus answer turn");
    let second = &requests[1];
    assert!(
        second
            .iter()
            .any(|message| text_of(message).contains("prefer the short path")),
        "the second request must carry the steering message: {second:?}"
    );
    assert!(
        second
            .iter()
            .any(|message| matches!(message, Message::User { .. })
                && text_of(message).contains("prefer the short path")),
        "steering is replayed as a user turn"
    );

    let unknown = handler.handle(SlashCommand::Steer {
        run_id: "no-such-run".into(),
        message: "hello".into(),
    });
    assert!(
        matches!(unknown, Err(TransportError::NoActiveRun(_))),
        "steering an unknown run is a typed no-active-run error, got {unknown:?}"
    );

    let _ = fs::remove_dir_all(&dir);
}
