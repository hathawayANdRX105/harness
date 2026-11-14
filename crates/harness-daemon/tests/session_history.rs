//! End-to-end test for session history: two runs in one session, driven
//! through the real daemon handler over a real Turso store.
//!
//! Purpose: prove that a run's messages are persisted and that the next run in
//! the same session starts from the replayed ledger.
//! Scenario: a scripted model records every request; run 1 tells it a
//! codeword, run 2 asks for it back in the same session.
//! Acceptance: the second model request contains run 1's user task and
//! assistant reply, and the ledger replays all four messages in order.

use std::collections::VecDeque;
use std::env;
use std::fs;
use std::process::id;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::iter as stream_iter;
use harness_builder::{HarnessBuilder, HarnessSpec, ModelSpec};
use harness_core::event::{EventSink, RunEvent};
use harness_core::message::{Content, Message, StopReason};
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
use harness_core::policy::{PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy};
use harness_core::run::{RunId, SessionId};
use harness_core::tool::ToolRequest;
use harness_daemon::tools::ToolCatalog;
use harness_daemon::wire::DaemonHandler;
use harness_scheduler::{RunStatus, Scheduler};
use harness_session::turso::TursoSessionStore;
use harness_session::{RecallStore, SessionStore};
use harness_transport::{CommandHandler, SlashCommand};

/// Model that records every request and answers with a scripted reply.
struct RecordingModel {
    info: ModelInfo,
    requests: Mutex<Vec<Vec<Message>>>,
    replies: Mutex<VecDeque<String>>,
}

impl RecordingModel {
    fn new(replies: &[&str]) -> Self {
        Self {
            info: ModelInfo {
                handle: "fake".into(),
                provider: "fake".into(),
                model: "fake-1".into(),
                context_window: 8192,
                supports_tool_use: true,
                supports_streaming: true,
            },
            requests: Mutex::new(Vec::new()),
            replies: Mutex::new(replies.iter().map(|s| s.to_string()).collect()),
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
impl Model for RecordingModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, req: &ModelRequest) -> ModelStream<'_> {
        self.requests
            .lock()
            .unwrap_or_else(|e| panic!("model lock poisoned: {e}"))
            .push(req.messages.clone());
        let reply = self
            .replies
            .lock()
            .unwrap_or_else(|e| panic!("reply lock poisoned: {e}"))
            .pop_front()
            .unwrap_or_else(|| "done".into());
        Box::pin(stream_iter(vec![
            ModelEvent::TextDelta(reply),
            ModelEvent::Stop(StopReason::EndTurn),
        ]))
    }
}

/// Event sink with no observers: the scheduler only needs a sink handle.
struct NullSink;

impl EventSink for NullSink {
    fn emit(&self, _event: RunEvent) {}
}

/// Allow-everything policy: this test never dispatches tools.
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

/// Submit one run and wait for the scheduler to record its terminal state.
async fn run_to_completion(
    handler: &DaemonHandler,
    scheduler: &Scheduler,
    task: &str,
    session: &str,
) -> String {
    let response = handler
        .handle(SlashCommand::Run {
            task: task.into(),
            session_id: Some(session.into()),
        })
        .unwrap_or_else(|e| panic!("run command failed: {e}"));
    let run_id = response["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("run response missing run_id: {response}"))
        .to_string();
    for _ in 0..500 {
        if matches!(
            scheduler.status(&RunId(run_id.clone())),
            Some(RunStatus::Finished(_))
        ) {
            return run_id;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run {run_id} did not finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_run_replays_first_run_history() {
    let dir = env::temp_dir().join(format!("harness-history-{}", id()));
    fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create temp dir: {e}"));
    let store = Arc::new(
        TursoSessionStore::open(dir.join("sessions.db"))
            .await
            .unwrap_or_else(|e| panic!("open store: {e}")),
    );

    let model = Arc::new(RecordingModel::new(&["OK-BANANA", "BANANA"]));
    let mut spec = HarnessSpec::new("session-history-test");
    spec.default_model = Some("fake".into());
    spec.models = vec![ModelSpec {
        handle: "fake".into(),
        provider: "fake".into(),
        model: "fake-1".into(),
        context_window: 8192,
        supports_tool_use: true,
        supports_streaming: true,
    }];
    let runtime = HarnessBuilder::new(spec)
        .with_model(Arc::clone(&model) as Arc<dyn Model>)
        .with_policy(Arc::new(AllowAll))
        .with_event_sink(Arc::new(NullSink))
        .with_cwd(dir.clone())
        .build()
        .unwrap_or_else(|e| panic!("build runtime: {e}"));

    let scheduler = Scheduler::new(4, 4);
    let recall: Arc<dyn RecallStore> = Arc::new(store.recall());
    let session_store: Arc<dyn SessionStore> = Arc::clone(&store) as Arc<dyn SessionStore>;
    let handler = DaemonHandler::new(
        scheduler.clone(),
        session_store,
        recall,
        Arc::new(ToolCatalog::new(Vec::new())),
        None,
    );
    handler.set_runtime(runtime);
    // Keep a receiver alive so the run pump has a channel to forward into;
    // the test never inspects the events.
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(64);
    handler.set_event_tx(event_tx);

    run_to_completion(
        &handler,
        &scheduler,
        "My codeword is BANANA. Reply with exactly OK and call no tools.",
        "history",
    )
    .await;
    run_to_completion(
        &handler,
        &scheduler,
        "What is my codeword? Reply with only the codeword, call no tools.",
        "history",
    )
    .await;

    let requests = model.requests();
    assert_eq!(requests.len(), 2, "one model call per run");
    let second = &requests[1];
    assert_eq!(
        second.len(),
        3,
        "second request must carry run 1's user and assistant messages"
    );
    assert!(
        text_of(&second[0]).contains("BANANA"),
        "run 1 task must lead the replayed transcript: {:?}",
        text_of(&second[0])
    );
    assert!(
        matches!(&second[1], Message::Assistant { .. })
            && text_of(&second[1]).contains("OK-BANANA"),
        "run 1 reply must follow its task: {:?}",
        text_of(&second[1])
    );
    assert!(
        text_of(&second[2]).contains("codeword"),
        "the new task must be appended after the history"
    );

    let ledger = store
        .replay_ledger(&SessionId("history".into()))
        .await
        .unwrap_or_else(|e| panic!("replay ledger: {e}"));
    assert_eq!(ledger.len(), 4, "two runs, one user and one assistant each");
    assert_eq!(
        ledger.iter().map(text_of).collect::<Vec<_>>(),
        vec![
            "My codeword is BANANA. Reply with exactly OK and call no tools.",
            "OK-BANANA",
            "What is my codeword? Reply with only the codeword, call no tools.",
            "BANANA",
        ],
        "the ledger must replay in run order"
    );

    let _ = fs::remove_dir_all(&dir);
}
