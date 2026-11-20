//! Turn vocabulary tests: construction and the pairing-safe ledger
//! primitives.
//!
//! Purpose: the turn is the single source of truth every processor reads,
//! so its invariants are pinned here — seed passthrough and the
//! tool-call/result pairing that `retract_messages` must preserve.
//!
//! Acceptance: a `Turn` built over a stub model carries its seed ledger
//! verbatim; `retract_messages` never orphans a tool call from its result.

use std::sync::Arc;

use async_trait::async_trait;

use harness_core::control::RunControl;
use harness_core::message::{Content, Message, ToolCall, ToolResultMessage};
use harness_core::model::{Model, ModelInfo, ModelRequest, ModelStream};
use harness_core::run::{RunContext, RunId, RunLimits, SessionId, Task};
use harness_core::topology::Turn;

struct StubModel(ModelInfo);

#[async_trait]
impl Model for StubModel {
    fn info(&self) -> &ModelInfo {
        &self.0
    }

    async fn stream(&self, _req: &ModelRequest) -> ModelStream<'_> {
        Box::pin(futures::stream::empty())
    }
}

fn sample_info() -> ModelInfo {
    ModelInfo {
        handle: "shell".into(),
        provider: "openai".into(),
        model: "gpt-test".into(),
        context_window: 8192,
        supports_tool_use: true,
        supports_streaming: true,
    }
}

fn sample_ctx() -> RunContext {
    RunContext {
        run_id: RunId("run-shell".into()),
        session_id: SessionId("session-shell".into()),
        task: Task {
            description: "shell probe".into(),
            source: None,
            deadline: None,
        },
        limits: RunLimits::default(),
        started_at_ms: 0,
    }
}

/// Purpose: the turn's seed ledger is the host's session replay and must
/// survive construction untouched.
/// Scenario: two user messages seeded, nothing else wired.
/// Acceptance: `ledger` holds both verbatim; `tools` and every artifacts
/// field start empty.
#[test]
fn turn_carries_seed_ledger_verbatim() {
    let model = Arc::new(StubModel(sample_info()));
    let seed = vec![
        Message::user_text("first user turn"),
        Message::user_text("second user turn"),
    ];
    let turn = Turn::new(model, RunControl::new(), sample_ctx(), seed);

    assert_eq!(turn.ledger.len(), 2, "seed messages ride the turn verbatim");
    assert!(
        turn.tools.is_empty(),
        "no processor has injected tools into a fresh turn"
    );
    assert!(
        turn.artifacts.content.is_empty(),
        "no assistant content exists before the first model stream"
    );
    assert!(
        turn.artifacts.calls.is_empty(),
        "no tool calls exist before the first model stream"
    );
    assert!(
        turn.artifacts.results.is_empty(),
        "no tool results exist before the first dispatch"
    );
}

/// Purpose: retracting a message range must never leave a tool call behind
/// without its result — the compactor's pairing invariant, enforced by the
/// turn's own primitive instead of every caller's discipline.
/// Scenario: ledger = [task, assistant(tool call A), assistant(tool call B),
/// result A, result B]; retract the range covering only call A.
/// Acceptance: both call A and result A are gone (the range closed under
/// pairing), call B and result B survive.
#[test]
fn retract_never_orphans_a_tool_call() {
    let model = Arc::new(StubModel(sample_info()));
    let mut turn = Turn::new(model, RunControl::new(), sample_ctx(), Vec::new());

    let call_a = Message::Assistant {
        content: vec![Content::ToolCall(ToolCall {
            id: "call-a".into(),
            name: "grep".into(),
            args: serde_json::json!({}),
        })],
    };
    let call_b = Message::Assistant {
        content: vec![Content::ToolCall(ToolCall {
            id: "call-b".into(),
            name: "grep".into(),
            args: serde_json::json!({}),
        })],
    };
    let result_a = Message::ToolResult(ToolResultMessage {
        tool_call_id: "call-a".into(),
        tool_name: "grep".into(),
        content: "a".into(),
        is_error: false,
    });
    let result_b = Message::ToolResult(ToolResultMessage {
        tool_call_id: "call-b".into(),
        tool_name: "grep".into(),
        content: "b".into(),
        is_error: false,
    });
    turn.append_message(Message::user_text("task"));
    turn.append_message(call_a);
    turn.append_message(call_b);
    turn.append_message(result_a);
    turn.append_message(result_b);

    // Retract the range covering only call A — the result sits outside it.
    turn.retract_messages(1..2);

    let remaining_ids: Vec<String> = turn
        .ledger
        .iter()
        .flat_map(|message| match message {
            Message::Assistant { content } => content
                .iter()
                .filter_map(|block| match block {
                    Content::ToolCall(call) => Some(call.id.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            Message::ToolResult(result) => vec![result.tool_call_id.clone()],
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(
        remaining_ids,
        vec!["call-b".to_string(), "call-b".to_string()],
        "call A and its result leave together; B's pair survives intact"
    );
}
