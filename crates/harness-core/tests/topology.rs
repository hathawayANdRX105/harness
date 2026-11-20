//! Topology vocabulary tests: construction, the ledger primitives, and the
//! routing edges.
//!
//! Purpose: the types are the contract every run-side crate builds on, so
//! their invariants are pinned here — seed passthrough, pairing-safe
//! retract, `Goto` routing and its failure mode.
//!
//! Acceptance: a `Turn` built over a stub model carries its seed ledger
//! verbatim; `retract_messages` never orphans a tool call from its result;
//! `Goto` jumps to a named node and an unknown name fails the run loudly.

use std::sync::Arc;

use async_trait::async_trait;

use harness_core::control::RunControl;
use harness_core::message::{Content, Message, ToolCall, ToolResultMessage};
use harness_core::model::{Model, ModelInfo, ModelRequest, ModelStream};
use harness_core::run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
use harness_core::topology::{Processor, Route, Topology, Turn};

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

struct StubProcessor;

#[async_trait]
impl Processor for StubProcessor {
    fn name(&self) -> &'static str {
        "stub"
    }

    async fn process(&self, _turn: &mut Turn) -> Route {
        Route::Next
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

/// Purpose: `Topology::node` is the registration surface the later slices
/// build the DAG with, so its counting must be honest from day one.
/// Scenario: register two stub processors into a fresh topology; also check
/// the empty default.
/// Acceptance: `len` and `is_empty` reflect the registrations.
#[test]
fn topology_counts_registered_nodes() {
    let topology = Topology::new()
        .node(Arc::new(StubProcessor))
        .node(Arc::new(StubProcessor));

    assert_eq!(topology.len(), 2, "both registered nodes are counted");
    assert!(
        !topology.is_empty(),
        "a topology with registered nodes reports non-empty"
    );
    assert!(
        Topology::new().is_empty(),
        "a topology with no registrations reports empty"
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

/// Purpose: `Route::Goto` is the conditional branch — a node jumps to a
/// named sibling and machining continues there.
/// Scenario: three nodes — a node that appends a marker, a routing node
/// that `Goto`s back to the marker, and a counter node behind them. The
/// routing node jumps backwards once, then lets the pass continue.
/// Acceptance: the marker lands twice (the jump revisited the first node)
/// and the pass reaches the final node.
#[test]
fn goto_jumps_to_a_named_node() {
    futures::executor::block_on(goto_jumps_to_a_named_node_async());
}

async fn goto_jumps_to_a_named_node_async() {
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Marker;
    struct Router {
        jumped: Arc<AtomicU32>,
    }
    struct Tail;

    #[async_trait]
    impl Processor for Marker {
        fn name(&self) -> &'static str {
            "marker"
        }

        async fn process(&self, turn: &mut Turn) -> Route {
            turn.append_message(Message::user_text("marker"));
            Route::Next
        }
    }

    #[async_trait]
    impl Processor for Router {
        fn name(&self) -> &'static str {
            "router"
        }

        async fn process(&self, _turn: &mut Turn) -> Route {
            if self.jumped.fetch_add(1, Ordering::SeqCst) == 0 {
                Route::Goto("marker")
            } else {
                Route::Next
            }
        }
    }

    #[async_trait]
    impl Processor for Tail {
        fn name(&self) -> &'static str {
            "tail"
        }

        async fn process(&self, _turn: &mut Turn) -> Route {
            Route::Next
        }
    }

    let jumped = Arc::new(AtomicU32::new(0));
    let topology = Topology::new()
        .node(Arc::new(Marker))
        .node(Arc::new(Router {
            jumped: Arc::clone(&jumped),
        }))
        .node(Arc::new(Tail));

    let model = Arc::new(StubModel(sample_info()));
    let mut turn = Turn::new(model, RunControl::new(), sample_ctx(), Vec::new());
    let route = topology.step(&mut turn).await;

    assert!(matches!(route, Route::Next), "the pass completes");
    assert_eq!(
        jumped.load(Ordering::SeqCst),
        2,
        "the router ran once forward, once after the jump"
    );
    let markers = turn
        .ledger
        .iter()
        .filter(|message| {
            matches!(
                message,
                Message::User { content } if content.iter().any(|block| {
                    matches!(block, Content::Text(text) if text == "marker")
                })
            )
        })
        .count();
    assert_eq!(markers, 2, "the jump revisited the marker node");
}

/// Purpose: a `Goto` to a name nothing registered is a topology authoring
/// bug, and the run fails loudly instead of silently skipping the route.
/// Scenario: one node that `Goto`s a name that was never registered.
/// Acceptance: the pass ends with `Route::Done(Error)` naming the target.
#[test]
fn goto_to_unknown_node_fails_loudly() {
    futures::executor::block_on(goto_to_unknown_node_fails_loudly_async());
}

async fn goto_to_unknown_node_fails_loudly_async() {
    struct Lost;

    #[async_trait]
    impl Processor for Lost {
        fn name(&self) -> &'static str {
            "lost"
        }

        async fn process(&self, _turn: &mut Turn) -> Route {
            Route::Goto("nowhere")
        }
    }

    let topology = Topology::new().node(Arc::new(Lost));
    let model = Arc::new(StubModel(sample_info()));
    let mut turn = Turn::new(model, RunControl::new(), sample_ctx(), Vec::new());

    match topology.step(&mut turn).await {
        Route::Done(RunStopReason::Error { message }) => {
            assert!(
                message.contains("nowhere"),
                "the error names the unknown target: {message}"
            );
        }
        _ => panic!("an unknown Goto must fail the run"),
    }
}
