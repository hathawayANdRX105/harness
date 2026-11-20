//! Chain tests: registration and the routing edges.
//!
//! Purpose: the chain is the assembly surface, so its contract is pinned
//! here — registration counting, `Goto` routing and its failure mode.
//!
//! Acceptance: `Chain::node` registrations are counted honestly; `Goto`
//! jumps to a named node and an unknown name fails the run loudly.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;

use harness_core::control::RunControl;
use harness_core::message::{Content, Message};
use harness_core::model::{Model, ModelInfo, ModelRequest, ModelStream};
use harness_core::run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
use harness_core::topology::{Processor, Route, Turn};
use harness_loop::Chain;

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

fn stub_turn() -> Turn {
    Turn::new(
        Arc::new(StubModel(sample_info())),
        RunControl::new(),
        sample_ctx(),
        Vec::new(),
    )
}

/// Purpose: `Chain::node` is the registration surface the driver assembles
/// the line with, so its counting must be honest from day one.
/// Scenario: register two stub processors into a fresh chain; also check
/// the empty default.
/// Acceptance: `len` and `is_empty` reflect the registrations.
#[test]
fn chain_counts_registered_nodes() {
    let chain = Chain::new()
        .node(Arc::new(StubProcessor))
        .node(Arc::new(StubProcessor));

    assert_eq!(chain.len(), 2, "both registered nodes are counted");
    assert!(
        !chain.is_empty(),
        "a chain with registered nodes reports non-empty"
    );
    assert!(
        Chain::new().is_empty(),
        "a chain with no registrations reports empty"
    );
}

/// Purpose: `Route::Goto` is the conditional branch — a node jumps to a
/// named sibling and machining continues there.
/// Scenario: three nodes — a node that appends a marker, a routing node
/// that `Goto`s back to the marker, and a tail node behind them. The
/// routing node jumps backwards once, then lets the pass continue.
/// Acceptance: the marker lands twice (the jump revisited the first node)
/// and the pass reaches the final node.
#[test]
fn goto_jumps_to_a_named_node() {
    futures::executor::block_on(goto_jumps_to_a_named_node_async());
}

async fn goto_jumps_to_a_named_node_async() {
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
    let chain = Chain::new()
        .node(Arc::new(Marker))
        .node(Arc::new(Router {
            jumped: Arc::clone(&jumped),
        }))
        .node(Arc::new(Tail));

    let mut turn = stub_turn();
    let route = chain.step(&mut turn).await;

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

/// Purpose: a `Goto` to a name nothing registered is a chain authoring
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

    let chain = Chain::new().node(Arc::new(Lost));
    let mut turn = stub_turn();

    match chain.step(&mut turn).await {
        Route::Done(RunStopReason::Error { message }) => {
            assert!(
                message.contains("nowhere"),
                "the error names the unknown target: {message}"
            );
        }
        _ => panic!("an unknown Goto must fail the run"),
    }
}
