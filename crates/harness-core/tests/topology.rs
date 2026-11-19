//! Topology shell (slice 1): the vocabulary types construct and stay out of
//! the legacy loop's way.
//!
//! Purpose: the shell exists so later slices implement processors against a
//! stable contract. These tests pin the constructor behavior that is real
//! today — seed passthrough, empty tool set and artifacts, node counting —
//! and deliberately touch no `todo!()` body: `drive`, `append_message`, and
//! `retract_messages` land with their slices.
//!
//! Acceptance: a `Turn` built over a stub model carries its seed ledger
//! verbatim and starts with no tools or artifacts; a `Topology` reports the
//! nodes registered into it.

use std::sync::Arc;

use async_trait::async_trait;

use harness_core::control::RunControl;
use harness_core::message::Message;
use harness_core::model::{Model, ModelInfo, ModelRequest, ModelStream};
use harness_core::run::{RunContext, RunId, RunLimits, SessionId, Task};
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
