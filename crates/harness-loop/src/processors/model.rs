//! The model node: the only processor that touches the per-token stream.

use std::sync::Arc;

use async_trait::async_trait;
use harness_core::event::{EventSink, RunEventKind};
use harness_core::model::ModelRequest;
use harness_core::run::RunStopReason;
use harness_core::topology::{Processor, Route, Turn};

use crate::dispatch::emit_event;
use crate::processors::pump::{StreamTurn, pump_stream};

/// The one node that holds the model stream.
///
/// Everything per-token lives in [`pump`]: building the request from the
/// ledger and tool set, racing the cancel token against the stream, folding
/// `ModelEvent`s into the turn's artifacts. The other nodes see only the
/// turn-boundary product (`TurnArtifacts`), never a raw event — that is the
/// hot-loop rule from `docs/topology-design.md` §5.
pub struct ModelProcessor {
    event_sink: Arc<dyn EventSink>,
}

impl ModelProcessor {
    pub fn new(event_sink: Arc<dyn EventSink>) -> Self {
        Self { event_sink }
    }
}

#[async_trait]
impl Processor for ModelProcessor {
    fn name(&self) -> &'static str {
        "model"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        // Claim the stream through an owned Arc so no borrow of `turn`
        // crosses the stream's lifetime.
        let model = Arc::clone(&turn.model);
        let request = ModelRequest {
            model: model.info().model.clone(),
            // One render per turn: the ledger is the source, the request is
            // the wire shape (same one-off clone the legacy `model_turn` did).
            messages: turn.ledger.clone(),
            tools: turn.tools.iter().map(|tool| tool.schema()).collect(),
            max_tokens: None,
            temperature: None,
        };
        emit_event(
            self.event_sink.as_ref(),
            &turn.ctx.run_id,
            &turn.ctx.session_id,
            &turn.seq,
            RunEventKind::TurnStarted {
                turn_idx: turn.turn_idx,
            },
        );
        let stream = model.stream(&request).await;
        let control = turn.control.clone();
        let outcome = pump_stream(
            stream,
            &turn.seq,
            self.event_sink.as_ref(),
            &turn.ctx,
            &control,
        )
        .await;
        // `TurnFinished` closes every turn that reached a boundary: the
        // cancelled path never saw a clean one (legacy behavior —
        // Cancelled skips the event, Ok and Error both emit).
        if !matches!(outcome, StreamTurn::Cancelled) {
            emit_event(
                self.event_sink.as_ref(),
                &turn.ctx.run_id,
                &turn.ctx.session_id,
                &turn.seq,
                RunEventKind::TurnFinished {
                    turn_idx: turn.turn_idx,
                },
            );
        }
        match outcome {
            StreamTurn::Ok { content, calls } => {
                turn.artifacts.content = content;
                turn.artifacts.calls = calls;
                Route::Next
            }
            StreamTurn::Cancelled => Route::Done(RunStopReason::Cancelled),
            StreamTurn::Error {
                message,
                partial_content,
                partial_calls,
            } => {
                turn.artifacts.content = partial_content;
                turn.artifacts.calls = partial_calls;
                Route::Done(RunStopReason::Error { message })
            }
        }
    }
}
