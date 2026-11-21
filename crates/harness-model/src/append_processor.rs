//! The response-side node that appends assistant content and gates the run.

use async_trait::async_trait;
use harness_core::message::Message;
use harness_core::run::RunStopReason;
use harness_core::topology::{Processor, Route, Turn};

use harness_core::transcript;

/// Append the model's assistant content to the ledger, then gate the run:
/// no tool calls means the model answered the user, so the run completes.
/// State-less: the transcript sink rides the turn (`RunSeams`).
pub struct AppendProcessor;

impl AppendProcessor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AppendProcessor {
    fn default() -> Self {
        Self
    }
}

#[async_trait]
impl Processor for AppendProcessor {
    fn name(&self) -> &'static str {
        "append"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        let content = std::mem::take(&mut turn.artifacts.content);
        transcript::push(
            &mut turn.ledger,
            turn.seams.transcript_sink.as_deref(),
            &turn.ctx.session_id,
            Message::Assistant { content },
        )
        .await;
        if turn.artifacts.calls.is_empty() {
            Route::Done(RunStopReason::Completed)
        } else {
            Route::Next
        }
    }
}
