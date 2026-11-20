//! The response-side node that appends assistant content and gates the run.

use std::sync::Arc;

use async_trait::async_trait;
use harness_core::message::Message;
use harness_core::run::RunStopReason;
use harness_core::topology::{Processor, Route, Turn};

use harness_core::transcript::{self, TranscriptSink};

/// Append the model's assistant content to the ledger, then gate the run:
/// no tool calls means the model answered the user, so the run completes.
pub struct AppendProcessor {
    transcript_sink: Option<Arc<dyn TranscriptSink>>,
}

impl AppendProcessor {
    pub fn new(transcript_sink: Option<Arc<dyn TranscriptSink>>) -> Self {
        Self { transcript_sink }
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
            self.transcript_sink.as_deref(),
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
