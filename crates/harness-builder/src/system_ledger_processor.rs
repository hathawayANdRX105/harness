//! The prompt-side node that seats the system ledger.

use async_trait::async_trait;
use harness_core::message::Message;
use harness_core::topology::{Processor, Route, Turn};

/// Ensure the system prompt ledger message leads the transcript.
///
/// The builder renders the spec's prompt sections into one `Message::System`
/// ledger; this node re-seats it at position 0 every turn so a compacted or
/// replayed transcript cannot drift it out of place.
pub struct SystemLedgerProcessor {
    prompt: Message,
}

impl SystemLedgerProcessor {
    pub fn new(prompt: Message) -> Self {
        Self { prompt }
    }
}

#[async_trait]
impl Processor for SystemLedgerProcessor {
    fn name(&self) -> &'static str {
        "system_ledger"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        if turn.ledger.first() != Some(&self.prompt) {
            turn.ledger.insert(0, self.prompt.clone());
        }
        Route::Next
    }
}
