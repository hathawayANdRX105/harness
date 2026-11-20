//! The prompt-side node that appends a declared instruction once.

use async_trait::async_trait;
use harness_core::message::Message;
use harness_core::topology::{Processor, Route, Turn};

/// Append the declared instruction as a user message once per transcript.
///
/// The `InjectInstruction` hook spec (harness-builder) materializes as one
/// of these nodes: idempotent — a transcript that already carries the
/// instruction (a resumed run) is left alone.
pub struct InstructionProcessor {
    instruction: Message,
}

impl InstructionProcessor {
    pub fn new(text: &str) -> Self {
        Self {
            instruction: Message::user_text(text),
        }
    }
}

#[async_trait]
impl Processor for InstructionProcessor {
    fn name(&self) -> &'static str {
        "instruction"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        if !turn.ledger.contains(&self.instruction) {
            turn.ledger.push(self.instruction.clone());
        }
        Route::Next
    }
}
