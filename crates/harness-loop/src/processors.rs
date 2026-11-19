//! Prompt-side processors: what the legacy `prepare_request` hook did, now
//! as `Processor` nodes the topology machines before the model call.
//!
//! Behavior is byte-for-byte the old `compose_prepare_hooks` composition:
//! the system ledger leads the transcript (slice 1 of the composition — it
//! must lead), then the declared instruction is appended once. Existing as
//! separate nodes rather than one closure pair so slice 4–5 can interleave
//! skill and retrieval nodes between them without re-plumbing.

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
