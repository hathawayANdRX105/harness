//! The `Compactor` trait: budget assessment and cancellable compaction
//! of the message ledger.
//!
//! Design provenance: harness-rs's `Compactor`
//! (`crates/harness-core/src/compactor.rs`) contributes the split between
//! reading budget and performing compaction. pi's `DurableHarness`
//! (`packages/durable/src/harness/compaction.ts`) contributes the
//! cancellable, checkpointed execution model that the trait signature
//! encodes.

use async_trait::async_trait;
use harness_core::message::Message;
use harness_core::model::ModelInfo;
use tokio_util::sync::CancellationToken;

use crate::budget::Budget;
use crate::checkpoint::CompactionCheckpoint;
use crate::stage::CompactionStage;

/// Outcome of one compaction pass.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionResult {
    /// Token count before compaction.
    pub before: u64,
    /// Token count after compaction.
    pub after: u64,
    /// Checkpoint for resume on cancellation.
    pub checkpoint: CompactionCheckpoint,
    /// Whether compaction completed all intended stages.
    pub completed: bool,
}

impl CompactionResult {
    /// Tokens freed by this compaction pass.
    pub fn saved(&self) -> u64 {
        self.before.saturating_sub(self.after)
    }
}

/// A compaction strategy: assess budget and compact a message ledger.
///
/// # Pairing invariant
///
/// Implementations **must preserve** tool-call / tool-result pairing: if an
/// assistant message that contains `Content::ToolCall(...)` is retained, the
/// matching `Message::ToolResult(...)` must also be retained so the reply
/// chain remains intact. A summarized region replaces an entire paired block
/// — it never leaves an orphaned call or result behind.
///
/// # Cancellation and checkpointing
///
/// `compact` receives a `CancellationToken`. When cancellation fires, it
/// should finish the current message and return a partial
/// [`CompactionResult`] whose `completed` is `false` and whose
/// `checkpoint` the caller can pass to a later `compact` call to resume.
///
/// # Thread safety
///
/// `read_budget` is synchronous and cheap. `compact` is async and may hold
/// the `&mut Vec<Message>` borrow across yield points — callers serialize
/// concurrent compaction on the same ledger.
#[async_trait]
pub trait Compactor: Send + Sync + 'static {
    /// Assess the budget from a message ledger.
    ///
    /// Synchronous and cheap (token estimation heuristics or a cached
    /// count). The returned `Budget` determines which
    /// [`CompactionStage`](crate::stage::CompactionStage) thresholds are
    /// exceeded and whether compaction is needed at all.
    fn read_budget(&self, messages: &[Message], info: &ModelInfo) -> Budget;

    /// Run compaction on the message ledger.
    ///
    /// When `checkpoint` is `Some`, the implementation resumes from that
    /// point — stages already applied by the checkpoint are skipped.
    ///
    /// Cancellation via `cancel` may leave a partial compaction; the
    /// returned [`CompactionResult`] carries a checkpoint for a later call
    /// to resume from.
    async fn compact(
        &self,
        messages: &mut Vec<Message>,
        budget: &Budget,
        checkpoint: Option<&CompactionCheckpoint>,
        cancel: &CancellationToken,
    ) -> CompactionResult;

    /// Return the stages this compactor uses, in activation order.
    fn stages(&self) -> &[CompactionStage];
}
