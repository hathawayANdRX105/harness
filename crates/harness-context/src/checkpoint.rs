//! Checkpoint for cancellable, resumable compaction.
//!
//! Design provenance: pi's durable context layer
//! (`packages/durable/src/harness/context.ts`) models compaction as a
//! cancellable Task — the Task writes a checkpoint at each stage boundary,
//! and a resumed run loads the checkpoint to avoid re-compacting already
//! summarized portions. This type captures the same state: which stages
//! have been applied and a cursor into the message list.

use serde::{Deserialize, Serialize};

/// A snapshot of compaction progress, for cancellation and resume.
///
/// When compaction is cancelled (via the `CancellationToken` passed to
/// [`Compactor::compact`](crate::Compactor::compact)), the returned
/// [`CompactionResult`](crate::CompactionResult) carries a checkpoint.
/// A later call passes it as `checkpoint` to skip already-applied stages
/// and resume from where compaction left off.
///
/// # Fields
///
/// * `applied_stages` — labels of stages already completed, in order.
/// * `summary_at` — index into the (compacted) message list; messages
///   before this point have been summarized or dropped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionCheckpoint {
    /// The labels of stages already applied, in order.
    pub applied_stages: Vec<String>,
    /// Index into the (compacted) message list — messages before this
    /// have been summarized or dropped.
    pub summary_at: usize,
}

impl CompactionCheckpoint {
    /// A fresh checkpoint: no stages applied, cursor at the start.
    pub fn fresh() -> Self {
        Self {
            applied_stages: Vec::new(),
            summary_at: 0,
        }
    }

    /// Whether no stages have been applied yet.
    pub fn is_fresh(&self) -> bool {
        self.applied_stages.is_empty() && self.summary_at == 0
    }
}
