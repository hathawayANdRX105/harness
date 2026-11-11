//! Budget type: used vs available tokens and the ratio that drives
//! compaction decisions.
//!
//! Design provenance: harness-rs's `Budget`
//! (`crates/harness-core/src/compactor.rs`), simplified to a pair of
//! token counters with a derived ratio. The budget is read-only after
//! construction — a `Compactor::read_budget` builds it and stages compare
//! against it.

use serde::{Deserialize, Serialize};

/// Memory budget: how many tokens are used vs available.
///
/// The compaction system reads the budget before deciding whether and how
/// aggressively to compact. A ratio near or above 1.0 means the context
/// is at capacity and one or more compaction stages should activate.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Budget {
    /// Tokens consumed by the current message ledger.
    pub used_tokens: u64,
    /// Tokens the model's context window (or a caller-defined ceiling)
    /// makes available.
    pub available_tokens: u64,
}

impl Budget {
    /// The fraction of the budget consumed, bounded to [0, ∞).
    ///
    /// Returns `f64::INFINITY` when `available_tokens` is zero (no room
    /// at all), so every threshold comparison activates.
    pub fn ratio(&self) -> f64 {
        if self.available_tokens == 0 {
            return f64::INFINITY;
        }
        self.used_tokens as f64 / self.available_tokens as f64
    }

    /// Whether the consumed ratio exceeds `threshold`.
    pub fn is_over(&self, threshold: f64) -> bool {
        self.ratio() > threshold
    }

    /// Compute the token surplus (available − used), saturated to zero.
    pub fn surplus(&self) -> u64 {
        self.available_tokens.saturating_sub(self.used_tokens)
    }
}
