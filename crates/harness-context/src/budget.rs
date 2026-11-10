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

#[cfg(test)]
mod budget_tests {
    use super::*;

    /// Purpose: the derived ratio reflects used/available.
    /// Scenario: half the budget consumed.
    /// Acceptance: ratio ≈ 0.5.
    #[test]
    fn ratio_half() {
        let b = Budget {
            used_tokens: 50,
            available_tokens: 100,
        };
        assert!((b.ratio() - 0.5).abs() < f64::EPSILON);
    }

    /// Purpose: a zero-available budget is infinite (activates every stage).
    /// Scenario: available_tokens = 0.
    /// Acceptance: ratio is INFINITY and is_over(1.0) is true.
    #[test]
    fn zero_available_is_infinite() {
        let b = Budget {
            used_tokens: 0,
            available_tokens: 0,
        };
        assert!(b.ratio().is_infinite());
        assert!(b.is_over(1.0));
    }

    /// Purpose: surplus is clamped to zero.
    /// Scenario: used exceeds available.
    /// Acceptance: surplus is zero.
    #[test]
    fn surplus_is_saturated() {
        let b = Budget {
            used_tokens: 150,
            available_tokens: 100,
        };
        assert_eq!(b.surplus(), 0);
    }

    /// Purpose: surplus matches available − used when there is room.
    /// Scenario: available > used.
    /// Acceptance: surplus = 50.
    #[test]
    fn surplus_positive() {
        let b = Budget {
            used_tokens: 50,
            available_tokens: 100,
        };
        assert_eq!(b.surplus(), 50);
    }
}
