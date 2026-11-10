//! Compaction stages: multi-level aggressiveness driven by budget ratios.
//!
//! Design provenance: harness-rs's `CompactionStage`
//! (`crates/harness-core/src/compactor.rs`) — five levels at 60/70/80/90/95%
//! thresholds. pi's coding-agent compaction
//! (`packages/durable/src/harness/compaction.ts`) uses the same staged
//! approach but derives thresholds from the model's context window; the
//! default ladder here is the harness-rs original.

use serde::{Deserialize, Serialize};

use crate::budget::Budget;

/// One level of compaction aggressiveness.
///
/// Each stage activates when the budget ratio exceeds [`threshold`], and
/// aims to compact until the ratio drops to [`target_ratio`]. Stages are
/// tried in order: soft stages first, more aggressive ones only when
/// budget pressure persists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionStage {
    /// Budget ratio that triggers this stage (0.0 – 1.0).
    pub threshold: f64,
    /// Target ratio after compaction (0.0 – 1.0).
    pub target_ratio: f64,
    /// Human-readable label used in logs, errors, and checkpoint tracking.
    pub label: String,
}

impl CompactionStage {
    /// Whether this stage's threshold is exceeded by `budget`.
    pub fn is_active(&self, budget: &Budget) -> bool {
        budget.is_over(self.threshold)
    }
}

/// The default five-stage compaction ladder from harness-rs.
///
/// | Stage    | Threshold | Target | Description        |
/// |----------|-----------|--------|--------------------|
/// | soft     | 60%       | 50%    | Mild compression   |
/// | medium   | 70%       | 50%    | Moderate           |
/// | firm     | 80%       | 45%    | Strong             |
/// | strong   | 90%       | 40%    | Very strong        |
/// | maximal  | 95%       | 30%    | Drop everything    |
pub fn default_stages() -> Vec<CompactionStage> {
    vec![
        CompactionStage {
            threshold: 0.60,
            target_ratio: 0.50,
            label: "soft".into(),
        },
        CompactionStage {
            threshold: 0.70,
            target_ratio: 0.50,
            label: "medium".into(),
        },
        CompactionStage {
            threshold: 0.80,
            target_ratio: 0.45,
            label: "firm".into(),
        },
        CompactionStage {
            threshold: 0.90,
            target_ratio: 0.40,
            label: "strong".into(),
        },
        CompactionStage {
            threshold: 0.95,
            target_ratio: 0.30,
            label: "maximal".into(),
        },
    ]
}

#[cfg(test)]
mod stage_tests {
    use super::*;

    /// Purpose: `is_active` returns true when budget ratio exceeds threshold.
    /// Scenario: 80% budget against 60% threshold.
    /// Acceptance: is_active is true.
    #[test]
    fn stage_activates_when_budget_exceeds_threshold() {
        let budget = Budget {
            used_tokens: 80,
            available_tokens: 100,
        };
        let stage = CompactionStage {
            threshold: 0.60,
            target_ratio: 0.50,
            label: "test".into(),
        };
        assert!(stage.is_active(&budget));
    }

    /// Purpose: `is_active` returns false when budget is below threshold.
    /// Scenario: 50% budget against 60% threshold.
    /// Acceptance: is_active is false.
    #[test]
    fn stage_inactive_when_budget_below_threshold() {
        let budget = Budget {
            used_tokens: 50,
            available_tokens: 100,
        };
        let stage = CompactionStage {
            threshold: 0.60,
            target_ratio: 0.50,
            label: "test".into(),
        };
        assert!(!stage.is_active(&budget));
    }

    /// Purpose: the default ladder has five stages in order.
    /// Scenario: call default_stages().
    /// Acceptance: returns five stages with increasing thresholds.
    #[test]
    fn default_ladder_has_five_stages() {
        let stages = default_stages();
        assert_eq!(stages.len(), 5);
        for window in stages.windows(2) {
            assert!(
                window[0].threshold < window[1].threshold,
                "stages must be in increasing threshold order"
            );
        }
    }
}
