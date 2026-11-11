//! Integration tests for `harness_context::CompactionStage` through the public
//! API.
//!
//! Purpose: pin stage activation and the default ladder ordering.
//! Scenario: compare budget ratios against stage thresholds.
//! Acceptance: is_active follows the threshold comparison; the default ladder
//! has five stages with increasing thresholds.

use harness_context::{Budget, CompactionStage, default_stages};

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
