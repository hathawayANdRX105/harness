//! Integration tests for `harness_context::Budget` through the public API.
//!
//! Purpose: pin the budget's derived values: ratio, is_over, surplus.
//! Scenario: construct Budget with various used/available token counts.
//! Acceptance: ratio equals used/available, zero available is infinite,
//! surplus saturates at zero.

use harness_context::Budget;

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
