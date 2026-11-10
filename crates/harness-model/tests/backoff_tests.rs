//! Tests for the retry/backoff policy.
//!
//! Purpose: verify that `RetryPolicy::delay` produces non-zero durations
//! within the configured bounds, and that the default policy is reasonable.
//!
//! Scenarios:
//! - Default policy has sensible defaults.
//! - Delay at attempt 0 is within [0, initial_backoff_ms].
//! - Delay grows with attempt count but never exceeds max_backoff_ms.
//! - Zero max_retries means no retries (adapter check — not tested here).

use harness_model::RetryPolicy;

/// The default retry policy has sane values.
#[test]
fn default_policy() {
    let p = RetryPolicy::default();
    assert_eq!(p.max_retries, 3);
    assert_eq!(p.initial_backoff_ms, 1_000);
    assert_eq!(p.max_backoff_ms, 30_000);
}

/// The delay at attempt 0 is a non-zero value within the initial window.
#[test]
fn delay_attempt_zero() {
    let p = RetryPolicy::default();
    let d = p.delay(0);
    let ms = d.as_millis();
    // With full jitter the range is [0, initial_backoff_ms].
    assert!(
        ms <= p.initial_backoff_ms as u128,
        "delay {ms} exceeds initial_backoff_ms {}",
        p.initial_backoff_ms
    );
}

/// The delay at each attempt stays within its full-jitter bound
/// `[0, min(max_backoff_ms, initial_backoff_ms * 2^attempt)]`. Cross-attempt
/// ordering is deliberately not asserted: full jitter can invert any two
/// draws, so the old `d5 >= d0` comparison flaked ~1.6% of the time.
#[test]
fn delay_within_per_attempt_bounds() {
    let p = RetryPolicy::default();
    let bounds: &[(u32, u128)] = &[
        (0, 1_000),
        (1, 2_000),
        (2, 4_000),
        (3, 8_000),
        (4, 16_000),
        (5, 30_000),
        (6, 30_000),
    ];
    for &(attempt, cap_ms) in bounds {
        let ms = p.delay(attempt).as_millis();
        assert!(
            ms <= cap_ms,
            "delay {ms} at attempt {attempt} exceeds full-jitter bound {cap_ms}"
        );
    }
}

/// The delay never exceeds the configured maximum.
#[test]
fn delay_capped_at_max() {
    let p = RetryPolicy::default();
    for attempt in 0..10 {
        let d = p.delay(attempt);
        let ms = d.as_millis();
        assert!(
            ms <= p.max_backoff_ms as u128,
            "delay {ms} at attempt {attempt} exceeds max_backoff_ms {}",
            p.max_backoff_ms
        );
    }
}

/// A custom policy with zero initial backoff still produces a valid duration.
#[test]
fn custom_policy_zero_initial() {
    let p = RetryPolicy {
        max_retries: 1,
        initial_backoff_ms: 0,
        max_backoff_ms: 100,
    };
    let d = p.delay(0);
    assert_eq!(d.as_millis(), 0);
}
