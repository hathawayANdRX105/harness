//! Retry and backoff policy for model API calls.
//!
//! Defines a `RetryPolicy` with exponential backoff and full-jitter — the
//! strategy that the `OpenAIModel` adapter applies before emitting a
//! terminal `Error` event.
//!
//! Design provenance: harness-rs's `RetryConfig`
//! (`crates/harness-models/src/backoff.rs`). Full jitter (`rand(0, cap)`)
//! follows the AWS-exponential-backoff recommended form for distributed
//! clients.
//!
//! # Builder boundary
//!
//! This crate defines the type. The builder crate (#10) reads `RetryPolicy`
//! from config and passes it to the model adapter.

use rand::Rng;
use std::time::Duration;

/// Exponential-backoff retry policy with full jitter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts before the adapter gives up and
    /// emits a terminal `Error` event. Zero means no retries.
    pub max_retries: u32,
    /// Initial backoff duration in milliseconds (the base for the
    /// exponential growth).
    pub initial_backoff_ms: u64,
    /// Maximum backoff cap in milliseconds — the delay never exceeds this
    /// value regardless of the retry count.
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 30_000,
        }
    }
}

impl RetryPolicy {
    /// Compute the backoff delay for the `attempt`-th retry (0-based).
    ///
    /// Uses **full jitter**: `delay = rand(0, min(cap, initial * 2^attempt))`.
    /// This spreads retry traffic across the window and avoids the thundering
    /// herd problem that equal-interval retries cause.
    pub fn delay(&self, attempt: u32) -> Duration {
        // 2^attempt with a safety ceiling — u64 can hold 2^63 before overflow
        let exponent = self
            .initial_backoff_ms
            .saturating_mul(1u64 << attempt.min(63));
        let cap = exponent.min(self.max_backoff_ms);
        // Full jitter: pick uniformly from [0, cap]
        let jitter = rand::thread_rng().gen_range(0..=cap);
        Duration::from_millis(jitter)
    }
}
