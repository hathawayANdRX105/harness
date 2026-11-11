//! Exponential-backoff reconnect supervisor for MCP connections.
//!
//! [`McpReconnect`] is a transparent retry layer: it retries the same
//! transport on transport-level failures ([`McpError::Transport`]) with
//! exponential backoff. It does **not** know how to re-establish a new
//! connection (no re-spawn logic) — that wiring is the daemon's job.
//! The practical effect: a hung stdio child whose pipes have closed returns
//! `Transport("server closed stdout")` on every retry and the wrapper gives
//! up after `max_attempts`, surfacing the last error.

use crate::error::McpError;
use crate::transport::McpTransport;
use rand::Rng;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread::sleep;
use std::time::Duration;

/// Exponential-backoff parameters.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Initial delay before the first retry, in ms.
    pub base_delay_ms: u64,
    /// Upper bound for the exponential backoff delay, in ms.
    pub max_delay_ms: u64,
    /// Maximum number of retry attempts.
    pub max_attempts: u32,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            base_delay_ms: 100,
            max_delay_ms: 10_000,
            max_attempts: 5,
        }
    }
}

/// Backoff delay for a given attempt number (0-based).
///
/// Full jitter: uniform in `[0, min(max_delay_ms, base_delay_ms * 2^attempt)]`,
/// matching `harness-model`'s `RetryPolicy::delay` — spreads reconnect
/// traffic instead of hammering the server in a thundering herd.
pub fn backoff_delay(attempt: u32, policy: &ReconnectPolicy) -> Duration {
    let exponent = policy.base_delay_ms.saturating_mul(1u64 << attempt.min(63));
    let cap = exponent.min(policy.max_delay_ms);
    let jitter = rand::thread_rng().gen_range(0..=cap);
    Duration::from_millis(jitter)
}

/// Retry wrapper around any [`McpTransport`].
///
/// Only retries [`McpError::Transport`] variants — protocol errors, server
/// errors, and timeouts are passed through immediately.
pub struct McpReconnect {
    inner: Arc<dyn McpTransport>,
    policy: ReconnectPolicy,
}

impl McpReconnect {
    pub fn new(transport: Arc<dyn McpTransport>, policy: ReconnectPolicy) -> Self {
        Self {
            inner: transport,
            policy,
        }
    }

    /// The wrapped transport, for inspection (tests, daemon wiring).
    pub fn inner(&self) -> &Arc<dyn McpTransport> {
        &self.inner
    }

    /// The policy in effect.
    pub fn policy(&self) -> &ReconnectPolicy {
        &self.policy
    }

    /// `Timeout` is deliberately not retryable: the request already burned
    /// its whole timeout budget, so five retries would stretch one call to
    /// several minutes. A dead link ([`McpError::Transport`]) fails fast and
    /// identically on retry, so only that is worth the backoff.
    fn is_retryable(e: &McpError) -> bool {
        matches!(e, McpError::Transport(_))
    }
}

impl McpTransport for McpReconnect {
    fn next_id(&self) -> u64 {
        self.inner.next_id()
    }

    fn notify(&self, line: &str) -> Result<(), McpError> {
        self.retry(|_| self.inner.notify(line))
    }

    fn roundtrip(&self, id: u64, line: &str, signal: &AtomicBool) -> Result<String, McpError> {
        self.retry(|_| self.inner.roundtrip(id, line, signal))
    }
}

impl McpReconnect {
    /// The retry walk both transport methods share: attempt, sleep the
    /// backoff curve between attempts, and give up immediately on an error
    /// that is not a dead link.
    fn retry<T, F>(&self, op: F) -> Result<T, McpError>
    where
        F: Fn(u32) -> Result<T, McpError>,
    {
        let mut last_err = McpError::Transport("no attempt made".into());
        for attempt in 1..=self.policy.max_attempts {
            if attempt > 1 {
                sleep(backoff_delay(attempt - 2, &self.policy));
            }
            match op(attempt) {
                Ok(value) => return Ok(value),
                Err(e) if Self::is_retryable(&e) => last_err = e,
                Err(e) => return Err(e),
            }
        }
        Err(last_err)
    }
}
