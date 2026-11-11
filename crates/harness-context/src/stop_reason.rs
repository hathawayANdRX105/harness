//! The `BudgetBlocked` stop reason: a round refused before the provider
//! call, because the context budget cannot fit the next turn.
//!
//! Semantically distinct from `harness_core::run::RunStopReason::Error`:
//! a round blocked by budget did not fail — it was not attempted. Callers
//! counting errors (retry budgets, observability dashboards) should not
//! count `BudgetBlocked`.
//!
//! Design provenance: core commit a848aa51 removed budget vocabulary
//! from `RunStopReason` (the existing variants are Completed, Cancelled,
//! MaxTurns, Error). The budget-blocked variant belongs here, in the
//! crate that owns the compaction vocabulary.

use serde::{Deserialize, Serialize};

/// A round refused before the provider call because the context could not
/// fit the next turn even after compaction.
///
/// The loop matches this alongside [`harness_core::run::RunStopReason`]
/// when terminating a round — see the loop's round-dispatch for the
/// combined match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetBlocked {
    /// Tokens the next turn would need (estimated).
    pub needed: u64,
    /// Tokens available after the last compaction attempt.
    pub available: u64,
    /// Which compaction stage was reached before giving up.
    pub stage: String,
}

impl BudgetBlocked {
    /// Machine-readable message the loop emits when it refuses a round before
    /// the provider call: `budget blocked: needed={needed} available={available} stage={stage}`.
    ///
    /// Core's `RunStopReason` is frozen (`Completed`, `Cancelled`, `MaxTurns`,
    /// `Error { message }`), so the budget-blocked distinction travels inside
    /// `RunStopReason::Error { message }`. Callers recover it with
    /// [`from_error_message`]; a dedicated `RunStopReason::BudgetBlocked`
    /// variant is proposed as a maintainer-ratified follow-up.
    pub fn error_message(&self) -> String {
        format!(
            "budget blocked: needed={} available={} stage={}",
            self.needed, self.available, self.stage
        )
    }

    /// Parse a message produced by [`error_message`] back into a
    /// [`BudgetBlocked`], so the budget-blocked distinction survives the
    /// frozen core enum. Returns `None` for any other string.
    pub fn from_error_message(message: &str) -> Option<Self> {
        let mut parts = message
            .strip_prefix("budget blocked: ")?
            .split_ascii_whitespace();
        let needed = parts
            .next()
            .and_then(|part| part.strip_prefix("needed="))
            .and_then(|value| value.parse::<u64>().ok())?;
        let available = parts
            .next()
            .and_then(|part| part.strip_prefix("available="))
            .and_then(|value| value.parse::<u64>().ok())?;
        let stage = parts
            .next()
            .and_then(|part| part.strip_prefix("stage="))
            .map(|value| value.to_string())?;
        (parts.next().is_none()).then_some(Self {
            needed,
            available,
            stage,
        })
    }
}
