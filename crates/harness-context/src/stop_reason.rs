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
