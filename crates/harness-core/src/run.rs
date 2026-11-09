//! Run identity, limits, and terminal reasons.
//!
//! Design provenance: nanoharness's `RunContext` vs `RunCheckpoint` split
//! (`core/schema.py:193`) — identity is immutable and carried by every event;
//! snapshots/resumption belong to harness-session (#12). `RunLimits` and the
//! max-turns stop reason come from harness-rs's `Policy`
//! (`crates/harness-core/src/context.rs:104`).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// Identifier of one run: unique within a daemon, carried by every `RunEvent`
/// and every `ToolRequest` so any component can correlate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub String);

/// Identifier of the conversation a run belongs to. Multiple runs may share
/// one session (resume, compaction, steering).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(pub String);

impl Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for RunId {
    fn from(s: String) -> Self {
        RunId(s)
    }
}

impl From<String> for SessionId {
    fn from(s: String) -> Self {
        SessionId(s)
    }
}

/// What the agent is working on. Carried by `RunContext` so a resumed or
/// compacted transcript still knows what it was asked to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub description: String,
    /// Where the task came from (a user message, an issue URL, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Unix epoch seconds, when the task must be finished by.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<i64>,
}

/// Resource ceiling for one run. `Default` mirrors harness-rs's `Policy`
/// defaults (50 iters, 150k input, 8k output) — enough for a coding session
/// without unbounded spend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLimits {
    pub max_turns: u32,
    pub max_input_tokens: u32,
    pub max_output_tokens: u32,
}

impl Default for RunLimits {
    fn default() -> Self {
        Self {
            max_turns: 50,
            max_input_tokens: 150_000,
            max_output_tokens: 8_000,
        }
    }
}

/// Immutable identity of one run. The daemon builds it per run; the loop
/// components receive it read-only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunContext {
    pub run_id: RunId,
    pub session_id: SessionId,
    pub task: Task,
    #[serde(default)]
    pub limits: RunLimits,
    /// Unix epoch milliseconds when the run started.
    pub started_at_ms: i64,
}

/// Why a run ended, at the loop level. Every terminal path a loop can take
/// has its own variant so a billing/observing host can tell them apart —
/// the distinction is the same one nanoharness makes between `RUN_COMPLETED`,
/// `RUN_CANCELLED` and `MAX_STEPS`.
///
/// No budget-blocked variant (yet): a round refused before a provider call
/// is distinct from an error, but budget vocabulary belongs to
/// harness-context (#5), which defines the stop reason when it lands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunStopReason {
    /// The model ended the turn naturally and no further turn was requested.
    Completed,
    /// `RunControl::cancel` fired — the caller ended the run, nothing broke.
    Cancelled,
    /// `RunLimits::max_turns` round-trips exhausted.
    MaxTurns,
    /// A failure encoded as a fatal event (see the stream-fn contract).
    Error { message: String },
}
