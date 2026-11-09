//! The ordered run event log: the daemon's wire contract, not a callback.
//!
//! Design provenance: nanoharness's `HarnessEvent` (`core/schema.py:241`) —
//! every event carries `run_id`, `session_id`, a monotonic `sequence` and a
//! timestamp, which makes the stream a *causally ordered log* a transport
//! can replay, persist or fan out. The kinds follow pi's two-layer split
//! (`packages/agent/src/types.ts:528-551`): provider-level deltas are
//! re-anchored onto run-level kinds (`TextDelta` rides `RunEvent` the way
//! `AssistantMessageEvent` rides pi's `message_update`) instead of inventing
//! a second vocabulary.
//!
//! Format caveat: `RunEvent` flattens its tagged `kind` onto the top-level
//! `type` tag, which round-trips through serde_json (the daemon's wire
//! format) but not through non-self-describing formats, and forbids ever
//! adding `deny_unknown_fields`. That is the price of the envelope shape
//! subscribers parse with `ev["type"]`.
//!
//! The trap this design exists to avoid: pi's `Agent` awaits every event
//! listener sequentially in the loop's critical path
//! (`packages/agent/src/agent.ts:619-621`), so one slow subscriber stalls
//! token streaming, tool execution and steering. [`EventSink::emit`] is
//! synchronous *by contract*: implementations push into a bounded channel
//! with `try_send` and a documented overflow policy — subscriber lag is
//! observable, never blocking.

use serde::{Deserialize, Serialize};

use crate::message::ToolCall;
use crate::policy::{ApprovalResult, PolicyOutcome, PolicyStage};
use crate::run::{RunId, RunStopReason, SessionId};

/// One entry in a run's ordered event log.
///
/// `seq` is monotonic per run and assigned by the emitter (the loop/scheduler
/// side, never the sink); a replay consumer uses it to detect gaps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEvent {
    pub run_id: RunId,
    pub session_id: SessionId,
    pub seq: u64,
    /// Unix epoch milliseconds.
    pub timestamp_ms: i64,
    /// Flattened onto the event itself: the discriminator a subscriber
    /// reads is `ev["type"]` (nanoharness's envelope shape), not a nested
    /// `ev["kind"]["type"]`.
    #[serde(flatten)]
    pub kind: RunEventKind,
}

/// The kinds of thing a run can report. `#[non_exhaustive]`: the compaction
/// and budget kinds land when harness-context (#5) is wired in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunEventKind {
    /// A run started: the task description, as the transport wants to show it.
    RunStarted { task: String },
    /// A run ended, for any terminal reason. See [`RunStopReason`].
    RunFinished { reason: RunStopReason },
    /// Steering message(s) were drained and will be replayed as user turns
    /// before the next model call (nanoharness's `STEERING_APPLIED`).
    SteeringApplied { message: String },
    /// One model round-trip begins.
    TurnStarted { turn: u32 },
    /// One model round-trip ended (stop reason at the model level).
    TurnFinished { turn: u32 },
    /// Streaming assistant text fragment (pi's anchoring of provider deltas
    /// onto run-level events).
    TextDelta { text: String },
    /// A parsed tool call from the stream, before any policy decision.
    ToolCall(ToolCall),
    /// A tool call passed policy (and approval if required) and started
    /// executing.
    ToolStarted { call_id: String, name: String },
    /// A tool call finished. `is_error` mirrors the value-shaped failure —
    /// the model-facing text follows as the transcript's `ToolResult`.
    ToolFinished {
        call_id: String,
        name: String,
        is_error: bool,
    },
    /// A policy decided (either stage). The transport surfaces denials and
    /// approvals-to-ask from here; the outcome is the machine-readable bit.
    PolicyDecided {
        stage: PolicyStage,
        outcome: PolicyOutcome,
        reason: String,
    },
    /// The broker asked a human; the run is parked on this call until it
    /// answers or the run is cancelled.
    ApprovalRequested { call_id: String },
    /// The broker's answer landed.
    ApprovalResolved {
        call_id: String,
        result: ApprovalResult,
    },
    /// A failure encoded as an event — the loop-level twin of the model
    /// stream's terminal `Error`.
    Error { message: String },
}

/// The observer seam: implementations push into a bounded channel and never
/// block the loop (see the module docs on pi's stall trap).
///
/// Errors cannot come back through here: an overflowed channel drops or
/// metrics-logs per its documented policy, it does not propagate into the
/// run. A transport that needs backpressure gets it by being the loop's own
/// queue, not by being an `EventSink`.
pub trait EventSink: Send + Sync + 'static {
    fn emit(&self, event: RunEvent);
}
