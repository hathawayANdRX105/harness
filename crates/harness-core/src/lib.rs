//! Harness core: the run lifecycle, message model, model stream contract,
//! tool contract, permission seams, ordered run events and run control —
//! the vocabulary every harness crate and every host seam consumes.
//!
//! # Design provenance
//!
//! This crate is **designed from three reference projects, by concept, not
//! ported** (the earlier direct port of the old harness was discarded):
//!
//! - **pi** (TypeScript) contributes the failure discipline — the stream-fn
//!   contract: streams never reject, failures are terminal events
//!   ([`model::ModelEvent::Error`]) — the item type carries no `Result`, so
//!   failure-as-error does not compile; the terminal discipline itself
//!   (nothing after `Stop`/`Error`) is on the adapter and documented; failures are values
//!   (`is_error` on [`tool::ToolResult`] and [`message::ToolResultMessage`]);
//!   and the message model, where the system prompt is a replayable
//!   *ledger* of named sections ([`message::Message::System`]) — how a
//!   daemon resumes a session without a second session-config object.
//!   (Section *removal* and tool-set mutations are the parts of pi's
//!   ledger we have not modeled yet — see issue #2 review.)
//! - **nanoharness** (Python) contributes the run-level surface —
//!   [`control::RunControl`] (await-able cancellation plus a steering
//!   mailbox), the ordered run event log with a sequence counter
//!   ([`event::RunEvent`]), the two-stage tool policy with
//!   `context_injection`/`output_suffix` payloads ([`policy`]), and the
//!   approval broker as its own seam — which we make `async` and
//!   cancellable, because nanoharness's synchronous broker would freeze a
//!   daemon's loop until the human answers.
//! - **harness-rs** (Rust) contributes the tool vocabulary —
//!   [`tool::ToolRisk`] (MCP-aligned, drives sandbox and permission
//!   decisions), the streaming tool-call delta shape, and `ModelInfo`
//!   catalog data — without its `Result`-wrapped deltas, its `DynModel`
//!   newtype, or its proc-macro/inventory registration (tools are assembled
//!   at runtime by the builder).
//!
//! # What is deliberately *not* here
//!
//! - Prompt assembly beyond the message ledger (compaction vocabulary and
//!   budget thresholds live in harness-context, #5).
//! - Session row shapes and persistence (harness-session, #12).
//! - Wire framing / JSON-RPC helpers (harness-transport, #9).
//! - `ToolRegistry`/`ToolCatalog` (the builder holds `Vec<Arc<dyn Tool>>`;
//!   one implementor does not justify a trait).
//! - Hook/Guide/Sensor traits (loop extension points are the loop's
//!   concern, #8; a hook system with string stages is nanoharness's
//!   weakest seam and is not repeated).
//!
//! # Dependency discipline
//!
//! `serde` / `serde_json` / `async-trait` / `futures-core` / `parking_lot`
//! / `thiserror` / `tokio-util` (only `sync::CancellationToken` — no
//! runtime, no reactor, no IO). Core never reads the environment and never
//! grows a dependency on an implementation crate: it is the leaf every
//! other harness crate stands on.

// Crate-root re-exports: the names every host seam uses bare.
pub use control::{RunControl, Steer};
pub use event::{EventSink, RunEvent, RunEventKind};
pub use message::{Content, Message, Role, StopReason, ToolCall, ToolResultMessage};
pub use model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream, Usage};
pub use policy::{
    ApprovalBroker, ApprovalResult, ExecutionStatus, PolicyDecision, PolicyOutcome, PolicyStage,
    ToolExecution, ToolPolicy, unattended_denied,
};
pub use run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
pub use tool::{Tool, ToolError, ToolRequest, ToolResult, ToolRisk, ToolSchema};

pub mod control;
pub mod event;
pub mod message;
pub mod model;
pub mod policy;
pub mod run;
pub mod tool;
