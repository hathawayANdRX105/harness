//! The harness core: the trait hub and the vocabulary every harness crate and
//! every host seam consumes.
//!
//! What lives here:
//!
//! - [`tool`]: the tool contract (`Tool` / `ToolCatalog` / `ToolExecutor`),
//!   the abort signal, the tool-call spec. Exactly one tool trait at the
//!   bottom of the tree, so no adapter per seam.
//! - [`event`]: the runtime event vocabulary the loop emits (the frozen wire
//!   contract subscribers parse).
//! - [`model`]: the upstream model contract — one `Model` trait whose
//!   `stream` vocabulary encodes failures as terminal events (pi's
//!   stream-fn contract), so retry/abort/compaction all speak events.
//! - [`context`]: the assembled prompt — blocks, turns, task, budget.
//! - [`session`]: session/run/message shapes the daemon, the store and the
//!   wire protocol agree on.
//! - [`framing`]: JSON-RPC value framing shared by the wire protocol, the
//!   MCP client and any line-based peer.
//!
//! Dependency discipline: serde / serde_json / async-trait / futures-core /
//! parking_lot / thiserror only — no runtime, no IO framework, no storage.
//! This crate is the leaf every other harness crate stands on: nothing in
//! `harness-*` may reach over it for these vocabularies, and it may never
//! gain a dependency on an implementation crate.

// Crate-root re-exports: the names every host seam uses bare.
pub use context::{Block, Budget, Context, Task, Turn, TurnRole};
pub use event::{AgentEvent, TurnStop};
pub use model::{Model, ModelEvent, ModelRequest, ModelStream, StopReason, Usage};
pub use session::{Attachment, RunRecord, SessionMessage, SessionRole, SessionSummary};
pub use tool::{
    AbortSignal, Action, MAX_OUTPUT_LINES, Tool, ToolCallSpec, ToolCatalog, ToolError,
    ToolExecutor, ToolResult, ToolSpec, arg_str, spill_dir, truncate_output,
};

pub mod context;
pub mod event;
pub mod framing;
pub mod model;
pub mod session;
pub mod tool;
