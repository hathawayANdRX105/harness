//! Three-channel transport layer for the harness daemon.
//!
//! This crate provides HTTP+SSE, WebSocket and Unix-domain-socket transport
//! servers that share a common **parse → dispatch → push** path:
//!
//! 1. A transport client sends a JSON-RPC 2.0 request (or a `/cmd` slash line).
//! 2. The request is parsed into a [`SlashCommand`] via [`parse_slash`] or
//!    [`command_from_jsonrpc`].
//! 3. The command is dispatched through the [`CommandHandler`] seam, which the
//!    daemon (#13) implements with the real run-loop operations.
//! 4. The result is sent back as a JSON-RPC 2.0 response.
//! 5. Concurrently, [`RunEvent`]s produced by the run loop are fanned out
//!    through a `broadcast::Sender` to every connected transport client.
//!
//! # Seams
//!
//! | Trait | Purpose | Implemented by |
//! |---|---|---|
//! | [`CommandHandler`] | Maps a parsed [`SlashCommand`] to a JSON-RPC response value | Daemon (#13) |
//! | [`EventLog`] | Reads persisted events by sequence number for replay | Scheduler/session (#11) |
//!
//! # Transport modules
//!
//! - [`http`] — HTTP+SSE: `POST /command` for JSON-RPC, `GET /events` for SSE.
//! - [`ws`] — WebSocket bidirectional JSON-RPC with event push.
//! - [`uds`] — Unix-domain-socket JSON-RPC over NDJSON (newline-delimited JSON).
//!
//! # Design provenance
//!
//! - The JSON-RPC 2.0 frame format follows the JSON-RPC specification
//!   (<https://www.jsonrpc.org/specification>).
//! - The typed error frame design comes from harness-rs
//!   (`crates/harness-serve/src/http.rs`): errors are structured values
//!   with numeric codes, not bare strings.
//! - The three-transport abstraction follows pi's transport interface
//!   (`packages/mcp/src/transports/`) where small transport implementations
//!   sit behind a shared dispatch surface.
//! - The slash command vocabulary is modelled on nanoharness's CLI
//!   (`core/cli.py`) and pi's run-control interface.

pub mod frame;
pub mod http;
pub mod slash;
pub mod uds;
pub mod ws;

use std::sync::Arc;

use harness_core::{EventSink, RunEvent, RunId};
use thiserror::Error;

pub use frame::{JsonRpcRequest, JsonRpcResponse, RpcError, error_code, run_event_to_sse};
pub use slash::{
    ParseSlashError, SlashCommand, command_from_jsonrpc, parse_slash, slash_to_jsonrpc,
};

// ── Shared transport state ────────────────────────────────────────────────

/// Shared state that every transport channel holds.
///
/// Created once by the daemon and cloned (via `Arc`) into each transport
/// handler.  The broadcast sender fans every [`RunEvent`] out to all
/// connected clients; the [`EventLog`] seam replays missed events on
/// reconnection; the [`CommandHandler`] seam turns parsed commands into
/// run actions.
pub struct TransportState {
    /// Command dispatch seam — implemented by the daemon (#13).
    pub handler: Arc<dyn CommandHandler>,
    /// Event persistence seam — implemented by scheduler/session (#11).
    pub event_log: Arc<dyn EventLog>,
    /// Broadcast channel sender: all transport connections subscribe here
    /// to receive live [`RunEvent`]s.
    pub event_tx: tokio::sync::broadcast::Sender<RunEvent>,
}

impl TransportState {
    /// Create a new transport state.
    ///
    /// The broadcast channel capacity defaults to 1024 — enough to absorb
    /// bursts from fast token streams without blocking the loop.
    pub fn new(handler: Arc<dyn CommandHandler>, event_log: Arc<dyn EventLog>) -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(1024);
        Self {
            handler,
            event_log,
            event_tx,
        }
    }

    /// Convenience: [`EventSink`] implementation that forwards every event
    /// into the broadcast channel.  Pass this to the run loop as the sink;
    /// the transport layer automatically receives every event.
    pub fn event_sink(&self) -> BroadcastSink {
        BroadcastSink {
            tx: self.event_tx.clone(),
        }
    }
}

/// An [`EventSink`] that pushes into the transport's broadcast channel.
///
/// The run loop uses this as its event output; the transport layer receives
/// every emitted event and fans it out to all connected clients.
#[derive(Clone)]
pub struct BroadcastSink {
    tx: tokio::sync::broadcast::Sender<RunEvent>,
}

impl EventSink for BroadcastSink {
    fn emit(&self, event: RunEvent) {
        // try_send drops the event if the channel is full — subscriber lag
        // is observable but never blocks the loop (see EventSink docs).
        let _ = self.tx.send(event);
    }
}

// ── Seams ─────────────────────────────────────────────────────────────────

/// Transport → daemon seam: converts a parsed [`SlashCommand`] into a
/// JSON-RPC response value.
///
/// Implementations are expected to:
/// - Validate that referenced runs exist.
/// - Return [`TransportError::NoActiveRun`] when a run id is unknown.
/// - Be lightweight (no I/O, no blocking) — heavy work belongs in the
///   loop, not in command dispatch.
pub trait CommandHandler: Send + Sync + 'static {
    /// Handle a parsed command and return a JSON value to be sent back as
    /// the JSON-RPC response result, or a [`TransportError`] for the error
    /// frame.
    fn handle(&self, cmd: SlashCommand) -> Result<serde_json::Value, TransportError>;
}

/// Event log seam: reads persisted events by sequence number for
/// reconnection replay.
///
/// When a WebSocket or SSE client reconnects with `after_seq`, the
/// transport calls [`read_events_since`] to replay the missed window
/// before subscribing to the live broadcast.
pub trait EventLog: Send + Sync + 'static {
    /// Return all events for `run_id` whose `seq` is strictly greater than
    /// `after_seq`, in ascending order.
    fn read_events_since(&self, run_id: &RunId, after_seq: u64) -> Vec<RunEvent>;
}

// ── Error type ────────────────────────────────────────────────────────────

/// A transport-level error.
///
/// Each variant maps to a JSON-RPC error code (see [`error_code`]) so the
/// transport can build a typed error frame without string-matching.
#[derive(Debug, Error)]
pub enum TransportError {
    /// The command name is not recognised.
    #[error("unknown command: {0}")]
    UnknownCommand(String),
    /// A required parameter is missing or malformed.
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
    /// The referenced run does not exist or has already ended.
    #[error("no active run: {0}")]
    NoActiveRun(String),
    /// An internal error occurred (serialisation, I/O, …).
    #[error("internal error: {0}")]
    Internal(String),
}

impl TransportError {
    /// Map this error to a JSON-RPC error code.
    pub fn code(&self) -> i32 {
        match self {
            Self::UnknownCommand(_) => error_code::METHOD_NOT_FOUND,
            Self::InvalidParams(_) => error_code::INVALID_PARAMS,
            Self::NoActiveRun(_) => error_code::NO_ACTIVE_RUN,
            Self::Internal(_) => error_code::INTERNAL_ERROR,
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Dispatch a parsed [`SlashCommand`] through the handler and produce a
/// JSON-RPC response value.
///
/// This is the shared dispatch step that all three transport channels use:
/// parse → dispatch → serialise response.
pub fn dispatch_command(
    handler: &dyn CommandHandler,
    cmd: SlashCommand,
    request_id: u64,
) -> JsonRpcResponse {
    match handler.handle(cmd) {
        Ok(result) => JsonRpcResponse::success(request_id, result),
        Err(err) => JsonRpcResponse::error(request_id, err.code(), err.to_string()),
    }
}
