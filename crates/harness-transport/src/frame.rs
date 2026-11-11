//! Wire framing for the three-channel transport: JSON-RPC 2.0 envelopes for
//! command/response/error, plus SSE event helpers for RunEvent push.
//!
//! Design: the JSON-RPC 2.0 shape follows <https://www.jsonrpc.org/specification>
//! using the minimal subset the transport needs — requests always carry an
//! integer id (no notifications: a method call without a response id would
//! lose error reporting), and errors carry typed codes so a client can
//! discriminate without string-matching.  The SSE helper wraps a RunEvent as
//! an SSE data frame with `event: run_event` — the same wire envelope shape
//! nanoharness subscribers parse with `ev["type"]`.

use harness_core::RunEvent;
use serde::{Deserialize, Serialize};

// ── JSON-RPC 2.0 ──────────────────────────────────────────────────────────

/// A JSON-RPC 2.0 request from a transport client.
///
/// Every request carries an integer id — the transport does not support
/// notifications.  The method name corresponds to one of the supported
/// slash commands (`run`, `cancel`, `steer`, `status`, `list`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    pub id: u64,
}

/// A JSON-RPC 2.0 response.  Exactly one of `result` or `error` is present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl JsonRpcResponse {
    /// Build a success response.
    pub fn success(id: u64, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Build an error response.
    pub fn error(id: u64, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(RpcError::new(code, message)),
        }
    }
}

/// A JSON-RPC 2.0 error object.
///
/// `code` discriminates the error category; `message` is human-readable.
/// This is the typed error frame design from harness-rs: errors are
/// structured values, not bare strings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

// ── JSON-RPC 2.0 error codes ──────────────────────────────────────────────

/// Predefined JSON-RPC 2.0 error codes plus transport-specific extensions.
pub mod error_code {
    /// Parse error — invalid JSON was received by the server.
    pub const PARSE_ERROR: i32 = -32700;
    /// Invalid Request — the JSON sent is not a valid Request object.
    pub const INVALID_REQUEST: i32 = -32600;
    /// Method not found — the method does not exist / is not available.
    pub const METHOD_NOT_FOUND: i32 = -32601;
    /// Invalid params — invalid method parameter(s).
    pub const INVALID_PARAMS: i32 = -32602;
    /// Internal error — internal JSON-RPC error.
    pub const INTERNAL_ERROR: i32 = -32603;

    /// Transport-specific: the referenced run does not exist or has ended.
    pub const NO_ACTIVE_RUN: i32 = -32000;
    /// Transport-specific: the slash command is unknown / unsupported.
    pub const UNKNOWN_COMMAND: i32 = -32001;
}

// ── SSE helpers ───────────────────────────────────────────────────────────

/// Build an SSE [`Event`] wrapping a [`RunEvent`].
///
/// The SSE event type is `run_event`; the data field carries the event's
/// JSON representation as a single line.
pub fn run_event_to_sse(event: &RunEvent) -> axum::response::sse::Event {
    axum::response::sse::Event::default()
        .event("run_event")
        .data(serde_json::to_string(event).unwrap_or_else(|_| "{}".into()))
}
