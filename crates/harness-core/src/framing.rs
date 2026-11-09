//! JSON-RPC value framing shared by the daemon wire protocol, the MCP client
//! and any line-based peer: encode/decode helpers plus frame classification.
//!
//! Nothing here is specific to a transport: a caller reads a line (UDS,
//! websocket, SSE data field), decodes it into a [`serde_json::Value`] and
//! classifies; the same three helpers serve every endpoint.

use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

/// JSON-RPC version string constant.
pub const JSONRPC: &str = "2.0";

/// Construct a JSON-RPC request object.
pub fn request_value(id: u64, method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": JSONRPC,
        "id": id,
        "method": method,
        "params": params,
    })
}

/// Construct a JSON-RPC notification object.
pub fn notification_value(method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": JSONRPC,
        "method": method,
        "params": params,
    })
}

/// Encode a JSON-RPC value as a single-line JSON string.
pub fn encode_line(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}

#[derive(Debug)]
/// Frame classification: which JSON-RPC message kind a decoded line represents.
///
/// The variants borrow the decoded [`Value`] (`id`/`method` as typed views) so a
/// caller can dispatch on the frame without re-parsing fields.
pub enum FrameKind<'a> {
    /// A call: both an id and a method.
    Request {
        /// The call id, as read from the value.
        id: u64,
        /// The method name.
        method: &'a str,
        /// The whole decoded frame.
        value: &'a Value,
    },
    /// A result or error for a prior request: an id and no method.
    Response {
        /// The id of the request being answered.
        id: u64,
        /// The whole decoded frame.
        value: &'a Value,
    },
    /// A one-way message: a method and no id.
    Notification {
        /// The method name.
        method: &'a str,
        /// The whole decoded frame.
        value: &'a Value,
    },
    /// Anything that is not a recognizable JSON-RPC frame.
    Ignored,
}

pub fn decode_line(line: &str) -> Option<Value> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    serde_json::from_str(trimmed).ok()
}

pub fn classify(value: &Value) -> FrameKind<'_> {
    let id = value.get("id");
    let method = value.get("method").and_then(Value::as_str);
    match (id, method) {
        (Some(id), Some(method)) => {
            // Non-numeric ids are not JSON-RPC calls; they classify as
            // Ignored, not as a Request.
            match id.as_u64() {
                Some(id) => FrameKind::Request { id, method, value },
                None => FrameKind::Ignored,
            }
        }
        (Some(id), None) => match id.as_u64() {
            Some(id) => FrameKind::Response { id, value },
            None => FrameKind::Ignored,
        },
        (None, Some(method)) => FrameKind::Notification { method, value },
        _ => FrameKind::Ignored,
    }
}

/// Write a JSON line to a writer and flush.
pub fn write_json_line<W: Write>(writer: &mut W, line: &str) -> io::Result<()> {
    writer.write_all(line.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// Read a line from a reader into a buffer. The trailing newline is
/// stripped: the caller receives the bare line, and EOF (zero bytes read)
/// reports `Ok(false)`.
pub fn read_line<R: BufRead>(reader: &mut R, buf: &mut String) -> io::Result<bool> {
    buf.clear();
    let n = reader.read_line(buf)?;
    if n == 0 {
        return Ok(false);
    }
    while buf.ends_with('\n') || buf.ends_with('\r') {
        buf.pop();
    }
    Ok(true)
}
