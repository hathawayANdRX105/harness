//! The [`McpTransport`] trait: one request/response channel to an MCP server.
//!
//! Implementations serialize concurrent callers — `roundtrip` is called from
//! tool executions on arbitrary threads. The trait uses `&AtomicBool` for
//! abort signalling so that implementations stay free of async runtimes (stdio
//! uses std::process + mpsc, HTTP uses ureq).

use std::sync::atomic::AtomicBool;
use std::time::Duration;

use serde_json::{Value, json};

use crate::error::McpError;

/// The MCP basic spec revision this client implements.
///
/// Design provenance: the MCP spec defines the protocol version as a string
/// negotiated during `initialize`. The version `"2025-06-18"` matches the
/// ferrite reference implementation's negotiated version.
pub const PROTOCOL_VERSION: &str = "2025-06-18";

/// Per-request budget. Matches the typical tool-execution timeout so a hung
/// server cannot wedge the agent loop.
pub const MCP_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval for polling the abort signal and the reply channel during a
/// blocking roundtrip. Fine enough that a 30 s timeout polls roughly 300
/// times, coarse enough that a tight loop does not burn a core.
pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Bound `tools/list` pagination: a server that keeps handing back a fresh
/// cursor must not spin the agent loop forever.
const MAX_TOOL_LIST_PAGES: usize = 64;

/// One request/response channel to an MCP server.
///
/// Implementations must serialize concurrent callers: `roundtrip` is called
/// from tool executions on arbitrary threads.
///
/// `signal` is an `AtomicBool` — when set to `true` the implementation should
/// abandon the in-flight call and return [`McpError::Aborted`].
pub trait McpTransport: Send + Sync {
    /// Send one framed JSON-RPC request and return the reply line for `id`.
    ///
    /// Lines without a matching `id` (server notifications, stale responses
    /// from earlier calls that timed out) must be consumed silently and never
    /// returned. Must poll `signal` and give up after [`MCP_TIMEOUT`].
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Aborted`] when `signal` is set, [`McpError::Timeout`]
    /// when the deadline passes, [`McpError::Transport`] on I/O errors, and
    /// [`McpError::Protocol`] when the server sends back a JSON-RPC error object.
    fn roundtrip(&self, id: u64, line: &str, signal: &AtomicBool) -> Result<String, McpError>;

    /// Send a JSON-RPC 2.0 notification: no `id`, no reply expected.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Transport`] when the write to the transport fails.
    fn notify(&self, line: &str) -> Result<(), McpError>;

    /// Next monotonic request id.
    fn next_id(&self) -> u64;
}

/// Serialize a JSON-RPC 2.0 request into a line.
pub fn request_line(id: u64, method: &str, params: &Value) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
    .to_string()
}

/// Serialize a JSON-RPC 2.0 notification (no `id`).
pub fn notification_line(method: &str, params: &Value) -> String {
    json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
    })
    .to_string()
}

/// Extract the `result` of a response; `error` and id mismatch become [`McpError`].
///
/// # Errors
///
/// Returns [`McpError::Protocol`] when the JSON is invalid, the `id` field is
/// missing or mismatched, or the response lacks both `result` and `error`.
/// Returns [`McpError::Server`] when the server sent a JSON-RPC error object.
pub fn parse_response(line: &str, id: u64) -> Result<Value, McpError> {
    let mut v: Value = serde_json::from_str(line)
        .map_err(|e| McpError::Protocol(format!("response is not valid JSON: {e}")))?;
    match v.get("id").and_then(Value::as_u64) {
        Some(got) if got == id => {}
        Some(got) => {
            return Err(McpError::Protocol(format!(
                "response id {got} does not match request id {id}"
            )));
        }
        None => return Err(McpError::Protocol("response has no `id`".into())),
    }
    if let Some(err) = v.get("error") {
        let code = err.get("code").and_then(Value::as_i64).unwrap_or_default();
        let message = err
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown error")
            .to_string();
        return Err(McpError::Server { code, message });
    }
    Ok(v.get_mut("result").map(std::mem::take).unwrap_or_default())
}

/// One full request: allocate an id, frame it, await the reply, unwrap `result`.
///
/// # Errors
///
/// Forwards [`McpTransport::roundtrip`] and [`parse_response`] errors.
pub fn request(
    transport: &dyn McpTransport,
    method: &str,
    params: &Value,
    signal: &AtomicBool,
) -> Result<Value, McpError> {
    let id = transport.next_id();
    let line = transport.roundtrip(id, &request_line(id, method, params), signal)?;
    parse_response(&line, id)
}

/// MCP handshake: `initialize`, then the `notifications/initialized` ack.
///
/// # Errors
///
/// Returns [`McpError::Protocol`] when the server's initialize response is
/// malformed, or forwards transport/protocol errors from the underlying
/// request and notification calls.
pub fn initialize(transport: &dyn McpTransport, signal: &AtomicBool) -> Result<Value, McpError> {
    let params = json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": {"name": "harness-mcp", "version": env!("CARGO_PKG_VERSION")},
    });
    let result = request(transport, "initialize", &params, signal)?;
    transport.notify(&notification_line("notifications/initialized", &json!({})))?;
    Ok(result)
}

/// Tool metadata returned by a remote MCP server.
///
/// Design provenance: ferrite's `mcp::tool::ToolMeta` — the name exposed to the
/// model is namespaced per server (`mcp__<server>__<tool>`) so two servers
/// cannot shadow each other. The `remote` field holds the name the server
/// knows, sent verbatim in `tools/call`.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolMeta {
    /// Name exposed to the model, namespaced per server.
    pub name: String,
    /// Name the server knows this tool by.
    pub remote: String,
    pub description: String,
    /// JSON Schema from the server's `inputSchema`.
    pub parameters: Value,
}

/// List all tools from a server, following `nextCursor` pagination.
///
/// Follows `nextCursor` until it is absent, empty, repeated, or reaches
/// [`MAX_TOOL_LIST_PAGES`], so paginated servers do not silently drop
/// everything past page one or spin forever on malformed cursors.
///
/// # Errors
///
/// Returns [`McpError::Protocol`] when the server's `tools/list` response
/// lacks a `tools` array, or forwards transport/protocol errors from the
/// underlying request loop.
pub fn list_tools(
    transport: &dyn McpTransport,
    server: &str,
    signal: &AtomicBool,
) -> Result<Vec<ToolMeta>, McpError> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let mut attempts = 0usize;
    loop {
        let params = cursor
            .as_ref()
            .map_or(json!({}), |c| json!({ "cursor": c }));
        let result = request(transport, "tools/list", &params, signal)?;
        attempts += 1;
        let items = result
            .get("tools")
            .and_then(Value::as_array)
            .ok_or_else(|| McpError::Protocol("tools/list result has no `tools` array".into()))?;
        for t in items {
            let Some(remote) = t.get("name").and_then(Value::as_str) else {
                continue;
            };
            if remote.is_empty() {
                continue;
            }
            let name = sanitize_tool_name(&format!("mcp__{server}__{remote}"));
            if name.is_empty() {
                // A tool whose name collapses entirely under sanitisation is
                // unusable — skip it rather than producing a zero-length name
                // that would confuse the dispatcher.
                continue;
            }
            out.push(ToolMeta {
                name,
                remote: remote.to_string(),
                description: t
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                parameters: t
                    .get("inputSchema")
                    .cloned()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            });
        }
        let next = match result.get("nextCursor") {
            Some(Value::Null) | None => break,
            Some(v) => match v.as_str() {
                Some("") | None => break,
                Some(s) => s,
            },
        };
        if cursor.as_deref() == Some(next) || attempts >= MAX_TOOL_LIST_PAGES {
            // Repeated cursor or runaway pagination — the server is broken.
            // Stop iterating to avoid an infinite loop.
            break;
        }
        cursor = Some(next.to_string());
    }
    Ok(out)
}

/// Coerce a tool name into `[a-zA-Z0-9_-]`, capped at 64 characters (all ASCII
/// after substitution, so also 64 bytes).
pub fn sanitize_tool_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect()
}

/// `tools/call` — invoke a remote tool and return the flattened text content.
///
/// Per the spec the result carries a `content` array of typed parts plus an
/// `isError` flag. Text parts are joined; `isError` becomes [`McpError::Tool`]
/// so the agent loop surfaces it like any other tool failure.
///
/// # Errors
///
/// Returns [`McpError::Tool`] when the server reports `isError: true`,
/// [`McpError::Server`] when the server returns a JSON-RPC error, and
/// forwards transport and protocol errors from the underlying request.
pub fn call_tool(
    transport: &dyn McpTransport,
    remote: &str,
    args: &Value,
    signal: &AtomicBool,
) -> Result<String, McpError> {
    let params = json!({
        "name": remote,
        // Servers reject a non-object `arguments`; normalise null/scalars away.
        "arguments": if args.is_object() { args.clone() } else { json!({}) },
    });
    let result = request(transport, "tools/call", &params, signal)?;
    let text = flatten_content(&result);
    if result.get("isError").and_then(Value::as_bool) == Some(true) {
        return Err(if text.is_empty() {
            McpError::Tool(format!("mcp tool `{remote}` failed"))
        } else {
            McpError::Tool(text)
        });
    }
    Ok(text)
}

/// Join the text parts of a `tools/call` result.
///
/// Non-text parts are rendered as compact JSON so nothing is silently dropped.
pub fn flatten_content(result: &Value) -> String {
    let Some(items) = result.get("content").and_then(Value::as_array) else {
        return String::new();
    };
    items
        .iter()
        .map(|part| match part.get("text").and_then(Value::as_str) {
            Some(t) => t.to_string(),
            None => part.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}
