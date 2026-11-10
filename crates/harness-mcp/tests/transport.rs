//! Tests for the harness-mcp protocol layer.
//!
//! Purpose: verify JSON-RPC framing, response validation, transport id
//! matching, pagination, tool-call error mapping, content flattening, name
//! sanitisation, and backoff bounds without spawning real servers — a fake
//! in-process `McpTransport` does the heavy lifting, and a real
//! `StdioTransport` driven by a `sh` stub is used only where the stdio
//! roundtrip loop itself is under test.
//!
//! Scenarios:
//! - `request_line` frames a valid JSON-RPC 2.0 request.
//! - `parse_response` accepts matching ids, rejects mismatched or missing
//!   ids, rejects invalid JSON, and maps server error objects.
//! - `StdioTransport::roundtrip` returns the reply whose `id` matches the
//!   request and silently skips notifications and stale replies.
//! - `StdioTransport` honours the per-server timeout from config.
//! - `Mcp::connect` performs the initialize handshake against a fake transport.
//! - `tools/list` follows `nextCursor` until exhausted and stops at the page cap.
//! - `tools/call` maps `isError: true` to `McpError::Tool`.
//! - `flatten_content` joins text parts and renders non-text parts as JSON.
//! - `sanitize_tool_name` coerces names to `[a-zA-Z0-9_-]`, capped at 64.
//! - `backoff_delay` stays within its full-jitter bounds.
//!
//! Acceptance: every test asserts concrete protocol behaviour; no
//! `unwrap`/`expect`, no `assert!(true)`.

use harness_mcp::client::Mcp;
use harness_mcp::config::McpServerConfig;
use harness_mcp::reconnect::{ReconnectPolicy, backoff_delay};
use harness_mcp::stdio::StdioTransport;
use harness_mcp::transport::{
    call_tool, flatten_content, list_tools, parse_response, request_line, sanitize_tool_name,
};
use harness_mcp::{McpError, McpTransport};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// An in-process `McpTransport` that replays canned replies in FIFO order
/// and records every line sent to it. The canned replies' `id` fields must
/// match the request ids the caller uses, since `parse_response` enforces
/// id matching.
struct FakeTransport {
    next: AtomicU64,
    sent: Mutex<Vec<String>>,
    replies: Mutex<Vec<String>>,
}

impl FakeTransport {
    fn with_replies(replies: Vec<String>) -> Self {
        Self {
            next: AtomicU64::new(1),
            sent: Mutex::new(Vec::new()),
            replies: Mutex::new(replies),
        }
    }

    /// Snapshot of the request lines sent through this transport.
    fn sent_lines(&self) -> Result<Vec<String>, McpError> {
        Ok(self
            .sent
            .lock()
            .map_err(|e| McpError::Transport(e.to_string()))?
            .clone())
    }
}

impl McpTransport for FakeTransport {
    fn next_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    fn notify(&self, line: &str) -> Result<(), McpError> {
        self.sent
            .lock()
            .map_err(|e| McpError::Transport(e.to_string()))?
            .push(line.to_string());
        Ok(())
    }

    fn roundtrip(&self, _id: u64, line: &str, _signal: &AtomicBool) -> Result<String, McpError> {
        self.sent
            .lock()
            .map_err(|e| McpError::Transport(e.to_string()))?
            .push(line.to_string());
        let mut replies = self
            .replies
            .lock()
            .map_err(|e| McpError::Transport(e.to_string()))?;
        if replies.is_empty() {
            return Err(McpError::Transport("no canned reply left".into()));
        }
        Ok(replies.remove(0))
    }
}

/// A minimal stdio server config spawning `sh` with a script argument.
fn stdio_config(script: &str, tool_call_timeout_ms: Option<u64>) -> McpServerConfig {
    McpServerConfig {
        name: "stdio-stub".into(),
        command: Some("sh".into()),
        args: vec!["-c".into(), script.into()],
        env: HashMap::new(),
        url: None,
        tool_call_timeout_ms,
    }
}

/// Purpose: `request_line` frames a valid JSON-RPC 2.0 request.
/// Scenario: `request_line` with an id, method, and params object.
/// Acceptance: the line parses as JSON carrying `jsonrpc: "2.0"`, the given
/// `id`, `method`, and `params` verbatim.
#[test]
fn request_line_frames_json_rpc_request() -> Result<(), Box<dyn std::error::Error>> {
    let line = request_line(42, "tools/list", &json!({"cursor": "abc"}));
    let value: Value = match serde_json::from_str(&line) {
        Ok(value) => value,
        Err(error) => panic!("request_line is not valid JSON: {error}"),
    };
    assert_eq!(value.get("jsonrpc").and_then(Value::as_str), Some("2.0"));
    assert_eq!(value.get("id").and_then(Value::as_u64), Some(42));
    assert_eq!(
        value.get("method").and_then(Value::as_str),
        Some("tools/list")
    );
    assert_eq!(value.get("params"), Some(&json!({"cursor": "abc"})));
    Ok(())
}

/// Purpose: `parse_response` accepts a well-formed reply with a matching id.
/// Scenario: a JSON-RPC success response whose `id` equals the request id.
/// Acceptance: the `result` object is returned.
#[test]
fn parse_response_accepts_matching_id() -> Result<(), Box<dyn std::error::Error>> {
    let line = r#"{"jsonrpc":"2.0","id":7,"result":{"tools":[]}}"#;
    let result = parse_response(line, 7)?;
    assert_eq!(
        result
            .get("tools")
            .and_then(Value::as_array)
            .map(|array| array.len()),
        Some(0)
    );
    Ok(())
}

/// Purpose: `parse_response` rejects a reply whose id mismatches the request.
/// Scenario: a success response carrying a different `id`.
/// Acceptance: an `McpError::Protocol` error is returned.
#[test]
fn parse_response_rejects_mismatched_id() -> Result<(), Box<dyn std::error::Error>> {
    let line = r#"{"jsonrpc":"2.0","id":3,"result":{}}"#;
    let error = match parse_response(line, 7) {
        Ok(value) => panic!("expected id mismatch, got {value:?}"),
        Err(error) => error,
    };
    match error {
        McpError::Protocol(_) => {}
        other => panic!("expected Protocol error, got {other:?}"),
    }
    Ok(())
}

/// Purpose: `parse_response` rejects a reply with no id.
/// Scenario: a JSON-RPC notification-shaped line passed as a response.
/// Acceptance: an `McpError::Protocol` error is returned.
#[test]
fn parse_response_rejects_missing_id() -> Result<(), Box<dyn std::error::Error>> {
    let line = r#"{"jsonrpc":"2.0","result":{}}"#;
    let error = match parse_response(line, 1) {
        Ok(value) => panic!("expected missing-id error, got {value:?}"),
        Err(error) => error,
    };
    match error {
        McpError::Protocol(_) => {}
        other => panic!("expected Protocol error, got {other:?}"),
    }
    Ok(())
}

/// Purpose: `parse_response` rejects invalid JSON.
/// Scenario: a line that is not a JSON object at all.
/// Acceptance: an `McpError::Protocol` error is returned.
#[test]
fn parse_response_rejects_invalid_json() -> Result<(), Box<dyn std::error::Error>> {
    let error = match parse_response("not json at all", 1) {
        Ok(value) => panic!("expected invalid-JSON error, got {value:?}"),
        Err(error) => error,
    };
    match error {
        McpError::Protocol(_) => {}
        other => panic!("expected Protocol error, got {other:?}"),
    }
    Ok(())
}

/// Purpose: `parse_response` maps server error objects to `McpError::Server`.
/// Scenario: a response carrying a JSON-RPC `error` object with code and message.
/// Acceptance: an `McpError::Server` error carrying the server's message.
#[test]
fn parse_response_maps_server_error() -> Result<(), Box<dyn std::error::Error>> {
    let line = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}"#;
    let error = match parse_response(line, 1) {
        Ok(value) => panic!("expected server error, got {value:?}"),
        Err(error) => error,
    };
    match error {
        McpError::Server { .. } => {}
        other => panic!("expected Server error, got {other:?}"),
    }
    assert!(error.to_string().contains("method not found"));
    Ok(())
}

/// Purpose: `StdioTransport::roundtrip` returns the reply matching the request id.
/// Scenario: a `sh` stub emits a notification (no `id`), a stale reply for
/// another id, then the reply matching the request id.
/// Acceptance: `roundtrip` returns the matching reply and silently skips the
/// notification and the stale reply; `parse_response` on it succeeds.
#[test]
fn stdio_roundtrip_matches_request_id() -> Result<(), Box<dyn std::error::Error>> {
    // The stub reads one line (the request) before replying: replying at
    // spawn time races the child's exit against the parent's write and fails
    // on CI with EPIPE.
    let transport = StdioTransport::spawn(&stdio_config(
        "read _ && printf '%s\\n' \
        '{\"jsonrpc\":\"2.0\",\"method\":\"notif\"}' \
        '{\"jsonrpc\":\"2.0\",\"id\":9,\"result\":{}}' \
        '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}'",
        None,
    ))?;
    let id = transport.next_id();
    assert_eq!(id, 1);
    let line = request_line(id, "tools/list", &json!({}));
    let reply = transport.roundtrip(id, &line, &AtomicBool::new(false))?;
    let result = parse_response(&reply, id)?;
    assert_eq!(result.get("ok").and_then(Value::as_bool), Some(true));
    Ok(())
}

/// Purpose: `StdioTransport` applies the per-server call timeout from config.
/// Scenario: `tool_call_timeout_ms` is small and the `sh` stub never replies.
/// Acceptance: `roundtrip` returns `McpError::Timeout` after the configured
/// timeout instead of waiting for the slow stub.
#[test]
fn stdio_roundtrip_honours_per_server_timeout() -> Result<(), Box<dyn std::error::Error>> {
    let transport = StdioTransport::spawn(&stdio_config("sleep 5", Some(50)))?;
    let id = transport.next_id();
    let line = request_line(id, "tools/list", &json!({}));
    let started = Instant::now();
    let error = match transport.roundtrip(id, &line, &AtomicBool::new(false)) {
        Ok(reply) => panic!("expected timeout, got {reply:?}"),
        Err(error) => error,
    };
    match error {
        McpError::Timeout => {}
        other => panic!("expected Timeout error, got {other:?}"),
    }
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "timeout fired after {:?}, expected at least 50 ms",
        started.elapsed()
    );
    Ok(())
}

/// Purpose: `Mcp::connect` completes the initialize handshake over a fake transport.
/// Scenario: the fake replies to `initialize`, then the client sends the
/// `notifications/initialized` notification and lists tools.
/// Acceptance: the client is constructed, exposes the server name, and the
/// transport recorded the initialized notification.
#[test]
fn mcp_connect_performs_handshake() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(FakeTransport::with_replies(vec![
        r#"{"jsonrpc":"2.0","id":1,"result":{"capabilities":{}}}"#.into(),
        r#"{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"echo"}]}}"#.into(),
    ]));
    let clone = Arc::clone(&transport);
    let config = McpServerConfig {
        name: "stub".into(),
        command: None,
        args: Vec::new(),
        env: HashMap::new(),
        url: None,
        tool_call_timeout_ms: None,
    };
    let mcp = Mcp::connect(&config, transport, &AtomicBool::new(false))?;
    assert_eq!(mcp.name(), "stub");
    let tools = mcp.list_tools();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "mcp__stub__echo");
    assert_eq!(tools[0].remote, "echo");
    let sent = clone.sent_lines()?;
    assert!(
        sent.iter()
            .any(|line| line.contains("notifications/initialized"))
    );
    Ok(())
}

/// Purpose: `list_tools` follows `nextCursor` until pagination is exhausted.
/// Scenario: two pages, the first carrying a cursor, the second none.
/// Acceptance: tools from both pages are returned with namespaced names.
#[test]
fn list_tools_follows_next_cursor_until_exhausted() -> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeTransport::with_replies(vec![
        r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"a"},{"name":"b"}],"nextCursor":"c1"}}"#.into(),
        r#"{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"c"}],"nextCursor":null}}"#.into(),
    ]);
    let tools = list_tools(&transport, "stub", &AtomicBool::new(false))?;
    assert_eq!(tools.len(), 3);
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<&str>>(),
        vec!["mcp__stub__a", "mcp__stub__b", "mcp__stub__c"]
    );
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.remote.as_str())
            .collect::<Vec<&str>>(),
        vec!["a", "b", "c"]
    );
    Ok(())
}

/// Purpose: `list_tools` stops when a cursor repeats.
/// Scenario: the second page returns the same cursor as the first.
/// Acceptance: iteration stops after two pages instead of looping forever.
#[test]
fn list_tools_stops_on_repeated_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeTransport::with_replies(vec![
        r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"a"}],"nextCursor":"c1"}}"#.into(),
        r#"{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"b"}],"nextCursor":"c1"}}"#.into(),
    ]);
    let tools = list_tools(&transport, "stub", &AtomicBool::new(false))?;
    assert_eq!(tools.len(), 2);
    Ok(())
}

/// Purpose: `list_tools` stops at the hard page cap.
/// Scenario: every page carries a fresh `nextCursor`, so only the cap can stop iteration.
/// Acceptance: exactly `MAX_TOOL_LIST_PAGES` pages are consumed and no extra request is made
/// (a missing 65th canned reply would surface as a transport error).
#[test]
fn list_tools_stops_at_page_cap() -> Result<(), Box<dyn std::error::Error>> {
    let mut replies = Vec::new();
    for page in 1..=64 {
        replies.push(
            format!(
                "{{\"jsonrpc\":\"2.0\",\"id\":{page},\"result\":{{\"tools\":[{{\"name\":\"tool{page}\"}}],\"nextCursor\":\"c{page}\"}}}}",
                page = page
            ),
        );
    }
    let transport = FakeTransport::with_replies(replies);
    let tools = list_tools(&transport, "stub", &AtomicBool::new(false))?;
    assert_eq!(tools.len(), 64);
    assert_eq!(tools[0].name, "mcp__stub__tool1");
    assert_eq!(tools[63].name, "mcp__stub__tool64");
    Ok(())
}

/// Purpose: `call_tool` maps a remote tool failure to `McpError::Tool`.
/// Scenario: the server replies with `isError: true` and a text content part.
/// Acceptance: the error carries the failure text.
#[test]
fn call_tool_maps_is_error() -> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeTransport::with_replies(vec![
        r#"{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"boom"}],"isError":true}}"#.into(),
    ]);
    let error = match call_tool(
        &transport,
        "remote-tool",
        &json!({}),
        &AtomicBool::new(false),
    ) {
        Ok(text) => panic!("expected tool failure, got {text:?}"),
        Err(error) => error,
    };
    match error {
        McpError::Tool(_) => {}
        other => panic!("expected Tool error, got {other:?}"),
    }
    assert!(error.to_string().contains("boom"));
    Ok(())
}

/// Purpose: `call_tool` returns flattened text content on success.
/// Scenario: two text content parts in the result.
/// Acceptance: the parts are joined with a newline.
#[test]
fn call_tool_flattens_text_content() -> Result<(), Box<dyn std::error::Error>> {
    let transport = FakeTransport::with_replies(vec![
        r#"{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"hello"},{"type":"text","text":"world"}],"isError":false}}"#.into(),
    ]);
    let text = call_tool(
        &transport,
        "remote-tool",
        &json!({}),
        &AtomicBool::new(false),
    )?;
    assert_eq!(text, "hello\nworld");
    Ok(())
}

/// Purpose: `flatten_content` joins text parts and renders non-text parts as JSON.
/// Scenario: a result with text, image, and another text part.
/// Acceptance: text parts appear verbatim, non-text parts appear as compact JSON.
#[test]
fn flatten_content_joins_text_and_json_encodes_rest() {
    let result = json!({
        "content": [
            {"type": "text", "text": "a"},
            {"type": "image", "data": "base64"},
            {"text": "b"},
        ]
    });
    assert_eq!(
        flatten_content(&result),
        "a\n{\"data\":\"base64\",\"type\":\"image\"}\nb"
    );
}

/// Purpose: `flatten_content` handles a missing content array.
/// Scenario: a result object without `content`.
/// Acceptance: an empty string is returned.
#[test]
fn flatten_content_without_content_is_empty() {
    assert_eq!(flatten_content(&json!({})), "");
}

/// Purpose: `sanitize_tool_name` coerces tool names into safe identifiers.
/// Scenario: names with special characters, mixed case, empty names, and long names.
/// Acceptance: runs of non-`[a-zA-Z0-9_-]` characters become `_`, output is capped
/// at 64 characters, and short valid names pass through unchanged.
#[test]
fn sanitize_tool_name_coerces_and_caps() {
    assert_eq!(sanitize_tool_name("my-tool"), "my-tool");
    assert_eq!(sanitize_tool_name("My Tool!"), "My_Tool_");
    assert_eq!(sanitize_tool_name("weird$name@host"), "weird_name_host");
    assert_eq!(sanitize_tool_name(""), "");
    let long = "a".repeat(100);
    assert_eq!(sanitize_tool_name(&long), "a".repeat(64));
    assert_eq!(sanitize_tool_name(&long).len(), 64);
}

/// Purpose: `backoff_delay` stays inside its full-jitter bound.
/// Scenario: attempts 0 through 9 with the default reconnect policy
/// (`base_delay_ms = 100`, `max_delay_ms = 10_000`).
/// Acceptance: every delay lies in `[0, min(10_000, 100 * 2^attempt)]` —
/// bounds only, since full jitter intentionally inverts cross-attempt ordering.
#[test]
fn backoff_delay_stays_within_bounds() {
    let policy = ReconnectPolicy::default();
    for attempt in 0..10 {
        let cap = (100u64.saturating_mul(1u64 << attempt)).min(10_000);
        let millis = backoff_delay(attempt, &policy).as_millis();
        assert!(
            millis <= cap as u128,
            "delay {millis} ms at attempt {attempt} exceeds full-jitter cap {cap} ms"
        );
    }
}
