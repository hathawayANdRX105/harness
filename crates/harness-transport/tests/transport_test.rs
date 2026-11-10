//! Three-channel contract tests: HTTP+SSE, WebSocket and UDS round-trips.
//!
//! Each test creates a mock [`CommandHandler`] and [`EventLog`], starts the
//! transport on a system-assigned port or temp socket, sends a JSON-RPC
//! command, and asserts the response — verifying the shared
//! **parse → dispatch → push** path through one channel.
//!
//! # Test isolation
//!
//! - HTTP and WS bind to `127.0.0.1:0` (system-assigned ports), so nextest
//!   process-per-test never conflicts.
//! - UDS creates a temp socket in the OS temp directory with a random name.
//! - Each test runs in its own process (nextest default), so global state
//!   cannot leak between tests.
//!
//! # Error handling
//!
//! Every test returns `Result` and propagates with `?`: the workspace denies
//! `unwrap`/`expect`, and a failed setup must fail the test rather than be
//! swallowed. A test that cannot reach its dependency fails; it never skips.
//!
//! # Design
//!
//! The mock handler returns a fixed JSON value for `/status` commands and
//! errors for unknown commands — the acceptance criteria assert that the
//! full round-trip (parse → dispatch → response) produces the expected
//! JSON-RPC frame.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use harness_core::{RunEvent, RunId};
use harness_transport::{
    CommandHandler, EventLog, TransportError, TransportState, slash::SlashCommand,
};

/// Every test's error type: any boxed error the transport stack can produce.
type TestResult = Result<(), Box<dyn std::error::Error>>;

// ── Mocks ───────────────────────────────────────────────────────────────────

/// A mock handler that knows one command: `Status { run_id: "test-run" }`
/// returns `{"status": "running"}`, everything else errors.
struct MockHandler;

impl CommandHandler for MockHandler {
    fn handle(&self, cmd: SlashCommand) -> Result<serde_json::Value, TransportError> {
        match cmd {
            SlashCommand::Status { run_id } if run_id == "test-run" => {
                Ok(serde_json::json!({"status": "running"}))
            }
            SlashCommand::Status { run_id } => Err(TransportError::NoActiveRun(run_id)),
            other => Err(TransportError::UnknownCommand(format!("{other:?}"))),
        }
    }
}

/// A mock event log that returns empty — reconnection replay is tested by
/// the scheduler/session crate (#11).
struct MockEventLog;

impl EventLog for MockEventLog {
    fn read_events_since(&self, _run_id: &RunId, _after_seq: u64) -> Vec<RunEvent> {
        Vec::new()
    }
}

/// Build shared state for one test.
fn test_state() -> Arc<TransportState> {
    Arc::new(TransportState::new(
        Arc::new(MockHandler),
        Arc::new(MockEventLog),
    ))
}

// ── HTTP+SSE round-trip ────────────────────────────────────────────────────

/// Purpose: verify that a JSON-RPC command sent via POST /command round-trips
/// through the handler and returns the expected response.
///
/// Scenario: start an HTTP transport on port 0, send a `status` command for
/// `test-run`, receive the response.
///
/// Acceptance: the response is a JSON-RPC success with `{"status": "running"}`
/// and the correct request id.
#[tokio::test]
async fn http_command_roundtrip() -> TestResult {
    let state = test_state();
    let app = harness_transport::http::http_routes(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    // The server task runs until the test drops the runtime; a bind/serve
    // failure surfaces through the request below, so the handle is discarded.
    let _server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // Give the server a moment to start listening.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "status",
        "params": {"run_id": "test-run"},
        "id": 1,
    });

    let resp = client
        .post(format!("http://{addr}/command"))
        .json(&req)
        .send()
        .await?;
    assert!(resp.status().is_success(), "HTTP 200 expected");

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert_eq!(body["result"]["status"], "running");
    assert!(body["error"].is_null() || body.get("error").is_none());
    Ok(())
}

/// Purpose: verify that an unknown command returns a JSON-RPC error.
///
/// Acceptance: the response carries an error object with
/// `code = -32601` (METHOD_NOT_FOUND).
#[tokio::test]
async fn http_unknown_command_returns_error() -> TestResult {
    let state = test_state();
    let app = harness_transport::http::http_routes(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let _server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "unknown",
        "id": 42,
    });

    let resp = client
        .post(format!("http://{addr}/command"))
        .json(&req)
        .send()
        .await?;
    assert!(
        resp.status().is_success(),
        "HTTP 200 even for error responses"
    );

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 42);
    let err = body.get("error").ok_or("error object present")?;
    assert_eq!(err["code"], -32601);
    Ok(())
}

// ── WebSocket round-trip ───────────────────────────────────────────────────

/// Purpose: verify that a JSON-RPC command sent over WebSocket round-trips
/// through the handler and returns the expected response.
///
/// Scenario: start a WebSocket transport on port 0, connect via
/// tokio-tungstenite, send a text JSON-RPC request, read the response.
///
/// Acceptance: the response is a JSON-RPC success with `{"status": "running"}`
/// and the correct request id.
#[tokio::test]
async fn ws_command_roundtrip() -> TestResult {
    let state = test_state();
    let app = harness_transport::ws::ws_routes(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let _server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Connect via tokio-tungstenite.
    let url = format!("ws://{addr}/ws");
    let (ws_stream, _) = tokio_tungstenite::connect_async(&url).await?;

    let (mut write, mut read) = ws_stream.split();

    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "status",
        "params": {"run_id": "test-run"},
        "id": 1,
    });
    let msg = tokio_tungstenite::tungstenite::Message::Text(req.to_string().into());
    write.send(msg).await?;

    // Read the response.
    let resp_msg = read
        .next()
        .await
        .ok_or("WS response should arrive")?
        .map_err(|e| format!("WS response should be ok: {e}"))?;

    let text = match resp_msg {
        tokio_tungstenite::tungstenite::Message::Text(t) => t,
        other => panic!("expected Text frame, got {other:?}"),
    };

    let body: serde_json::Value = serde_json::from_str(text.as_str())?;
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert_eq!(body["result"]["status"], "running");
    Ok(())
}

// ── UDS round-trip ─────────────────────────────────────────────────────────

/// Purpose: verify that a JSON-RPC command sent over a Unix domain socket
/// round-trips through the handler and returns the expected response.
///
/// Scenario: start a UDS transport on a temp socket, connect via
/// `tokio::net::UnixStream`, send an NDJSON request, read the response.
///
/// Acceptance: the response is a JSON-RPC success with `{"status": "running"}`
/// and the correct request id.
#[tokio::test]
async fn uds_command_roundtrip() -> TestResult {
    let state = test_state();

    let socket_dir = std::env::temp_dir();
    let socket_path = socket_dir.join(format!(
        "harness-transport-test-{}.sock",
        std::process::id()
    ));
    // Remove leftover from a previous crash.
    let _ = tokio::fs::remove_file(&socket_path).await;

    let uds_path_str = socket_path.to_string_lossy().to_string();
    let config = harness_transport::uds::UdsConfig {
        socket_path: uds_path_str.clone(),
        auth_token: None,
    };

    let _server = tokio::spawn(async move {
        let _ = harness_transport::uds::serve_uds(config, state).await;
    });

    // Give the server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let stream = tokio::net::UnixStream::connect(&socket_path).await?;

    let (reader, mut writer) = tokio::io::split(stream);
    use tokio::io::AsyncWriteExt;

    let mut reader = tokio::io::BufReader::new(reader);
    use tokio::io::AsyncBufReadExt;

    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "status",
        "params": {"run_id": "test-run"},
        "id": 1,
    });

    let mut line = req.to_string();
    line.push('\n');
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await?;

    // Read the response line.
    let mut response_line = String::new();
    reader.read_line(&mut response_line).await?;

    let body: serde_json::Value = serde_json::from_str(response_line.trim())?;
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert_eq!(body["result"]["status"], "running");

    // Clean up the socket file.
    let _ = tokio::fs::remove_file(&socket_path).await;
    Ok(())
}
