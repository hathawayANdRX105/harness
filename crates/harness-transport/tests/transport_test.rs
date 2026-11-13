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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use harness_core::{RunEvent, RunEventKind, RunId, SessionId};
use harness_transport::{
    CommandHandler, EventLog, TransportError, TransportState, frame::error_code,
    slash::SlashCommand,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

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

/// A recording event log: remembers every `(run_id, after_seq)` replay query
/// and every session resolved through `latest_run_for_session`, and returns
/// its canned events filtered by `after_seq`.
struct ReplayEventLog {
    calls: std::sync::Mutex<Vec<(RunId, u64)>>,
    /// Sessions passed to `latest_run_for_session`, in query order.
    resolved: std::sync::Mutex<Vec<String>>,
    /// The run returned for each resolved session; missing means no runs.
    latest_runs: std::sync::Mutex<HashMap<String, RunId>>,
    events: Vec<RunEvent>,
}

#[async_trait]
impl EventLog for ReplayEventLog {
    async fn read_events_since(&self, run_id: &RunId, after_seq: u64) -> Vec<RunEvent> {
        held(&self.calls).push((run_id.clone(), after_seq));
        self.events
            .iter()
            .filter(|event| event.seq > after_seq)
            .cloned()
            .collect()
    }

    async fn latest_run_for_session(&self, session_id: &SessionId) -> Option<RunId> {
        held(&self.resolved).push(session_id.0.clone());
        held(&self.latest_runs).get(&session_id.0).cloned()
    }
}

/// Recover a poisoned mutex instead of failing the test: the guarded data is
/// plain state collected for assertions.
fn held<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A mock event log that returns empty — reconnection replay is tested by
/// the scheduler/session crate (#11).
struct MockEventLog;

#[async_trait]
impl EventLog for MockEventLog {
    async fn read_events_since(&self, _run_id: &RunId, _after_seq: u64) -> Vec<RunEvent> {
        Vec::new()
    }
    async fn latest_run_for_session(&self, _session_id: &SessionId) -> Option<RunId> {
        None
    }
}

/// Build shared state for one test.
fn test_state() -> Arc<TransportState> {
    Arc::new(TransportState::new(
        Arc::new(MockHandler),
        Arc::new(MockEventLog),
    ))
}

/// Poll `check` every 50 ms until it returns `Ok` or `deadline` passes,
/// returning the last error string on timeout.  Replaces fixed sleeps
/// that race against server startup.
async fn poll_until<T, F, Fut>(mut check: F, deadline: Instant) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    loop {
        match check().await {
            Ok(value) => return Ok(value),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e),
        }
    }
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

    // Wait for the server to accept connections (bounded poll instead of a
    // fixed sleep that can race against startup).
    let deadline = Instant::now() + Duration::from_secs(5);
    let client = reqwest::Client::new();
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "status",
        "params": {"run_id": "test-run"},
        "id": 1,
    });
    let url = format!("http://{addr}/command");
    let resp = poll_until(
        || {
            let client = client.clone();
            let url = url.clone();
            let req = req.clone();
            async move {
                client
                    .post(&url)
                    .json(&req)
                    .send()
                    .await
                    .map_err(|e| e.to_string())
            }
        },
        deadline,
    )
    .await
    .map_err(|e| format!("HTTP server did not accept connections: {e}"))?;
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

    // Wait for the server to accept connections (bounded poll instead of a fixed sleep).
    let deadline = Instant::now() + Duration::from_secs(5);
    let client = reqwest::Client::new();
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "unknown",
        "id": 42,
    });
    let url = format!("http://{addr}/command");
    let resp = poll_until(
        || {
            let client = client.clone();
            let url = url.clone();
            let req = req.clone();
            async move {
                client
                    .post(&url)
                    .json(&req)
                    .send()
                    .await
                    .map_err(|e| e.to_string())
            }
        },
        deadline,
    )
    .await
    .map_err(|e| format!("HTTP server did not accept connections: {e}"))?;
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
// tokio-tungstenite, send a text JSON-RPC request, read the response.
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

    // Wait for the server to accept connections (bounded poll instead of a fixed sleep).
    let deadline = Instant::now() + Duration::from_secs(5);
    let url = format!("ws://{addr}/ws");
    let (ws_stream, _) = poll_until(
        || {
            let url = url.clone();
            async move {
                tokio_tungstenite::connect_async(&url)
                    .await
                    .map_err(|e| e.to_string())
            }
        },
        deadline,
    )
    .await
    .map_err(|e| format!("WebSocket server did not accept connections: {e}"))?;

    let (mut write, mut read) = ws_stream.split();

    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "status",
        "params": {"run_id": "test-run"},
        "id": 1,
    });
    let msg = Message::Text(req.to_string().into());
    write.send(msg).await?;

    // Read the response.
    let resp_msg = read
        .next()
        .await
        .ok_or("WS response should arrive")?
        .map_err(|e| format!("WS response should be ok: {e}"))?;

    let text = match resp_msg {
        Message::Text(t) => t,
        other => panic!("expected Text frame, got {other:?}"),
    };

    let body: serde_json::Value = serde_json::from_str(text.as_str())?;
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 1);
    assert_eq!(body["result"]["status"], "running");
    Ok(())
}
// ── WebSocket unknown-id handling ───────────────────────────────────────

/// Purpose: an unparseable WebSocket frame must yield `id: null`, not `id: 0`.
///
/// Scenario: connect a WebSocket client, send a text frame that is not
/// valid JSON-RPC, then read the server's response frame.
///
/// Acceptance: the response is a JSON-RPC error frame with
/// `code = -32700` (PARSE_ERROR) and `id: null` — a client can never
/// mistake it for a reply to a legitimate request carrying `id: 0`.
#[tokio::test]
async fn ws_unparseable_frame_yields_null_id() -> TestResult {
    let state = test_state();
    let app = harness_transport::ws::ws_routes(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let _server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // Wait for the server to accept connections (bounded poll instead of a fixed sleep).
    let deadline = Instant::now() + Duration::from_secs(5);
    let url = format!("ws://{addr}/ws");
    let (ws_stream, _) = poll_until(
        || {
            let url = url.clone();
            async move {
                tokio_tungstenite::connect_async(&url)
                    .await
                    .map_err(|e| e.to_string())
            }
        },
        deadline,
    )
    .await
    .map_err(|e| format!("WebSocket server did not accept connections: {e}"))?;

    let (mut write, mut read) = ws_stream.split();

    // Garbage that is not JSON-RPC at all: the server cannot know the id.
    write
        .send(Message::Text("this is not JSON-RPC".into()))
        .await?;

    let resp_msg = read
        .next()
        .await
        .ok_or("WS error response should arrive")?
        .map_err(|e| format!("WS response should be ok: {e}"))?;

    let text = match resp_msg {
        Message::Text(t) => t,
        other => return Err(format!("expected Text frame, got {other:?}").into()),
    };

    let body: serde_json::Value = serde_json::from_str(text.as_str())?;
    assert_eq!(body["jsonrpc"], "2.0");
    assert!(
        body["id"].is_null(),
        "unknown request id must be null in the response, never 0"
    );
    let err = body.get("error").ok_or("error object present")?;
    assert_eq!(err["code"], -32700, "unparseable frame is a parse error");
    Ok(())
}

// ── WebSocket session binding ─────────────────────────────────────────────

/// Purpose: `subscribe` accepts an optional `session_id` and replays the
/// run's persisted events from `after_seq` through the EventLog seam.
/// Scenario: a server whose log holds two past events (seq 4 and 5); a
/// client subscribes with `{run_id, session_id, after_seq: 3}`; a probe
/// event is pushed on the broadcast channel to trigger the historical
/// replay before live forwarding.
/// Acceptance: the subscribe reply echoes `session_id`; the client receives
/// the two replayed events and then the live probe; the seam recorded the
/// requested `(run_id, after_seq)`.
#[tokio::test]
async fn ws_subscribe_with_session_id_replays_events() -> TestResult {
    let run_id = RunId("test-run".into());
    let session_id = SessionId("session-1".into());
    let log = Arc::new(ReplayEventLog {
        calls: std::sync::Mutex::new(Vec::new()),
        resolved: std::sync::Mutex::new(Vec::new()),
        latest_runs: std::sync::Mutex::new(HashMap::new()),
        events: vec![run_event(4, "replayed-one"), run_event(5, "replayed-two")],
    });
    let state = Arc::new(TransportState::new(Arc::new(MockHandler), log.clone()));
    let app = harness_transport::ws::ws_routes(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let url = format!("ws://{addr}/ws");
    let (ws_stream, _) = poll_until(
        || {
            let url = url.clone();
            async move {
                tokio_tungstenite::connect_async(&url)
                    .await
                    .map_err(|e| e.to_string())
            }
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .map_err(|e| format!("WebSocket server did not accept connections: {e}"))?;
    let (mut write, mut read) = ws_stream.split();

    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "subscribe",
        "params": {
            "run_id": run_id.0,
            "session_id": session_id.0,
            "after_seq": 3,
        },
        "id": 7,
    });
    write.send(Message::Text(req.to_string().into())).await?;
    let resp_msg = read
        .next()
        .await
        .ok_or("subscribe response should arrive")?
        .map_err(|e| format!("subscribe response: {e}"))?;
    let text = match resp_msg {
        Message::Text(t) => t,
        other => return Err(format!("expected Text frame, got {other:?}").into()),
    };
    let body: serde_json::Value = serde_json::from_str(text.as_str())?;
    assert_eq!(body["id"], 7, "response id echoes the request");
    assert_eq!(
        body["result"]["subscribed"], run_id.0,
        "subscribe reply names the run"
    );
    assert_eq!(
        body["result"]["session_id"], session_id.0,
        "subscribe reply echoes the session"
    );

    // Nudge the broadcast channel: the push loop replays history on the
    // first live event it receives, before forwarding that event.
    state.event_tx.send(run_event(9, "live"))?;

    let mut texts = Vec::new();
    for _ in 0..3 {
        let frame = read
            .next()
            .await
            .ok_or("expected replay or live frame")?
            .map_err(|e| format!("frame read: {e}"))?;
        let Message::Text(t) = frame else {
            return Err(format!("expected Text frame, got {frame:?}").into());
        };
        let ev: serde_json::Value = serde_json::from_str(t.as_str())?;
        texts.push(
            ev["text"]
                .as_str()
                .ok_or("event frame carries no text")?
                .to_string(),
        );
    }
    assert_eq!(
        texts,
        vec!["replayed-one", "replayed-two", "live"],
        "replayed events precede the live event"
    );

    let calls = held(&log.calls);
    assert_eq!(calls.len(), 1, "exactly one replay query");
    assert_eq!(calls[0].0, run_id, "replay query names the run");
    assert_eq!(calls[0].1, 3, "replay starts after the requested seq");
    Ok(())
}
/// Connect to the test server at `addr`, retrying until it accepts, send one
/// JSON-RPC request, and return its parsed response together with the still
/// open stream halves so the caller can read replayed or live frames.
async fn ws_subscribe(
    addr: &std::net::SocketAddr,
    id: u64,
    params: serde_json::Value,
) -> Result<
    (
        serde_json::Value,
        impl futures_util::sink::SinkExt<Message>,
        impl futures_util::stream::StreamExt<
            Item = Result<Message, tokio_tungstenite::tungstenite::Error>,
        > + std::marker::Unpin,
    ),
    Box<dyn std::error::Error>,
> {
    let (mut write, mut read) = poll_until(
        || {
            let addr = *addr;
            async move {
                let url = format!("ws://{addr}/ws");
                tokio_tungstenite::connect_async(&url)
                    .await
                    .map(|(stream, _)| stream.split())
                    .map_err(|e| e.to_string())
            }
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .map_err(|e| format!("WebSocket server did not accept connections: {e}"))?;

    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "subscribe",
        "params": params,
        "id": id,
    });
    write.send(Message::Text(req.to_string().into())).await?;
    let resp_msg = read
        .next()
        .await
        .ok_or("subscribe response should arrive")?
        .map_err(|e| e.to_string())?;
    let text = match resp_msg {
        Message::Text(t) => t,
        other => return Err(format!("expected Text frame, got {other:?}").into()),
    };
    let body: serde_json::Value = serde_json::from_str(text.as_str())?;
    Ok((body, write, read))
}

/// Read `n` JSON event frames from the stream and return their `text`
/// fields, in order.
async fn read_event_texts(
    read: &mut (
             impl futures_util::stream::StreamExt<
        Item = Result<Message, tokio_tungstenite::tungstenite::Error>,
    > + std::marker::Unpin
         ),
    n: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut texts = Vec::with_capacity(n);
    for _ in 0..n {
        let frame = read
            .next()
            .await
            .ok_or("stream ended before all frames arrived")?
            .map_err(|e| e.to_string())?;
        let Message::Text(t) = frame else {
            return Err(format!("expected Text frame, got {frame:?}").into());
        };
        let ev: serde_json::Value = serde_json::from_str(t.as_str())?;
        texts.push(
            ev["text"]
                .as_str()
                .ok_or("event frame carries no text")?
                .to_string(),
        );
    }
    Ok(texts)
}

// ── WebSocket session-only subscribe (issue #9) ──────────────────────────

/// Purpose: `subscribe` with `session_id` but no `run_id` resolves the
/// session's latest run through the EventLog seam and binds to it (#9).
/// Scenario: the log maps "session-1" → "test-run" and holds two past
/// events (seq 4 and 5); the client subscribes with `{session_id,
/// after_seq: 3}` only; a probe event is broadcast to trigger the replay.
/// Acceptance: the reply names the resolved run and echoes the session;
/// the seam recorded exactly one session query; replay names the resolved
/// run starting after the requested seq and precedes the live event.
#[tokio::test]
async fn ws_subscribe_session_id_only_resolves_latest_run() -> TestResult {
    let run_id = RunId("test-run".into());
    let session_id = SessionId("session-1".into());
    let log = Arc::new(ReplayEventLog {
        calls: std::sync::Mutex::new(Vec::new()),
        resolved: std::sync::Mutex::new(Vec::new()),
        latest_runs: std::sync::Mutex::new(HashMap::from([(session_id.0.clone(), run_id.clone())])),
        events: vec![run_event(4, "replayed-one"), run_event(5, "replayed-two")],
    });
    let state = Arc::new(TransportState::new(Arc::new(MockHandler), log.clone()));
    let app = harness_transport::ws::ws_routes(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (body, _, mut read) = ws_subscribe(
        &addr,
        8,
        serde_json::json!({ "session_id": session_id.0, "after_seq": 3 }),
    )
    .await?;
    assert_eq!(body["id"], 8, "response id echoes the request");
    assert_eq!(
        body["result"]["subscribed"], run_id.0,
        "subscribe reply names the session's latest run"
    );
    assert_eq!(
        body["result"]["session_id"], session_id.0,
        "subscribe reply echoes the session"
    );

    state.event_tx.send(run_event(9, "live"))?;
    let texts = read_event_texts(&mut read, 3).await?;
    assert_eq!(
        texts,
        vec!["replayed-one", "replayed-two", "live"],
        "replayed events precede the live event"
    );

    let calls = held(&log.calls);
    assert_eq!(calls.len(), 1, "exactly one replay query");
    assert_eq!(calls[0].0, run_id, "replay query names the resolved run");
    assert_eq!(calls[0].1, 3, "replay starts after the requested seq");
    let resolved = held(&log.resolved);
    assert_eq!(resolved.len(), 1, "exactly one session resolution");
    assert_eq!(resolved[0], session_id.0, "resolution names the session");
    Ok(())
}

/// Purpose: `subscribe` with a `session_id` that has no runs is rejected
/// instead of binding to a nonexistent run.
/// Scenario: the log resolves no sessions; the client subscribes with
/// `{session_id}` only.
/// Acceptance: the response is an `INVALID_REQUEST` error (-32600) whose
/// message is "no runs for session"; the session was resolved once but no
/// replay query is recorded.
#[tokio::test]
async fn ws_subscribe_session_id_without_runs_is_rejected() -> TestResult {
    let session_id = SessionId("session-1".into());
    let log = Arc::new(ReplayEventLog {
        calls: std::sync::Mutex::new(Vec::new()),
        resolved: std::sync::Mutex::new(Vec::new()),
        latest_runs: std::sync::Mutex::new(HashMap::new()),
        events: Vec::new(),
    });
    let state = Arc::new(TransportState::new(Arc::new(MockHandler), log.clone()));
    let app = harness_transport::ws::ws_routes(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (body, _, _) =
        ws_subscribe(&addr, 9, serde_json::json!({ "session_id": session_id.0 })).await?;
    assert_eq!(body["id"], 9, "response id echoes the request");
    let error = body.get("error").ok_or("expected an error response")?;
    assert_eq!(
        error["code"],
        error_code::INVALID_REQUEST,
        "a session without runs is INVALID_REQUEST"
    );
    assert_eq!(
        error["message"], "no runs for session",
        "the error names the missing run"
    );

    let calls = held(&log.calls);
    assert!(calls.is_empty(), "no replay query is recorded");
    let resolved = held(&log.resolved);
    assert_eq!(resolved.len(), 1, "the session was still resolved once");
    Ok(())
}

fn run_event(seq: u64, text: &str) -> RunEvent {
    RunEvent {
        run_id: RunId("test-run".into()),
        session_id: SessionId("session-1".into()),
        seq,
        timestamp_ms: 1_700_000_000_000 + seq as i64,
        kind: RunEventKind::TextDelta {
            text: text.to_string(),
        },
    }
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

    // Wait for the server to bind (bounded poll instead of a fixed sleep).
    let deadline = Instant::now() + Duration::from_secs(5);
    let stream = poll_until(
        || {
            let socket_path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(&socket_path)
                    .await
                    .map_err(|e| e.to_string())
            }
        },
        deadline,
    )
    .await
    .map_err(|e| format!("UDS server did not accept connections: {e}"))?;

    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = tokio::io::BufReader::new(reader);

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
