//! Contract tests for the OpenAI-compatible model adapter.
//!
//! Purpose: verify that `OpenAIModel` correctly maps SSE streaming
//! responses to `ModelEvent` values under real HTTP conditions, using a
//! mock axum server.
//!
//! Scenarios covered:
//! - Text-only streaming: multiple content deltas → `TextDelta` events.
//! - Tool call streaming: ToolCallStart → ToolCallArgs → ToolCallEnd.
//! - Connection failure → terminal `Error` event.
//! - Non-2xx HTTP response → terminal `Error` event.
//! - DeepSeek reasoning: `reasoning_content` → `ReasoningDelta`.
//! - Usage event from final chunk.
//!
//! Each test starts a local axum server on a random port, creates an
//! `OpenAIModel` pointed at that server, sends a `ModelRequest`, and
//! collects the stream of `ModelEvent` values for assertion.
//!
//! Acceptance criteria: every event is a `ModelEvent`, never a `Result`;
//! terminal events (`Stop` / `Error`) end the stream; the stream never
//! panics.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::Event;
use axum::routing::post;
use axum::{Json, Router};
use futures::StreamExt;
use harness_core::message::{Message, StopReason};
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest};
use harness_model::{Dialect, OpenAIModel, RetryPolicy};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a minimal `ModelInfo` for testing.
fn test_model_info() -> ModelInfo {
    ModelInfo {
        handle: "test-model".into(),
        provider: "test".into(),
        model: "test-model".into(),
        context_window: 4096,
        supports_tool_use: true,
        supports_streaming: true,
    }
}

/// Create a minimal `ModelRequest` with a single user message.
fn test_request() -> ModelRequest {
    ModelRequest {
        model: "test-model".into(),
        messages: vec![Message::user_text("Hello")],
        tools: vec![],
        max_tokens: None,
        temperature: None,
    }
}

/// A retry policy with minimal backoff for fast test execution.
fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        max_retries: 1,
        initial_backoff_ms: 1,
        max_backoff_ms: 10,
    }
}

/// A mock SSE server that returns predetermined chunks.
///
/// Each entry in `responses` is raw JSON (without SSE framing). The axum
/// `Sse` response adds `data:` prefix and `\n\n` delimiters automatically.
struct MockSseServer {
    responses: Vec<String>,
    status: u16,
}

impl MockSseServer {
    fn new(responses: Vec<String>) -> Self {
        Self {
            responses,
            status: 200,
        }
    }
}

/// Start a mock server on a random port and return its address and a
/// handle that controls what responses it sends.
async fn start_mock() -> (SocketAddr, Arc<Mutex<MockSseServer>>) {
    let state = Arc::new(Mutex::new(MockSseServer::new(vec![])));
    let app_state = Arc::clone(&state);

    let first_state = Arc::clone(&app_state);

    let app = Router::new()
        .route(
            "/v1/chat/completions",
            post(move |_body: Json<serde_json::Value>| {
                let state = Arc::clone(&first_state);
                async move {
                    let guard = state.lock().await;
                    if guard.status != 200 {
                        let code = StatusCode::from_u16(guard.status)
                            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                        return (code, "mock error").into_response();
                    }

                    let chunks = guard.responses.clone();
                    // Each chunk is raw JSON; axum Sse wraps it in SSE framing.
                    // The SSE stream item must be a Result, not a bare Event.
                    let stream = futures::stream::iter(chunks.into_iter().map(|json| {
                        Ok::<_, std::convert::Infallible>(Event::default().data(json))
                    }));

                    axum::response::sse::Sse::new(stream).into_response()
                }
            }),
        )
        .route(
            "/chat/completions",
            post(move |_body: Json<serde_json::Value>| {
                let state = Arc::clone(&app_state);
                async move {
                    let guard = state.lock().await;
                    if guard.status != 200 {
                        let code = StatusCode::from_u16(guard.status)
                            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                        return (code, "mock error").into_response();
                    }

                    let chunks = guard.responses.clone();
                    let stream = futures::stream::iter(chunks.into_iter().map(|json| {
                        Ok::<_, std::convert::Infallible>(Event::default().data(json))
                    }));

                    axum::response::sse::Sse::new(stream).into_response()
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("bind mock server: {e}"));
    let addr = listener
        .local_addr()
        .unwrap_or_else(|e| panic!("get addr: {e}"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (addr, state)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Text-only streaming: multiple content deltas produce `TextDelta` events
/// followed by a `Stop(EndTurn)`.
#[tokio::test]
async fn text_streaming() {
    let (addr, state) = start_mock().await;
    let base_url = format!("http://{addr}/v1");

    // Raw JSON chunks (axum SSE adds the framing).
    {
        let mut s = state.lock().await;
        s.responses = vec![
            r#"{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"},"index":0}]}"#.into(),
            r#"{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"content":" world"},"index":0}]}"#.into(),
            r#"{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":5}}"#.into(),
            "[DONE]".into(),
        ];
    }

    let model =
        OpenAIModel::new(test_model_info(), &base_url, "sk-test").with_retry_policy(fast_retry());
    let stream = model.stream(&test_request()).await;

    let events: Vec<ModelEvent> = stream.collect().await;
    assert_eq!(events.len(), 4, "expected 4 events, got {events:?}");

    // 1. TextDelta "Hello"
    assert!(
        matches!(&events[0], ModelEvent::TextDelta(t) if t == "Hello"),
        "expected TextDelta('Hello'), got {:?}",
        events[0]
    );

    // 2. TextDelta " world"
    assert!(
        matches!(&events[1], ModelEvent::TextDelta(t) if t == " world"),
        "expected TextDelta(' world'), got {:?}",
        events[1]
    );

    // 3. Usage
    assert!(
        matches!(&events[2], ModelEvent::Usage(u) if u.input_tokens == 10 && u.output_tokens == 5),
        "expected Usage(10,5), got {:?}",
        events[2]
    );

    // 4. Stop
    assert_eq!(events[3], ModelEvent::Stop(StopReason::EndTurn));
}

/// Tool call streaming: the delta sequence produces
/// ToolCallStart → ToolCallArgs → ToolCallEnd → Stop.
#[tokio::test]
async fn tool_call_streaming() {
    let (addr, state) = start_mock().await;
    let base_url = format!("http://{addr}/v1");

    {
        let mut s = state.lock().await;
        s.responses = vec![
            // First delta: id + name
            r#"{"id":"2","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_abc","type":"function","function":{"name":"get_weather","arguments":""}}]},"index":0}]}"#.into(),
            // Second delta: arguments (first part)
            r#"{"id":"2","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"location\":"}}]},"index":0}]}"#.into(),
            // Third delta: arguments (second part)
            r#"{"id":"2","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"NYC\"}"}}]},"index":0}]}"#.into(),
            // Finish reason
            r#"{"id":"2","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"tool_calls","index":0}]}"#.into(),
            "[DONE]".into(),
        ];
    }

    let model =
        OpenAIModel::new(test_model_info(), &base_url, "sk-test").with_retry_policy(fast_retry());
    let stream = model.stream(&test_request()).await;

    let events: Vec<ModelEvent> = stream.collect().await;
    assert_eq!(events.len(), 5, "expected 5 events, got {events:?}");

    // 1. ToolCallStart
    assert!(
        matches!(&events[0], ModelEvent::ToolCallStart { id, name } if id == "call_abc" && name == "get_weather"),
        "expected ToolCallStart(call_abc, get_weather), got {:?}",
        events[0]
    );

    // 2. ToolCallArgs (first)
    assert!(
        matches!(&events[1], ModelEvent::ToolCallArgs { id, partial_json } if id == "call_abc" && partial_json == "{\"location\":"),
        "expected ToolCallArgs, got {:?}",
        events[1]
    );

    // 3. ToolCallArgs (second)
    assert!(
        matches!(&events[2], ModelEvent::ToolCallArgs { id, partial_json } if id == "call_abc" && partial_json == "\"NYC\"}"),
        "expected ToolCallArgs, got {:?}",
        events[2]
    );

    // 4. ToolCallEnd
    assert!(
        matches!(&events[3], ModelEvent::ToolCallEnd { id } if id == "call_abc"),
        "expected ToolCallEnd(call_abc), got {:?}",
        events[3]
    );

    // 5. Stop
    assert_eq!(events[4], ModelEvent::Stop(StopReason::ToolUse));
}

/// Connection failure (no server) produces a terminal `Error` event.
#[tokio::test]
async fn connection_failure_produces_error() {
    // Point at a port where nothing is listening
    let base_url = "http://127.0.0.1:1/v1".to_string();
    let model =
        OpenAIModel::new(test_model_info(), &base_url, "sk-test").with_retry_policy(fast_retry());
    let stream = model.stream(&test_request()).await;

    let events: Vec<ModelEvent> = stream.collect().await;
    assert!(!events.is_empty(), "expected at least one event");

    // The last event must be an Error (terminal)
    let last = events
        .last()
        .unwrap_or_else(|| panic!("at least one event"));
    assert!(
        matches!(last, ModelEvent::Error { .. }),
        "expected Error event, got {last:?}"
    );
}

/// A non-2xx HTTP response produces a terminal `Error` event.
#[tokio::test]
async fn http_error_produces_error() {
    let (addr, state) = start_mock().await;
    let base_url = format!("http://{addr}/v1");

    {
        let mut s = state.lock().await;
        s.status = 401;
    }

    let model = OpenAIModel::new(test_model_info(), &base_url, "sk-bad-key")
        .with_retry_policy(fast_retry());
    let stream = model.stream(&test_request()).await;

    let events: Vec<ModelEvent> = stream.collect().await;
    assert!(!events.is_empty(), "expected at least one event");

    let last = events
        .last()
        .unwrap_or_else(|| panic!("at least one event"));
    assert!(
        matches!(last, ModelEvent::Error { .. }),
        "expected Error event, got {last:?}"
    );
}

/// DeepSeek dialect: `reasoning_content` maps to `ReasoningDelta`.
#[tokio::test]
async fn deepseek_reasoning_delta() {
    let (addr, state) = start_mock().await;
    let base_url = format!("http://{addr}/v1");

    {
        let mut s = state.lock().await;
        s.responses = vec![
            r#"{"id":"3","object":"chat.completion.chunk","choices":[{"delta":{"reasoning_content":"Let me think..."},"index":0}]}"#.into(),
            r#"{"id":"3","object":"chat.completion.chunk","choices":[{"delta":{"content":"The answer is 42"},"index":0}]}"#.into(),
            r#"{"id":"3","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}"#.into(),
            "[DONE]".into(),
        ];
    }

    let model = OpenAIModel::new(test_model_info(), &base_url, "sk-test")
        .with_dialect(Dialect::DeepSeek)
        .with_retry_policy(fast_retry());
    let stream = model.stream(&test_request()).await;

    let events: Vec<ModelEvent> = stream.collect().await;
    assert_eq!(events.len(), 3, "expected 3 events, got {events:?}");

    // 1. ReasoningDelta
    assert!(
        matches!(&events[0], ModelEvent::ReasoningDelta(r) if r == "Let me think..."),
        "expected ReasoningDelta, got {:?}",
        events[0]
    );

    // 2. TextDelta
    assert!(
        matches!(&events[1], ModelEvent::TextDelta(t) if t == "The answer is 42"),
        "expected TextDelta, got {:?}",
        events[1]
    );

    // 3. Stop
    assert_eq!(events[2], ModelEvent::Stop(StopReason::EndTurn));
}

/// The stream-fn contract: even with no server, the stream never panics
/// and returns a terminal Error event.
#[tokio::test]
async fn stream_fn_contract_no_panic_on_failure() {
    let base_url = "http://127.0.0.1:1/v1".to_string();
    let model =
        OpenAIModel::new(test_model_info(), &base_url, "sk-test").with_retry_policy(fast_retry());
    let stream = model.stream(&test_request()).await;

    // Collect should never panic
    let events: Vec<ModelEvent> = stream.collect().await;

    // The last event must be Error (terminal)
    assert!(!events.is_empty());
    let last = events.last().unwrap_or_else(|| panic!("events"));
    assert!(matches!(last, ModelEvent::Error { .. }));

    // Nothing after Error (stream ended)
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ModelEvent::Error { .. }))
            .count(),
        1,
        "exactly one Error event expected"
    );
}

/// Multiple tool calls in a single delta produce separate Start events.
#[tokio::test]
async fn multiple_tool_calls_in_one_delta() {
    let (addr, state) = start_mock().await;
    let base_url = format!("http://{addr}/v1");

    {
        let mut s = state.lock().await;
        s.responses = vec![
            // Two tool calls starting in the same chunk
            r#"{"id":"4","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}},{"index":1,"id":"call_2","type":"function","function":{"name":"get_time","arguments":""}}]},"index":0}]}"#.into(),
            // Arguments for both
            r#"{"id":"4","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"location\":"}},{"index":1,"function":{"arguments":"{\"tz\":"}}]},"index":0}]}"#.into(),
            r#"{"id":"4","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"NYC\"}"}},{"index":1,"function":{"arguments":"\"EST\"}"}}]},"finish_reason":"tool_calls","index":0}]}"#.into(),
            "[DONE]".into(),
        ];
    }

    let model =
        OpenAIModel::new(test_model_info(), &base_url, "sk-test").with_retry_policy(fast_retry());
    let stream = model.stream(&test_request()).await;

    let events: Vec<ModelEvent> = stream.collect().await;
    // Expected: ToolCallStart(x2) + ToolCallArgs(x4) + ToolCallEnd(x2) + Stop = 9
    assert_eq!(events.len(), 9, "expected 9 events, got {events:?}");

    // Verify the start events
    assert!(
        matches!(&events[0], ModelEvent::ToolCallStart { id, name } if id == "call_1" && name == "get_weather")
    );
    assert!(
        matches!(&events[1], ModelEvent::ToolCallStart { id, name } if id == "call_2" && name == "get_time")
    );
}
