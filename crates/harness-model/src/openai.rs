//! OpenAI-compatible model adapter: HTTP transport, SSE delta parsing,
//! tool-call incremental assembly, and the [`Model`] trait implementation.
//!
//! # Design provenance
//!
//! - **pi** (`packages/agent/src/stream-fn.ts`) contributes the failure
//!   discipline — the stream never rejects; every failure becomes a
//!   terminal `ModelEvent::Error`.
//! - **harness-rs** (`crates/harness-models/src/openai.rs`) contributes
//!   the ToolCallStart/Args/End incremental assembly and the request-body
//!   serialization format.
//!
//! # SSE parsing
//!
//! The SSE parser is hand-written (no `eventsource` dependency). It scans
//! a byte buffer for `data:` lines terminated by ASCII `\n\n`, so multi-byte
//! UTF-8 is never split across chunk boundaries. This is
//! deliberately minimal — only the fields that OpenAI and DeepSeek actually
//! send are parsed. A full SSE implementation would need retry/reconnect
//! fields, event-id tracking, and multi-line data; those are unnecessary
//! for a request-response LLM API.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use harness_core::message::{Content, Message, StopReason};
use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream, Usage};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::backoff::RetryPolicy;
use crate::dialect::Dialect;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// An OpenAI-compatible model adapter.
///
/// Implements the [`Model`] trait by sending HTTP requests to an
/// OpenAI-compatible `/chat/completions` endpoint and translating SSE
/// streaming chunks into [`ModelEvent`] values.
///
/// # Stream-fn contract
///
/// The stream returned by [`Model::stream`] never carries a `Result` —
/// every failure (connection error, non-2xx response, JSON parse error,
/// mid-stream disconnect) maps to a terminal `ModelEvent::Error`. The
/// stream itself never panics or returns `Err`.
/// A stalled upstream must surface a terminal `ModelEvent::Error`, never
/// hang: the stream-fn contract requires every failure to surface as a
/// terminal error rather than a blocked request.
///
/// `CONNECT_TIMEOUT` bounds the TCP/TLS handshake; `REQUEST_TIMEOUT` bounds
/// the whole request, including a stalled SSE body read. Either expiry
/// becomes a transport error, retried with backoff, and ends as a terminal
/// `Error` once retries are exhausted.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub struct OpenAIModel {
    info: ModelInfo,
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    api_key_valid: bool,
    dialect: Dialect,
    retry_policy: RetryPolicy,
}

impl OpenAIModel {
    /// Create a new OpenAIModel with the given identity and endpoint.
    ///
    /// `info` is the catalog entry describing this model (handle, provider,
    /// context window, etc.).  `base_url` is the OpenAI-compatible API root
    /// (e.g. `https://api.openai.com/v1`); the adapter appends
    /// `/chat/completions` automatically.  `api_key` is the Bearer token.
    pub fn new(info: ModelInfo, base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        let api_key = api_key.into();
        // Reject control characters up front: reqwest fails to encode them
        // into a header value at send time, so validate in `new()` and
        // surface a clear terminal error at stream time instead.
        let api_key_valid = is_valid_api_key(&api_key);
        // Timeouts so a stalled upstream surfaces a terminal `Error` event
        // instead of hanging (stream-fn contract). The builder-level
        // User-Agent was dropped: if construction falls back to the default
        // client, per-request headers in `attempt_send` keep the wire UA
        // correct regardless.
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|err| {
                eprintln!("harness-model: custom client unavailable ({err}); using default");
                reqwest::Client::new()
            });
        Self {
            info,
            client,
            base_url: base_url.into(),
            api_key,
            api_key_valid,
            dialect: Dialect::OpenAI,
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Override the provider dialect (affects delta field parsing).
    pub fn with_dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Override the retry policy for transient failures.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }
}

#[async_trait]
impl Model for OpenAIModel {
    fn info(&self) -> &ModelInfo {
        &self.info
    }

    async fn stream(&self, req: &ModelRequest) -> ModelStream<'_> {
        // An API key carrying ASCII control characters can never be encoded
        // into an `Authorization` header — surface a clear terminal error
        // instead of a mysterious transport failure downstream.
        if !self.api_key_valid {
            let (tx, rx) = mpsc::unbounded_channel::<ModelEvent>();
            let _ = tx.send(ModelEvent::Error {
                message: "invalid API key: value is not encodable as an HTTP header value".into(),
            });
            return Box::pin(futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|event| (event, rx))
            }));
        }

        // Clone everything we own so the returned stream is 'static
        let client = self.client.clone();
        let url = format!("{}/chat/completions", self.base_url);
        let api_key = self.api_key.clone();
        let retry_policy = self.retry_policy;
        let dialect = self.dialect;

        let body = build_chat_request(req);

        // The channel-based stream bridge
        let (tx, rx) = mpsc::unbounded_channel::<ModelEvent>();

        tokio::spawn(async move {
            run_stream(client, url, api_key, body, retry_policy, dialect, tx).await;
        });

        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        }))
    }
}

// ---------------------------------------------------------------------------
// Internal: request body serialization
// ---------------------------------------------------------------------------

/// The wire-format request body for the `/chat/completions` endpoint.
#[derive(Debug, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ChatTool>>,
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ChatToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ChatToolCall {
    id: String,
    #[serde(rename = "type")]
    type_: String,
    function: ChatFunction,
}

#[derive(Debug, Serialize)]
struct ChatFunction {
    name: String,
    arguments: String,
}

#[derive(Debug, Serialize)]
struct ChatTool {
    #[serde(rename = "type")]
    type_: String,
    function: ChatToolDefinition,
}

#[derive(Debug, Serialize)]
struct ChatToolDefinition {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

/// Convert a core `ModelRequest` into the wire-format `ChatRequest`.
fn build_chat_request(req: &ModelRequest) -> ChatRequest {
    let messages: Vec<ChatMessage> = req.messages.iter().filter_map(map_message).collect();

    let tools = if req.tools.is_empty() {
        None
    } else {
        Some(
            req.tools
                .iter()
                .map(|t| ChatTool {
                    type_: "function".into(),
                    function: ChatToolDefinition {
                        name: t.name.clone(),
                        description: t.description.clone(),
                        parameters: t.input.clone(),
                    },
                })
                .collect(),
        )
    };

    ChatRequest {
        model: req.model.clone(),
        messages,
        stream: true,
        max_tokens: req.max_tokens,
        temperature: req.temperature,
        tools,
    }
}

/// Map a core `Message` to the wire-format `ChatMessage`.
fn map_message(msg: &Message) -> Option<ChatMessage> {
    match msg {
        Message::System { sections } => {
            // Render the section ledger: join all Some values in key order.
            let text = sections
                .values()
                .filter_map(|content| content.as_ref())
                .cloned()
                .collect::<Vec<_>>()
                .join("\n\n");
            let content = if text.is_empty() {
                None
            } else {
                Some(serde_json::Value::String(text))
            };
            Some(ChatMessage {
                role: "system".into(),
                content,
                tool_calls: None,
                tool_call_id: None,
            })
        }
        Message::User { content } => {
            let text: String = content
                .iter()
                .filter_map(|block| match block {
                    Content::Text(t) => Some(t.as_str()),
                    Content::Reasoning(r) => Some(r.as_str()),
                    Content::ToolCall(_) => None,
                    // `Content` is `#[non_exhaustive]` in harness-core:
                    // future block kinds have no OpenAI wire mapping.
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n\n");
            let content = if text.is_empty() {
                None
            } else {
                Some(serde_json::Value::String(text))
            };
            Some(ChatMessage {
                role: "user".into(),
                content,
                tool_calls: None,
                tool_call_id: None,
            })
        }
        Message::Assistant { content } => {
            let mut text_parts: Vec<String> = Vec::new();
            let mut tool_calls: Vec<ChatToolCall> = Vec::new();

            for block in content {
                match block {
                    Content::Text(t) => text_parts.push(t.clone()),
                    Content::Reasoning(_) => {
                        // OpenAI/DeepSeek do not echo reasoning content back
                        // in the request body — it is response-only.
                    }
                    Content::ToolCall(tc) => {
                        tool_calls.push(ChatToolCall {
                            id: tc.id.clone(),
                            type_: "function".into(),
                            function: ChatFunction {
                                name: tc.name.clone(),
                                arguments: tc.args.to_string(),
                            },
                        });
                    }
                    // `Content` is `#[non_exhaustive]`: future block kinds are
                    // not sent upstream (the adapter speaks text/reasoning/tool_calls).
                    _ => {}
                }
            }

            let text = text_parts.join("\n\n");
            let content = if text.is_empty() {
                None
            } else {
                Some(serde_json::Value::String(text))
            };
            let tool_calls = if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            };

            Some(ChatMessage {
                role: "assistant".into(),
                content,
                tool_calls,
                tool_call_id: None,
            })
        }
        Message::ToolResult(tm) => Some(ChatMessage {
            role: "tool".into(),
            content: Some(serde_json::Value::String(tm.content.clone())),
            tool_calls: None,
            tool_call_id: Some(tm.tool_call_id.clone()),
        }),
        // `Message` is `#[non_exhaustive]` in harness-core: a future variant
        // this adapter does not understand is dropped, not mislabeled.
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Internal: stream execution (retry loop + SSE parser)
// ---------------------------------------------------------------------------

/// The outcome of a single HTTP attempt.
enum AttemptOutcome {
    /// Successful 2xx response with SSE body.
    Success(reqwest::Response),
    /// Non-retryable failure (4xx, auth) — emit terminal error immediately.
    Fatal(String),
    /// Transient failure (connection, 5xx) — caller may retry.
    Transient(String),
}

/// Run the full request lifecycle: attempts, retries, SSE parsing, event
/// emission. Runs in a spawned tokio task; all results go through `tx`.
async fn run_stream(
    client: reqwest::Client,
    url: String,
    api_key: String,
    body: ChatRequest,
    retry_policy: RetryPolicy,
    dialect: Dialect,
    tx: mpsc::UnboundedSender<ModelEvent>,
) {
    let mut attempt: u32 = 0;
    loop {
        let outcome = attempt_send(&client, &url, &api_key, &body).await;

        match outcome {
            AttemptOutcome::Success(response) => {
                // Successful response — parse the SSE stream
                parse_sse_stream(response, dialect, &tx).await;
                return;
            }
            AttemptOutcome::Transient(_err) if attempt < retry_policy.max_retries => {
                // Transient failure — retry after backoff
                let delay = retry_policy.delay(attempt);
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            AttemptOutcome::Transient(err) => {
                // Exhausted retries — terminal error
                let _ = tx.send(ModelEvent::Error {
                    message: format!("after {} retries: {err}", retry_policy.max_retries),
                });
                return;
            }
            AttemptOutcome::Fatal(err) => {
                // Non-retryable (4xx) — terminal error immediately
                let _ = tx.send(ModelEvent::Error { message: err });
                return;
            }
        }
    }
}
/// Whether an API key can be encoded into an HTTP header value: free of
/// ASCII control characters (0x00-0x1F and 0x7F), which reqwest rejects at
/// send time. Validated in [`OpenAIModel::new`] so [`stream`](OpenAIModel::stream)
/// can emit a clear terminal error instead of a cryptic transport failure.
fn is_valid_api_key(api_key: &str) -> bool {
    api_key.bytes().all(|byte| !(byte < 0x20 || byte == 0x7F))
}

/// Send one request and classify the result.
async fn attempt_send(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    body: &ChatRequest,
) -> AttemptOutcome {
    let response = match client
        .post(url)
        .header("Authorization", format!("Bearer {api_key}"))
        // Per-request User-Agent: client construction can fall back to the
        // default reqwest client (whose blocklisted default UA would
        // otherwise leak onto the wire), so the UA is set on every request
        // regardless of the builder outcome.
        .header(
            "user-agent",
            concat!("harness-model/", env!("CARGO_PKG_VERSION")),
        )
        .header("Content-Type", "application/json")
        .json(body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return AttemptOutcome::Transient(format!("connection failed: {e}"));
        }
    };

    let status = response.status();
    if status.is_success() {
        AttemptOutcome::Success(response)
    } else if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
        // 5xx, 408 (Request Timeout) and 429 (Too Many Requests) —
        // transient: back off and retry instead of a terminal error.
        let error_body = response.text().await.unwrap_or_default();
        AttemptOutcome::Transient(format!("HTTP {status}: {error_body}"))
    } else {
        // 400/401/403/404 and other 4xx — fatal (auth, bad request).
        let error_body = response.text().await.unwrap_or_default();
        AttemptOutcome::Fatal(format!("HTTP {status}: {error_body}"))
    }
}

// ---------------------------------------------------------------------------
// Internal: SSE parsing
// ---------------------------------------------------------------------------

/// Parse the SSE response body into `ModelEvent` values sent through `tx`.
async fn parse_sse_stream(
    response: reqwest::Response,
    dialect: Dialect,
    tx: &mpsc::UnboundedSender<ModelEvent>,
) {
    let mut byte_stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    let mut tool_registry = ToolCallRegistry::new();
    // Tracks whether a terminal event (Stop or Error) has been emitted.
    // Once true, all subsequent SSE data is ignored — the stream-fn
    // contract prohibits events after a terminal event.
    let mut terminated = false;

    while let Some(chunk_result) = byte_stream.next().await {
        let bytes = match chunk_result {
            Ok(b) => b,
            Err(e) => {
                // Transport failure mid-stream — terminal error
                let _ = tx.send(ModelEvent::Error {
                    message: format!("stream interrupted: {e}"),
                });
                return;
            }
        };

        // Append raw bytes to the buffer; `\r\n` is normalized to `\n` so
        // SSE `\n\n` event boundaries survive chunking under either line
        // ending convention. An incomplete multi-byte UTF-8 tail is retained
        // until the next chunk completes it — no character is ever split
        // across the decode boundary.
        buffer.extend(normalize_crlf(&bytes));

        // Extract and emit all complete SSE events from the buffer
        while let Some(sse_data) = extract_sse_data(&mut buffer) {
            // Handle stream-end sentinel
            if sse_data == "[DONE]" {
                if !terminated {
                    for event in tool_registry.finish() {
                        let _ = tx.send(event);
                    }
                    let _ = tx.send(ModelEvent::Stop(StopReason::EndTurn));
                    terminated = true;
                }
                continue;
            }

            if terminated {
                continue;
            }

            // Parse the delta JSON
            match parse_delta(&sse_data, dialect, &mut tool_registry) {
                Ok(events) => {
                    for event in events {
                        if matches!(&event, ModelEvent::Stop(_) | ModelEvent::Error { .. }) {
                            terminated = true;
                        }
                        if tx.send(event).is_err() {
                            return;
                        }
                    }
                }
                Err(msg) => {
                    let _ = tx.send(ModelEvent::Error { message: msg });
                    return;
                }
            }
        }
    }

    // Stream ended naturally without a terminal event.
    if !terminated {
        for event in tool_registry.finish() {
            let _ = tx.send(event);
        }
        let _ = tx.send(ModelEvent::Stop(StopReason::EndTurn));
    }
}

/// Extract the `data:` payload from the next complete SSE event in `buf`.
///
/// Events are delimited by ASCII `\n\n` only, so a multi-byte UTF-8
/// sequence can never be split across the decode boundary: an incomplete
/// tail is retained in `buf` until the next chunk completes it. Each
/// complete event is decoded lossily — lossiness applies to whole events
/// only, never to partial chunks.
///
/// Removes the consumed bytes from `buf`. Returns `None` when no complete
/// event is available (waiting for more data).
fn extract_sse_data(buf: &mut Vec<u8>) -> Option<String> {
    let delim_pos = buf.windows(2).position(|window| window == b"\n\n")?;
    let event_block = buf[..delim_pos].to_vec();
    buf.drain(..delim_pos + 2);

    // Decode only complete events: a partial multi-byte sequence stays in
    // the tail until the rest of it arrives.
    let event_block = String::from_utf8_lossy(&event_block);

    // Collect all `data:` lines, joining multi-line values with `\n`
    // (the SSE spec allows data to span several lines).
    let mut data = String::new();
    for line in event_block.lines() {
        let line = line.trim_start();
        if let Some(value) = line.strip_prefix("data:") {
            let trimmed = value.trim();
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(trimmed);
        }
    }

    if data.is_empty() { None } else { Some(data) }
}

/// Replace CRLF with LF throughout `chunk` so SSE `\n\n` event boundaries
/// survive chunking regardless of the server's line-ending convention. A
/// `\r` split across the chunk boundary is left in place: it is harmless
/// because a lone `\r` never forms a `\n\n` delimiter, and `\r\n\n`
/// reconstructed across chunks still contains a genuine `\n\n`.
fn normalize_crlf(chunk: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunk.len());
    let mut rest = chunk;
    while let Some(pos) = rest.windows(2).position(|window| window == b"\r\n") {
        out.extend_from_slice(&rest[..pos]);
        out.push(b'\n');
        rest = &rest[pos + 2..];
    }
    out.extend_from_slice(rest);
    out
}

// ---------------------------------------------------------------------------
// Internal: delta JSON → ModelEvent mapping
// ---------------------------------------------------------------------------

/// Parse one SSE data payload into zero or more `ModelEvent` values.
fn parse_delta(
    json_data: &str,
    dialect: Dialect,
    tool_registry: &mut ToolCallRegistry,
) -> Result<Vec<ModelEvent>, String> {
    let chunk: SseChunk =
        serde_json::from_str(json_data).map_err(|e| format!("JSON parse error: {e}"))?;

    let mut events: Vec<ModelEvent> = Vec::new();

    // Usage (usually from the final chunk)
    if let Some(usage) = chunk.usage {
        events.push(ModelEvent::Usage(Usage {
            input_tokens: usage.prompt_tokens.unwrap_or(0),
            output_tokens: usage.completion_tokens.unwrap_or(0),
        }));
    }

    for choice in chunk.choices {
        let SseDeltaData {
            content,
            reasoning_content,
            tool_calls,
            ..
        } = choice.delta;

        // Text delta
        if let Some(text) = content {
            events.push(ModelEvent::TextDelta(text));
        }

        // Reasoning delta (DeepSeek dialect only)
        if let Some(reasoning) = reasoning_content
            && dialect == Dialect::DeepSeek
        {
            events.push(ModelEvent::ReasoningDelta(reasoning));
        }

        // Tool call deltas — incremental assembly by SSE index
        for tc in tool_calls {
            // `function_args` borrows all of `tc`, so own the arguments before
            // moving `id` out of the delta.
            let index = tc.index;
            let name = tc.function_name();
            let args = tc.function_args().map(str::to_owned);
            let mut tc_events = tool_registry.track(index, tc.id, name, args.as_deref());
            events.append(&mut tc_events);
        }

        // Finish reason
        if let Some(reason) = choice.finish_reason {
            match reason.as_str() {
                "stop" => {
                    events.extend(tool_registry.finish());
                    events.push(ModelEvent::Stop(StopReason::EndTurn));
                }
                "length" | "max_tokens" => {
                    events.extend(tool_registry.finish());
                    events.push(ModelEvent::Stop(StopReason::MaxTokens));
                }
                "tool_calls" => {
                    events.extend(tool_registry.finish());
                    events.push(ModelEvent::Stop(StopReason::ToolUse));
                }
                _ => {
                    events.extend(tool_registry.finish());
                    events.push(ModelEvent::Stop(StopReason::EndTurn));
                }
            }
        }
    }

    Ok(events)
}

// ---------------------------------------------------------------------------
// Internal: SSE chunk deserialization types
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize)]
struct SseChunk {
    #[serde(default)]
    choices: Vec<SseChoice>,
    #[serde(default)]
    usage: Option<SseUsage>,
}

#[derive(Debug, serde::Deserialize)]
struct SseChoice {
    delta: SseDeltaData,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct SseDeltaData {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<SseToolCallDelta>,
}

// The OpenAI streaming API uses snake_case field names which serde
// matches by default — no rename attribute needed.

#[derive(Debug, serde::Deserialize)]
struct SseToolCallDelta {
    index: u64,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<SseFunctionDelta>,
}

impl SseToolCallDelta {
    fn function_name(&self) -> Option<String> {
        self.function.as_ref().and_then(|f| f.name.clone())
    }

    fn function_args(&self) -> Option<&str> {
        self.function.as_ref().and_then(|f| f.arguments.as_deref())
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct SseFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct SseUsage {
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    completion_tokens: Option<u64>,
}

// ---------------------------------------------------------------------------
// Internal: tool-call registry for incremental delta assembly
// ---------------------------------------------------------------------------

/// Tracks in-progress tool calls during SSE delta parsing, keyed by the
/// SSE `index` field.
///
/// The OpenAI streaming API sends tool calls as an interleaved sequence of
/// deltas identified by `index`:
///
/// 1. First delta at index N: `{index: 0, id: "call_1", function: {name: "f", arguments: ""}}`
/// 2. Subsequent deltas at index N: `{index: 0, function: {arguments: "{\"loc\":"}}`
/// 3. Final arguments: `{index: 0, function: {arguments: "\"NYC\"}"}}`
struct ToolCallRegistry {
    /// Tool calls indexed by their SSE `index` field.
    by_index: HashMap<u64, ToolCallBuilder>,
}

#[derive(Debug)]
struct ToolCallBuilder {
    id: String,
    name: String,
    args: String,
}

impl ToolCallRegistry {
    fn new() -> Self {
        Self {
            by_index: HashMap::new(),
        }
    }

    /// Track one tool-call delta at `index`. Returns the `ModelEvent`s
    /// that the caller should emit for this delta.
    ///
    /// The first delta for a given index (one with `id` set) produces a
    /// `ToolCallStart`. Subsequent deltas (arguments only) produce
    /// `ToolCallArgs`. An empty `arguments` string does not produce a
    /// `ToolCallArgs` event — the first content-bearing arguments delta
    /// will.
    fn track(
        &mut self,
        index: u64,
        id: Option<String>,
        name: Option<String>,
        arguments: Option<&str>,
    ) -> Vec<ModelEvent> {
        let builder = self
            .by_index
            .entry(index)
            .or_insert_with(|| ToolCallBuilder {
                id: String::new(),
                name: String::new(),
                args: String::new(),
            });

        let mut events: Vec<ModelEvent> = Vec::new();

        if let Some(tc_id) = id {
            let tc_name = name.unwrap_or_default();
            builder.id = tc_id.clone();
            builder.name = tc_name.clone();
            events.push(ModelEvent::ToolCallStart {
                id: tc_id,
                name: tc_name,
            });
        }

        if let Some(args) = arguments
            && !args.is_empty()
        {
            builder.args.push_str(args);
            events.push(ModelEvent::ToolCallArgs {
                id: builder.id.clone(),
                partial_json: args.to_string(),
            });
        }

        events
    }

    /// Emit `ToolCallEnd` for every pending tool call and clear the
    /// registry.
    fn finish(&mut self) -> Vec<ModelEvent> {
        self.by_index
            .drain()
            .map(|(_, tc)| ModelEvent::ToolCallEnd { id: tc.id })
            .collect()
    }
}

// Tests live in crates/harness-model/tests/ — see the contract tests.
