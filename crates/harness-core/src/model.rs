//! The upstream model contract: one `Model` trait, one request shape, one
//! event vocabulary — and the stream-fn contract.
//!
//! Design provenance: pi's `StreamFn` (`packages/agent/src/stream-fn.ts`)
//! contributes the failure semantics — **the stream never errors; failures
//! arrive as terminal [`ModelEvent::Error`] events**. JS documents this in
//! prose; Rust puts it in the type: `ModelStream`'s item type is
//! `ModelEvent`, with no `Result` wrapper. harness-rs's `Model`/`ModelInfo`
//! (`crates/harness-core/src/model.rs`) contributes the delta vocabulary
//! (ToolCallStart/Args/End so a stream rebuilds tool calls incrementally)
//! and the catalog data — minus its `Result<ModelDelta, ModelError>`
//! wrapping, minus the `DynModel` newtype that works around an
//! `async_trait` recursion bug we simply do not create.
//!
//! There is deliberately no `complete`: a headless daemon is streaming-only;
//! a caller that wants a whole turn accumulates deltas, and errors are
//! values along the way.

use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::message::{Message, StopReason};

/// One completion request, provider-neutral: the transcript (system ledger
/// messages included — adapters render the sections into the provider's
/// stable prompt prefix), the tool catalog as the provider sees it, and the
/// few knobs the run controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    /// Resolved model id the adapter dials; alias resolution happened
    /// upstream (harness-config / harness-model).
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<crate::tool::ToolSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
}

/// One event on a model stream.
///
/// **Terminal events** are [`ModelEvent::Stop`] and [`ModelEvent::Error`]:
/// implementations must not emit anything after a terminal event, and
/// consumers may stop polling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ModelEvent {
    /// Incremental assistant text.
    TextDelta(String),
    /// Incremental reasoning trace (DeepSeek-style `reasoning_content`).
    ReasoningDelta(String),
    /// The model started requesting a tool call; `id` keys the later args
    /// and the eventual `ToolCall`/`ToolResult` correlation.
    ToolCallStart { id: String, name: String },
    /// Partial JSON arguments of a tool call — a streaming loop assembles
    /// the full args from these alone (harness-rs's delta shape).
    ToolCallArgs { id: String, partial_json: String },
    /// One tool call's arguments finished parsing.
    ToolCallEnd { id: String },
    /// Token accounting, whenever the provider reports it (usually with the
    /// final chunk).
    Usage(Usage),
    /// Clean stop: the model finished the turn or hit the token cap.
    Stop(StopReason),
    /// Failure encoded as an event: connection, transport, authentication or
    /// parse failures land here. Always terminal — the stream-fn contract.
    Error { message: String },
}

/// Token accounting for one model call. Provider-specific cache fields are
/// deferred — add them when an adapter needs them to bill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// Uniform catalog entry for a configured model (harness-rs's `ModelInfo`,
/// minus cost fields — pricing belongs to the metering layer, not core).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInfo {
    /// The user-chosen logical identifier (logs, config selectors).
    pub handle: String,
    /// Which provider adapter serves this model.
    pub provider: String,
    /// The wire-protocol model id sent to the provider.
    pub model: String,
    /// Context window in tokens; compaction calibrates against it.
    pub context_window: u32,
    #[serde(default = "default_true")]
    pub supports_tool_use: bool,
    #[serde(default = "default_true")]
    pub supports_streaming: bool,
}

fn default_true() -> bool {
    true
}

/// `Pin<Box<dyn Stream<Item = ModelEvent> + Send + 'a>>` — one completion
/// request's event stream, borrowing the `Model` for its lifetime.
pub type ModelStream<'a> = Pin<Box<dyn Stream<Item = ModelEvent> + Send + 'a>>;

/// One upstream model. Implementations live in harness-model; the loop and
/// the tests speak only this trait.
///
/// The stream itself cannot fail: see the stream-fn contract above. A
/// mid-stream failure arrives as the terminal `Error` event, so retry,
/// cancellation and compaction all react to the same vocabulary.
#[async_trait]
pub trait Model: Send + Sync + 'static {
    /// Which model this adapter serves (catalog surface for routing).
    fn info(&self) -> &ModelInfo;

    /// Stream events for one request.
    async fn stream(&self, req: &ModelRequest) -> ModelStream<'_>;
}
