//! The upstream model contract: `Model` trait, request shape, and the stream
//! event vocabulary.
//!
//! The trait is a port of harness-rs's `Model` (which has concrete adapters
//! in `harness-models`) with one deliberate change to the failure semantics:
//! **the stream never errors — failures arrive as terminal
//! [`ModelEvent::Error`] events.** This is pi's `StreamFn` contract
//! (`packages/agent/src/stream-fn.ts`: the stream function must never throw;
//! failures are encoded as protocol events ending in a stop reason). Keeping
//! the invariant here makes retry, abort and compaction all speak the same
//! event vocabulary: the loop reacts to an error event exactly like it reacts
//! to a stop event, and no error path can unwind the loop.
//!
//! The trait is sync-headed but async-bodied: `stream` returns a stream
//! because adapters do their HTTP work incrementally; the trait stays free of
//! any runtime dependency (`futures_core::Stream` only), so `core` remains
//! the leaf every other harness crate stands on.

use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::context::{Block, Turn};
use crate::tool::{ToolCallSpec, ToolSpec};

/// `Pin<Box<dyn Stream<Item = ModelEvent> + Send + 'a>>` — one completion
/// request's event stream. The lifetime borrows the `Model` (the adapter
/// usually clones its client into the stream and ignores it).
pub type ModelStream<'a> = Pin<Box<dyn Stream<Item = ModelEvent> + Send + 'a>>;

/// One upstream model. Implementations live in `harness-model` (OpenAI-
/// compatible, dialects); the loop only speaks this trait, so a mock in
/// `tests/` drives the whole contract.
#[async_trait]
pub trait Model: Send + Sync + 'static {
    /// Stable identifier for logging and routing (e.g. the resolved alias
    /// target this adapter serves).
    fn id(&self) -> &str;

    /// Stream events for one completion request.
    ///
    /// Failures — connection refused, rejected request, parse error mid-
    /// stream — arrive as [`ModelEvent::Error`], the terminal event. The
    /// returned future itself only fails on programmer error (panics).
    async fn stream(&self, req: &ModelRequest) -> ModelStream<'_>;
}

/// One completion request, provider-neutral: the resolved model id, the
/// assembled prompt pieces ([`Block`]s for the stable prefix, [`Turn`]s for
/// history), the tool catalog as the provider sees it, and the few knobs the
/// loop controls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRequest {
    /// Resolved model id the adapter dials (alias resolution happened
    /// upstream, in harness-model/harness-config).
    pub model: String,
    pub system: Vec<Block>,
    pub messages: Vec<Turn>,
    pub tools: Vec<ToolSpec>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
}

/// One event on a model stream.
///
/// Terminal events are [`ModelEvent::Stop`] and [`ModelEvent::Error`]:
/// implementations must not emit anything after a terminal event, and
/// consumers may stop polling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ModelEvent {
    /// Incremental assistant text.
    TextDelta(String),
    /// Incremental reasoning trace (DeepSeek-style `reasoning_content`).
    ReasoningDelta(String),
    /// A complete parsed tool call from the stream.
    ToolCall(ToolCallSpec),
    /// Token accounting, whenever the provider reports it (usually with the
    /// final chunk).
    Usage(Usage),
    /// Clean stop: the model finished the turn, or hit the token cap.
    Stop(StopReason),
    /// Failure encoded as an event (the stream-fn contract): connection,
    /// transport, authentication or parse failures land here. Always
    /// terminal.
    Error { message: String },
}

/// Why a stream stopped cleanly. Caller-level cancellation is **not** here:
/// dropping the stream ends iteration; aborted rounds surface as
/// [`crate::event::TurnStop::Aborted`] at the loop level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum StopReason {
    EndTurn,
    MaxTokens,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}
