//! The model stream pump: per-token machinery owned by [`super::model`].
//!
//! This is the `futures` / `tokio:macros` / `serde_json` corner of the
//! crate — the dependency manifest justifies each of those by this file
//! alone. Nothing outside the model node sees a raw `ModelEvent`.

use std::sync::atomic::AtomicU64;

use futures::{StreamExt, pin_mut};
use harness_core::RunControl;
use harness_core::event::{EventSink, RunEventKind};
use harness_core::message::{Content, ToolCall};
use harness_core::model::{ModelEvent, ModelStream};
use harness_core::run::RunContext;

use crate::dispatch::emit_event;

/// The outcome of pumping one model stream.
pub(crate) enum StreamTurn {
    /// Stream ended cleanly (via `ModelEvent::Stop`).
    Ok {
        content: Vec<Content>,
        calls: Vec<ToolCall>,
    },
    /// The run was cancelled while the stream was open — no turn content.
    Cancelled,
    /// Stream ended with `ModelEvent::Error`.
    Error {
        message: String,
        partial_content: Vec<Content>,
        partial_calls: Vec<ToolCall>,
    },
}

/// Accumulated state while pumping one model stream: the assistant content
/// blocks and the in-progress tool call argument buffer.
struct PumpState {
    text: String,
    reasoning: String,
    tool_calls: Vec<ToolCall>,
    // Accumulator for in-progress tool call args: (id, name, args_json_fragments)
    partial: Option<(String, String, String)>,
}

/// One classified stream event: keep pumping, stop at a clean terminal, or fail.
enum PumpStep {
    /// Keep polling the stream.
    Continue,
    /// `ModelEvent::Stop`: the model finished its turn cleanly.
    Break,
    /// `ModelEvent::Error`: stop and return the error message.
    Error(String),
}

/// Classify one stream event: update the accumulators, emit non-terminal
/// events, and report whether pumping should stop.
///
/// A `ModelEvent::Stop` ends the turn cleanly; `ModelEvent::Error` ends it
/// with the partial content accumulated so far. Everything else is absorbed
/// into `state` and pumping continues.
fn absorb_event(
    event: &ModelEvent,
    state: &mut PumpState,
    seq: &AtomicU64,
    sink: &dyn EventSink,
    ctx: &RunContext,
) -> PumpStep {
    match event {
        ModelEvent::TextDelta(t) => {
            state.text.push_str(t);
            emit_event(
                sink,
                &ctx.run_id,
                &ctx.session_id,
                seq,
                RunEventKind::TextDelta { text: t.clone() },
            );
            PumpStep::Continue
        }
        ModelEvent::ReasoningDelta(r) => {
            state.reasoning.push_str(r);
            // Reasoning deltas have no dedicated RunEventKind yet; they are
            // preserved in Content::Reasoning blocks.
            PumpStep::Continue
        }
        ModelEvent::ToolCallStart { id, name } => {
            state.partial = Some((id.clone(), name.clone(), String::new()));
            PumpStep::Continue
        }
        ModelEvent::ToolCallArgs { id, partial_json } => {
            if let Some((cur_id, _, args)) = state.partial.as_mut()
                && cur_id == id
            {
                args.push_str(partial_json);
            }
            PumpStep::Continue
        }
        ModelEvent::ToolCallEnd { id } => {
            if let Some((_, name, args_str)) = state.partial.as_mut()
                && !state.tool_calls.iter().any(|tc| tc.id == *id)
            {
                let args: serde_json::Value =
                    serde_json::from_str(args_str).unwrap_or(serde_json::Value::Null);
                state.tool_calls.push(ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    args,
                });
            }
            state.partial = None;
            PumpStep::Continue
        }
        ModelEvent::Usage(_usage) => {
            // Token accounting — no dedicated RunEventKind in this version.
            PumpStep::Continue
        }
        ModelEvent::Stop(_reason) => {
            // All StopReason variants (EndTurn, ToolUse, MaxTokens) are
            // clean terminators — the model finished its turn.
            PumpStep::Break
        }
        ModelEvent::Error { message } => PumpStep::Error(message.clone()),
        // ModelEvent is #[non_exhaustive]: future variants are ignored at
        // the stream level (they are not terminal).
        _ => PumpStep::Continue,
    }
}

/// Pump a model stream: emit events, accumulate content and tool calls.
///
/// # Termination discipline
///
/// After receiving `ModelEvent::Stop` or `ModelEvent::Error` (terminal
/// events) the function stops polling and ignores any subsequent events.
/// This is the receiving side of the stream-fn contract (core documents
/// the emission side).
pub(crate) async fn pump_stream(
    stream: ModelStream<'_>,
    seq: &AtomicU64,
    sink: &dyn EventSink,
    ctx: &RunContext,
    control: &RunControl,
) -> StreamTurn {
    pin_mut!(stream);

    let mut state = PumpState {
        text: String::new(),
        reasoning: String::new(),
        tool_calls: Vec::new(),
        partial: None,
    };
    // The stream race is the drop protocol's teeth: cancelling mid-stream
    // stops the model request immediately instead of burning tokens into a
    // void until the stream happens to return.
    let cancel = control.cancel_token();

    loop {
        let event = tokio::select! {
            _ = cancel.cancelled() => return StreamTurn::Cancelled,
            next = stream.next() => match next {
                Some(event) => event,
                None => break,
            },
        };
        match absorb_event(&event, &mut state, seq, sink, ctx) {
            PumpStep::Continue => {}
            PumpStep::Break => break,
            PumpStep::Error(message) => {
                let mut content = Vec::new();
                if !state.reasoning.is_empty() {
                    content.push(Content::Reasoning(state.reasoning));
                }
                if !state.text.is_empty() {
                    content.push(Content::Text(state.text));
                }
                return StreamTurn::Error {
                    message,
                    partial_content: content,
                    partial_calls: state.tool_calls,
                };
            }
        }
    }

    // Build assistant content blocks from accumulated text, reasoning and
    // tool calls.
    let mut content = Vec::new();
    if !state.reasoning.is_empty() {
        content.push(Content::Reasoning(state.reasoning));
    }
    if !state.text.is_empty() {
        content.push(Content::Text(state.text));
    }
    for tc in &state.tool_calls {
        content.push(Content::ToolCall(tc.clone()));
    }

    StreamTurn::Ok {
        content,
        calls: state.tool_calls,
    }
}
