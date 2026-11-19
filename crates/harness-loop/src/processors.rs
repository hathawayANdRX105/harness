//! Processor nodes for the agent loop's topology.
//!
//! Three phases of nodes live here:
//!
//! - prompt-side: what the legacy `prepare_request` hook did — the system
//!   ledger leads the transcript, the instruction is appended once;
//! - the model node: the one place that holds the model stream;
//! - response-side: append the assistant content and gate the run, then
//!   drive the tool batch through the policy → approval → execute → after
//!   chain.
//!
//! Slice 5 note: the response side is one batch-level processor rather than
//! the design doc's three (approval / tool-exec / output), because the
//! six-step chain interleaves per tool call in sequential mode — splitting
//! it by stage would reorder events relative to legacy behavior. The split
//! remains the follow-up once per-call state rides `TurnArtifacts`.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use async_trait::async_trait;
use futures::{StreamExt, pin_mut};
use harness_core::RunControl;
use harness_core::event::{EventSink, RunEventKind};
use harness_core::message::{Content, Message, ToolCall, ToolResultMessage};
use harness_core::model::{ModelEvent, ModelRequest, ModelStream};
use harness_core::policy::{ApprovalBroker, ToolPolicy};
use harness_core::run::{RunContext, RunStopReason};
use harness_core::topology::{Processor, Route, Turn};

use crate::dispatch::{DispatchCtx, dispatch_batch, emit_event};
use crate::hooks::{LoopHooks, ToolExecutionMode};
use crate::transcript::{self, TranscriptSink};

// ---------------------------------------------------------------------------
// Prompt-side nodes
// ---------------------------------------------------------------------------

/// Ensure the system prompt ledger message leads the transcript.
///
/// The builder renders the spec's prompt sections into one `Message::System`
/// ledger; this node re-seats it at position 0 every turn so a compacted or
/// replayed transcript cannot drift it out of place.
pub struct SystemLedgerProcessor {
    prompt: Message,
}

impl SystemLedgerProcessor {
    pub fn new(prompt: Message) -> Self {
        Self { prompt }
    }
}

#[async_trait]
impl Processor for SystemLedgerProcessor {
    fn name(&self) -> &'static str {
        "system_ledger"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        if turn.ledger.first() != Some(&self.prompt) {
            turn.ledger.insert(0, self.prompt.clone());
        }
        Route::Next
    }
}

/// Append the declared instruction as a user message once per transcript.
///
/// The `InjectInstruction` hook spec (harness-builder) materializes as one
/// of these nodes: idempotent — a transcript that already carries the
/// instruction (a resumed run) is left alone.
pub struct InstructionProcessor {
    instruction: Message,
}

impl InstructionProcessor {
    pub fn new(text: &str) -> Self {
        Self {
            instruction: Message::user_text(text),
        }
    }
}

#[async_trait]
impl Processor for InstructionProcessor {
    fn name(&self) -> &'static str {
        "instruction"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        if !turn.ledger.contains(&self.instruction) {
            turn.ledger.push(self.instruction.clone());
        }
        Route::Next
    }
}

// ---------------------------------------------------------------------------
// The model node
// ---------------------------------------------------------------------------

/// The one node that holds the model stream.
///
/// Everything per-token lives here: building the request from the ledger
/// and tool set, racing the cancel token against the stream, folding
/// `ModelEvent`s into the turn's artifacts. The other nodes see only the
/// turn-boundary product (`TurnArtifacts`), never a raw event — that is the
/// hot-loop rule from `docs/topology-design.md` §5.
pub struct ModelProcessor {
    event_sink: Arc<dyn EventSink>,
}

impl ModelProcessor {
    pub fn new(event_sink: Arc<dyn EventSink>) -> Self {
        Self { event_sink }
    }
}

#[async_trait]
impl Processor for ModelProcessor {
    fn name(&self) -> &'static str {
        "model"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        // Claim the stream through an owned Arc so no borrow of `turn`
        // crosses the stream's lifetime.
        let model = Arc::clone(&turn.model);
        let request = ModelRequest {
            model: model.info().model.clone(),
            // One render per turn: the ledger is the source, the request is
            // the wire shape (same one-off clone the legacy `model_turn` did).
            messages: turn.ledger.clone(),
            tools: turn.tools.iter().map(|tool| tool.schema()).collect(),
            max_tokens: None,
            temperature: None,
        };
        emit_event(
            self.event_sink.as_ref(),
            &turn.ctx.run_id,
            &turn.ctx.session_id,
            &turn.seq,
            RunEventKind::TurnStarted {
                turn_idx: turn.turn_idx,
            },
        );
        let stream = model.stream(&request).await;
        let control = turn.control.clone();
        let outcome = pump_stream(
            stream,
            &turn.seq,
            self.event_sink.as_ref(),
            &turn.ctx,
            &control,
        )
        .await;
        // `TurnFinished` closes every turn that reached a boundary: the
        // cancelled path never saw a clean one (legacy behavior —
        // Cancelled skips the event, Ok and Error both emit).
        if !matches!(outcome, StreamTurn::Cancelled) {
            emit_event(
                self.event_sink.as_ref(),
                &turn.ctx.run_id,
                &turn.ctx.session_id,
                &turn.seq,
                RunEventKind::TurnFinished {
                    turn_idx: turn.turn_idx,
                },
            );
        }
        match outcome {
            StreamTurn::Ok { content, calls } => {
                turn.artifacts.content = content;
                turn.artifacts.calls = calls;
                Route::Next
            }
            StreamTurn::Cancelled => Route::Done(RunStopReason::Cancelled),
            StreamTurn::Error {
                message,
                partial_content,
                partial_calls,
            } => {
                turn.artifacts.content = partial_content;
                turn.artifacts.calls = partial_calls;
                Route::Done(RunStopReason::Error { message })
            }
        }
    }
}

/// The outcome of pumping one model stream.
enum StreamTurn {
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
async fn pump_stream(
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

// ---------------------------------------------------------------------------
// Response-side nodes
// ---------------------------------------------------------------------------

/// Append the model's assistant content to the ledger, then gate the run:
/// no tool calls means the model answered the user, so the run completes.
pub struct AppendProcessor {
    transcript_sink: Option<Arc<dyn TranscriptSink>>,
}

impl AppendProcessor {
    pub fn new(transcript_sink: Option<Arc<dyn TranscriptSink>>) -> Self {
        Self { transcript_sink }
    }
}

#[async_trait]
impl Processor for AppendProcessor {
    fn name(&self) -> &'static str {
        "append"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        let content = std::mem::take(&mut turn.artifacts.content);
        transcript::push(
            &mut turn.ledger,
            self.transcript_sink.as_deref(),
            &turn.ctx.session_id,
            Message::Assistant { content },
        )
        .await;
        if turn.artifacts.calls.is_empty() {
            Route::Done(RunStopReason::Completed)
        } else {
            Route::Next
        }
    }
}

/// Drive the tool batch through the policy → approval → execute → after
/// chain, then append the results to the ledger and run the post-batch
/// seams (`finish_turn`, `should_terminate_tool_batch`).
///
/// Legacy `dispatch_turn` territory: a terminal batch condition (approval
/// or tool-level cancellation) becomes `Route::Done`; the
/// `should_terminate` hook's clean early stop becomes `Done(Completed)`.
pub struct ToolDispatchProcessor {
    policy: Arc<dyn ToolPolicy>,
    approval_broker: Option<Arc<dyn ApprovalBroker>>,
    hooks: LoopHooks,
    event_sink: Arc<dyn EventSink>,
    tool_mode: ToolExecutionMode,
    transcript_sink: Option<Arc<dyn TranscriptSink>>,
}

impl ToolDispatchProcessor {
    pub fn new(
        policy: Arc<dyn ToolPolicy>,
        approval_broker: Option<Arc<dyn ApprovalBroker>>,
        hooks: LoopHooks,
        event_sink: Arc<dyn EventSink>,
        tool_mode: ToolExecutionMode,
        transcript_sink: Option<Arc<dyn TranscriptSink>>,
    ) -> Self {
        Self {
            policy,
            approval_broker,
            hooks,
            event_sink,
            tool_mode,
            transcript_sink,
        }
    }
}

#[async_trait]
impl Processor for ToolDispatchProcessor {
    fn name(&self) -> &'static str {
        "tool_dispatch"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        let calls = std::mem::take(&mut turn.artifacts.calls);
        let tcx = DispatchCtx {
            turn_idx: turn.turn_idx,
            ctx: &turn.ctx,
            tools: &turn.tools,
            policy: self.policy.as_ref(),
            approval_broker: &self.approval_broker,
            hooks: &self.hooks,
            seq: &turn.seq,
            sink: self.event_sink.as_ref(),
            control: &turn.control,
        };
        let (result_msgs, terminal_reason) = dispatch_batch(&calls, &tcx, self.tool_mode).await;

        if let Some(reason) = terminal_reason {
            push_tool_results(
                &mut turn.ledger,
                self.transcript_sink.as_deref(),
                &turn.ctx,
                result_msgs,
            )
            .await;
            return Route::Done(reason);
        }

        let n_results = result_msgs.len();
        push_tool_results(
            &mut turn.ledger,
            self.transcript_sink.as_deref(),
            &turn.ctx,
            result_msgs,
        )
        .await;

        // Hook: finish_turn observes the completed turn.
        if let Some(hook) = &self.hooks.finish_turn {
            hook(turn.turn_idx, &turn.ledger);
        }

        // Hook: should_terminate_tool_batch short-circuits the batch.
        if let Some(hook) = &self.hooks.should_terminate_tool_batch {
            let tool_results = recent_tool_results(&turn.ledger, n_results);
            if hook(&calls, &tool_results) {
                return Route::Done(RunStopReason::Completed);
            }
        }

        Route::Next
    }
}

/// Append a dispatched tool batch to the transcript, mirroring each result to
/// the host sink in dispatch order.
pub(crate) async fn push_tool_results(
    transcript: &mut Vec<Message>,
    transcript_sink: Option<&dyn TranscriptSink>,
    ctx: &RunContext,
    results: Vec<ToolResultMessage>,
) {
    for result in results {
        transcript::push(
            transcript,
            transcript_sink,
            &ctx.session_id,
            Message::ToolResult(result),
        )
        .await;
    }
}

/// The `ToolResult` messages produced by the most recent tool batch: the
/// last `n_results` transcript entries, filtered to tool results.
pub(crate) fn recent_tool_results(
    transcript: &[Message],
    n_results: usize,
) -> Vec<ToolResultMessage> {
    transcript
        .iter()
        .rev()
        .take(n_results)
        .filter_map(|msg| match msg {
            Message::ToolResult(tm) => Some(tm.clone()),
            _ => None,
        })
        .collect()
}
