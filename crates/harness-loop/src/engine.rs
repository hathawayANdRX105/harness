//! The agent loop: model stream -> tool dispatch -> repeat.
//!
//! Design provenance: pi's `agentLoop` (`packages/agent/src/agent-loop.ts`),
//! harness-rs's `RunLoop` (`crates/harness-loop/src/lib.rs`), and
//! NanoHarness's `engine.run()` (`core/engine.py`). The three converge on the
//! shape -- a free function that owns one turn loop, with no persistent state
//! beyond the transcript `Vec<Message>` -- but differ in how hooks and policies
//! are wired. This implementation uses straight `Arc<dyn Fn>` callbacks (pi
//! shape) and core's `ToolPolicy` / `ApprovalBroker` seams (NanoHarness shape),
//! not harness-rs's `Hook` trait system.
//!
//! # Stateless loop
//!
//! The `run` function is a bare `async fn` that receives everything it needs
//! through parameters. There is no `Agent` struct, no `Engine` builder. All
//! per-call state is in the `transcript` vector and the sequence counter --
//! neither survives the function's return. This is the opposite of
//! NanoHarness's stateful `Engine` class, which bundles context, session and
//! loop into one object.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use futures::{StreamExt, pin_mut};
use harness_core::event::RunEventKind;
use harness_core::message::{Content, Message, ToolCall};
use harness_core::model::{Model, ModelEvent, ModelRequest};
use harness_core::run::{RunContext, RunStopReason};
use harness_core::tool::{Tool, ToolSchema};
use harness_core::{EventSink, RunControl};

use crate::dispatch::{DispatchCtx, dispatch_batch, emit_event};
use crate::hooks::{LoopHooks, ToolExecutionMode};

/// Dependencies the loop needs to run one turn-loop.
///
/// The loop is stateless: all per-run state lives in the `Vec<Message>`
/// transcript and the sequence counter inside `run()`. `Arc<LoopDeps>` is
/// cheap to clone and share across runs.
pub struct LoopDeps {
    /// The upstream model this loop calls.
    pub model: Arc<dyn Model>,
    /// Registered tools, indexed by name at dispatch time (linear scan,
    /// acceptable for tens of tools).
    pub tools: Vec<Arc<dyn Tool>>,
    /// Fire-and-forget event sink -- never blocks the critical path.
    pub event_sink: Arc<dyn EventSink>,
    /// Two-stage tool policy (before / after). Must be cheap and sync.
    pub policy: Arc<dyn harness_core::policy::ToolPolicy>,
    /// Optional approval broker for tools that require human approval.
    pub approval_broker: Option<Arc<dyn harness_core::policy::ApprovalBroker>>,
    /// Pi-style config callbacks.
    pub hooks: LoopHooks,
    /// How tools are dispatched within one turn.
    pub tool_mode: ToolExecutionMode,
}

impl std::fmt::Debug for LoopDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopDeps")
            .field("model_info", &self.model.info())
            .field("n_tools", &self.tools.len())
            .field("policy", &"...")
            .field(
                "approval_broker",
                &self.approval_broker.as_ref().map(|_| "..."),
            )
            .field("hooks", &self.hooks)
            .field("tool_mode", &self.tool_mode)
            .finish()
    }
}

/// The core turn loop.
///
/// The loop state machine:
///
/// ```text
///   Start
///     |
///     v
///   Cancelled? --Yes--> Cancelled
///     | No
///     v
///   MaxTurns? --Yes--> MaxTurns
///     | No
///     v
///   Drain steer / inject user
///     |
///     v
///   Stream model (events)
///     |
///     v
///   Error? --Yes--> Error
///     | No
///     v
///   Tool calls?
///   Yes |     | No
///       v     v
///   Dispatch   Completed
///     |
///     v
///   Terminate batch? --Yes--> Completed
///     | No
///     +--> next turn
/// ```
///
/// Every exit path emits `RunFinished` before returning.
pub async fn run(ctx: RunContext, deps: Arc<LoopDeps>, control: RunControl) -> RunStopReason {
    let mut transcript: Vec<Message> = Vec::new();
    let seq = AtomicU64::new(0);

    emit_event(
        &*deps.event_sink,
        &ctx.run_id,
        &ctx.session_id,
        &seq,
        RunEventKind::RunStarted {
            task: ctx.task.description.clone(),
        },
    );

    let mut turn: u32 = 0;

    loop {
        // -- 1. Cancellation check (turn boundary) --
        if control.is_cancelled() {
            return finish(RunStopReason::Cancelled, &ctx, &*deps.event_sink, &seq);
        }

        // -- 2. Steering drain --
        {
            let steering = control.drain_steering();
            if !steering.is_empty() {
                let msg = steering
                    .iter()
                    .map(|s| s.message.as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                emit_event(
                    &*deps.event_sink,
                    &ctx.run_id,
                    &ctx.session_id,
                    &seq,
                    RunEventKind::SteeringApplied {
                        message: msg.clone(),
                    },
                );
                transcript.push(Message::user_text(msg));
            }
        }

        // -- 2b. Seed the transcript with the task on the first turn --
        // The task description is the run's user message; without this the
        // first upstream request carries an empty `messages` array, which
        // every provider rejects.
        if turn == 0 && transcript.is_empty() && !ctx.task.description.is_empty() {
            transcript.push(Message::user_text(ctx.task.description.clone()));
        }

        // -- 3. Max-turns check --
        if turn >= ctx.limits.max_turns {
            return finish(RunStopReason::MaxTurns, &ctx, &*deps.event_sink, &seq);
        }

        // -- 4. Hook: prepare_request --
        if let Some(ref hook) = deps.hooks.prepare_request {
            transcript = hook(transcript);
        }

        // -- 5. Build request and model turn --
        let tools_schemas: Vec<ToolSchema> = deps.tools.iter().map(|t| t.schema()).collect();
        let request = ModelRequest {
            model: deps.model.info().model.clone(),
            messages: transcript.clone(),
            tools: tools_schemas,
            max_tokens: None,
            temperature: None,
        };

        emit_event(
            &*deps.event_sink,
            &ctx.run_id,
            &ctx.session_id,
            &seq,
            RunEventKind::TurnStarted { turn },
        );

        let stream = deps.model.stream(&request).await;
        let model_turn = pump_stream(stream, &seq, &*deps.event_sink, &ctx).await;

        // Handle terminal cases from the stream first.
        let (tool_calls, assistant_content) = match model_turn {
            StreamTurn::Ok { content, calls } => {
                emit_event(
                    &*deps.event_sink,
                    &ctx.run_id,
                    &ctx.session_id,
                    &seq,
                    RunEventKind::TurnFinished { turn },
                );
                (calls, content)
            }
            StreamTurn::Error {
                message,
                partial_content,
                partial_calls,
            } => {
                emit_event(
                    &*deps.event_sink,
                    &ctx.run_id,
                    &ctx.session_id,
                    &seq,
                    RunEventKind::TurnFinished { turn },
                );
                transcript.push(Message::Assistant {
                    content: partial_content,
                });
                if !partial_calls.is_empty() {
                    let msgs =
                        dispatch_and_collect(&partial_calls, turn, &ctx, &deps, &control, &seq)
                            .await;
                    transcript.extend(msgs);
                }
                return finish(
                    RunStopReason::Error { message },
                    &ctx,
                    &*deps.event_sink,
                    &seq,
                );
            }
        };

        // Push assistant message.
        transcript.push(Message::Assistant {
            content: assistant_content,
        });

        // -- 6. No tool calls -> completed --
        if tool_calls.is_empty() {
            return finish(RunStopReason::Completed, &ctx, &*deps.event_sink, &seq);
        }

        // -- 7. Dispatch tool calls --
        let dctx = DispatchCtx {
            turn,
            ctx: &ctx,
            tools: &deps.tools,
            policy: &*deps.policy,
            approval_broker: &deps.approval_broker,
            hooks: &deps.hooks,
            seq: &seq,
            sink: &*deps.event_sink,
            control: &control,
        };
        let (result_msgs, terminal_reason) =
            dispatch_batch(&tool_calls, &dctx, deps.tool_mode).await;

        // If dispatch returned a terminal reason (cancellation), return it.
        if let Some(reason) = terminal_reason {
            transcript.extend(result_msgs.into_iter().map(Message::ToolResult));
            return finish(reason, &ctx, &*deps.event_sink, &seq);
        }

        let n_results = result_msgs.len();
        transcript.extend(result_msgs.into_iter().map(Message::ToolResult));

        // -- 8. Hook: finish_turn --
        if let Some(ref hook) = deps.hooks.finish_turn {
            hook(turn, &transcript);
        }

        // -- 9. Hook: should_terminate_tool_batch --
        let tool_results: Vec<harness_core::message::ToolResultMessage> = transcript
            .iter()
            .rev()
            .take(n_results)
            .filter_map(|msg| {
                if let Message::ToolResult(tr) = msg {
                    Some(tr.clone())
                } else {
                    None
                }
            })
            .collect();

        if let Some(ref hook) = deps.hooks.should_terminate_tool_batch
            && hook(&tool_calls, &tool_results)
        {
            return finish(RunStopReason::Completed, &ctx, &*deps.event_sink, &seq);
        }

        turn += 1;
    }
}

/// The outcome of pumping one model stream.
enum StreamTurn {
    /// Stream ended cleanly (via `ModelEvent::Stop`).
    Ok {
        content: Vec<Content>,
        calls: Vec<ToolCall>,
    },
    /// Stream ended with `ModelEvent::Error`.
    Error {
        message: String,
        partial_content: Vec<Content>,
        partial_calls: Vec<ToolCall>,
    },
}

/// Pump a model stream: emit events, accumulate content and tool calls.
///
/// # Termination discipline
///
/// After receiving `ModelEvent::Stop` or `ModelEvent::Error` (terminal events)
/// the function stops polling and ignores any subsequent events. This is the
/// receiving side of the stream-fn contract (core documents the emission side).
async fn pump_stream(
    stream: harness_core::model::ModelStream<'_>,
    seq: &AtomicU64,
    sink: &dyn EventSink,
    ctx: &RunContext,
) -> StreamTurn {
    pin_mut!(stream);

    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    // Accumulator for in-progress tool call args: (id, name, args_json_fragments)
    let mut partial: Option<(String, String, String)> = None;

    while let Some(event) = stream.next().await {
        match event {
            ModelEvent::TextDelta(t) => {
                text.push_str(&t);
                emit_event(
                    sink,
                    &ctx.run_id,
                    &ctx.session_id,
                    seq,
                    RunEventKind::TextDelta { text: t },
                );
            }
            ModelEvent::ReasoningDelta(r) => {
                reasoning.push_str(&r);
                // Reasoning deltas have no dedicated RunEventKind yet;
                // they are preserved in Content::Reasoning blocks.
            }
            ModelEvent::ToolCallStart { id, name } => {
                partial = Some((id, name, String::new()));
            }
            ModelEvent::ToolCallArgs { id, partial_json } => {
                if let Some((ref cur_id, _, ref mut args)) = partial
                    && cur_id == &id
                {
                    args.push_str(&partial_json);
                }
            }
            ModelEvent::ToolCallEnd { id } => {
                if let Some((_, name, ref args_str)) = partial
                    && !tool_calls.iter().any(|tc| tc.id == id)
                {
                    let args: serde_json::Value =
                        serde_json::from_str(args_str).unwrap_or(serde_json::Value::Null);
                    tool_calls.push(ToolCall {
                        id: id.clone(),
                        name,
                        args,
                    });
                }
                partial = None;
            }
            ModelEvent::Usage(_usage) => {
                // Token accounting -- no dedicated RunEventKind in this version.
            }
            ModelEvent::Stop(_reason) => {
                // All StopReason variants (EndTurn, ToolUse, MaxTokens) are
                // clean terminators -- the model finished its turn.
                break;
            }
            ModelEvent::Error { message } => {
                let mut content = Vec::new();
                if !reasoning.is_empty() {
                    content.push(Content::Reasoning(reasoning));
                }
                if !text.is_empty() {
                    content.push(Content::Text(text));
                }
                return StreamTurn::Error {
                    message,
                    partial_content: content,
                    partial_calls: tool_calls,
                };
            }
            // ModelEvent is #[non_exhaustive]: future variants are ignored
            // at the stream level (they are not terminal).
            _ => {}
        }
    }

    // Build assistant content block from accumulated text, reasoning and tool calls.
    let mut content = Vec::new();
    if !reasoning.is_empty() {
        content.push(Content::Reasoning(reasoning));
    }
    if !text.is_empty() {
        content.push(Content::Text(text));
    }
    for tc in &tool_calls {
        content.push(Content::ToolCall(tc.clone()));
    }

    StreamTurn::Ok {
        content,
        calls: tool_calls,
    }
}

/// Dispatch tool calls and collect results as transcript messages.
async fn dispatch_and_collect(
    calls: &[ToolCall],
    turn: u32,
    ctx: &RunContext,
    deps: &LoopDeps,
    control: &RunControl,
    seq: &AtomicU64,
) -> Vec<Message> {
    let dctx = DispatchCtx {
        turn,
        ctx,
        tools: &deps.tools,
        policy: &*deps.policy,
        approval_broker: &deps.approval_broker,
        hooks: &deps.hooks,
        seq,
        sink: &*deps.event_sink,
        control,
    };
    let (results, _terminal) = dispatch_batch(calls, &dctx, deps.tool_mode).await;
    results.into_iter().map(Message::ToolResult).collect()
}

/// Emit `RunFinished` and return the reason.
fn finish(
    reason: RunStopReason,
    ctx: &RunContext,
    sink: &dyn EventSink,
    seq: &AtomicU64,
) -> RunStopReason {
    emit_event(
        sink,
        &ctx.run_id,
        &ctx.session_id,
        seq,
        RunEventKind::RunFinished {
            reason: reason.clone(),
        },
    );
    reason
}
