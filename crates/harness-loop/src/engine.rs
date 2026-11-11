//! The agent loop: model stream -> tool dispatch -> turn loop.
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
//! per-call state is in the `Vec<Message>` transcript and the sequence counter
//! -- neither survives the function's return. This is the opposite of
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

use harness_context::{Budget, BudgetBlocked, Compactor};

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
    /// Optional context budget: used vs available tokens plus the derived
    /// ratio. The builder sets it from `[budget]`; its presence, together
    /// with `compactor`, enables the turn-boundary budget gate that runs
    /// before the provider call and can refuse a round as `BudgetBlocked`.
    pub budget: Option<Budget>,
    /// Optional compactor: assesses the budget and compacts the transcript.
    /// The builder sets it from the compaction plan; its presence, together
    /// with `budget`, enables the turn-boundary budget gate.
    pub compactor: Option<Arc<dyn Compactor>>,
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
            .field("budget", &self.budget)
            .field("compactor", &self.compactor.as_ref().map(|_| "set"))
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
///   Budget blocked? --Yes--> Error
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

        // -- 4b. Budget gate: refuse the round before the provider call when
        //    the context budget cannot fit the next turn, even after one
        //    compaction pass. Emits `RunEventKind::Error` carrying a
        //    structured `budget blocked: ...` message and returns
        //    `RunStopReason::Error` with the same string -- BudgetBlocked
        //    semantics without changing the frozen `RunStopReason` enum. --
        if let Some(reason) =
            enforce_budget_gate(&deps, &mut transcript, &ctx, &control, &seq).await
        {
            return reason;
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
        let model_turn = pump_stream(stream, &seq, &*deps.event_sink, &ctx, &control).await;

        // Handle terminal cases from the stream first.
        let (tool_calls, assistant_content) = match model_turn {
            StreamTurn::Cancelled => {
                return finish(RunStopReason::Cancelled, &ctx, &*deps.event_sink, &seq);
            }
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

/// Turn-boundary budget gate: runs before the provider call so a round is
/// refused (not failed) when the context cannot fit the next turn.
///
/// When both `budget` and `compactor` are configured: refresh the budget
/// from the current transcript; when a compaction stage is active, run
/// compaction once -- cancellable via the run's cancel token -- then re-check.
/// If the budget is still over a stage threshold, emit a
/// `RunEventKind::Error` carrying [`BudgetBlocked::error_message`] and return
/// `RunStopReason::Error` carrying the same message. Callers recover the
/// distinction with [`BudgetBlocked::from_error_message`]; core's
/// `RunStopReason` is frozen and has no budget-blocked variant.
///
/// Returns `None` when the gate passes or is unconfigured (either dep
/// missing), so the turn proceeds to the provider call.
async fn enforce_budget_gate(
    deps: &LoopDeps,
    transcript: &mut Vec<Message>,
    ctx: &RunContext,
    control: &RunControl,
    seq: &AtomicU64,
) -> Option<RunStopReason> {
    let (_, Some(compactor)) = (&deps.budget, &deps.compactor) else {
        return None;
    };

    // Cancellation observed at the boundary wins: don't compact a cancelled run.
    if control.is_cancelled() {
        return Some(finish(
            RunStopReason::Cancelled,
            ctx,
            &*deps.event_sink,
            seq,
        ));
    }

    let info = deps.model.info();
    let budget = compactor.read_budget(transcript, info);
    let stage = compactor.stages().iter().find(|s| s.is_active(&budget))?;

    // Compaction is the only relief available before refusing the round;
    // run it once, cancellable via the run's cancel token.
    let _result = compactor
        .compact(transcript, &budget, None, &control.cancel_token())
        .await;

    if control.is_cancelled() {
        return Some(finish(
            RunStopReason::Cancelled,
            ctx,
            &*deps.event_sink,
            seq,
        ));
    }

    let budget = compactor.read_budget(transcript, info);
    let still_blocked = compactor.stages().iter().any(|s| s.is_active(&budget));
    if !still_blocked {
        return None;
    }

    let blocked = BudgetBlocked {
        needed: budget.used_tokens,
        available: budget.available_tokens,
        stage: stage.label.clone(),
    };
    let message = blocked.error_message();
    emit_event(
        &*deps.event_sink,
        &ctx.run_id,
        &ctx.session_id,
        seq,
        RunEventKind::Error {
            message: message.clone(),
        },
    );
    Some(finish(
        RunStopReason::Error { message },
        ctx,
        &*deps.event_sink,
        seq,
    ))
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
    control: &RunControl,
) -> StreamTurn {
    pin_mut!(stream);

    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    // Accumulator for in-progress tool call args: (id, name, args_json_fragments)
    let mut partial: Option<(String, String, String)> = None;
    let cancel = control.cancel_token();

    loop {
        // The stream race is the drop protocol's teeth: cancelling mid-stream
        // stops the model request immediately instead of burning tokens into
        // a void until the stream happens to return.
        let event = tokio::select! {
            _ = cancel.cancelled() => return StreamTurn::Cancelled,
            next = stream.next() => match next {
                Some(event) => event,
                None => break,
            },
        };
        match &event {
            ModelEvent::TextDelta(t) => {
                text.push_str(t);
                emit_event(
                    sink,
                    &ctx.run_id,
                    &ctx.session_id,
                    seq,
                    RunEventKind::TextDelta { text: t.clone() },
                );
            }
            ModelEvent::ReasoningDelta(r) => {
                reasoning.push_str(r);
                // Reasoning deltas have no dedicated RunEventKind yet;
                // they are preserved in Content::Reasoning blocks.
            }
            ModelEvent::ToolCallStart { id, name } => {
                partial = Some((id.clone(), name.clone(), String::new()));
            }
            ModelEvent::ToolCallArgs { id, partial_json } => {
                if let Some((cur_id, _, args)) = partial.as_mut()
                    && cur_id == id
                {
                    args.push_str(partial_json);
                }
            }
            ModelEvent::ToolCallEnd { id } => {
                if let Some((_, name, args_str)) = partial.as_mut()
                    && !tool_calls.iter().any(|tc| tc.id == *id)
                {
                    let args: serde_json::Value =
                        serde_json::from_str(args_str).unwrap_or(serde_json::Value::Null);
                    tool_calls.push(ToolCall {
                        id: id.clone(),
                        name: name.clone(),
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
                    message: message.clone(),
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
