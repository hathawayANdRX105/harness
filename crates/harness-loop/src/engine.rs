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
use harness_core::message::{Content, Message, ToolCall, ToolResultMessage};
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

/// Turn boundary for one loop iteration: cancellation check, steering drain,
/// task seed on the first turn, and the max-turns check.
///
/// Returns `Ok` when the turn may proceed, or `Err` with the stop reason
/// when the run ends at this boundary. The caller emits `RunFinished`
/// exactly once for the returned reason and returns.
fn turn_boundary(
    control: &RunControl,
    deps: &LoopDeps,
    transcript: &mut Vec<Message>,
    ctx: &RunContext,
    seq: &AtomicU64,
    turn: u32,
) -> Result<(), RunStopReason> {
    // Cancellation observed at the boundary wins: no steering is drained and
    // no new turn starts.
    if control.is_cancelled() {
        return Err(RunStopReason::Cancelled);
    }

    // Drain queued steering into the transcript as one user message, and emit
    // one `SteeringApplied` event per non-empty drain.
    let steering = control.drain_steering();
    if !steering.is_empty() {
        let msg = steering
            .iter()
            .map(|s| s.message.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        emit_event(
            deps.event_sink.as_ref(),
            &ctx.run_id,
            &ctx.session_id,
            seq,
            RunEventKind::SteeringApplied {
                message: msg.clone(),
            },
        );
        transcript.push(Message::user_text(msg));
    }

    // Seed the first turn with the task description: every provider rejects
    // an empty `messages` array on the first upstream request.
    if turn == 0 && transcript.is_empty() && !ctx.task.description.is_empty() {
        transcript.push(Message::user_text(ctx.task.description.clone()));
    }

    // Max-turns: refuse to start another turn once the turn budget is spent.
    if turn >= ctx.limits.max_turns {
        return Err(RunStopReason::MaxTurns);
    }

    Ok(())
}

/// Build the model request for one turn and pump its stream: emit
/// `TurnStarted`, call the provider, and return the stream outcome.
async fn model_turn(
    ctx: &RunContext,
    deps: &LoopDeps,
    control: &RunControl,
    seq: &AtomicU64,
    turn: u32,
    transcript: &[Message],
) -> StreamTurn {
    let tools_schemas: Vec<ToolSchema> = deps.tools.iter().map(|t| t.schema()).collect();
    let request = ModelRequest {
        model: deps.model.info().model.clone(),
        messages: transcript.to_vec(),
        tools: tools_schemas,
        max_tokens: None,
        temperature: None,
    };

    emit_event(
        deps.event_sink.as_ref(),
        &ctx.run_id,
        &ctx.session_id,
        seq,
        RunEventKind::TurnStarted { turn },
    );

    let stream = deps.model.stream(&request).await;
    pump_stream(stream, seq, deps.event_sink.as_ref(), ctx, control).await
}

/// Emit `RunStarted` once, at the beginning of a run.
fn emit_run_started(ctx: &RunContext, event_sink: &dyn EventSink, seq: &AtomicU64) {
    emit_event(
        event_sink,
        &ctx.run_id,
        &ctx.session_id,
        seq,
        RunEventKind::RunStarted {
            task: ctx.task.description.clone(),
        },
    );
}

/// Emit the `TurnFinished` event that closes one model turn.
fn emit_turn_finished(ctx: &RunContext, event_sink: &dyn EventSink, seq: &AtomicU64, turn: u32) {
    emit_event(
        event_sink,
        &ctx.run_id,
        &ctx.session_id,
        seq,
        RunEventKind::TurnFinished { turn },
    );
}

/// The `ToolResult` messages produced by the most recent tool batch: the
/// last `n_results` transcript entries, filtered to tool results.
fn recent_tool_results(transcript: &[Message], n_results: usize) -> Vec<ToolResultMessage> {
    transcript
        .iter()
        .rev()
        .take(n_results)
        .filter_map(|msg| match msg {
            Message::ToolResult(tr) => Some(tr.clone()),
            _ => None,
        })
        .collect()
}

/// Dispatch one tool batch, extend the transcript with the results, and run
/// the post-batch hooks.
///
/// Returns `Ok` with the number of results appended, `Err(Cancelled)` when
/// the batch hit a cancellation (the results are already in the transcript),
/// or `Err(Completed)` when the should-terminate hook short-circuited the
/// run -- the caller finishes the run with the returned reason.
async fn dispatch_turn(
    calls: &[ToolCall],
    turn: u32,
    ctx: &RunContext,
    deps: &LoopDeps,
    control: &RunControl,
    seq: &AtomicU64,
    transcript: &mut Vec<Message>,
) -> Result<usize, RunStopReason> {
    let dctx = DispatchCtx {
        turn,
        ctx,
        tools: &deps.tools,
        policy: &*deps.policy,
        approval_broker: &deps.approval_broker,
        hooks: &deps.hooks,
        seq,
        sink: deps.event_sink.as_ref(),
        control,
    };
    let (result_msgs, terminal_reason) = dispatch_batch(calls, &dctx, deps.tool_mode).await;

    if let Some(reason) = terminal_reason {
        transcript.extend(result_msgs.into_iter().map(Message::ToolResult));
        return Err(reason);
    }

    let n_results = result_msgs.len();
    transcript.extend(result_msgs.into_iter().map(Message::ToolResult));

    // Hook: finish_turn observes the completed turn.
    if let Some(hook) = &deps.hooks.finish_turn {
        hook(turn, transcript);
    }

    // Hook: should_terminate_tool_batch short-circuits the tool batch.
    if let Some(hook) = &deps.hooks.should_terminate_tool_batch {
        let tool_results = recent_tool_results(transcript, n_results);
        if hook(calls, &tool_results) {
            return Err(RunStopReason::Completed);
        }
    }

    Ok(n_results)
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

    emit_run_started(&ctx, deps.event_sink.as_ref(), &seq);

    let mut turn: u32 = 0;

    loop {
        // Turn boundary: cancellation, steering drain, task seed, max-turns.
        if let Err(reason) = turn_boundary(&control, &deps, &mut transcript, &ctx, &seq, turn) {
            return finish(&ctx, reason, deps.event_sink.as_ref(), &seq);
        }

        // Hook: prepare_request transforms the transcript before the model call.
        if let Some(hook) = &deps.hooks.prepare_request {
            transcript = hook(transcript);
        }

        // Budget gate: refuse the round before the provider call when the
        // context cannot fit another turn, even after one compaction pass.
        if let Some(reason) =
            enforce_budget_gate(&deps, &mut transcript, &ctx, &control, &seq).await
        {
            return reason;
        }

        let stream_outcome = model_turn(&ctx, &deps, &control, &seq, turn, &transcript).await;
        let (tool_calls, assistant_content) = match handle_stream_outcome(
            &stream_outcome,
            &ctx,
            &deps,
            &control,
            &seq,
            turn,
            &mut transcript,
        )
        .await
        {
            Ok(pair) => pair,
            Err(reason) => return finish(&ctx, reason, deps.event_sink.as_ref(), &seq),
        };

        transcript.push(Message::Assistant {
            content: assistant_content,
        });

        // No tool calls: the model answered the user, the run is done.
        if tool_calls.is_empty() {
            return finish(
                &ctx,
                RunStopReason::Completed,
                deps.event_sink.as_ref(),
                &seq,
            );
        }

        let dispatched = dispatch_turn(
            &tool_calls,
            turn,
            &ctx,
            &deps,
            &control,
            &seq,
            &mut transcript,
        )
        .await;

        if let Err(reason) = dispatched {
            return finish(&ctx, reason, deps.event_sink.as_ref(), &seq);
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
            ctx,
            RunStopReason::Cancelled,
            deps.event_sink.as_ref(),
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
            ctx,
            RunStopReason::Cancelled,
            deps.event_sink.as_ref(),
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
        deps.event_sink.as_ref(),
        &ctx.run_id,
        &ctx.session_id,
        seq,
        RunEventKind::Error {
            message: message.clone(),
        },
    );
    Some(finish(
        ctx,
        RunStopReason::Error { message },
        deps.event_sink.as_ref(),
        seq,
    ))
}

/// The caller's terminal handling for a pumped stream outcome.
///
/// Cancelled / Error return the stop reason (the caller finishes the run);
/// Ok yields the assistant content plus the tool calls to dispatch. Errors
/// with partial tool calls still backfill their results before returning so
/// the transcript never breaks the tool-call/result pairing.
async fn handle_stream_outcome(
    outcome: &StreamTurn,
    ctx: &RunContext,
    deps: &LoopDeps,
    control: &RunControl,
    seq: &AtomicU64,
    turn: u32,
    transcript: &mut Vec<Message>,
) -> Result<(Vec<ToolCall>, Vec<Content>), RunStopReason> {
    if matches!(outcome, StreamTurn::Ok { .. } | StreamTurn::Error { .. }) {
        emit_turn_finished(ctx, deps.event_sink.as_ref(), seq, turn);
    }
    match outcome {
        StreamTurn::Cancelled => Err(RunStopReason::Cancelled),
        StreamTurn::Ok { content, calls } => Ok((calls.clone(), content.clone())),
        StreamTurn::Error {
            message,
            partial_content,
            partial_calls,
        } => {
            transcript.push(Message::Assistant {
                content: partial_content.clone(),
            });
            if !partial_calls.is_empty() {
                transcript.extend(
                    dispatch_and_collect(partial_calls, turn, ctx, deps, control, seq).await,
                );
            }
            Err(RunStopReason::Error {
                message: message.clone(),
            })
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
    /// The run was cancelled while the stream was open -- no turn content.
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
            // Token accounting -- no dedicated RunEventKind in this version.
            PumpStep::Continue
        }
        ModelEvent::Stop(_reason) => {
            // All StopReason variants (EndTurn, ToolUse, MaxTokens) are
            // clean terminators -- the model finished its turn.
            PumpStep::Break
        }
        ModelEvent::Error { message } => PumpStep::Error(message.clone()),
        // ModelEvent is #[non_exhaustive]: future variants are ignored at the
        // stream level (they are not terminal).
        _ => PumpStep::Continue,
    }
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

    // Build assistant content block from accumulated text, reasoning and tool calls.
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
        sink: deps.event_sink.as_ref(),
        control,
    };
    let (results, _terminal) = dispatch_batch(calls, &dctx, deps.tool_mode).await;
    results.into_iter().map(Message::ToolResult).collect()
}

/// Emit `RunFinished` and return the reason.
fn finish(
    ctx: &RunContext,
    reason: RunStopReason,
    event_sink: &dyn EventSink,
    seq: &AtomicU64,
) -> RunStopReason {
    emit_event(
        event_sink,
        &ctx.run_id,
        &ctx.session_id,
        seq,
        RunEventKind::RunFinished {
            reason: reason.clone(),
        },
    );
    reason
}
