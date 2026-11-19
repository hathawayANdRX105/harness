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

use harness_core::event::RunEventKind;
use harness_core::message::{Message, ToolCall, ToolResultMessage};
use harness_core::model::{Model, ModelInfo};
use harness_core::run::{RunContext, RunStopReason};
use harness_core::tool::Tool;
use harness_core::topology::{Processor, Route, Topology, Turn};
use harness_core::{EventSink, RunControl};

use harness_context::{Budget, BudgetBlocked, Compactor};

use crate::dispatch::{DispatchCtx, dispatch_batch, emit_event};
use crate::hooks::{LoopHooks, ToolExecutionMode};
use crate::processors::ModelProcessor;
use crate::transcript::{self, TranscriptSink};

/// Dependencies the loop needs to run one turn-loop.
/// The loop is stateless: all per-run state lives in the `Turn` `run()`
/// builds from the seed (ledger, control, sequence counter). `Arc<LoopDeps>`
/// is cheap to clone and share across runs.
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
    /// The prompt-side machining chain: `Processor` nodes the run passes
    /// the turn through before the model call (system ledger, injected
    /// instructions, later skills and retrieval). The response side joins
    /// this topology in slices 4–5.
    pub prompt_topology: Topology,
    /// How tools are dispatched within one turn.
    pub tool_mode: ToolExecutionMode,
    /// Optional context budget: used vs available tokens plus the derived
    /// ratio. The builder sets it from `[budget]`. Its `available_tokens` is
    /// the host's per-run input ceiling: the turn-boundary gate caps the
    /// compactor's model-window estimate with it, so a configured limit
    /// actually gates compaction. Without a compactor the gate stays off.
    pub budget: Option<Budget>,
    /// Optional compactor: assesses the budget and compacts the transcript.
    /// The builder sets it from the compaction plan; its presence, together
    /// with `budget`, enables the turn-boundary budget gate.
    pub compactor: Option<Arc<dyn Compactor>>,
    /// Transcript to start from: the host's replay of the session ledger.
    /// Empty for a fresh session. Seeded messages are already persisted, so
    /// they are never mirrored to `transcript_sink`.
    pub seed: Vec<Message>,
    /// Optional sink for every message this run appends (task, steering,
    /// assistant, tool results). `None` keeps the run in memory only.
    pub transcript_sink: Option<Arc<dyn TranscriptSink>>,
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
            .field("seed_len", &self.seed.len())
            .field(
                "transcript_sink",
                &self.transcript_sink.as_ref().map(|_| "set"),
            )
            .field("hooks", &self.hooks)
            .field("prompt_topology", &self.prompt_topology)
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
async fn turn_boundary(
    control: &RunControl,
    deps: &LoopDeps,
    transcript: &mut Vec<Message>,
    ctx: &RunContext,
    seq: &AtomicU64,
    turn_idx: u32,
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
        transcript::push_steering(
            transcript,
            deps.transcript_sink.as_deref(),
            &ctx.session_id,
            Message::user_text(msg),
        )
        .await;
    }

    // Seed the first turn with the task description: every provider rejects
    // an empty `messages` array on the first upstream request. A seeded
    // transcript already holds the previous runs of this session, so the
    // task is appended regardless of whether the transcript is empty.
    if turn_idx == 0 && !ctx.task.description.is_empty() {
        transcript::push(
            transcript,
            deps.transcript_sink.as_deref(),
            &ctx.session_id,
            Message::user_text(ctx.task.description.clone()),
        )
        .await;
    }

    // Max-turns: refuse to start another turn once the turn budget is spent.
    if turn_idx >= ctx.limits.max_turns {
        return Err(RunStopReason::MaxTurns);
    }

    Ok(())
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
    turn_idx: u32,
    ctx: &RunContext,
    deps: &LoopDeps,
    control: &RunControl,
    seq: &AtomicU64,
    transcript: &mut Vec<Message>,
) -> Result<usize, RunStopReason> {
    let dctx = DispatchCtx {
        turn_idx,
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
        push_tool_results(transcript, deps, ctx, result_msgs).await;
        return Err(reason);
    }

    let n_results = result_msgs.len();
    push_tool_results(transcript, deps, ctx, result_msgs).await;

    // Hook: finish_turn observes the completed turn.
    if let Some(hook) = &deps.hooks.finish_turn {
        hook(turn_idx, transcript);
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

/// Append a dispatched tool batch to the transcript, mirroring each result to
/// the host sink in dispatch order.
async fn push_tool_results(
    transcript: &mut Vec<Message>,
    deps: &LoopDeps,
    ctx: &RunContext,
    results: Vec<ToolResultMessage>,
) {
    for result in results {
        transcript::push(
            transcript,
            deps.transcript_sink.as_deref(),
            &ctx.session_id,
            Message::ToolResult(result),
        )
        .await;
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
    // One Turn per run: the ledger, control, run identity and sequence
    // counter live here from now on, and the legacy helpers below borrow
    // them back by field until their slices fold them into processors.
    let mut turn = Turn::new(
        Arc::clone(&deps.model),
        control,
        ctx.clone(),
        deps.seed.clone(),
    );
    // The model node runs per run; slice 6 wires it into the
    // builder-assembled topology proper.
    let model_processor = ModelProcessor::new(Arc::clone(&deps.event_sink));
    // The tool manifest rides the turn — `ModelProcessor` renders it into
    // the request. Slice 5 makes this a ToolProcessor node.
    turn.tools = deps.tools.clone();

    emit_run_started(&ctx, deps.event_sink.as_ref(), &turn.seq);

    let mut turn_idx: u32 = 0;

    loop {
        // Turn boundary: cancellation, steering drain, task seed, max-turns.
        let boundary = turn_boundary(
            &turn.control,
            &deps,
            &mut turn.ledger,
            &turn.ctx,
            &turn.seq,
            turn_idx,
        )
        .await;
        if let Err(reason) = boundary {
            return finish(&ctx, reason, deps.event_sink.as_ref(), &turn.seq);
        }

        // Prompt-side machining: the topology's processors reshape the
        // ledger before the model call (system ledger, instructions, later
        // skills and retrieval). A processor that returns Done ends the
        // run here; Goto routing is slice 5.
        if let Route::Done(reason) = deps.prompt_topology.step(&mut turn).await {
            return finish(&ctx, reason, deps.event_sink.as_ref(), &turn.seq);
        }

        // Budget gate: refuse the round before the provider call when the
        // context cannot fit another turn, even after one compaction pass.
        if let Some(reason) =
            enforce_budget_gate(&deps, &mut turn.ledger, &turn.ctx, &turn.control, &turn.seq).await
        {
            return reason;
        }

        // Model turn: the one node that touches the stream. The turn index
        // rides the turn so the processor can correlate its events.
        turn.turn_idx = turn_idx;
        match model_processor.process(&mut turn).await {
            Route::Next => {}
            Route::Goto(_) => todo!("slice 5: named-node routing"),
            Route::Done(reason) => {
                // Error backfill: partial content and tool calls still get
                // their ledger entries, so tool-call/result pairing never
                // breaks. Slice 5 moves this into response-side nodes.
                if let RunStopReason::Error { .. } = reason {
                    backfill_partial_turn(&mut turn, &deps).await;
                }
                return finish(&ctx, reason, deps.event_sink.as_ref(), &turn.seq);
            }
        }
        let assistant_content = std::mem::take(&mut turn.artifacts.content);
        transcript::push(
            &mut turn.ledger,
            deps.transcript_sink.as_deref(),
            &turn.ctx.session_id,
            Message::Assistant {
                content: assistant_content,
            },
        )
        .await;

        // No tool calls: the model answered the user, the run is done.
        if turn.artifacts.calls.is_empty() {
            return finish(
                &ctx,
                RunStopReason::Completed,
                deps.event_sink.as_ref(),
                &turn.seq,
            );
        }

        let dispatched = dispatch_turn(
            &turn.artifacts.calls,
            turn_idx,
            &turn.ctx,
            &deps,
            &turn.control,
            &turn.seq,
            &mut turn.ledger,
        )
        .await;

        if let Err(reason) = dispatched {
            return finish(&ctx, reason, deps.event_sink.as_ref(), &turn.seq);
        }

        turn_idx += 1;
    }
}

/// Error-path backfill: the model stream died mid-turn, so the partial
/// assistant content and any tool calls still get their ledger entries.
/// Legacy `handle_stream_outcome` territory; slice 5 folds this into the
/// response-side nodes.
async fn backfill_partial_turn(turn: &mut Turn, deps: &LoopDeps) {
    let partial_content = std::mem::take(&mut turn.artifacts.content);
    let partial_calls = std::mem::take(&mut turn.artifacts.calls);
    transcript::push(
        &mut turn.ledger,
        deps.transcript_sink.as_deref(),
        &turn.ctx.session_id,
        Message::Assistant {
            content: partial_content,
        },
    )
    .await;
    if !partial_calls.is_empty() {
        let results = dispatch_and_collect(
            &partial_calls,
            turn.turn_idx,
            &turn.ctx,
            deps,
            &turn.control,
            &turn.seq,
        )
        .await;
        push_tool_results(&mut turn.ledger, deps, &turn.ctx, results).await;
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
/// The gate's view of the budget: the compactor's model-window estimate,
/// capped by the host's configured per-run input limit.
///
/// The two come from different places — the compactor reads
/// `ModelInfo.context_window`, the host maps `[budget].max_input_tokens` into
/// `LoopDeps.budget` — so the tighter value wins. Without the cap a configured
/// limit would never gate anything.
fn gate_budget(
    compactor: &dyn Compactor,
    transcript: &[Message],
    info: &ModelInfo,
    configured: Option<&Budget>,
) -> Budget {
    let mut budget = compactor.read_budget(transcript, info);
    if let Some(configured) = configured {
        budget.available_tokens = budget.available_tokens.min(configured.available_tokens);
    }
    budget
}

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
    let budget = gate_budget(compactor.as_ref(), transcript, info, deps.budget.as_ref());
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

    let budget = gate_budget(compactor.as_ref(), transcript, info, deps.budget.as_ref());
    let still_blocked = compactor.stages().iter().any(|s| s.is_active(&budget));
    if !still_blocked {
        return None;
    }

    // `stage` names the first stage that was active when the gate ran, not
    // the deepest one the compactor reached: the applied labels live in the
    // compaction result's checkpoint, which the gate does not keep.
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

/// Dispatch tool calls and collect their results, in call order.
async fn dispatch_and_collect(
    calls: &[ToolCall],
    turn_idx: u32,
    ctx: &RunContext,
    deps: &LoopDeps,
    control: &RunControl,
    seq: &AtomicU64,
) -> Vec<ToolResultMessage> {
    let dctx = DispatchCtx {
        turn_idx,
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
    results
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
