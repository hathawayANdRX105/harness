//! The agent loop driver: one stateless `run()` over two topologies.
//!
//! This crate is the **assembly and driving layer** only — it owns no
//! processor implementations. The nodes are domain-owned:
//!
//! - prompt phase: `SystemLedgerProcessor` / `InstructionProcessor`
//!   (harness-builder, the facade's default prompt nodes) and
//!   `CompactProcessor` (harness-context, the domain that owns
//!   `Budget` / `Compactor`);
//! - response phase: `ModelProcessor` / `AppendProcessor` (harness-model,
//!   the domain that owns the model stream) and `ToolDispatchProcessor`
//!   (harness-tools, the domain that owns tool execution).
//!
//! `run()` drives the turn loop: the boundary (cancellation, steering,
//! task seed, max-turns), then the prompt chain (machining the ledger,
//! ending in the budget gate), then the response chain (model → append
//! gate → tool dispatch). Everything else is the driver's error-path glue.
//!
//! Design provenance: pi's `agentLoop` (`packages/agent/src/agent-loop.ts`),
//! harness-rs's `RunLoop` (`crates/harness-loop/src/lib.rs`), and
//! NanoHarness's `engine.run()` (`core/engine.py`). The three converge on
//! the shape -- a free function that owns one turn loop, with no persistent
//! state beyond the transcript `Vec<Message>` -- but differ in how hooks and
//! policies are wired. This implementation uses straight `Arc<dyn Fn>`
//! callbacks (pi shape) and core's `ToolPolicy` / `ApprovalBroker` seams
//! (NanoHarness shape), not harness-rs's `Hook` trait system.
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

use crate::chain::Chain;
use harness_core::event::{EventSink, RunEventKind, emit_event};
use harness_core::message::Message;
use harness_core::model::Model;
use harness_core::policy::{ApprovalBroker, ToolPolicy};
use harness_core::run::{RunContext, RunStopReason};
use harness_core::tool::Tool;
use harness_core::topology::{Route, Turn};
use harness_core::transcript::{self, TranscriptSink};
use harness_core::{LoopHooks, RunControl, ToolExecutionMode};
use harness_model::{AppendProcessor, ModelProcessor};
use harness_tools::{DispatchCtx, ToolDispatchProcessor, dispatch_batch, push_tool_results};

/// Dependencies the loop needs to run one turn-loop.
///
/// The loop is stateless: all per-run state lives in the `Turn` `run()`
/// builds from the seed (ledger, control, sequence counter). `Arc<LoopDeps>`
/// is cheap to clone and share across runs.
///
/// Both topologies are assembled **by the caller** (the builder for a spec,
/// or a host by hand): this struct is the driving contract, not an assembly
/// point. `response_chain` is the response chain (model → append gate →
/// tool dispatch) and `prompt_chain` the prompt chain ending in the
/// budget gate when one is configured.
pub struct LoopDeps {
    /// The upstream model this loop calls.
    pub model: Arc<dyn Model>,
    /// Registered tools, indexed by name at dispatch time (linear scan,
    /// acceptable for tens of tools).
    pub tools: Vec<Arc<dyn Tool>>,
    /// Fire-and-forget event sink -- never blocks the critical path.
    pub event_sink: Arc<dyn EventSink>,
    /// Two-stage tool policy (before / after). Must be cheap and sync.
    pub policy: Arc<dyn ToolPolicy>,
    /// Optional approval broker for tools that require human approval.
    pub approval_broker: Option<Arc<dyn ApprovalBroker>>,
    /// Pi-style config callbacks. The driver uses them for the error-path
    /// backfill dispatch; the tool-dispatch node holds its own copy.
    pub hooks: LoopHooks,
    /// How tools are dispatched within one turn.
    pub tool_mode: ToolExecutionMode,
    /// The prompt-side machining chain, ending in the budget gate when one
    /// is configured: `Processor` nodes the run passes the turn through
    /// before the model call (system ledger, injected instructions, budget
    /// gate, later skills and retrieval).
    pub prompt_chain: Chain,
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
            .field("seed_len", &self.seed.len())
            .field(
                "transcript_sink",
                &self.transcript_sink.as_ref().map(|_| "set"),
            )
            .field("hooks", &self.hooks)
            .field("prompt_chain", &self.prompt_chain)
            .field("tool_mode", &self.tool_mode)
            .finish()
    }
}

/// Turn boundary for one loop iteration: cancellation check, steering drain,
/// task seed on the first turn, and the max-turns check.
///
/// Returns `Ok` when the turn may proceed, or `Err` with the stop reason
/// when the run ends at this boundary. The caller emits `RunFinished`
impl LoopDeps {
    /// The response-phase chain the driver installs: model → append
    /// (gate) → tool dispatch, assembled from the domain-owned nodes
    /// (`harness-model`, `harness-tools`) over **this** `LoopDeps`' own
    /// sinks.
    ///
    /// Assembly happens here, at run time, rather than at build time: the
    /// scheduler and the daemon swap `event_sink` / `transcript_sink` per
    /// run (the bus sink for a scheduler run, the session sink for a daemon
    /// run), and chain nodes capture the sink at construction — building
    /// the chain here means the nodes always see the sink the run will
    /// actually use.
    pub fn response_chain(&self) -> Chain {
        Chain::new()
            .node(Arc::new(ModelProcessor::new(Arc::clone(&self.event_sink))))
            .node(Arc::new(AppendProcessor::new(self.transcript_sink.clone())))
            .node(Arc::new(ToolDispatchProcessor::new(
                Arc::clone(&self.policy),
                self.approval_broker.clone(),
                self.hooks.clone(),
                Arc::clone(&self.event_sink),
                self.tool_mode,
                self.transcript_sink.clone(),
            )))
    }
}

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
///   Drain steer / inject user
///     |
///     v
///   Prompt chain (ledger machining, budget gate)
///     |--Done--> stop
///     v
///   Response chain: model stream → append gate → tool dispatch
///     |--Done--> stop (error: backfill partial tool calls first)
///     v
///   next turn
/// ```
///
/// Every exit path emits `RunFinished` before returning.
pub async fn run(ctx: RunContext, deps: Arc<LoopDeps>, control: RunControl) -> RunStopReason {
    // One Turn per run: the ledger, control, run identity and sequence
    // counter live here from now on.
    let mut turn = Turn::new(
        Arc::clone(&deps.model),
        control,
        ctx.clone(),
        deps.seed.clone(),
    );
    // The tool manifest rides the turn — the model and dispatch nodes
    // render it.
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

        // Prompt-side machining: the chain's processors reshape the
        // ledger before the model call (system ledger, instructions, budget
        // gate). A processor that returns Done ends the run here.
        if let Route::Done(reason) = deps.prompt_chain.step(&mut turn).await {
            return finish(&ctx, reason, deps.event_sink.as_ref(), &turn.seq);
        }

        // Response phase: the model node pumps the stream, the append node
        // gates the run (no tool calls → Completed), the dispatch node runs
        // the batch.
        turn.turn_idx = turn_idx;
        if let Route::Done(reason) = deps.response_chain().step(&mut turn).await {
            // Error backfill: partial content and tool calls still get
            // their ledger entries, so tool-call/result pairing never
            // breaks.
            if let RunStopReason::Error { .. } = reason {
                backfill_partial_turn(&mut turn, &deps).await;
            }
            return finish(&ctx, reason, deps.event_sink.as_ref(), &turn.seq);
        }

        turn_idx += 1;
    }
}

/// Error-path backfill: the model stream died mid-turn, so the partial
/// assistant content and any tool calls still get their ledger entries.
/// Legacy `handle_stream_outcome` territory.
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
        let tcx = DispatchCtx {
            turn_idx: turn.turn_idx,
            ctx: &turn.ctx,
            tools: &deps.tools,
            policy: &*deps.policy,
            approval_broker: &deps.approval_broker,
            hooks: &deps.hooks,
            seq: &turn.seq,
            sink: deps.event_sink.as_ref(),
            control: &turn.control,
        };
        let (results, _terminal) = dispatch_batch(&partial_calls, &tcx, deps.tool_mode).await;
        push_tool_results(
            &mut turn.ledger,
            deps.transcript_sink.as_deref(),
            &turn.ctx,
            results,
        )
        .await;
    }
}

/// Emit `RunFinished` and return the reason.
pub(crate) fn finish(
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
