//! The turn-boundary budget gate — this crate's only consumer of
//! `harness-context`.
//!
//! The dependency manifest justifies `harness-context` by this file alone:
//! the gate reads the compactor's budget view of the transcript and runs
//! one compaction pass when a stage is over. Everything else in the loop
//! works on `harness-core` trait objects.

use std::sync::atomic::AtomicU64;

use harness_context::{Budget, BudgetBlocked, Compactor};
use harness_core::RunControl;
use harness_core::event::RunEventKind;
use harness_core::message::Message;
use harness_core::model::ModelInfo;
use harness_core::run::{RunContext, RunStopReason};

use crate::dispatch::emit_event;
use crate::engine::{LoopDeps, finish};

/// Turn-boundary budget gate: runs before the provider call so a round is
/// refused (not failed) when the context cannot fit the next turn.
///
/// When both `budget` and `compactor` are configured: refresh the budget
/// from the current transcript; when a compaction stage is active, run
/// compaction once — cancellable via the run's cancel token — then re-check.
/// If the budget is still over a stage threshold, emit a
/// `RunEventKind::Error` carrying [`BudgetBlocked::error_message`] and return
/// `RunStopReason::Error` carrying the same message. Callers recover the
/// distinction with [`BudgetBlocked::from_error_message`]; core's
/// `RunStopReason` is frozen and has no budget-blocked variant.
///
/// Returns `None` when the gate passes or is unconfigured (either dep
/// missing), so the turn proceeds to the provider call.
pub(crate) async fn enforce_budget_gate(
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
