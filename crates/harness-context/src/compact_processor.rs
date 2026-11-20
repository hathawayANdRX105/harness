//! The compaction processor: machines the ledger down to fit the budget.
//!
//! Ownership rationale: compaction is a context-management rule — it reads
//! the compactor's view of the transcript and runs one replacement pass —
//! so the processor lives in the crate that owns `Budget` / `Compactor`,
//! next to the compaction stages it evaluates. The driver installs it as
//! the **last node of the prompt chain**, so compaction sees the
//! prompt-machined ledger before the model call, which is where the
//! legacy `run()` had it.
//!
//! The `Compactor` trait is the injected **strategy**: how a ledger region
//! becomes a digest summary is the strategy's business; *when* to compact,
//! *whether* the round is still refused afterward, and the
//! `BudgetBlocked`-shaped termination are this node's.

use std::sync::Arc;

use async_trait::async_trait;
use harness_core::event::{EventSink, RunEventKind, emit_event};
use harness_core::message::Message;
use harness_core::model::ModelInfo;
use harness_core::run::RunStopReason;
use harness_core::topology::{Processor, Route, Turn};

use crate::{Budget, BudgetBlocked, Compactor};

/// Compact the ledger to fit the budget, and refuse the round when it
/// still cannot.
///
/// When a compaction stage is active over the current budget: run the
/// strategy once — cancellable via the run's cancel token — then re-check.
/// If a stage is still active, emit a `RunEventKind::Error` carrying
/// [`BudgetBlocked::error_message`] and end the run with
/// `RunStopReason::Error` carrying the same message. Callers recover the
/// distinction with [`BudgetBlocked::from_error_message`]; core's
/// `RunStopReason` is frozen and has no budget-blocked variant.
///
/// Cancellation observed at the gate wins: a cancelled run is not
/// compacted. The host's configured per-run input cap (mapped from
/// `[budget].max_input_tokens`) is internal policy: it caps the
/// strategy's own model-window estimate so a configured limit actually
/// gates.
pub struct CompactProcessor {
    /// The replacement strategy: how the ledger shrinks. Injected so
    /// every compaction implementation stays substitutable.
    strategy: Arc<dyn Compactor>,
    /// The host's configured per-run input ceiling, mapped from
    /// `[budget].max_input_tokens`. `None` leaves the strategy's own
    /// model-window estimate ungated.
    configured: Option<Budget>,
    event_sink: Arc<dyn EventSink>,
}

impl CompactProcessor {
    pub fn new(
        strategy: Arc<dyn Compactor>,
        configured: Option<Budget>,
        event_sink: Arc<dyn EventSink>,
    ) -> Self {
        Self {
            strategy,
            configured,
            event_sink,
        }
    }

    /// The processor's view of the budget: the strategy's model-window
    /// estimate, capped by the host's configured per-run input limit.
    ///
    /// The two come from different places — the strategy reads
    /// `ModelInfo.context_window`, the host maps `[budget].max_input_tokens`
    /// in — so the tighter value wins. Without the cap a configured limit
    /// would never gate anything.
    fn budget_view(&self, transcript: &[Message], info: &ModelInfo) -> Budget {
        let mut budget = self.strategy.read_budget(transcript, info);
        if let Some(configured) = &self.configured {
            budget.available_tokens = budget.available_tokens.min(configured.available_tokens);
        }
        budget
    }
}

#[async_trait]
impl Processor for CompactProcessor {
    fn name(&self) -> &'static str {
        "compact"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        // Cancellation observed at the boundary wins: don't compact a
        // cancelled run.
        if turn.control.is_cancelled() {
            return Route::Done(RunStopReason::Cancelled);
        }

        let info = turn.model.info();
        let budget = self.budget_view(&turn.ledger, info);
        let Some(stage) = self
            .strategy
            .stages()
            .iter()
            .find(|stage| stage.is_active(&budget))
        else {
            return Route::Next;
        };

        // Compaction is the only relief available before refusing the round;
        // run it once, cancellable via the run's cancel token.
        let _result = self
            .strategy
            .compact(
                &mut turn.ledger,
                &budget,
                None,
                &turn.control.cancel_token(),
            )
            .await;

        if turn.control.is_cancelled() {
            return Route::Done(RunStopReason::Cancelled);
        }

        let budget = self.budget_view(&turn.ledger, info);
        let still_blocked = self
            .strategy
            .stages()
            .iter()
            .any(|stage| stage.is_active(&budget));
        if !still_blocked {
            return Route::Next;
        }

        // `stage` names the first stage that was active when the processor
        // ran, not the deepest one the strategy reached: the applied labels
        // live in the compaction result's checkpoint, which the processor
        // does not keep.
        let blocked = BudgetBlocked {
            needed: budget.used_tokens,
            available: budget.available_tokens,
            stage: stage.label.clone(),
        };
        let message = blocked.error_message();
        emit_event(
            self.event_sink.as_ref(),
            &turn.ctx.run_id,
            &turn.ctx.session_id,
            &turn.seq,
            RunEventKind::Error {
                message: message.clone(),
            },
        );
        Route::Done(RunStopReason::Error { message })
    }
}
