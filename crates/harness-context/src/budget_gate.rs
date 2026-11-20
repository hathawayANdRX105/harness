//! The turn-boundary budget gate as a topology node.
//!
//! Ownership rationale: the gate is a context-management rule — it reads the
//! compactor's view of the transcript and runs one compaction pass — so it
//! lives in the crate that owns `Budget` / `Compactor`, next to the
//! compaction stages it evaluates. The driver installs it as the **last
//! node of the prompt phase**, so compaction sees the prompt-machined
//! ledger before the model call, which is where the legacy `run()` had it.

use std::sync::Arc;

use async_trait::async_trait;
use harness_core::event::{EventSink, RunEventKind, emit_event};
use harness_core::message::Message;
use harness_core::model::ModelInfo;
use harness_core::run::RunStopReason;
use harness_core::topology::{Processor, Route, Turn};

use crate::{Budget, BudgetBlocked, Compactor};

/// Refuse the round before the provider call when the context cannot fit
/// the next turn, even after one compaction pass.
///
/// When a compaction stage is active over the current budget: run compaction
/// once — cancellable via the run's cancel token — then re-check. If a stage
/// is still active, emit a `RunEventKind::Error` carrying
/// [`BudgetBlocked::error_message`] and end the run with
/// `RunStopReason::Error` carrying the same message. Callers recover the
/// distinction with [`BudgetBlocked::from_error_message`]; core's
/// `RunStopReason` is frozen and has no budget-blocked variant.
///
/// Cancellation observed at the gate wins: a cancelled run is not compacted.
pub struct BudgetGateProcessor {
    compactor: Arc<dyn Compactor>,
    /// The host's configured per-run input ceiling, mapped from
    /// `[budget].max_input_tokens`. `None` leaves the compactor's own
    /// model-window estimate ungated.
    configured: Option<Budget>,
    event_sink: Arc<dyn EventSink>,
}

impl BudgetGateProcessor {
    pub fn new(
        compactor: Arc<dyn Compactor>,
        configured: Option<Budget>,
        event_sink: Arc<dyn EventSink>,
    ) -> Self {
        Self {
            compactor,
            configured,
            event_sink,
        }
    }

    /// The gate's view of the budget: the compactor's model-window estimate,
    /// capped by the host's configured per-run input limit.
    ///
    /// The two come from different places — the compactor reads
    /// `ModelInfo.context_window`, the host maps `[budget].max_input_tokens`
    /// in — so the tighter value wins. Without the cap a configured limit
    /// would never gate anything.
    fn gate_budget(&self, transcript: &[Message], info: &ModelInfo) -> Budget {
        let mut budget = self.compactor.read_budget(transcript, info);
        if let Some(configured) = &self.configured {
            budget.available_tokens = budget.available_tokens.min(configured.available_tokens);
        }
        budget
    }
}

#[async_trait]
impl Processor for BudgetGateProcessor {
    fn name(&self) -> &'static str {
        "budget_gate"
    }

    async fn process(&self, turn: &mut Turn) -> Route {
        // Cancellation observed at the boundary wins: don't compact a
        // cancelled run.
        if turn.control.is_cancelled() {
            return Route::Done(RunStopReason::Cancelled);
        }

        let info = turn.model.info();
        let budget = self.gate_budget(&turn.ledger, info);
        let Some(stage) = self
            .compactor
            .stages()
            .iter()
            .find(|stage| stage.is_active(&budget))
        else {
            return Route::Next;
        };

        // Compaction is the only relief available before refusing the round;
        // run it once, cancellable via the run's cancel token.
        let _result = self
            .compactor
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

        let budget = self.gate_budget(&turn.ledger, info);
        let still_blocked = self
            .compactor
            .stages()
            .iter()
            .any(|stage| stage.is_active(&budget));
        if !still_blocked {
            return Route::Next;
        }

        // `stage` names the first stage that was active when the gate ran,
        // not the deepest one the compactor reached: the applied labels
        // live in the compaction result's checkpoint, which the gate does
        // not keep.
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
