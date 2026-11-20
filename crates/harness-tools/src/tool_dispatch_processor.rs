//! The response-side node that drives the tool batch through dispatch.

use std::sync::Arc;

use async_trait::async_trait;

use harness_core::message::{Message, ToolResultMessage};
use harness_core::policy::{ApprovalBroker, ToolPolicy};
use harness_core::run::{RunContext, RunStopReason};
use harness_core::topology::{Processor, Route, Turn};
use harness_core::transcript::{self, TranscriptSink};
use harness_core::{LoopHooks, ToolExecutionMode};

use crate::dispatch::{DispatchCtx, dispatch_batch};
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
    tool_mode: ToolExecutionMode,
    transcript_sink: Option<Arc<dyn TranscriptSink>>,
}

impl ToolDispatchProcessor {
    pub fn new(
        policy: Arc<dyn ToolPolicy>,
        approval_broker: Option<Arc<dyn ApprovalBroker>>,
        hooks: LoopHooks,
        tool_mode: ToolExecutionMode,
        transcript_sink: Option<Arc<dyn TranscriptSink>>,
    ) -> Self {
        Self {
            policy,
            approval_broker,
            hooks,
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
            sink: turn.event_sink.as_ref(),
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
pub async fn push_tool_results(
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
