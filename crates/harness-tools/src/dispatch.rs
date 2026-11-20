//! Tool dispatch: policy -> approval -> execution -> after-stage evaluation.
//!
//! Design provenance: nanoharness's `ToolExecutor` (`core/engine.py:113-175`)
//! contributes the seam ordering -- before-policy, approval if required,
//! execution, after-policy -- with failures as values at every stage. pi's
//! `agent-loop.ts:341-431` contributes the parallel/sequential batch dispatch
//! and the `shouldTerminateToolBatch` shortcut. harness-rs's
//! `crates/harness-loop/src/lib.rs` contributes the error-to-ToolResult
//! mapping (not-found / invalid-args / permission each becomes `is_error: true`
//! with a human-readable message).
//!
//! The dispatch path is the same for every tool in a turn.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use futures::future::join_all;
use harness_core::event::{RunEventKind, emit_event};
use harness_core::message::{ToolCall, ToolResultMessage};
use harness_core::policy::{
    ApprovalBroker, ApprovalResult, ExecutionStatus, PolicyDecision, PolicyOutcome, PolicyStage,
    ToolExecution, ToolPolicy,
};
use harness_core::run::{RunContext, RunStopReason};
use harness_core::tool::{Tool, ToolError, ToolRequest, ToolResult};
use harness_core::topology::Turn;
use harness_core::{EventSink, LoopHooks, RunControl, ToolExecutionMode};
/// Shared context for one dispatch batch, passed to every tool-level call.
pub struct DispatchCtx<'a> {
    pub turn_idx: u32,
    pub ctx: &'a RunContext,
    pub tools: &'a [Arc<dyn Tool>],
    pub policy: &'a dyn ToolPolicy,
    pub approval_broker: &'a Option<Arc<dyn ApprovalBroker>>,
    pub hooks: &'a LoopHooks,
    pub seq: &'a AtomicU64,
    pub sink: &'a dyn EventSink,
    pub control: &'a RunControl,
}

impl<'a> DispatchCtx<'a> {
    /// Derive a dispatch context from the turn plus the three policy seams.
    ///
    /// Six of the nine fields are turn state (index, identity, tools, seq,
    /// event sink, run control) — the single source of truth — so the only
    /// hand-supplied inputs are the run-level policy seams the caller
    /// already holds. Assembling this struct in one place keeps the turn
    /// and the dispatch context from drifting apart.
    pub fn from_turn(
        turn: &'a Turn,
        policy: &'a dyn ToolPolicy,
        approval_broker: &'a Option<Arc<dyn ApprovalBroker>>,
        hooks: &'a LoopHooks,
    ) -> Self {
        Self {
            turn_idx: turn.turn_idx,
            ctx: &turn.ctx,
            tools: &turn.tools,
            policy,
            approval_broker,
            hooks,
            seq: &turn.seq,
            sink: turn.event_sink.as_ref(),
            control: &turn.control,
        }
    }
}

/// The outcome of dispatching one tool call.
#[derive(Debug, Clone)]
pub(crate) struct DispatchOutcome {
    /// The `ToolResultMessage` to push into the transcript.
    pub result_msg: ToolResultMessage,
    /// Whether dispatch hit a terminal condition that should stop the run.
    pub terminal: Option<RunStopReason>,
}

/// Run one tool through the full pipeline: before-hook, before-stage policy,
/// approval (if required), execution, after-stage policy, result message.
///
/// Every failure is captured as a `ToolResultMessage` with `is_error: true`;
/// the function only surfaces a terminal condition for cancellation (approval
/// or tool-level) -- execution errors do not abort the run.
async fn dispatch_one(call: &ToolCall, tcx: &DispatchCtx<'_>) -> DispatchOutcome {
    let request = ToolRequest {
        call_id: call.id.clone(),
        name: call.name.clone(),
        args: call.args.clone(),
        run_id: tcx.ctx.run_id.clone(),
        session_id: tcx.ctx.session_id.clone(),
        turn_idx: tcx.turn_idx,
    };

    // --- Hook: before_tool_call ---
    if let Some(hook) = &tcx.hooks.before_tool_call
        && let Err(reason) = hook(&request)
    {
        return error_result(call, reason);
    }

    // --- Before-stage policy ---
    let before_decision = before_policy(tcx, &request);

    // --- Approval (if policy requires it) ---
    match approval_stage(tcx, call, &request, &before_decision).await {
        Ok(()) => {}
        Err(outcome) => return outcome,
    }

    // --- Execute ---
    let tool_result = match execute_stage(tcx, call).await {
        Ok(tool_result) => tool_result,
        Err(outcome) => return outcome,
    };
    let is_error = tool_result.is_error;

    emit_event(
        tcx.sink,
        &tcx.ctx.run_id,
        &tcx.ctx.session_id,
        tcx.seq,
        RunEventKind::ToolFinished {
            call_id: call.id.clone(),
            name: call.name.clone(),
            is_error,
        },
    );

    // --- After-stage policy + result message ---
    let result_msg = after_stage(tcx, call, &request, &before_decision, tool_result);

    DispatchOutcome {
        result_msg,
        terminal: None,
    }
}

/// Before-stage policy: decide `BeforeTool` and emit the `PolicyDecided` event.
fn before_policy(tcx: &DispatchCtx<'_>, request: &ToolRequest) -> PolicyDecision {
    let before_decision = tcx.policy.decide(PolicyStage::BeforeTool, request, None);
    emit_event(
        tcx.sink,
        &tcx.ctx.run_id,
        &tcx.ctx.session_id,
        tcx.seq,
        RunEventKind::PolicyDecided {
            stage: PolicyStage::BeforeTool,
            outcome: before_decision.outcome,
            reason: before_decision.reason.clone(),
        },
    );
    before_decision
}

/// Approval gate: when the before-stage policy requires approval, ask the
/// broker and translate the answer into a continue/stop signal.
///
/// No broker configured => fail closed with an error result. Denial => error
/// result. Broker cancellation => terminal `Cancelled`.
async fn approval_stage(
    tcx: &DispatchCtx<'_>,
    call: &ToolCall,
    request: &ToolRequest,
    before: &PolicyDecision,
) -> Result<(), DispatchOutcome> {
    match before.outcome {
        PolicyOutcome::Allow => Ok(()),
        PolicyOutcome::Deny => Err(error_result(call, before.reason.clone())),
        PolicyOutcome::RequireApproval => {
            let Some(broker) = tcx.approval_broker.as_ref() else {
                return Err(error_result(
                    call,
                    format!(
                        "tool `{}` requires approval but no broker is configured",
                        call.name
                    ),
                ));
            };

            emit_event(
                tcx.sink,
                &tcx.ctx.run_id,
                &tcx.ctx.session_id,
                tcx.seq,
                RunEventKind::ApprovalRequested {
                    call_id: call.id.clone(),
                },
            );

            let result = broker
                .request(request, before, &tcx.control.cancel_token())
                .await;

            emit_event(
                tcx.sink,
                &tcx.ctx.run_id,
                &tcx.ctx.session_id,
                tcx.seq,
                RunEventKind::ApprovalResolved {
                    call_id: call.id.clone(),
                    result: result.clone(),
                },
            );

            match result {
                ApprovalResult::Denied { reason } => Err(error_result(call, reason)),
                ApprovalResult::Cancelled => Err(cancelled_result(call)),
                // Approved or future variants -- continue to execution.
                _ => Ok(()),
            }
        }
    }
}

/// Execute one tool: resolve registration, emit `ToolStarted`, invoke, and
/// map outcomes. Cancellation is terminal; other failures become error results.
async fn execute_stage(
    tcx: &DispatchCtx<'_>,
    call: &ToolCall,
) -> Result<ToolResult, DispatchOutcome> {
    let Some(tool) = tcx.tools.iter().find(|t| t.name() == call.name) else {
        return Err(error_result(
            call,
            format!("tool `{}` is not registered", call.name),
        ));
    };

    emit_event(
        tcx.sink,
        &tcx.ctx.run_id,
        &tcx.ctx.session_id,
        tcx.seq,
        RunEventKind::ToolStarted {
            call_id: call.id.clone(),
            name: call.name.clone(),
        },
    );

    match tool
        .invoke(call.args.clone(), &tcx.control.cancel_token())
        .await
    {
        Ok(r) => Ok(r),
        Err(ToolError::Cancelled) => Err(cancelled_result(call)),
        Err(e) => Ok(ToolResult {
            content: e.to_string(),
            is_error: true,
            trace: None,
        }),
    }
}

/// After-stage policy decision, output suffix, and result message: decide
/// `AfterTool`, apply the winning `output_suffix`, run the `after_tool_call`
/// hook, and build the `ToolResultMessage`.
fn after_stage(
    tcx: &DispatchCtx<'_>,
    call: &ToolCall,
    request: &ToolRequest,
    before: &PolicyDecision,
    tool_result: ToolResult,
) -> ToolResultMessage {
    let is_error = tool_result.is_error;

    let execution = ToolExecution {
        status: if is_error {
            ExecutionStatus::Error
        } else {
            ExecutionStatus::Success
        },
        result: Some(tool_result.clone()),
    };
    let after_decision = tcx
        .policy
        .decide(PolicyStage::AfterTool, request, Some(&execution));
    emit_event(
        tcx.sink,
        &tcx.ctx.run_id,
        &tcx.ctx.session_id,
        tcx.seq,
        RunEventKind::PolicyDecided {
            stage: PolicyStage::AfterTool,
            outcome: after_decision.outcome,
            reason: after_decision.reason.clone(),
        },
    );

    // Apply `output_suffix` from either decision: the after-stage decision
    // wins when both stages set one.
    let mut content = tool_result.content;
    let suffix = after_decision
        .output_suffix
        .as_deref()
        .or(before.output_suffix.as_deref());
    if let Some(s) = suffix {
        if !content.is_empty() {
            content.push('\n');
        }
        content.push_str(s);
    }

    let result_msg = ToolResultMessage {
        tool_call_id: call.id.clone(),
        tool_name: call.name.clone(),
        content,
        is_error,
    };

    // Hook: after_tool_call observes the finished call.
    if let Some(hook) = &tcx.hooks.after_tool_call {
        hook(request, &result_msg);
    }

    result_msg
}

/// Error result message for one tool call: `is_error: true`, never terminal.
fn error_result(call: &ToolCall, reason: String) -> DispatchOutcome {
    DispatchOutcome {
        result_msg: ToolResultMessage {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: reason,
            is_error: true,
        },
        terminal: None,
    }
}

/// Terminal cancelled result message for one tool call.
fn cancelled_result(call: &ToolCall) -> DispatchOutcome {
    DispatchOutcome {
        result_msg: ToolResultMessage {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: String::new(),
            is_error: true,
        },
        terminal: Some(RunStopReason::Cancelled),
    }
}

/// Dispatch an entire batch of tool calls.
///
/// Respects [`ToolExecutionMode`]: parallel runs all calls concurrently via
/// `join_all`; sequential runs them one at a time so each result feeds the next.
pub async fn dispatch_batch(
    calls: &[ToolCall],
    tcx: &DispatchCtx<'_>,
    mode: ToolExecutionMode,
) -> (Vec<ToolResultMessage>, Option<RunStopReason>) {
    match mode {
        ToolExecutionMode::Sequential => {
            let mut results = Vec::with_capacity(calls.len());
            for call in calls {
                let outcome = dispatch_one(call, tcx).await;
                if let Some(reason) = &outcome.terminal {
                    results.push(outcome.result_msg);
                    return (results, Some(reason.clone()));
                }
                results.push(outcome.result_msg);
            }
            (results, None)
        }
        ToolExecutionMode::Parallel => {
            // Capture individually-owned data for each parallel future.
            let ctx_owned = tcx.ctx.clone();
            let tools_owned = tcx.tools.to_vec();
            let broker_owned = tcx.approval_broker.clone();
            let hooks_owned = tcx.hooks.clone();
            let control_owned = tcx.control.clone();
            let turn_idx = tcx.turn_idx;
            let policy: &dyn ToolPolicy = tcx.policy;
            let seq: &AtomicU64 = tcx.seq;
            let sink: &dyn EventSink = tcx.sink;

            let futures: Vec<_> = calls
                .iter()
                .map(|call| {
                    let call = call.clone();
                    let ctx = ctx_owned.clone();
                    let tools = tools_owned.clone();
                    let broker = broker_owned.clone();
                    let hooks = hooks_owned.clone();
                    let control = control_owned.clone();
                    async move {
                        let inner_tcx = DispatchCtx {
                            turn_idx,
                            ctx: &ctx,
                            tools: &tools,
                            policy,
                            approval_broker: &broker,
                            hooks: &hooks,
                            seq,
                            sink,
                            control: &control,
                        };
                        dispatch_one(&call, &inner_tcx).await
                    }
                })
                .collect();

            let outcomes = join_all(futures).await;
            let mut results = Vec::with_capacity(outcomes.len());
            let mut terminal = None;
            for outcome in outcomes {
                if terminal.is_none() {
                    terminal = outcome.terminal;
                }
                results.push(outcome.result_msg);
            }
            (results, terminal)
        }
    }
}
