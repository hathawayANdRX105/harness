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
use harness_core::event::{RunEvent, RunEventKind};
use harness_core::message::{ToolCall, ToolResultMessage};
use harness_core::policy::{
    ApprovalBroker, ApprovalResult, ExecutionStatus, PolicyOutcome, PolicyStage, ToolExecution,
    ToolPolicy,
};
use harness_core::run::{RunContext, RunId, RunStopReason, SessionId};
use harness_core::tool::{Tool, ToolError, ToolRequest, ToolResult};
use harness_core::{EventSink, RunControl};

use crate::hooks::{LoopHooks, ToolExecutionMode};

/// Shared context for one dispatch batch, passed to every tool-level call.
pub(crate) struct DispatchCtx<'a> {
    pub(crate) turn: u32,
    pub(crate) ctx: &'a RunContext,
    pub(crate) tools: &'a [Arc<dyn Tool>],
    pub(crate) policy: &'a dyn ToolPolicy,
    pub(crate) approval_broker: &'a Option<Arc<dyn ApprovalBroker>>,
    pub(crate) hooks: &'a LoopHooks,
    pub(crate) seq: &'a AtomicU64,
    pub(crate) sink: &'a dyn EventSink,
    pub(crate) control: &'a RunControl,
}

/// The outcome of dispatching one tool call.
#[derive(Debug, Clone)]
pub(crate) struct DispatchOutcome {
    /// The `ToolResultMessage` to push into the transcript.
    pub result_msg: ToolResultMessage,
    /// Whether dispatch hit a terminal condition that should stop the run.
    pub terminal: Option<RunStopReason>,
}

/// Run one tool through the full pipeline: policy -> approval -> execute -> after-policy.
///
/// Every failure is captured as a `ToolResultMessage` with `is_error: true`;
/// the function only returns a terminal condition for cancellation (approval
/// or tool-level) -- errors from execution do not abort the run.
async fn dispatch_one(call: &ToolCall, tcx: &DispatchCtx<'_>) -> DispatchOutcome {
    let request = ToolRequest {
        call_id: call.id.clone(),
        name: call.name.clone(),
        args: call.args.clone(),
        run_id: tcx.ctx.run_id.clone(),
        session_id: tcx.ctx.session_id.clone(),
        turn: tcx.turn,
    };

    // --- Hook: before_tool_call ---
    if let Some(ref hook) = tcx.hooks.before_tool_call
        && let Err(reason) = hook(&request)
    {
        return DispatchOutcome {
            result_msg: ToolResultMessage {
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                content: reason,
                is_error: true,
            },
            terminal: None,
        };
    }

    // --- Before-stage policy ---
    let before_decision = tcx.policy.decide(PolicyStage::BeforeTool, &request, None);
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

    // --- Approval (if policy requires it) ---
    match before_decision.outcome {
        PolicyOutcome::Allow => {}
        PolicyOutcome::RequireApproval => {
            let Some(broker) = tcx.approval_broker else {
                return DispatchOutcome {
                    result_msg: ToolResultMessage {
                        tool_call_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        content: format!(
                            "tool `{}` requires approval but no broker is configured",
                            call.name
                        ),
                        is_error: true,
                    },
                    terminal: None,
                };
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
                .request(&request, &before_decision, &tcx.control.cancel_token())
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
                ApprovalResult::Denied { reason } => {
                    return DispatchOutcome {
                        result_msg: ToolResultMessage {
                            tool_call_id: call.id.clone(),
                            tool_name: call.name.clone(),
                            content: reason,
                            is_error: true,
                        },
                        terminal: None,
                    };
                }
                ApprovalResult::Cancelled => {
                    return DispatchOutcome {
                        result_msg: ToolResultMessage {
                            tool_call_id: call.id.clone(),
                            tool_name: call.name.clone(),
                            content: String::new(),
                            is_error: true,
                        },
                        terminal: Some(RunStopReason::Cancelled),
                    };
                }
                _ => {} // Approved or future variants -- continue to execution.
            }
        }
        PolicyOutcome::Deny => {
            return DispatchOutcome {
                result_msg: ToolResultMessage {
                    tool_call_id: call.id.clone(),
                    tool_name: call.name.clone(),
                    content: before_decision.reason.clone(),
                    is_error: true,
                },
                terminal: None,
            };
        }
    }

    // --- Execute ---
    let Some(tool) = tcx.tools.iter().find(|t| t.name() == call.name) else {
        return DispatchOutcome {
            result_msg: ToolResultMessage {
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                content: format!("tool `{}` is not registered", call.name),
                is_error: true,
            },
            terminal: None,
        };
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

    let tool_result = tool
        .invoke(call.args.clone(), &tcx.control.cancel_token())
        .await;

    let (tool_result, terminal) = match tool_result {
        Ok(r) => (r, None),
        Err(ToolError::Cancelled) => {
            return DispatchOutcome {
                result_msg: ToolResultMessage {
                    tool_call_id: call.id.clone(),
                    tool_name: call.name.clone(),
                    content: "tool call was cancelled".into(),
                    is_error: true,
                },
                terminal: Some(RunStopReason::Cancelled),
            };
        }
        Err(e) => (
            ToolResult {
                content: e.to_string(),
                is_error: true,
                trace: None,
            },
            None,
        ),
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

    // --- After-stage policy ---
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
        .decide(PolicyStage::AfterTool, &request, Some(&execution));
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

    // Apply output_suffix from either decision.
    let mut content = tool_result.content;
    let suffix = after_decision
        .output_suffix
        .as_deref()
        .or(before_decision.output_suffix.as_deref());
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

    // --- Hook: after_tool_call ---
    if let Some(ref hook) = tcx.hooks.after_tool_call {
        hook(&request, &result_msg);
    }

    DispatchOutcome {
        result_msg,
        terminal,
    }
}

/// Dispatch an entire batch of tool calls.
///
/// Respects [`ToolExecutionMode`]: parallel runs all calls concurrently via
/// `join_all`; sequential runs them one at a time so each result feeds the next.
pub(crate) async fn dispatch_batch(
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
            let turn = tcx.turn;
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
                            turn,
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

/// Emit one `RunEvent` through the sink, assigning the next sequence number.
///
/// Uses `AtomicU64` so concurrent dispatch (parallel mode) can safely share
/// the counter.
#[inline]
pub(crate) fn emit_event(
    sink: &dyn EventSink,
    run_id: &RunId,
    session_id: &SessionId,
    seq: &AtomicU64,
    kind: RunEventKind,
) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let event = RunEvent {
        run_id: run_id.clone(),
        session_id: session_id.clone(),
        seq: seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        timestamp_ms: now_ms,
        kind,
    };
    sink.emit(event);
}
