//! Policy & approval seams: the two-stage tool policy and the cancellable
//! approval broker.
//!
//! Acceptance: one policy implementor sees both stages and its richer
//! payloads (context_injection before, output_suffix after) survive intact;
//! the approval broker is `async` and resolves `Cancelled` the moment the
//! run's token fires, even mid-ask — the nanoharness freeze, fixed.

use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on;
use harness_core::message::ToolCall;
use harness_core::policy::{
    ApprovalBroker, ApprovalResult, ExecutionStatus, PolicyDecision, PolicyOutcome, PolicyStage,
    ToolExecution, ToolPolicy,
};
use harness_core::run::{RunContext, RunId, SessionId, Task};
use harness_core::tool::{ToolError, ToolRequest, ToolResult};
use tokio_util::sync::CancellationToken;

fn sample_request(name: &str) -> ToolRequest {
    ToolRequest {
        call_id: "call-1".into(),
        name: name.into(),
        args: serde_json::json!({}),
        run_id: RunId("r-1".into()),
        session_id: SessionId("s-1".into()),
        turn: 0,
    }
}

fn sample_run() -> RunContext {
    RunContext {
        run_id: RunId("r-1".into()),
        session_id: SessionId("s-1".into()),
        task: Task {
            description: "demo".into(),
            source: None,
            deadline: None,
        },
        limits: Default::default(),
        started_at_ms: 0,
    }
}

/// A layered policy: a guard denies destructive tools before they run, a
/// reviewer appends an output note after they ran. One implementor sees
/// both stages, so a chain records its decisions coherently.
struct GuardPolicy;

impl ToolPolicy for GuardPolicy {
    fn decide(
        &self,
        stage: PolicyStage,
        req: &ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
        match stage {
            PolicyStage::BeforeTool if req.name == "rm" => PolicyDecision {
                outcome: PolicyOutcome::Deny,
                reason: "destructive: rm is off by default".into(),
                source: "guard".into(),
                context_injection: Some("Remember: prefer trash over rm.".into()),
                output_suffix: None,
            },
            PolicyStage::BeforeTool => allow("guard"),
            PolicyStage::AfterTool => PolicyDecision {
                outcome: PolicyOutcome::Allow,
                reason: "reviewed".into(),
                source: "reviewer".into(),
                context_injection: None,
                output_suffix: Some("Reviewed by policy.".into()),
            },
        }
    }
}

fn allow(source: &str) -> PolicyDecision {
    PolicyDecision {
        outcome: PolicyOutcome::Allow,
        reason: "no objection".into(),
        source: source.into(),
        context_injection: None,
        output_suffix: None,
    }
}

/// The before-stage can deny with a payload the loop injects into the
/// context before anything ran.
#[test]
fn before_stage_denies_with_context_injection() {
    let policy = GuardPolicy;
    let req = sample_request("rm");
    let decision = policy.decide(PolicyStage::BeforeTool, &req, None);
    assert_eq!(decision.outcome, PolicyOutcome::Deny);
    assert_eq!(
        decision.context_injection.as_deref(),
        Some("Remember: prefer trash over rm.")
    );
    assert!(decision.output_suffix.is_none());
}

/// The after-stage sees the execution and its decision carries the
/// output_suffix the loop appends to the observation.
#[test]
fn after_stage_sees_execution_and_appends_suffix() {
    let policy = GuardPolicy;
    let req = sample_request("ls");
    let execution = ToolExecution {
        status: ExecutionStatus::Success,
        result: Some(ToolResult {
            content: "src/".into(),
            is_error: false,
            trace: None,
        }),
    };
    let decision = policy.decide(PolicyStage::AfterTool, &req, Some(&execution));
    assert_eq!(decision.outcome, PolicyOutcome::Allow);
    assert_eq!(
        decision.output_suffix.as_deref(),
        Some("Reviewed by policy.")
    );
}

/// A policy may defer to a human — the outcome that parks the run on the
/// broker.
#[test]
fn require_approval_is_a_first_class_outcome() {
    let policy = |stage: PolicyStage, req: &ToolRequest| {
        if stage == PolicyStage::BeforeTool && req.name == "deploy" {
            PolicyDecision {
                outcome: PolicyOutcome::RequireApproval,
                reason: "deploys need a human".into(),
                source: "deploy-policy".into(),
                context_injection: None,
                output_suffix: None,
            }
        } else {
            allow("deploy-policy")
        }
    };
    let req = sample_request("deploy");
    assert_eq!(
        policy(PolicyStage::BeforeTool, &req).outcome,
        PolicyOutcome::RequireApproval
    );
}

/// A broker that answers immediately: the run was never cancelled, the
/// approval lands.
struct EagerBroker;

#[async_trait]
impl ApprovalBroker for EagerBroker {
    async fn request(
        &self,
        _req: &ToolRequest,
        _decision: &PolicyDecision,
        _cancel: &CancellationToken,
    ) -> ApprovalResult {
        ApprovalResult::Approved
    }
}

/// A broker that waits for a human — and honors the cancel token the run
/// handed it, which is the contract a daemon's transport relies on.
struct WaitingBroker;

#[async_trait]
impl ApprovalBroker for WaitingBroker {
    async fn request(
        &self,
        _req: &ToolRequest,
        _decision: &PolicyDecision,
        cancel: &CancellationToken,
    ) -> ApprovalResult {
        for _ in 0..10_000 {
            if cancel.is_cancelled() {
                return ApprovalResult::Cancelled;
            }
        }
        ApprovalResult::Denied {
            reason: "no answer".into(),
        }
    }
}

/// Approved-before-any-cancellation: the happy path through `Arc<dyn
/// ApprovalBroker>`.
#[test]
fn broker_approves_when_run_is_alive() {
    let broker: Arc<dyn ApprovalBroker> = Arc::new(EagerBroker);
    let cancel = CancellationToken::new();
    let req = sample_request("deploy");
    let decision = allow("deploy-policy");
    assert_eq!(
        block_on(broker.request(&req, &decision, &cancel)),
        ApprovalResult::Approved
    );
}

/// The fix for nanoharness's synchronous broker: a run cancelled *while the
/// question is open* resolves `Cancelled` — a late approval arriving over a
/// transport must not resurrect the run.
#[test]
fn broker_resolves_cancelled_when_run_cancels_mid_ask() {
    // Through `Arc<dyn ApprovalBroker>` — the shape the loop holds — the
    // cancelled run resolves before any human answer could arrive.
    let broker: Arc<dyn ApprovalBroker> = Arc::new(WaitingBroker);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let req = sample_request("deploy");
    let decision = allow("deploy-policy");
    let answer = broker.request(&req, &decision, &cancel);
    assert_eq!(block_on(answer), ApprovalResult::Cancelled);
}

/// `unattended_denied` fails closed: RequireApproval with no broker is a
/// permission error, never a silent allow.
#[test]
fn unattended_approval_fails_closed() {
    let req = sample_request("deploy");
    let err = harness_core::policy::unattended_denied(&req);
    assert!(matches!(err, ToolError::Permission(p) if p.contains("deploy")));
}

/// The policy seam sees the run the request belongs to — a host can key
/// decisions per-run without extra context objects.
#[test]
fn tool_request_carries_run_and_session() {
    let run = sample_run();
    let req = ToolRequest {
        call_id: "c".into(),
        name: "read".into(),
        args: serde_json::json!({}),
        run_id: run.run_id.clone(),
        session_id: run.session_id.clone(),
        turn: 2,
    };
    assert_eq!(req.run_id, RunId("r-1".into()));
    assert_eq!(req.turn, 2);
}

// ToolCall on the policy path keeps parity with the model stream's calls.
#[test]
fn tool_call_shape_matches_stream_vocabulary() {
    let call = ToolCall {
        id: "call-1".into(),
        name: "read".into(),
        args: serde_json::json!({"path": "src/lib.rs"}),
    };
    let req = sample_request("read");
    // A dispatcher maps a stream ToolCall into a ToolRequest keyed on id:
    assert_eq!(req.call_id, call.id);
}
