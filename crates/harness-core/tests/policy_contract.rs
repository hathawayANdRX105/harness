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
use harness_core::policy::{
    ApprovalBroker, ApprovalResult, ExecutionStatus, PolicyDecision, PolicyOutcome, PolicyStage,
    ToolExecution, ToolPolicy,
};
use harness_core::run::{RunId, SessionId};
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

/// A policy may defer to a human — `RequireApproval` parks the run on the
/// broker. Exercised through the real trait surface, not a closure.
struct DeployPolicy;

impl ToolPolicy for DeployPolicy {
    fn decide(
        &self,
        stage: PolicyStage,
        req: &ToolRequest,
        _execution: Option<&ToolExecution>,
    ) -> PolicyDecision {
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
    }
}

#[test]
fn require_approval_parks_on_the_broker() {
    let policy = DeployPolicy;
    let req = sample_request("deploy");
    let decision = policy.decide(PolicyStage::BeforeTool, &req, None);
    assert_eq!(decision.outcome, PolicyOutcome::RequireApproval);
    assert_eq!(decision.source, "deploy-policy");
    // Any other tool stays Allow under the same policy — the gate is
    // per-tool, not a blanket defer.
    let other = sample_request("read");
    assert_eq!(
        policy.decide(PolicyStage::BeforeTool, &other, None).outcome,
        PolicyOutcome::Allow
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

/// The policy seam's wire shape is pinned: a persisted ToolRequest (the
/// audit trail) round-trips with its run/session correlation intact.
#[test]
fn tool_request_round_trips_with_run_correlation() -> serde_json::Result<()> {
    let req = ToolRequest {
        call_id: "call-1".into(),
        name: "read".into(),
        args: serde_json::json!({"path": "src/lib.rs"}),
        run_id: RunId("r-9".into()),
        session_id: SessionId("s-2".into()),
        turn: 3,
    };
    let wire = serde_json::to_value(&req)?;
    assert_eq!(wire["run_id"], "r-9");
    assert_eq!(wire["session_id"], "s-2");
    assert_eq!(wire["turn"], 3);
    let back: ToolRequest = serde_json::from_value(wire)?;
    assert_eq!(back, req);
    Ok(())
}
