//! The permission seams: a two-stage tool policy, plus an approval broker
//! that is async and cancellable.
//!
//! Design provenance: nanoharness's `ToolPolicyProtocol` /
//! `ApprovalBrokerProtocol` (`core/base.py:43-63`) contribute the split —
//! policy decides, broker asks a human, and both see the run-aware
//! `ToolRequest`. `PolicyDecision` carries nanoharness's richer payloads
//! (`core/schema.py:114`): `context_injection` (injected into the context
//! before the tool runs) and `output_suffix` (appended to the observation
//! afterwards) — a guardrail reminder and an output redaction are both
//! first-class decisions, not booleans.
//!
//! What we deliberately do *not* inherit: nanoharness's broker is a
//! synchronous call, which in a headless daemon freezes the loop until the
//! human answers — here `request` is `async` and takes the run's
//! `CancellationToken`, and the contract requires it to resolve `Cancelled`
//! when the run is cancelled mid-ask. An approval that arrives one second
//! after the user cancelled the run must not restart the loop.
//!
//! Where harness-rs and pi stand on this: permission in pi is an
//! afterToolCall-shaped `beforeToolCall` hook returning `{block, terminate}`
//! — no context injection, no broker separation; harness-rs routes
//! permissions through its generic `Hook` system
//! (`crates/harness-core/src/permissions.rs:154`), which means a policy has
//! to impersonate a hook to see `PreToolUse`. The dedicated trait is the
//! seam a daemon layers policy *chains* onto (config file + runtime rules +
//! session overrides).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::tool::{ToolError, ToolRequest, ToolResult};

/// Which side of a tool execution the policy is judging. One trait method
/// covers both, so a single implementor sees the whole decision history
/// (nanoharness's `PolicyStage`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyStage {
    BeforeTool,
    AfterTool,
}

/// What a policy said about a tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub outcome: PolicyOutcome,
    /// Why — machine-readable short reason, surfaced to the model on denial.
    pub reason: String,
    /// Which policy decided (a chain records the winner).
    pub source: String,
    /// Injected into the context as a system block *before* the tool runs.
    /// A guardrail reminder or a task-specific constraint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_injection: Option<String>,
    /// Appended to the observation after the tool ran — an output redaction
    /// notice, a diff summary, anything a post-hoc analysis wants the model
    /// to see alongside the raw output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_suffix: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyOutcome {
    Allow,
    Deny,
    RequireApproval,
}

/// The state of the tool call the after-stage judges. Mirrors nanoharness's
/// `ToolExecution` (`core/schema.py:80`): the statuses a host needs to tell
/// apart (denied and blocked are different facts, cancelled is never an
/// error).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecution {
    pub status: ExecutionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ToolResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Success,
    Error,
    Denied,
    Blocked,
    Cancelled,
}

/// A tool policy: decide before and after each tool call.
///
/// `decide` is sync and cheap — it runs in the loop's dispatch path. Needing
/// the network or the model to judge a call is a sign the decision belongs
/// in an approval broker instead.
pub trait ToolPolicy: Send + Sync + 'static {
    fn decide(
        &self,
        stage: PolicyStage,
        req: &ToolRequest,
        execution: Option<&ToolExecution>,
    ) -> PolicyDecision;
}

/// What an approval broker answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ApprovalResult {
    Approved,
    Denied {
        reason: String,
    },
    /// The run was cancelled while the question was open. Distinct from
    /// `Denied`: the caller is ending the run, and a later approval arriving
    /// over a transport must not resurrect it.
    Cancelled,
}

/// Ask a human to decide a call the policy deferred.
///
/// The contract, mandatory for every implementation:
/// - `request` must observe `cancel` and return [`ApprovalResult::Cancelled`]
///   the moment the run is cancelled, even mid-ask;
/// - it must not poll the run's state — cancellation arrives through the
///   token it was handed, the one channel that cannot race.
///
/// Implementations live wherever the transport does (a broker that relays
/// the question over SSE/WebSocket and races the answer against the cancel
/// token); core defines the seam so the loop never blocks on a human.
#[async_trait]
pub trait ApprovalBroker: Send + Sync + 'static {
    async fn request(
        &self,
        req: &ToolRequest,
        decision: &PolicyDecision,
        cancel: &CancellationToken,
    ) -> ApprovalResult;
}

/// Convenience: the default deny when no broker is configured and policy
/// said `RequireApproval`. Failing closed is the only safe default — the
/// alternative is running a call nobody authorized.
pub fn unattended_denied(req: &ToolRequest) -> ToolError {
    ToolError::Permission(format!(
        "tool `{}` requires approval but no broker is configured",
        req.name
    ))
}
