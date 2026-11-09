//! The tool contract: one async `Tool` trait, risk classification, and the
//! value-shaped result.
//!
//! Design provenance: harness-rs contributes the async trait shape and
//! `ToolRisk` (`crates/harness-core/src/tool.rs`) — risk is MCP-aligned
//! (`readOnlyHint` / `destructiveHint` / `idempotentHint`) and drives
//! sandbox and permission decisions. pi contributes the failure semantics
//! (`packages/agent/src/types.ts:478-511`): results carry `is_error` and
//! survive intact for non-LLM consumers — failures are values, never
//! exceptions. nanoharness's `ToolExecutorProtocol` boundary-comment
//! ("may apply a sandbox or remote worker") is why `invoke` is `async` and
//! receives the cancellation token: a remote or isolated worker needs both.
//!
//! Arguments stay `serde_json::Value` on purpose: tools discovered at
//! runtime (MCP servers, third-party plugins) cannot be typed at compile
//! time, and validation belongs to the schema at registration, not to the
//! trait.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::run::RunId;

/// Risk class of a tool — drives sandbox and permission decisions. Names are
/// MCP-aligned (readOnlyHint, destructiveHint, idempotentHint), with
/// `Network` as an independent axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum ToolRisk {
    /// No side effects.
    ReadOnly,
    /// Side effects exist but repeating is safe (e.g. `cargo fmt`).
    Idempotent,
    /// Modifies state in a way that may not be safely repeated.
    Destructive,
    /// Talks to the network. Independent of read/write risk.
    Network,
}

/// API-facing definition of a tool: name, description, and the JSON Schema
/// for its input — the shape the model sees and registry validates against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSchema {
    pub name: String,
    pub description: String,
    /// JSON Schema of the tool's input.
    pub input: serde_json::Value,
}

/// One tool execution's outcome. Model-facing content plus `is_error` (the
/// value-shaped failure) and an optional `trace` sized for logs and replay —
/// not necessarily for the model (harness-rs's split).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResult {
    /// Model-facing output text.
    pub content: String,
    /// True when the tool failed — the model sees the failure as text, not
    /// a missing answer.
    #[serde(default)]
    pub is_error: bool,
    /// Output sized for `tracing` / replay logs but not the model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
}

/// Why a tool call did not produce a result. Errors are values here too:
/// the loop backfills a failed execution into the transcript as a
/// `ToolResult` with `is_error`; these variants are for the dispatch layer
/// and the host, not for the model.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ToolError {
    /// Dispatched to a name nothing registered. The hint names the nearest
    /// registered tool — it goes back to the model so a typo costs one
    /// round trip, not a dead turn (harness-rs's `NotFound{hint}`).
    #[error("tool `{name}` not found{hint}")]
    NotFound { name: String, hint: String },
    /// Arguments failed schema validation.
    #[error("invalid args for `{name}`: {reason}")]
    InvalidArgs { name: String, reason: String },
    /// Execution failed with a tool-specific reason.
    #[error("execution failed: {0}")]
    Exec(String),
    /// A policy refused the call before execution.
    #[error("permission denied: {0}")]
    Permission(String),
    /// The run's cancellation fired while the tool was in flight.
    #[error("tool call cancelled")]
    Cancelled,
}

/// Run-aware envelope for everything the policy/approval seams see — the
/// shape nanoharness calls `ToolRequest` (`core/schema.py:92`): the call and
/// the run/session it belongs to, so a policy can decide per-run and
/// per-tool without extra context objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRequest {
    pub call_id: String,
    pub name: String,
    pub args: serde_json::Value,
    pub run_id: RunId,
    pub session_id: crate::run::SessionId,
    /// Which turn of the run issued the call (0-based).
    #[serde(default)]
    pub turn: u32,
}

/// One model-callable tool.
///
/// `invoke` is async (remote/sandboxed workers — nanoharness's executor
/// boundary), races the passed `CancellationToken`, and returns either a
/// value-shaped result or a dispatch-level error. `risk` lets a policy
/// decide without running anything; `schema` is what the registry advertises
/// to the model.
#[async_trait]
pub trait Tool: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn schema(&self) -> ToolSchema;
    fn risk(&self) -> ToolRisk;
    async fn invoke(
        &self,
        args: serde_json::Value,
        cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError>;
}
