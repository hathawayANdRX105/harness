//! Pi-style config callbacks for the agent loop.
//!
//! Design provenance: pi's `AgentHooks` (`packages/agent/src/agent-loop.ts:44-68`)
//! contributes the callback shape -- `prepareRequest` transforms the transcript
//! before each model call, `finishTurn` observes the completed turn,
//! `shouldTerminateToolBatch` short-circuits after a tool batch. All hooks are
//! plain `Arc<dyn Fn>` pointers (no Hook/Guide/Sensor trait system -- core v2
//! deliberately avoids that abstraction).
//!
//! Every field is optional; unset hooks are no-ops. The callbacks are `Fn` (not
//! `FnMut`) so a single [`LoopHooks`] can be shared across many runs without
//! locking.

use std::sync::Arc;

use harness_core::message::{Message, ToolCall, ToolResultMessage};
use harness_core::tool::ToolRequest;

/// Type alias for the `prepare_request` hook.
pub(crate) type PrepareRequestHook = Arc<dyn Fn(Vec<Message>) -> Vec<Message> + Send + Sync>;
/// Type alias for the `finish_turn` hook.
pub(crate) type FinishTurnHook = Arc<dyn Fn(u32, &[Message]) + Send + Sync>;
/// Type alias for the `before_tool_call` hook.
pub(crate) type BeforeToolCallHook = Arc<dyn Fn(&ToolRequest) -> Result<(), String> + Send + Sync>;
/// Type alias for the `after_tool_call` hook.
pub(crate) type AfterToolCallHook = Arc<dyn Fn(&ToolRequest, &ToolResultMessage) + Send + Sync>;
/// Type alias for the `should_terminate_tool_batch` hook.
pub(crate) type ShouldTerminateHook =
    Arc<dyn Fn(&[ToolCall], &[ToolResultMessage]) -> bool + Send + Sync>;

/// How tools are dispatched within one turn.
///
/// Design provenance: pi's `executionMode` (`packages/agent/src/agent.ts:124`)
/// -- parallel is the default (all tool calls fly concurrently); sequential
/// runs them one at a time so each result feeds the next call in the same turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ToolExecutionMode {
    /// All tool calls in one turn run concurrently.
    #[default]
    Parallel,
    /// Tool calls run one after another, FIFO.
    Sequential,
}

/// Callbacks that extend the loop at specific expansion points.
///
/// Each field is optional; `Default` is all `None`. Build a struct with
/// `LoopHooks { before_tool_call: Some(handler), ..Default::default() }`
/// to wire only the hooks you need.
#[derive(Default, Clone)]
pub struct LoopHooks {
    /// Called with the current transcript **before** building the
    /// `ModelRequest`. Returning a different `Vec<Message>` lets the hook
    /// insert, remove or reorder messages -- for example a context-window
    /// compactor or a session-level instruction overlay.
    ///
    /// Pi name: `prepareRequest`.
    pub prepare_request: Option<PrepareRequestHook>,

    /// Called after a full turn (model response + tool batch, or just the
    /// model response when no tools were requested) completes. Receives the
    /// zero-based turn index and the current transcript.
    ///
    /// Pi name: `finishTurn`.
    pub finish_turn: Option<FinishTurnHook>,

    /// Called **before** each tool call. Return `Err(reason)` to block the
    /// call -- the reason is surfaced to the model as a `ToolResult` with
    /// `is_error: true`. A policy deny also blocks the call; this hook is an
    /// additional seam that does not replace the policy chain.
    ///
    /// Pi name: `beforeToolCall`.
    pub before_tool_call: Option<BeforeToolCallHook>,

    /// Called **after** each tool call completes. Receives the request and
    /// the result that will be pushed into the transcript. Useful for
    /// observability, logging, or side-effect recording.
    ///
    /// Pi name: `afterToolCall`.
    pub after_tool_call: Option<AfterToolCallHook>,

    /// Called after a batch of tool calls finishes. Return `true` to skip
    /// the next model turn and terminate the loop with `RunStopReason::Completed`.
    /// This is pi's `shouldTerminateToolBatch` -- the sandbox or the host uses
    /// it to stop early when the answer is already satisfactory.
    ///
    /// Note: termination via this hook is a clean `Completed`, not an error.
    pub should_terminate_tool_batch: Option<ShouldTerminateHook>,
}

impl std::fmt::Debug for LoopHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopHooks")
            .field(
                "prepare_request",
                &self.prepare_request.as_ref().map(|_| "..."),
            )
            .field("finish_turn", &self.finish_turn.as_ref().map(|_| "..."))
            .field(
                "before_tool_call",
                &self.before_tool_call.as_ref().map(|_| "..."),
            )
            .field(
                "after_tool_call",
                &self.after_tool_call.as_ref().map(|_| "..."),
            )
            .field(
                "should_terminate_tool_batch",
                &self.should_terminate_tool_batch.as_ref().map(|_| "..."),
            )
            .finish()
    }
}
