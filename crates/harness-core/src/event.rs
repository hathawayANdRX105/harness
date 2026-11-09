//! Events emitted by the agent loop, for transport/UI consumption.
//!
//! Per-LLM-round shape: `TurnStart`, streamed text deltas, tool dispatch
//! start/end, turn end. The serde shape is the **frozen cross-crate event
//! contract**: `{"type":"turn_start"}`,
//! `{"type":"assistant_text","delta":…}`, `{"type":"tool_call","id":…,
//! "name":…,"args":…}` (flattened `ToolCallSpec`), `{"type":"tool_start",…}`,
//! `{"type":"tool_result",…}`, `{"type":"turn_end","stop_reason":…}`.
//! Subscribers parse these strings, so variant renames are breaking changes.

use serde::{Deserialize, Serialize};

// A completed tool call extracted from the stream: the type is owned by
// `harness_core::tool` (the bottom of the vocabulary), the model stream
// re-exports it, and this event vocabulary re-exports *that* type rather than
// defining a second one — `AgentEvent::ToolCall` must carry exactly the spec
// the stream produced.
pub use crate::tool::ToolCallSpec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    /// One LLM round-trip begins (before the stream, after maintenance).
    TurnStart,
    AssistantText {
        delta: String,
    },
    AssistantReasoning {
        delta: String,
    },
    /// Tool call parsed from the stream (not yet executed).
    ToolCall(ToolCallSpec),
    /// Tool dispatch begins.
    ToolStart {
        id: String,
        name: String,
    },
    ToolResult {
        id: String,
        name: String,
        result: String,
    },
    TurnEnd {
        stop_reason: TurnStop,
    },
}

/// Loop-level stop reasons (`error`/`max_turns` added on top of the model's
/// stop set).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStop {
    EndTurn,
    MaxTokens,
    Aborted,
    Error,
    /// `LoopConfig::max_turns` LLM round-trips exhausted.
    MaxTurns,
    /// The request could not be made to fit: content the compaction region
    /// requires keeping is itself over budget, so no compression reaches it
    /// and **no provider call was made**.
    ///
    /// Its own stop reason rather than `Error` because the round did not fail
    /// at anything — it was refused before it started. A caller counting
    /// errors would otherwise bill a round that never ran.
    BudgetBlocked {
        required_chars: u64,
        available_chars: u64,
    },
}
