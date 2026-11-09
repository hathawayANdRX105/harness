//! The message and content model the whole harness speaks: pi's
//! `Message`/content-block design (`packages/ai/src/types.ts:430-665`),
//! with harness-rs's naming and Rust shapes.
//!
//! The deliberate difference from a "system prompt + history list" design
//! (harness-rs's `Context{system,guides,history}`): the system prompt is a
//! **ledger**. A `Message::System` carries `sections`, a named map, and
//! replaying the transcript's system messages reconstructs the current prompt
//! — which is how pi hot-swaps prompt sections mid-session
//! (`ai/src/types.ts:559-585`), and what a daemon needs to resume a session
//! from storage without a second "session config" object. A section set to
//! `None` removes it from the prompt (pi's `null` semantics); tool-set
//! mutations are intentionally not modeled here — the toolset is owned by
//! the loop (#8) and the builder (#10), not the transcript.
//!
//! Multimodal blocks beyond text are deferred (pi ships image content;
//! harness-rs also carries audio). They join `Content` when the loop adopts
//! them — the enum is `#[non_exhaustive]` for exactly that reason.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Who authored a message. Tool results are their own message kind, not a
/// role — a `ToolResult` always answers one `Content::ToolCall`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
}

/// One content block within a message — pi's content-block model: a message
/// is a vector of blocks, and tool calls live *inside* the assistant message
/// that requested them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Content {
    /// Plain text.
    Text(String),
    /// Provider-specific reasoning trace (DeepSeek `reasoning_content`,
    /// Anthropic thinking blocks). Must be echoed back to the provider on
    /// subsequent calls or the API rejects the request — pin it in the
    /// transcript like harness-rs does, not as a side channel.
    Reasoning(String),
    /// A tool call the assistant requested, parsed from the model stream.
    ToolCall(ToolCall),
}

/// A parsed tool call from the model stream: the id the provider assigned
/// (answers arrive keyed on it), the tool name, and the raw arguments.
/// Shared by [`crate::model::ModelEvent`] and the tool-execution pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub args: serde_json::Value,
}

/// One message in the transcript. The transcript is the source of truth for
/// everything downstream: model requests, compaction, session persistence
/// and event replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Message {
    /// The prompt ledger: a named map of sections. A later `System` message
    /// supersedes an earlier one section by section: a `Some` value replaces
    /// the section, a `None` removes it (pi's `null` semantics — a guardrail
    /// lifted mid-session is a removal, not an empty bullet). Replay of the
    /// transcript's system messages reconstructs the prompt — no separate
    /// session-config object.
    System {
        sections: BTreeMap<String, Option<String>>,
    },
    /// User input, possibly multimodal (steering messages arrive this way).
    User { content: Vec<Content> },
    /// Assistant output: text, reasoning, and the tool calls it requested.
    Assistant { content: Vec<Content> },
    /// The answer to one `Content::ToolCall`, matched by `tool_call_id`.
    /// `is_error` marks a failed tool: failures are **values** (pi's
    /// `AgentToolResult`), surfaced to the model as text, not unwound.
    ToolResult(ToolResultMessage),
}

impl Message {
    /// Convenience: a user text message (the common case for prompts,
    /// steering, and tests).
    pub fn user_text(text: impl Into<String>) -> Self {
        Message::User {
            content: vec![Content::Text(text.into())],
        }
    }

    /// The role that authored this message, for adapters that need it.
    pub fn role(&self) -> Role {
        match self {
            Message::System { .. } => Role::System,
            Message::User { .. } => Role::User,
            Message::Assistant { .. } => Role::Assistant,
            Message::ToolResult(_) => Role::Assistant,
        }
    }
}

/// The answer to one tool call, as its own message kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResultMessage {
    /// Matches `ToolCall::id` — this is the correlation the provider needs.
    pub tool_call_id: String,
    pub tool_name: String,
    /// Model-facing output text. Adapters render `is_error` into it.
    pub content: String,
    /// True when the tool failed — the value-shaped failure contract.
    #[serde(default)]
    pub is_error: bool,
}

/// Message-level reason a model turn stopped. Caller-level cancellation is
/// not here: dropping the stream ends iteration; cancelled runs surface as
/// [`crate::run::RunStopReason::Cancelled`] at the loop level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StopReason {
    /// The model finished its reply.
    EndTurn,
    /// The model stopped to run the tools it requested.
    ToolUse,
    /// Token cap reached mid-generation.
    MaxTokens,
}
