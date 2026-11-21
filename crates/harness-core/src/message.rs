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

impl Content {
    /// The storage/replay classification of this block: text and reasoning
    /// are assistant output, a tool call is its own unit kind.
    pub fn kind(&self) -> MessageKind {
        match self {
            Content::Text(_) | Content::Reasoning(_) => MessageKind::Assistant,
            Content::ToolCall(_) => MessageKind::ToolCall,
        }
    }
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

    /// The storage/replay classification of this message. `User` messages
    /// classify as [`MessageKind::Prompt`]; a steering injection is a
    /// store-side provenance override onto that kind, not a message
    /// variant. Assistant messages with mixed blocks still split per
    /// block at storage time — see [`Content::kind`].
    pub fn kind(&self) -> MessageKind {
        match self {
            Message::System { .. } => MessageKind::System,
            Message::User { .. } => MessageKind::Prompt,
            Message::Assistant { .. } => MessageKind::Assistant,
            Message::ToolResult(_) => MessageKind::ToolResult,
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

/// How a message is classified for storage, replay and provenance.
///
/// One taxonomy for the whole workspace: the session store splits ledger
/// messages into *units* labeled by this kind, so the classification lives
/// next to [`Message`] instead of being re-derived downstream. `Steering`
/// is the one kind that is not a pure projection — it marks a `User`
/// message injected mid-run (a store-side provenance override), so the
/// store applies it rather than deriving it from the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// A user prompt.
    Prompt,
    /// A steering message replayed as a user turn.
    Steering,
    /// An assistant text or reasoning block.
    Assistant,
    /// An assistant tool-call block.
    ToolCall,
    /// A tool result message.
    ToolResult,
    /// A prompt-ledger section delta.
    System,
}

impl MessageKind {
    /// The stable wire/database string.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Steering => "steering",
            Self::Assistant => "assistant",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::System => "system",
        }
    }

    /// Parse the wire/database string.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "prompt" => Ok(Self::Prompt),
            "steering" => Ok(Self::Steering),
            "assistant" => Ok(Self::Assistant),
            "tool_call" => Ok(Self::ToolCall),
            "tool_result" => Ok(Self::ToolResult),
            "system" => Ok(Self::System),
            other => Err(format!("unknown message kind {other:?}")),
        }
    }
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
