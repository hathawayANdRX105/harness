//! The session unit ledger: one row per conversational block, with the index
//! (ids, scope, ordering, parent) and the content (`payload_json`) in the same
//! Turso row.
//!
//! Design provenance: ferrite's `context_events` ledger
//! (`session/src/context_events.rs`) and its petgraph shadow
//! (`session/src/graph_projector.rs`). The message model of `harness-core`
//! carries blocks inside one `Message`; the ledger splits those blocks because
//! a tool call is its own unit and a graph node needs a stable id.
//!
//! # Splitting and assembling
//!
//! [`split_message`] turns one [`Message`] into one or more units:
//!
//! | Message | Units |
//! |---|---|
//! | `System { sections }` | one `system` unit carrying the section map |
//! | `User { content }` | one `prompt` unit carrying the content blocks |
//! | `Assistant { content }` | one `assistant` unit per text/reasoning block, one `tool_call` unit per call |
//! | `ToolResult` | one `tool_result` unit |
//!
//! [`assemble_messages`] reverses that: consecutive units with the same
//! `message_seq` become one message, assistant and tool-call blocks merge back
//! into a single `Assistant { content }`, and `message_seq` becomes the
//! [`MessageEntry`] sequence the store has always returned.
//!
//! The graph keeps only [`Unit`] (the index fields); payloads stay in Turso
//! and are read back by event id, exactly like ferrite's unit/graph split.

use harness_core::message::{Content, Message, ToolCall, ToolResultMessage};
use harness_core::run::SessionId;

use crate::store::{MessageEntry, SessionStoreError};

/// The branch new units are appended to. Branching arrives with the fork
/// feature; every session starts on this one.
pub(crate) const MAIN_BRANCH: &str = "main";
/// The epoch new units are appended to. Rewinds advance it; a fresh session
/// starts here.
pub(crate) const INITIAL_EPOCH: i64 = 1;

/// What one ledger row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitKind {
    /// A user prompt (or a steering message replayed as one).
    Prompt,
    /// A text or reasoning block of an assistant message.
    Assistant,
    /// A tool call block of an assistant message.
    ToolCall,
    /// A tool result message.
    ToolResult,
    /// A prompt-ledger section delta.
    System,
}

impl UnitKind {
    /// The stable wire/database string.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Assistant => "assistant",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::System => "system",
        }
    }

    /// Parse the database string.
    pub fn parse(value: &str) -> Result<Self, SessionStoreError> {
        match value {
            "prompt" => Ok(Self::Prompt),
            "assistant" => Ok(Self::Assistant),
            "tool_call" => Ok(Self::ToolCall),
            "tool_result" => Ok(Self::ToolResult),
            "system" => Ok(Self::System),
            other => Err(SessionStoreError::Internal(format!(
                "unknown unit kind {other:?}"
            ))),
        }
    }
}

/// One ledger row's index fields: exactly what the graph needs.
///
/// Content never enters the graph; [`StoredUnit`] carries it alongside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unit {
    /// Stable unit id: `<session>:<branch>:<epoch>:<event_order>`.
    pub event_id: String,
    /// Session that owns the unit.
    pub session_id: SessionId,
    /// Branch the unit was appended on.
    pub branch_id: String,
    /// Branch epoch the unit belongs to.
    pub epoch: i64,
    /// Append order inside the session.
    pub event_order: u64,
    /// The message this unit belongs to; the store's message sequence.
    pub message_seq: u64,
    /// Message role (`user`, `assistant`, `tool`, `system`).
    pub role: String,
    /// What this row is.
    pub kind: UnitKind,
    /// The unit this one continues, or `None` at the root of the scope.
    pub parent_event_id: Option<String>,
    /// Creation timestamp.
    pub created_at_ms: i64,
}

/// One ledger row: index plus the content payload.
#[derive(Debug, Clone)]
pub struct StoredUnit {
    pub unit: Unit,
    pub payload_json: String,
}

/// The scope a new append lands in, read from `session_heads`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HeadState {
    pub session_id: SessionId,
    pub branch_id: String,
    pub epoch: i64,
    pub head_event_id: Option<String>,
    pub last_event_order: u64,
    pub last_message_seq: u64,
}

/// The unit id for one append slot.
pub(crate) fn event_id(session_id: &SessionId, branch_id: &str, epoch: i64, order: u64) -> String {
    format!("{}:{branch_id}:{epoch}:{order}", session_id.0)
}

/// Split one message into ledger units, chained in order.
///
/// The head supplies the scope, the next message sequence, the first free
/// `event_order`, and the parent the first block continues. Each returned
/// payload is the JSON stored in `payload_json`.
pub(crate) fn split_message(
    head: &HeadState,
    message: &Message,
    now_ms: i64,
) -> Result<Vec<(Unit, String)>, SessionStoreError> {
    let blocks = message_blocks(message)?;
    let message_seq = head.last_message_seq + 1;
    let start_order = head.last_event_order + 1;
    let mut units = Vec::with_capacity(blocks.len());
    let mut parent = head.head_event_id.clone();
    for (index, (kind, role, payload)) in blocks.into_iter().enumerate() {
        let order = start_order + index as u64;
        let id = event_id(&head.session_id, &head.branch_id, head.epoch, order);
        units.push((
            Unit {
                event_id: id.clone(),
                session_id: head.session_id.clone(),
                branch_id: head.branch_id.clone(),
                epoch: head.epoch,
                event_order: order,
                message_seq,
                role: role.to_string(),
                kind,
                parent_event_id: parent.clone(),
                created_at_ms: now_ms,
            },
            payload,
        ));
        parent = Some(id);
    }
    Ok(units)
}

/// The blocks one message contributes, each with its payload JSON.
fn message_blocks(
    message: &Message,
) -> Result<Vec<(UnitKind, &'static str, String)>, SessionStoreError> {
    match message {
        Message::System { sections } => Ok(vec![(
            UnitKind::System,
            "system",
            serde_json::to_string(sections).map_err(SessionStoreError::Serialization)?,
        )]),
        Message::User { content } => Ok(vec![(
            UnitKind::Prompt,
            "user",
            serde_json::to_string(content).map_err(SessionStoreError::Serialization)?,
        )]),
        Message::Assistant { content } => assistant_blocks(content),
        Message::ToolResult(result) => Ok(vec![(
            UnitKind::ToolResult,
            "tool",
            serde_json::to_string(result).map_err(SessionStoreError::Serialization)?,
        )]),
        // `Message` is `#[non_exhaustive]`: a new variant needs an explicit
        // payload mapping before it can be persisted.
        _ => Err(SessionStoreError::Internal(
            "unsupported message variant for the unit ledger".into(),
        )),
    }
}

/// One unit per assistant content block; an empty message keeps one empty
/// unit so its existence and its message sequence survive.
fn assistant_blocks(
    content: &[Content],
) -> Result<Vec<(UnitKind, &'static str, String)>, SessionStoreError> {
    if content.is_empty() {
        return Ok(vec![(UnitKind::Assistant, "assistant", "null".to_string())]);
    }
    content
        .iter()
        .map(|block| match block {
            Content::ToolCall(call) => Ok((
                UnitKind::ToolCall,
                "assistant",
                serde_json::to_string(call).map_err(SessionStoreError::Serialization)?,
            )),
            Content::Text(_) | Content::Reasoning(_) => Ok((
                UnitKind::Assistant,
                "assistant",
                serde_json::to_string(block).map_err(SessionStoreError::Serialization)?,
            )),
            // `Content` is `#[non_exhaustive]`; a new block kind must be
            // mapped explicitly before it can be persisted.
            _ => Err(SessionStoreError::Internal(
                "unsupported content variant for the unit ledger".into(),
            )),
        })
        .collect()
}

/// Assemble ordered ledger rows back into messages.
///
/// Rows must be in `event_order` order. Consecutive rows with the same
/// `message_seq` form one message; assistant and tool-call rows merge into one
/// `Assistant { content }` preserving block order.
pub fn assemble_messages(units: &[StoredUnit]) -> Result<Vec<MessageEntry>, SessionStoreError> {
    let mut entries = Vec::new();
    let mut group: Vec<&StoredUnit> = Vec::new();
    for stored in units {
        if group
            .last()
            .is_some_and(|last| last.unit.message_seq != stored.unit.message_seq)
        {
            entries.push(assemble_group(&group)?);
            group.clear();
        }
        group.push(stored);
    }
    if !group.is_empty() {
        entries.push(assemble_group(&group)?);
    }
    Ok(entries)
}

/// Assemble one message's rows.
fn assemble_group(group: &[&StoredUnit]) -> Result<MessageEntry, SessionStoreError> {
    let first = group
        .first()
        .ok_or_else(|| SessionStoreError::Internal("empty unit group".into()))?;
    let message = match first.unit.kind {
        UnitKind::System => Message::System {
            sections: decode(&first.payload_json)?,
        },
        UnitKind::Prompt => Message::User {
            content: decode(&first.payload_json)?,
        },
        UnitKind::ToolResult => {
            Message::ToolResult(decode::<ToolResultMessage>(&first.payload_json)?)
        }
        UnitKind::Assistant | UnitKind::ToolCall => {
            let mut content = Vec::new();
            for stored in group {
                match stored.unit.kind {
                    UnitKind::Assistant => {
                        let payload: Option<Content> = decode(&stored.payload_json)?;
                        if let Some(block) = payload {
                            content.push(block);
                        }
                    }
                    UnitKind::ToolCall => {
                        content.push(Content::ToolCall(decode::<ToolCall>(&stored.payload_json)?));
                    }
                    other => {
                        return Err(SessionStoreError::Internal(format!(
                            "message {} mixes assistant blocks with {}",
                            first.unit.message_seq,
                            other.as_str()
                        )));
                    }
                }
            }
            Message::Assistant { content }
        }
    };
    Ok(MessageEntry {
        seq: first.unit.message_seq,
        message,
    })
}

/// Decode one JSON payload.
fn decode<T: serde::de::DeserializeOwned>(json: &str) -> Result<T, SessionStoreError> {
    serde_json::from_str(json).map_err(SessionStoreError::Serialization)
}
