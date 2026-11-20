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
use sha2::{Digest, Sha256};

use crate::SessionStoreError;

/// The branch new units are appended to. Branching arrives with the fork
/// feature; every session starts on this one.
pub const MAIN_BRANCH: &str = "main";
/// The epoch new units are appended to. Rewinds advance it; a fresh session
/// starts here.
pub const INITIAL_EPOCH: i64 = 1;

/// The classification a unit row carries.
///
/// Core's `MessageKind` under its storage-side name: the taxonomy is
/// defined once, next to `Message`, so a new message variant and its
/// storage kind stay in step.
pub use harness_core::message::MessageKind as UnitKind;

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
    /// The run this unit came from, when it was produced inside one.
    pub run_id: Option<String>,
    /// The turn this unit belongs to, when the run reported one.
    /// Who produced the content: `user`, `model`, `tool`, or `system`.
    pub source_kind: String,
    /// For `tool_result` rows: the tool call this result answers.
    pub tool_call_id: Option<String>,
    /// Position-derived idempotency key (run + message + block index).
    /// `run_id:message_seq:block` for run-tagged appends; `None` when the
    /// append carries no run provenance (dedupe is run-scoped by design).
    pub idempotency_key: Option<String>,
    /// sha256 over the fixed index+content field list (excluding the parent
    /// edge — ferrite's known P1 limitation).
    pub integrity_hash: String,
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
pub struct HeadState {
    pub session_id: SessionId,
    pub branch_id: String,
    pub epoch: i64,
    pub head_event_id: Option<String>,
    pub last_event_order: u64,
    pub last_message_seq: u64,
}

/// The unit id for one append slot.
pub fn event_id(session_id: &SessionId, branch_id: &str, epoch: i64, order: u64) -> String {
    format!("{}:{branch_id}:{epoch}:{order}", session_id.0)
}

/// Split one message into ledger units, chained in order.
///
/// The head supplies the scope, the next message sequence, the first free
/// `event_order`, and the parent the first block continues. `run_id` records
/// provenance when the append happens inside a run; `kind_override` labels a
/// whole message as one `steering` unit instead of its default split. Each
/// returned payload is the JSON stored in `payload_json`.
pub fn split_message(
    head: &HeadState,
    run_id: Option<&str>,
    kind_override: Option<UnitKind>,
    message: &Message,
    now_ms: i64,
) -> Result<Vec<(Unit, String)>, SessionStoreError> {
    let mut blocks = message_blocks(message)?;
    if let Some(UnitKind::Steering) = kind_override {
        // A steering unit carries the whole user content: it is one decision,
        // not a conversation turn.
        blocks = vec![(
            UnitKind::Steering,
            "user",
            "user",
            serde_json::to_string(&message_user_content(message)?)
                .map_err(SessionStoreError::Serialization)?,
        )];
    }
    let message_seq = head.last_message_seq + 1;
    let start_order = head.last_event_order + 1;
    let mut units = Vec::with_capacity(blocks.len());
    let mut parent = head.head_event_id.clone();
    for (index, (kind, role, source_kind, payload)) in blocks.into_iter().enumerate() {
        let order = start_order + index as u64;
        let id = event_id(&head.session_id, &head.branch_id, head.epoch, order);
        let mut unit = Unit {
            event_id: id.clone(),
            session_id: head.session_id.clone(),
            branch_id: head.branch_id.clone(),
            epoch: head.epoch,
            event_order: order,
            message_seq,
            role: role.to_string(),
            kind,
            run_id: run_id.map(str::to_string),
            source_kind: source_kind.to_string(),
            tool_call_id: None,
            idempotency_key: run_id.map(|run| format!("{run}:m{message_seq}:b{index}")),
            integrity_hash: String::new(),
            parent_event_id: parent.clone(),
            created_at_ms: now_ms,
        };
        if let UnitKind::ToolCall = kind {
            let call: ToolCall =
                serde_json::from_str(&payload).map_err(SessionStoreError::Serialization)?;
            unit.tool_call_id = Some(call.id);
        }
        unit.integrity_hash = integrity_hash(&unit, &payload)?;
        units.push((unit, payload));
        parent = Some(id);
    }
    Ok(units)
}

/// The user content of a message, for whole-message unit overrides.
fn message_user_content(message: &Message) -> Result<Vec<Content>, SessionStoreError> {
    match message {
        Message::User { content } => Ok(content.clone()),
        other => Err(SessionStoreError::Internal(format!(
            "steering override requires a user message, got {other:?}"
        ))),
    }
}

/// sha256 over the fixed index+content field list, `U+001F`-joined. The
/// parent edge is excluded — ferrite's known P1 limitation.
pub fn integrity_hash(unit: &Unit, payload: &str) -> Result<String, SessionStoreError> {
    let mut hasher = Sha256::new();
    for field in [
        unit.session_id.0.as_str(),
        unit.branch_id.as_str(),
        &unit.epoch.to_string(),
        &unit.event_order.to_string(),
        &unit.message_seq.to_string(),
        unit.role.as_str(),
        unit.kind.as_str(),
        payload,
    ] {
        hasher.update(field.as_bytes());
        hasher.update([0x1f]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// The blocks one message contributes, each with its payload JSON.
fn message_blocks(
    message: &Message,
) -> Result<Vec<(UnitKind, &'static str, &'static str, String)>, SessionStoreError> {
    match message {
        Message::System { sections } => Ok(vec![(
            UnitKind::System,
            "system",
            "system",
            serde_json::to_string(sections).map_err(SessionStoreError::Serialization)?,
        )]),
        Message::User { content } => Ok(vec![(
            UnitKind::Prompt,
            "user",
            "user",
            serde_json::to_string(content).map_err(SessionStoreError::Serialization)?,
        )]),
        Message::Assistant { content } => assistant_blocks(content),
        Message::ToolResult(result) => Ok(vec![(
            UnitKind::ToolResult,
            "tool",
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
/// unit so its existence and its message sequence survive. Block kinds come
/// from `Content::kind` (core): total, so the taxonomy cannot drift from
/// the message model.
fn assistant_blocks(
    content: &[Content],
) -> Result<Vec<(UnitKind, &'static str, &'static str, String)>, SessionStoreError> {
    if content.is_empty() {
        return Ok(vec![(
            UnitKind::Assistant,
            "assistant",
            "model",
            "null".to_string(),
        )]);
    }
    content
        .iter()
        .map(|block| {
            // A tool-call unit's payload is the call itself — the store
            // indexes `tool_call_id` out of it; every other block
            // serializes whole.
            let payload = match block {
                Content::ToolCall(call) => serde_json::to_string(call),
                _ => serde_json::to_string(block),
            }
            .map_err(SessionStoreError::Serialization)?;
            Ok((block.kind(), "assistant", "model", payload))
        })
        .collect()
}

/// One persisted message with its assigned sequence number.
///
/// `seq` is monotonically increasing per session, assigned by the store on
/// append. The consumer uses seq to verify the order and integrity of the
/// ledger during recovery. Defined beside [`assemble_messages`], the
/// function that produces it.
#[derive(Debug, Clone)]
pub struct MessageEntry {
    /// Monotonic sequence number within the session.
    pub seq: u64,
    /// The stored message.
    pub message: Message,
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
        // A steering unit replays as a user turn: the model treats it like
        // any other instruction; the ledger keeps the injection point visible.
        UnitKind::Steering => Message::User {
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
