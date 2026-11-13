//! The `SessionStore` port trait: the persistence seam for session data.
//!
//! Design provenance: pi's `durable/src/harness/` committed-ledger pattern
//! (every mutation is append-only, the ledger is the source of truth for
//! recovery) combined with NanoHarness's `RunContext`/`RunCheckpoint` split
//! (`core/schema.py:193`): identity is immutable and carried by core types;
//! the store holds only the *ledger* and the *event log* that can be replayed
//! after a crash. There is no second "session config" object — replaying the
//! ledger reconstructs the prompt (see [`replay_ledger`]).
//!
//! Two persistent objects live here:
//! - **Message ledger** (`Vec<Message>`): system sections carry `None` removal
//!   semantics (later `System` messages supersede earlier ones per-section).
//!   Recovery replays every `System` message to rebuild the prompt + transcript.
//! - **RunEvent log**: ordered by `seq`. Recovery detects gaps and can
//!   re-issue missing events. Every event carries `run_id`, `session_id`,
//!   `seq`, and a timestamp.
//!
//! # Hard constraint
//!
//! Implementations must be `Send + Sync + 'static` so they can be shared
//! across task boundaries. All port methods are `async` — the daemon pump
//! awaits them directly; no `spawn_blocking` wrappers are needed.

use async_trait::async_trait;
use harness_core::event::RunEvent;
use harness_core::message::Message;
use harness_core::run::{RunId, SessionId};

/// Errors from session storage operations.
#[derive(Debug, thiserror::Error)]
pub enum SessionStoreError {
    /// The named session does not exist.
    #[error("session {0} not found")]
    SessionNotFound(SessionId),

    /// A session with this id already exists.
    #[error("session {0} already exists")]
    SessionAlreadyExists(SessionId),

    /// The underlying storage backend reported an error.
    #[error("storage backend error: {0}")]
    Storage(String),

    /// Message or event serialization to/from JSON failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// The run already carries an event with this seq (append-only log:
    /// re-applying an already-persisted event is a no-op, not an error).
    #[error("run {0} already has an event at seq {1}")]
    DuplicateSeq(RunId, u64),

    /// An internal invariant was violated (e.g. corrupt data).
    #[error("internal error: {0}")]
    Internal(String),
}

/// One persisted message with its assigned sequence number.
///
/// `seq` is monotonically increasing per session, assigned by the store on
/// append. The consumer uses seq to verify the order and integrity of the
/// ledger during recovery.
#[derive(Debug, Clone)]
pub struct MessageEntry {
    /// Monotonic sequence number within the session.
    pub seq: u64,
    /// The stored message.
    pub message: Message,
}

/// The storage port for session persistence.
///
/// # Design rationale
///
/// The trait is deliberately narrow: it exposes only the append-and-replay
/// operations the scheduler (#11) and the loop need. There is no update or
/// delete — the message ledger and event log are append-only. Compaction
/// (summarising old turns) is a separate concern that will land on top of
/// this trait when the compaction vocabulary (#5) ships.
///
/// Every method returns `Result` with [`SessionStoreError`]. Storage
/// backends are expected to be reachable; a missing database is a hard
/// error, not a degraded-path skip.
#[async_trait]
pub trait SessionStore: Send + Sync + 'static {
    /// Create a new, empty session.
    ///
    /// Returns [`SessionStoreError::SessionAlreadyExists`] when a session
    /// with `id` already exists.
    async fn create_session(&self, id: &SessionId) -> Result<(), SessionStoreError>;

    /// Return `true` when a session with this id exists.
    async fn session_exists(&self, id: &SessionId) -> Result<bool, SessionStoreError>;

    /// Append a message to the session's transcript ledger.
    ///
    /// The store assigns the next monotonic sequence number. Returns the
    /// assigned seq.
    async fn append_message(
        &self,
        session_id: &SessionId,
        message: &Message,
    ) -> Result<u64, SessionStoreError>;

    /// Load every message for a session, ordered by sequence number.
    async fn load_messages(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<MessageEntry>, SessionStoreError>;

    /// Replay the message ledger to reconstruct the full transcript.
    ///
    /// System messages are accumulated section by section: a later `System`
    /// message supersedes an earlier one per-section. A section set to
    /// `None` removes it from the prompt (pi's `null` semantics — a
    /// guardrail lifted mid-session is a removal, not an empty bullet).
    ///
    /// The returned vector starts with the accumulated `System` message (if
    /// any system messages were recorded) followed by all non-system
    /// messages (User, Assistant, ToolResult) in playback order. This is
    /// the complete transcript ready to be sent to the model.
    async fn replay_ledger(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<Message>, SessionStoreError>;

    /// Append an event to a run's event log.
    ///
    /// The event's `seq` is assigned by the emitter (the loop or scheduler),
    /// not by the store. The store asserts that the seq is new per run.
    /// Re-applying an event whose `idempotency_key` (run_id + seq) is already
    /// present is a no-op: the stored row is returned and nothing is written.
    async fn append_event(&self, event: &RunEvent) -> Result<(), SessionStoreError>;

    /// Load every event for a run, ordered by sequence number.
    async fn events_for_run(&self, run_id: &RunId) -> Result<Vec<RunEvent>, SessionStoreError>;

    /// Find gaps in a run's event sequence.
    ///
    /// Returns the missing sequence numbers (contiguous gaps in `1..=max`).
    /// Returns an empty vec when the sequence is contiguous or when there
    /// are no events for the run.
    async fn check_event_gaps(&self, run_id: &RunId) -> Result<Vec<u64>, SessionStoreError>;

    /// Return the ids of every run that has at least one persisted event,
    /// latest run first (by the most recent event's row id).
    ///
    /// There is no separate runs table: runs are derivable from the event
    /// log (`DISTINCT run_id` over `run_events`), which is what makes the
    /// daemon's restart hydration possible without a second schema object.
    async fn run_ids(&self) -> Result<Vec<RunId>, SessionStoreError>;
    /// Return the most recent run for `session_id`, or `None` when the
    /// session has no runs.
    ///
    /// "Most recent" is the run whose latest event carries the highest
    /// `seq` — the event log is append-only, so the highest seq is the
    /// latest write. Returns `None` when the session has no runs (or is
    /// unknown entirely); neither can be resumed, which is exactly what the
    /// transport needs to answer a `session_id`-only `subscribe`.
    async fn latest_run_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<RunId>, SessionStoreError>;

    /// Delete a session and all its data (messages and events).
    async fn delete_session(&self, id: &SessionId) -> Result<(), SessionStoreError>;
}

/// Default replay implementation: accumulate system sections by replay order,
/// then return the collapsed system message plus all non-system messages.
///
/// This is the canonical ledger-replay algorithm. It lives here as a free
/// function so backends that cannot load-and-process in one shot (e.g. a
/// streaming cursor backend) can still share the logic.
///
/// # Panics
///
/// Panics only on programmer error (a corrupt message that deserialized
/// but violates the ledger invariant); production backends catch this at
/// the storage layer and return `SessionStoreError::Internal`.
pub fn replay_messages(messages: &[MessageEntry]) -> Vec<Message> {
    let mut sections: std::collections::BTreeMap<String, Option<String>> =
        std::collections::BTreeMap::new();
    let mut transcript: Vec<Message> = Vec::new();

    for entry in messages {
        match &entry.message {
            Message::System {
                sections: msg_sections,
            } => {
                // Apply section changes: Some replaces, None removes.
                for (key, value) in msg_sections {
                    sections.insert(key.clone(), value.clone());
                }
            }
            // A bare `_` keeps future `Message` variants flowing through the
            // replay (the enum is `#[non_exhaustive]`); the value comes from
            // `entry.message` because `_` binds nothing to clone.
            _ => {
                transcript.push(entry.message.clone());
            }
        }
    }

    // Build the front matter: a single System message with accumulated sections.
    if sections.is_empty() {
        return transcript;
    }

    let mut result = Vec::with_capacity(transcript.len() + 1);
    result.push(Message::System { sections });
    result.append(&mut transcript);
    result
}
