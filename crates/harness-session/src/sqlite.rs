//! SQLite-backed [`SessionStore`] implementation.
//!
//! Uses `rusqlite` with the `bundled` feature so no system SQLite is needed.
//! The connection is wrapped in `parking_lot::Mutex` for interior mutability;
//! each public method locks the connection for the duration of one transaction.
//!
//! # Schema
//!
//! Three tables plus indexes:
//!
//! ```sql
//! -- Session identity (one row per session).
//! CREATE TABLE sessions (
//!     session_id   TEXT PRIMARY KEY,
//!     created_at_ms INTEGER NOT NULL
//! );
//!
//! -- Message ledger: append-only, ordered by seq per session.
//! -- `None`-section removal is applied at replay time, not at storage time.
//! CREATE TABLE messages (
//!     id           INTEGER PRIMARY KEY AUTOINCREMENT,
//!     session_id   TEXT NOT NULL,
//!     seq          INTEGER NOT NULL,
//!     message_json TEXT NOT NULL,
//!     created_at_ms INTEGER NOT NULL,
//!     FOREIGN KEY (session_id) REFERENCES sessions(session_id),
//!     UNIQUE(session_id, seq)
//! );
//!
//! -- Run event log: append-only, ordered by seq per run.
//! -- seq is assigned by the emitter, not auto-incremented.
//! CREATE TABLE run_events (
//!     id           INTEGER PRIMARY KEY AUTOINCREMENT,
//!     run_id       TEXT NOT NULL,
//!     session_id   TEXT NOT NULL,
//!     seq          INTEGER NOT NULL,
//!     event_json   TEXT NOT NULL,
//!     timestamp_ms INTEGER NOT NULL,
//!     FOREIGN KEY (session_id) REFERENCES sessions(session_id),
//!     UNIQUE(run_id, seq)
//! );
//! ```
//!
//! # Recovery path
//!
//! 1. Load all `messages` for a session ordered by `seq`.
//! 2. Pass them to [`replay_messages`] which applies `None`-section
//!    removal and produces a full transcript.
//! 3. Load all `run_events` for a run ordered by `seq`.
//! 4. Call [`check_event_gaps`] to detect missing sequence numbers.
//!    Missing seqs mean events lost between the last flush and the crash;
//!    the consumer re-emits them.
//!
//! # Thread safety
//!
//! `parking_lot::Mutex` (fair, no-poison) guards a single `rusqlite::Connection`.
//! SQLite in WAL mode would allow concurrent reads, but v1 is non-concurrent
//! (one session at a time), so the mutex is sufficient.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use harness_core::event::RunEvent;
use harness_core::message::Message;
use harness_core::run::{RunId, SessionId};
use parking_lot::Mutex;
use rusqlite::{Connection, params};

use crate::store::{MessageEntry, SessionStore, SessionStoreError, replay_messages};

/// SQLite-backed [`SessionStore`].
pub struct SqliteSessionStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteSessionStore {
    /// Open or create a database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SessionStoreError> {
        let conn = Connection::open(path.as_ref())
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.initialize()?;
        Ok(store)
    }

    /// Open an in-memory database (useful for tests).
    pub fn in_memory() -> Result<Self, SessionStoreError> {
        let conn =
            Connection::open_in_memory().map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.initialize()?;
        Ok(store)
    }

    /// Create the schema tables and indexes if they do not exist.
    fn initialize(&self) -> Result<(), SessionStoreError> {
        let conn = self.conn.lock();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sessions (
                 session_id   TEXT PRIMARY KEY,
                 created_at_ms INTEGER NOT NULL
             );

             CREATE TABLE IF NOT EXISTS messages (
                 id            INTEGER PRIMARY KEY AUTOINCREMENT,
                 session_id    TEXT NOT NULL,
                 seq           INTEGER NOT NULL,
                 message_json  TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 FOREIGN KEY (session_id) REFERENCES sessions(session_id),
                 UNIQUE(session_id, seq)
             );

             CREATE TABLE IF NOT EXISTS run_events (
                 id           INTEGER PRIMARY KEY AUTOINCREMENT,
                 run_id       TEXT NOT NULL,
                 session_id   TEXT NOT NULL,
                 seq          INTEGER NOT NULL,
                 event_json   TEXT NOT NULL,
                 timestamp_ms INTEGER NOT NULL,
                 FOREIGN KEY (session_id) REFERENCES sessions(session_id),
                 UNIQUE(run_id, seq)
             );

             CREATE INDEX IF NOT EXISTS idx_messages_session_seq
                 ON messages(session_id, seq);

             CREATE INDEX IF NOT EXISTS idx_run_events_run_seq
                 ON run_events(run_id, seq);",
        )
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Current unix epoch millisecond timestamp.
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

impl SessionStore for SqliteSessionStore {
    fn create_session(&self, id: &SessionId) -> Result<(), SessionStoreError> {
        let conn = self.conn.lock();
        let now = Self::now_ms();
        let changed = conn
            .execute(
                "INSERT INTO sessions (session_id, created_at_ms) VALUES (?1, ?2)",
                params![id.0, now],
            )
            .map_err(|e| {
                if let rusqlite::Error::SqliteFailure(f, _) = &e
                    && f.code == rusqlite::ErrorCode::ConstraintViolation
                {
                    return SessionStoreError::SessionAlreadyExists(id.clone());
                }
                SessionStoreError::Storage(e.to_string())
            })?;
        if changed == 0 {
            return Err(SessionStoreError::Storage(
                "create_session inserted zero rows".into(),
            ));
        }
        Ok(())
    }

    fn session_exists(&self, id: &SessionId) -> Result<bool, SessionStoreError> {
        let conn = self.conn.lock();
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sessions WHERE session_id = ?1",
                params![id.0],
                |row| row.get(0),
            )
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        Ok(exists)
    }

    fn append_message(
        &self,
        session_id: &SessionId,
        message: &Message,
    ) -> Result<u64, SessionStoreError> {
        let mut conn = self.conn.lock();
        let now = Self::now_ms();

        // Assign the next seq and insert in one transaction: the seq read and
        // the insert must be atomic so concurrent appends cannot observe a gap
        // or hand out the same seq twice.
        let tx = conn
            .transaction()
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        // Assign next seq: max existing + 1, or 1 for first message.
        let next_seq: u64 = tx
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM messages WHERE session_id = ?1",
                params![session_id.0],
                |row| row.get(0),
            )
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        let message_json = serde_json::to_string(message)?;

        tx.execute(
            "INSERT INTO messages (session_id, seq, message_json, created_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![session_id.0, next_seq, message_json, now],
        )
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        tx.commit()
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        Ok(next_seq)
    }

    fn load_messages(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<MessageEntry>, SessionStoreError> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT seq, message_json FROM messages WHERE session_id = ?1 ORDER BY seq")
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        let rows = stmt
            .query_map(params![session_id.0], |row| {
                let seq: u64 = row.get(0)?;
                let json_str: String = row.get(1)?;
                Ok((seq, json_str))
            })
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        let mut entries = Vec::new();
        for row in rows {
            let (seq, json_str) = row.map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let message: Message =
                serde_json::from_str(&json_str).map_err(SessionStoreError::Serialization)?;
            entries.push(MessageEntry { seq, message });
        }

        Ok(entries)
    }

    fn replay_ledger(&self, session_id: &SessionId) -> Result<Vec<Message>, SessionStoreError> {
        let entries = self.load_messages(session_id)?;
        Ok(replay_messages(&entries))
    }

    fn append_event(&self, event: &RunEvent) -> Result<(), SessionStoreError> {
        let conn = self.conn.lock();
        let event_json = serde_json::to_string(event).map_err(SessionStoreError::Serialization)?;

        conn.execute(
            "INSERT INTO run_events (run_id, session_id, seq, event_json, timestamp_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.run_id.0,
                event.session_id.0,
                event.seq,
                event_json,
                event.timestamp_ms
            ],
        )
        .map_err(|e| {
            if let rusqlite::Error::SqliteFailure(f, _) = &e
                && f.code == rusqlite::ErrorCode::ConstraintViolation
            {
                return SessionStoreError::Internal(format!(
                    "duplicate seq {} for run {}",
                    event.seq, event.run_id.0
                ));
            }
            SessionStoreError::Storage(e.to_string())
        })?;

        Ok(())
    }

    fn events_for_run(&self, run_id: &RunId) -> Result<Vec<RunEvent>, SessionStoreError> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT event_json FROM run_events WHERE run_id = ?1 ORDER BY seq")
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        let rows = stmt
            .query_map(params![run_id.0], |row| {
                let json_str: String = row.get(0)?;
                Ok(json_str)
            })
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        let mut events = Vec::new();
        for row in rows {
            let json_str = row.map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let event: RunEvent =
                serde_json::from_str(&json_str).map_err(SessionStoreError::Serialization)?;
            events.push(event);
        }

        Ok(events)
    }

    fn check_event_gaps(&self, run_id: &RunId) -> Result<Vec<u64>, SessionStoreError> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT seq FROM run_events WHERE run_id = ?1 ORDER BY seq")
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        let rows = stmt
            .query_map(params![run_id.0], |row| {
                let seq: u64 = row.get(0)?;
                Ok(seq)
            })
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        let mut seqs: Vec<u64> = Vec::new();
        for row in rows {
            let seq = row.map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            seqs.push(seq);
        }

        if seqs.len() < 2 {
            return Ok(Vec::new());
        }

        // Total missing seqs beyond which the ledger is treated as corrupt:
        // materializing every missing number between widely-spaced seqs can
        // OOM, so cap the count and surface an internal error instead.
        const MAX_GAPS: u64 = 10_000;

        let mut gaps = Vec::new();
        let mut total = 0u64;
        for pair in seqs.windows(2) {
            let (start, end) = (pair[0], pair[1]);
            if end > start + 1 {
                total += end - start - 1;
                if total > MAX_GAPS {
                    return Err(SessionStoreError::Internal(format!(
                        "event gap count exceeds {MAX_GAPS} for run {}; ledger may be corrupt",
                        run_id.0
                    )));
                }
                gaps.extend((start + 1)..end);
            }
        }
        Ok(gaps)
    }

    fn delete_session(&self, id: &SessionId) -> Result<(), SessionStoreError> {
        let mut conn = self.conn.lock();
        let tx = conn
            .transaction()
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        tx.execute("DELETE FROM messages WHERE session_id = ?1", params![id.0])
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        tx.execute(
            "DELETE FROM run_events WHERE session_id = ?1",
            params![id.0],
        )
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let deleted = tx
            .execute("DELETE FROM sessions WHERE session_id = ?1", params![id.0])
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        tx.commit()
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        if deleted == 0 {
            return Err(SessionStoreError::SessionNotFound(id.clone()));
        }

        Ok(())
    }
}
