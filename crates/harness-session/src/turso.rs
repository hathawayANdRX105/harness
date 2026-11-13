//! Turso (libsql)-backed [`SessionStore`] implementation.
//!
//! Replaces the former rusqlite-backed store (`sqlite.rs`). The design is
//! ferrite's proven turso model (`crates/app-db/src/turso.rs`), adapted to
//! harness's three-table session schema.
//!
//! ## Connection model
//!
//! Two paths are kept separate so a transaction never becomes a critical
//! section:
//!
//! - **Direct statements** (`execute` / `query`) share one `Connection` held
//!   in an `Arc`, serialized by a `tokio::Mutex` gate. turso's `Connection`
//!   cannot tolerate overlapping async operations
//!   (`concurrent use forbidden` / `connection is busy`), so every direct
//!   statement takes the gate.
//! - **Transactions** (`append_message`) open a **separate connection** from
//!   the `Database` handle: turso transactions are connection-level state, and
//!   a separate connection keeps the transaction from holding the gate for its
//!   whole lifetime.
//!
//! Concurrency is left to the engine: `BEGIN IMMEDIATE` plus `busy_timeout`
//! queue at the file level.
//!
//! ## Schema
//!
//! Three tables, all `CREATE TABLE IF NOT EXISTS` so `open` is idempotent and
//! safe on both fresh and existing database files:
//!
//! ```sql
//! CREATE TABLE sessions (
//!     session_id   TEXT PRIMARY KEY,
//!     created_at_ms INTEGER NOT NULL
//! );
//!
//! CREATE TABLE messages (
//!     id            INTEGER PRIMARY KEY AUTOINCREMENT,
//!     session_id    TEXT NOT NULL,
//!     seq           INTEGER NOT NULL,
//!     message_json  TEXT NOT NULL,
//!     created_at_ms INTEGER NOT NULL,
//!     FOREIGN KEY (session_id) REFERENCES sessions(session_id),
//!     UNIQUE(session_id, seq)
//! );
//!
//! CREATE TABLE run_events (
//!     id           INTEGER PRIMARY KEY AUTOINCREMENT,
//!     run_id       TEXT NOT NULL,
//!     session_id   TEXT NOT NULL,
//!     seq          INTEGER NOT NULL,
//!     event_json   TEXT NOT NULL,
//!     timestamp_ms INTEGER NOT NULL,
//!     idempotency_key TEXT UNIQUE NOT NULL,
//!     FOREIGN KEY (session_id) REFERENCES sessions(session_id),
//!     UNIQUE(run_id, seq)
//! );
//! ```
//!
//! `idempotency_key` is `run_id:seq`. Appends use `INSERT ... ON CONFLICT
//! DO NOTHING` so re-applying an already-persisted event (for example after a
//! restart replay) returns the stored row and writes nothing. The `UNIQUE`
//! constraint on `idempotency_key` also backs the index, so no separate
//! `CREATE INDEX` is needed for it.
//!
//! ## Persistence
//!
//! The index/graph shadow (see [`crate::graph`]) is derived read-only state:
//! it is rebuilt from the `run_events` ledger on open and updated on append,
//! and it is **never persisted** (ferrite's 0106 lesson — a persistent graph
//! file stales silently). Only the ledger tables above are durable.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use harness_core::event::RunEvent;
use harness_core::message::Message;
use harness_core::run::{RunId, SessionId};
use tokio::sync::Mutex;

use crate::recall::{RecallHit, RecallScope, RecallStore};
use crate::store::{MessageEntry, SessionStore, SessionStoreError, replay_messages};
use turso::{Builder, Connection, Database, params};

/// Turso-backed [`SessionStore`].
///
/// One shared `Connection` (`conn`) serves direct statements, serialized by
/// `gate`. Transactions open a separate connection via `database`.
pub struct TursoSessionStore {
    /// `Database` handle that opened `conn` (internally `Arc`, cheap to
    /// clone): used when a transaction opens a separate connection.
    database: Database,
    /// Connection shared by direct statements (`execute` / `query`).
    conn: Arc<Connection>,
    /// Serialization gate for the shared connection: turso's `Connection`
    /// cannot tolerate overlapping async operations. Every direct statement
    /// takes it; transactions use a separate connection and never hold it.
    gate: Arc<Mutex<()>>,
}

impl TursoSessionStore {
    /// Open or create a database at `path`, run the schema migration and
    /// enable WAL.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, SessionStoreError> {
        let database = Builder::new_local(
            path.as_ref()
                .to_str()
                .ok_or_else(|| SessionStoreError::Storage("invalid database path".into()))?,
        )
        // Tantivy-backed `CREATE INDEX ... USING fts` powers the recall
        // search (turso has no SQLite FTS5 module — see the recall module
        // docs). The flag is experimental but required for the index syntax.
        .experimental_index_method(true)
        .build()
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let conn = database
            .connect()
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let store = Self {
            database,
            conn: Arc::new(conn),
            gate: Arc::new(Mutex::new(())),
        };
        store.migrate().await?;
        Ok(store)
    }

    /// Create the schema tables and indexes if they do not exist.
    ///
    /// All statements are `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT
    /// EXISTS`, so re-running on an existing file is a no-op.
    async fn migrate(&self) -> Result<(), SessionStoreError> {
        let _guard = self.gate.lock().await;
        self.conn
            .execute_batch(
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
                     idempotency_key TEXT UNIQUE NOT NULL,
                     FOREIGN KEY (session_id) REFERENCES sessions(session_id),
                     UNIQUE(run_id, seq)
                 );

                 CREATE INDEX IF NOT EXISTS idx_messages_session_seq
                     ON messages(session_id, seq);

                 CREATE INDEX IF NOT EXISTS idx_run_events_run_seq
                     ON run_events(run_id, seq);

                 CREATE INDEX IF NOT EXISTS idx_run_events_fts
                     ON run_events USING fts (event_json)
                     WITH (tokenizer = 'ngram');",
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        // WAL so readers and the single writer do not block each other.
        let mut rows = self
            .conn
            .query("PRAGMA journal_mode=WAL", ())
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        while let Some(_row) = rows
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
        {
            // consume the pragma result row
        }
        Ok(())
    }

    /// Open a separate connection for a transaction: turso transactions are
    /// connection-level state, and a separate connection keeps the
    /// transaction from holding the shared gate for its whole lifetime.
    fn tx_conn(&self) -> Result<Connection, SessionStoreError> {
        self.database
            .connect()
            .map_err(|e| SessionStoreError::Storage(e.to_string()))
    }

    /// Current unix epoch millisecond timestamp.
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

#[async_trait]
impl SessionStore for TursoSessionStore {
    async fn create_session(&self, id: &SessionId) -> Result<(), SessionStoreError> {
        let _guard = self.gate.lock().await;
        let now = Self::now_ms();
        let changed = self
            .conn
            .execute(
                "INSERT INTO sessions (session_id, created_at_ms) VALUES (?1, ?2)",
                params![id.0.clone(), now],
            )
            .await
            .map_err(|e| {
                if matches!(e, turso::Error::Constraint(_)) {
                    SessionStoreError::SessionAlreadyExists(id.clone())
                } else {
                    SessionStoreError::Storage(e.to_string())
                }
            })?;
        if changed == 0 {
            return Err(SessionStoreError::Storage(
                "create_session inserted zero rows".into(),
            ));
        }
        Ok(())
    }

    async fn session_exists(&self, id: &SessionId) -> Result<bool, SessionStoreError> {
        let _guard = self.gate.lock().await;
        let mut rows = self
            .conn
            .query(
                "SELECT COUNT(*) > 0 FROM sessions WHERE session_id = ?1",
                params![id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
            .ok_or_else(|| SessionStoreError::Storage("session_exists returned no row".into()))?;
        let exists: bool = row
            .get(0)
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        Ok(exists)
    }

    async fn append_message(
        &self,
        session_id: &SessionId,
        message: &Message,
    ) -> Result<u64, SessionStoreError> {
        // The seq read and the insert must be atomic so concurrent appends
        // cannot observe a gap or hand out the same seq twice: do both inside
        // one transaction on a separate connection.
        let mut conn = self.tx_conn()?;
        let tx = conn
            .transaction()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let next_seq: u64 = tx
            .query(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM messages WHERE session_id = ?1",
                params![session_id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
            .ok_or_else(|| SessionStoreError::Storage("next_seq query returned no row".into()))?
            .get(0)
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let message_json =
            serde_json::to_string(message).map_err(SessionStoreError::Serialization)?;
        let now = Self::now_ms();
        tx.execute(
            "INSERT INTO messages (session_id, seq, message_json, created_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![session_id.0.clone(), next_seq, message_json, now],
        )
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        tx.commit()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        Ok(next_seq)
    }

    async fn load_messages(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<MessageEntry>, SessionStoreError> {
        let _guard = self.gate.lock().await;
        let mut rows = self
            .conn
            .query(
                "SELECT seq, message_json FROM messages WHERE session_id = ?1 ORDER BY seq",
                params![session_id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let mut entries = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
        {
            let seq: u64 = row
                .get(0)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let json_str: String = row
                .get(1)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let message: Message =
                serde_json::from_str(&json_str).map_err(SessionStoreError::Serialization)?;
            entries.push(MessageEntry { seq, message });
        }
        Ok(entries)
    }

    async fn replay_ledger(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<Message>, SessionStoreError> {
        let entries = self.load_messages(session_id).await?;
        Ok(replay_messages(&entries))
    }

    async fn append_event(&self, event: &RunEvent) -> Result<(), SessionStoreError> {
        let event_json = serde_json::to_string(event).map_err(SessionStoreError::Serialization)?;
        // idempotency_key = run_id:seq re-anchors the append-only log:
        // re-applying an already-persisted event is a no-op (ON CONFLICT DO
        // NOTHING) rather than a DuplicateSeq error.
        let idempotency_key = format!("{}:{}", event.run_id.0, event.seq);
        let changed = self
            .conn
            .execute(
                "INSERT INTO run_events (run_id, session_id, seq, event_json, timestamp_ms, idempotency_key)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(idempotency_key) DO NOTHING",
                params![
                    event.run_id.0.clone(),
                    event.session_id.0.clone(),
                    event.seq,
                    event_json,
                    event.timestamp_ms,
                    idempotency_key
                ],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let _ = changed; // 0 = duplicate (no-op), 1 = inserted — both are success
        Ok(())
    }

    async fn events_for_run(&self, run_id: &RunId) -> Result<Vec<RunEvent>, SessionStoreError> {
        let _guard = self.gate.lock().await;
        let mut rows = self
            .conn
            .query(
                "SELECT event_json FROM run_events WHERE run_id = ?1 ORDER BY seq",
                params![run_id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let mut events = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
        {
            let json_str: String = row
                .get(0)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let event: RunEvent =
                serde_json::from_str(&json_str).map_err(SessionStoreError::Serialization)?;
            events.push(event);
        }
        Ok(events)
    }

    async fn check_event_gaps(&self, run_id: &RunId) -> Result<Vec<u64>, SessionStoreError> {
        let events = self.events_for_run(run_id).await?;
        let mut seqs: Vec<u64> = events.iter().map(|event| event.seq).collect();
        seqs.sort();
        seqs.dedup();
        let mut gaps = Vec::new();
        if let (Some(first), Some(_last)) = (seqs.first(), seqs.last()) {
            let mut expected = *first;
            for seq in seqs {
                while expected < seq {
                    gaps.push(expected);
                    expected += 1;
                }
                expected = seq + 1;
            }
        }
        Ok(gaps)
    }

    async fn run_ids(&self) -> Result<Vec<RunId>, SessionStoreError> {
        let _guard = self.gate.lock().await;
        let mut rows = self
            .conn
            .query(
                "SELECT run_id FROM run_events GROUP BY run_id ORDER BY MAX(id) DESC",
                params![],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let mut ids = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
        {
            let id: String = row
                .get(0)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            ids.push(RunId(id));
        }
        Ok(ids)
    }

    async fn latest_run_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<RunId>, SessionStoreError> {
        let _guard = self.gate.lock().await;
        let mut rows = self
            .conn
            .query(
                "SELECT run_id FROM run_events WHERE session_id = ?1
                 GROUP BY run_id
                 ORDER BY MAX(seq) DESC
                 LIMIT 1",
                params![session_id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let Some(row) = rows
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };
        let id: String = row
            .get(0)
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        Ok(Some(RunId(id)))
    }

    async fn delete_session(&self, id: &SessionId) -> Result<(), SessionStoreError> {
        let _guard = self.gate.lock().await;
        // Check existence within this same gate-held section: calling
        // `session_exists` here would re-acquire the gate and deadlock
        // (tokio::Mutex is not reentrant).
        let exists: bool = self
            .conn
            .query(
                "SELECT COUNT(*) > 0 FROM sessions WHERE session_id = ?1",
                params![id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
            .is_some_and(|row| row.get::<bool>(0).unwrap_or(false));
        if !exists {
            return Err(SessionStoreError::SessionNotFound(id.clone()));
        }
        self.conn
            .execute(
                "DELETE FROM messages WHERE session_id = ?1",
                params![id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        self.conn
            .execute(
                "DELETE FROM run_events WHERE session_id = ?1",
                params![id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        self.conn
            .execute(
                "DELETE FROM sessions WHERE session_id = ?1",
                params![id.0.clone()],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Recall: Tantivy-backed full-text search over run_events
// ---------------------------------------------------------------------------

/// [`RecallStore`] over the store's Tantivy `USING fts` index on
/// `run_events.event_json`.
///
/// The index covers the serialized event payload, so searches match any text
/// the events carry (task descriptions, deltas, tool output) — including
/// substrings via the `ngram` tokenizer, which also makes CJK queries work.
/// JSON structural keys are searchable noise; the session scope and the
/// returned `seq`/run identity are the callers' real filters.
pub struct TursoFtsRecall {
    /// Same shared connection the store uses (direct statements only).
    conn: Arc<Connection>,
    /// Same serialization gate as the store's direct statements.
    gate: Arc<Mutex<()>>,
}

impl TursoSessionStore {
    /// Build a recall handle sharing this store's connection and gate.
    ///
    /// The FTS index itself is created in [`TursoSessionStore::open`]'s
    /// migration — recall searches hit the engine-maintained index directly.
    pub fn recall(&self) -> TursoFtsRecall {
        TursoFtsRecall {
            conn: self.conn.clone(),
            gate: self.gate.clone(),
        }
    }
}

#[async_trait::async_trait]
impl RecallStore for TursoFtsRecall {
    async fn search(
        &self,
        query: &str,
        scope: &RecallScope,
    ) -> Result<Vec<RecallHit>, SessionStoreError> {
        let _guard = self.gate.lock().await;
        let limit = scope.limit.max(1);
        // The FTS index is on the serialized event payload; matches are
        // ranked by the engine's BM25 score and highlighted for display.
        let sql = format!(
            "SELECT run_id, session_id, seq, \
                    fts_highlight(event_json, '≫', '≪', '{query}') AS snippet, \
                    fts_score(event_json, '{query}') AS score \
             FROM run_events \
             WHERE fts_match(event_json, '{query}') {} \
             ORDER BY score DESC \
             LIMIT {limit}",
            scope
                .session_id
                .as_ref()
                .map_or(String::new(), |id| format!(
                    "AND session_id = '{}'",
                    id.0.replace('\'', "''")
                )),
        );
        let mut rows = self
            .conn
            .query(&sql, ())
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let mut hits = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
        {
            let run_id: String = row
                .get(0)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let session_id: String = row
                .get(1)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let seq: u64 = row
                .get(2)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let snippet: String = row
                .get(3)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            let score: f64 = row
                .get(4)
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            hits.push(RecallHit {
                run_id: RunId(run_id),
                session_id: SessionId(session_id),
                seq,
                snippet,
                score,
            });
        }
        Ok(hits)
    }
}
