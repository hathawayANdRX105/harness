//! Turso (libsql)-backed [`SessionStore`] implementation.
//!
//! Replaces the former rusqlite-backed store (`sqlite.rs`). The design is
//! ferrite's proven turso model (`crates/app-db/src/turso.rs`), adapted to
//! harness's three-table session schema.
//!
//! ## Connection model
//!
//! Two paths share one write gate:
//!
//! - **Direct statements** (`execute` / `query`) share one `Connection` held
//!   in an `Arc`, serialized by a `tokio::Mutex` gate. turso's `Connection`
//!   cannot tolerate overlapping async operations
//!   (`concurrent use forbidden` / `connection is busy`), so every direct
//!   statement takes the gate.
//! - **Transactions** (`append_message`) open a **separate connection** from
//!   the `Database` handle, because `transaction()` needs `&mut Connection`.
//!   The transaction holds the same gate for its lifetime: a concurrent event
//!   append on the shared connection would otherwise commit between `BEGIN`
//!   and `COMMIT` and invalidate the transaction's snapshot.
//!
//! Within the process the gate serializes every writer (message sink, event
//! pump, session setup). Across processes the engine queues on the file lock:
//! `BEGIN IMMEDIATE` plus a 5 s `busy_timeout` on both connections.
//!
//! ## Schema
//!
//! Four tables, all `CREATE TABLE IF NOT EXISTS` so `open` is idempotent and
//! safe on both fresh and existing database files:
//!
//! ```sql
//! CREATE TABLE sessions (
//!     session_id   TEXT PRIMARY KEY,
//!     created_at_ms INTEGER NOT NULL
//! );
//!
//! -- One row per conversational block: index columns + content, like
//! -- ferrite's context_events. `parent_event_id` is the graph edge.
//! CREATE TABLE units (
//!     event_id        TEXT PRIMARY KEY,
//!     session_id      TEXT NOT NULL,
//!     branch_id       TEXT NOT NULL,
//!     epoch           INTEGER NOT NULL,
//!     event_order     INTEGER NOT NULL,
//!     message_seq     INTEGER NOT NULL,
//!     role            TEXT NOT NULL,
//!     kind            TEXT NOT NULL,
//!     payload_json    TEXT NOT NULL,
//!     parent_event_id TEXT,
//!     created_at_ms   INTEGER NOT NULL,
//!     FOREIGN KEY (session_id) REFERENCES sessions(session_id),
//!     UNIQUE(session_id, branch_id, epoch, event_order)
//! );
//!
//! CREATE TABLE session_heads (
//!     session_id       TEXT PRIMARY KEY,
//!     branch_id        TEXT NOT NULL,
//!     epoch            INTEGER NOT NULL,
//!     head_event_id    TEXT,
//!     last_event_order INTEGER NOT NULL,
//!     last_message_seq INTEGER NOT NULL
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
//! it is rebuilt from the `units` ledger on open and updated on append, and it
//! is **never persisted** (ferrite's 0106 lesson — a persistent graph file
//! stales silently). Only the ledger tables above are durable: `units` holds
//! the index columns *and* the payload, `session_heads` holds each scope's
//! head pointer, and `run_events` stays the wire log.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use harness_core::event::RunEvent;
use harness_core::message::Message;
use harness_core::run::{RunId, SessionId};
use tokio::sync::Mutex;

use crate::graph::GraphIndex;
use crate::recall::{RecallHit, RecallScope, RecallStore};
use crate::store::{MessageEntry, SessionStore, SessionStoreError, replay_messages};
use crate::unit::{
    HeadState, INITIAL_EPOCH, MAIN_BRANCH, StoredUnit, Unit, UnitKind, assemble_messages,
    split_message,
};
use turso::{Builder, Connection, Database, params};

/// Upper bound on cached per-session graphs. The graph is derived, so
/// eviction is always safe; the cap keeps a client that opens unbounded
/// session ids from growing the cache forever.
const MAX_CACHED_GRAPHS: usize = 64;

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
    /// Per-session derived graph, built on first replay and updated on append.
    /// Turso stays the source of truth; dropping this cache only costs a
    /// rebuild.
    graphs: StdMutex<HashMap<SessionId, Arc<GraphIndex>>>,
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
            graphs: StdMutex::new(HashMap::new()),
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

                 CREATE TABLE IF NOT EXISTS units (
                     event_id        TEXT PRIMARY KEY,
                     session_id      TEXT NOT NULL,
                     branch_id       TEXT NOT NULL,
                     epoch           INTEGER NOT NULL,
                     event_order     INTEGER NOT NULL,
                     message_seq     INTEGER NOT NULL,
                     role            TEXT NOT NULL,
                     kind            TEXT NOT NULL,
                     payload_json    TEXT NOT NULL,
                     parent_event_id TEXT,
                     created_at_ms   INTEGER NOT NULL,
                     FOREIGN KEY (session_id) REFERENCES sessions(session_id),
                     UNIQUE(session_id, branch_id, epoch, event_order)
                 );

                 CREATE TABLE IF NOT EXISTS session_heads (
                     session_id        TEXT PRIMARY KEY,
                     branch_id         TEXT NOT NULL,
                     epoch             INTEGER NOT NULL,
                     head_event_id     TEXT,
                     last_event_order  INTEGER NOT NULL,
                     last_message_seq  INTEGER NOT NULL,
                     FOREIGN KEY (session_id) REFERENCES sessions(session_id)
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

                 CREATE INDEX IF NOT EXISTS idx_units_session_order
                     ON units(session_id, event_order);

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
        Self::set_busy_timeout(&self.conn).await?;
        Ok(())
    }

    /// Open a separate connection for a transaction: turso transactions are
    /// connection-level state, and `transaction()` needs `&mut Connection`.
    /// The caller holds the write gate for the transaction's lifetime, so the
    /// separate connection never races the shared one.
    ///
    /// The connection carries the same busy timeout as the shared one: the
    /// message writer and the event pump are separate connections, so the
    /// loser of the file-level write race must wait instead of failing.
    async fn tx_conn(&self) -> Result<Connection, SessionStoreError> {
        let mut conn = self
            .database
            .connect()
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        Self::set_busy_timeout(&conn).await?;
        // BEGIN IMMEDIATE: the transaction takes the write lock up front, so
        // concurrent writers queue on busy_timeout instead of failing at the
        // first write with SQLITE_BUSY_SNAPSHOT.
        conn.set_transaction_behavior(turso::transaction::TransactionBehavior::Immediate);
        Ok(conn)
    }

    /// Let a contending writer queue for the file lock instead of failing
    /// with `database is locked`.
    async fn set_busy_timeout(conn: &Connection) -> Result<(), SessionStoreError> {
        let mut rows = conn
            .query("PRAGMA busy_timeout = 5000", ())
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        while let Some(_row) = rows
            .next()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?
        {
            // consume the pragma result row
        }
        // SQLite defaults foreign_keys off; the schema's REFERENCES clauses
        // only enforce integrity when this is on. apply to both the shared
        // and transaction connections.
        let mut rows = conn
            .query("PRAGMA foreign_keys = ON", ())
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

    /// Current unix epoch millisecond timestamp.
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Every unit of a session, in append order.
    async fn load_units(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<StoredUnit>, SessionStoreError> {
        let _guard = self.gate.lock().await;
        load_units_from(&self.conn, session_id).await
    }

    /// The session's head row, creating the default one for a session that
    /// predates the unit ledger.
    async fn load_head(&self, session_id: &SessionId) -> Result<HeadState, SessionStoreError> {
        let _guard = self.gate.lock().await;
        ensure_head_from(&self.conn, session_id).await
    }

    /// Units and head from one gated snapshot: the graph and its HEAD must be
    /// read under the same gate, or an append committing between the two
    /// reads can leave the cached graph pointing at a unit it does not hold.
    async fn load_snapshot(
        &self,
        session_id: &SessionId,
    ) -> Result<(Vec<StoredUnit>, HeadState), SessionStoreError> {
        let _guard = self.gate.lock().await;
        let head = ensure_head_from(&self.conn, session_id).await?;
        let units = load_units_from(&self.conn, session_id).await?;
        Ok((units, head))
    }

    /// The session's derived graph plus, on a cold build, the units it was
    /// built from (so the replay path does not read them twice), plus the
    /// head row that names the current scope.
    async fn graph_for(
        &self,
        session_id: &SessionId,
    ) -> Result<(Arc<GraphIndex>, Option<Vec<StoredUnit>>, HeadState), SessionStoreError> {
        // Clone out of the cache in its own scope: the std guard is not Send,
        // and the head read below awaits.
        let cached = {
            let cache = self
                .graphs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.get(session_id).cloned()
        };
        if let Some(graph) = cached {
            let head = self.load_head(session_id).await?;
            return Ok((graph, None, head));
        }
        let (units, head) = self.load_snapshot(session_id).await?;
        let graph = Arc::new(GraphIndex::new());
        let metas: Vec<Unit> = units.iter().map(|stored| stored.unit.clone()).collect();
        graph.rebuild(&metas);
        // Only trust a stored head that the rebuilt graph actually contains.
        // A ghost head (a unit committed after this snapshot) would make the
        // ancestor walk empty and silently blank the transcript.
        if let Some(head_event_id) = head.head_event_id.as_deref()
            && graph.contains(head_event_id)
        {
            graph.set_head(session_id, &head.branch_id, head.epoch, head_event_id);
        }
        let mut cache = self
            .graphs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cache.len() >= MAX_CACHED_GRAPHS && !cache.contains_key(session_id) {
            // The graph is derived: dropping every entry costs one rebuild per
            // touched session and bounds memory under a client that opens
            // unbounded session ids.
            cache.clear();
        }
        let graph = cache
            .entry(session_id.clone())
            .or_insert_with(|| Arc::clone(&graph))
            .clone();
        Ok((graph, Some(units), head))
    }

    /// Units named by `ids`, read in chunks so the chain, not the whole
    /// session, pays for its payloads.
    async fn load_units_by_ids(
        &self,
        session_id: &SessionId,
        ids: &[String],
    ) -> Result<Vec<StoredUnit>, SessionStoreError> {
        const CHUNK: usize = 200;
        let _guard = self.gate.lock().await;
        let mut by_id: HashMap<String, StoredUnit> = HashMap::with_capacity(ids.len());
        for chunk in ids.chunks(CHUNK) {
            let placeholders: Vec<String> = (0..chunk.len())
                .map(|index| format!("?{}", index + 2))
                .collect();
            let sql = format!(
                "SELECT event_id, branch_id, epoch, event_order, message_seq, role, kind,
                        payload_json, parent_event_id, created_at_ms
                   FROM units
                  WHERE session_id = ?1 AND event_id IN ({})",
                placeholders.join(", ")
            );
            let mut values: Vec<String> = Vec::with_capacity(chunk.len() + 1);
            values.push(session_id.0.clone());
            values.extend(chunk.iter().cloned());
            let mut rows = self
                .conn
                .query(&sql, turso::params_from_iter(values))
                .await
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?
            {
                let stored = decode_unit_row(&row, session_id)?;
                by_id.insert(stored.unit.event_id.clone(), stored);
            }
        }
        ids.iter()
            .map(|id| {
                by_id.remove(id).ok_or_else(|| {
                    SessionStoreError::Internal(format!(
                        "graph chain references unit {id} missing from the ledger"
                    ))
                })
            })
            .collect()
    }

    /// Fold a committed append into a warm graph; a cold cache is left alone
    /// and rebuilds on the next replay.
    fn update_graph(&self, session_id: &SessionId, units: &[(Unit, String)], head_event_id: &str) {
        let cache = self
            .graphs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(graph) = cache.get(session_id) else {
            return;
        };
        let metas: Vec<Unit> = units.iter().map(|(unit, _)| unit.clone()).collect();
        graph.append(&metas, head_event_id);
    }
}

/// Every unit of a session, in append order, through an already-gated
/// connection.
async fn load_units_from(
    conn: &Connection,
    session_id: &SessionId,
) -> Result<Vec<StoredUnit>, SessionStoreError> {
    let mut rows = conn
        .query(
            "SELECT event_id, branch_id, epoch, event_order, message_seq, role, kind,
                    payload_json, parent_event_id, created_at_ms
               FROM units
              WHERE session_id = ?1
              ORDER BY branch_id, epoch, event_order",
            params![session_id.0.clone()],
        )
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let mut units = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?
    {
        units.push(decode_unit_row(&row, session_id)?);
    }
    Ok(units)
}

/// Decode one unit row.
fn decode_unit_row(
    row: &turso::Row,
    session_id: &SessionId,
) -> Result<StoredUnit, SessionStoreError> {
    let event_id: String = row
        .get(0)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let branch_id: String = row
        .get(1)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let epoch: i64 = row
        .get(2)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let event_order: i64 = row
        .get(3)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let message_seq: i64 = row
        .get(4)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let role: String = row
        .get(5)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let kind: String = row
        .get(6)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let payload_json: String = row
        .get(7)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let parent_event_id: Option<String> = row
        .get(8)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let created_at_ms: i64 = row
        .get(9)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    Ok(StoredUnit {
        unit: Unit {
            event_id,
            session_id: session_id.clone(),
            branch_id,
            epoch,
            event_order: u64::try_from(event_order)
                .map_err(|_| SessionStoreError::Internal("negative unit event_order".into()))?,
            message_seq: u64::try_from(message_seq)
                .map_err(|_| SessionStoreError::Internal("negative unit message_seq".into()))?,
            role,
            kind: UnitKind::parse(&kind)?,
            parent_event_id,
            created_at_ms,
        },
        payload_json,
    })
}

/// Ensure the session has a head row, then read it; the caller holds the gate.
async fn ensure_head_from(
    conn: &Connection,
    session_id: &SessionId,
) -> Result<HeadState, SessionStoreError> {
    // Only write when the row is genuinely absent: replay is otherwise a
    // read-only path and should not take a write lock.
    if let Some(head) = read_head(conn, session_id).await? {
        return Ok(head);
    }
    conn.execute(
        "INSERT OR IGNORE INTO session_heads
             (session_id, branch_id, epoch, head_event_id, last_event_order, last_message_seq)
         VALUES (?1, ?2, ?3, NULL, 0, 0)",
        params![session_id.0.clone(), MAIN_BRANCH, INITIAL_EPOCH],
    )
    .await
    .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    read_head(conn, session_id).await?.ok_or_else(|| {
        SessionStoreError::Internal(format!("session {} has no head row", session_id.0))
    })
}

/// Ensure a head row exists inside the append transaction and read it.
async fn ensure_head_tx(
    conn: &Connection,
    session_id: &SessionId,
) -> Result<HeadState, SessionStoreError> {
    conn.execute(
        "INSERT OR IGNORE INTO session_heads
             (session_id, branch_id, epoch, head_event_id, last_event_order, last_message_seq)
         VALUES (?1, ?2, ?3, NULL, 0, 0)",
        params![session_id.0.clone(), MAIN_BRANCH, INITIAL_EPOCH],
    )
    .await
    .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let mut rows = conn
        .query(
            "SELECT branch_id, epoch, head_event_id, last_event_order, last_message_seq
               FROM session_heads WHERE session_id = ?1",
            params![session_id.0.clone()],
        )
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    decode_head_row(&mut rows, session_id)
        .await?
        .ok_or_else(|| {
            SessionStoreError::Internal(format!("session {} has no head row", session_id.0))
        })
}

/// Insert one message's units.
async fn insert_units_tx(
    conn: &Connection,
    units: &[(Unit, String)],
) -> Result<(), SessionStoreError> {
    for (unit, payload_json) in units {
        conn.execute(
            "INSERT INTO units
                 (event_id, session_id, branch_id, epoch, event_order, message_seq,
                  role, kind, payload_json, parent_event_id, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                unit.event_id.clone(),
                unit.session_id.0.clone(),
                unit.branch_id.clone(),
                unit.epoch,
                unit.event_order as i64,
                unit.message_seq as i64,
                unit.role.clone(),
                unit.kind.as_str(),
                payload_json.clone(),
                unit.parent_event_id.clone(),
                unit.created_at_ms,
            ],
        )
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    }
    Ok(())
}

/// Move the session head to the last unit of the message just inserted.
async fn advance_head_tx(
    conn: &Connection,
    session_id: &SessionId,
    last: &Unit,
    message_seq: u64,
) -> Result<(), SessionStoreError> {
    conn.execute(
        "UPDATE session_heads
            SET head_event_id = ?2, last_event_order = ?3, last_message_seq = ?4
          WHERE session_id = ?1",
        params![
            session_id.0.clone(),
            last.event_id.clone(),
            last.event_order as i64,
            message_seq as i64,
        ],
    )
    .await
    .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    Ok(())
}

/// Reorder `units` to the ancestor id order, erroring on a missing id.
fn order_by_ids(
    units: Vec<StoredUnit>,
    ids: &[String],
) -> Result<Vec<StoredUnit>, SessionStoreError> {
    let mut by_id: HashMap<String, StoredUnit> = units
        .into_iter()
        .map(|stored| (stored.unit.event_id.clone(), stored))
        .collect();
    ids.iter()
        .map(|id| {
            by_id.remove(id).ok_or_else(|| {
                SessionStoreError::Internal(format!(
                    "graph chain references unit {id} missing from the ledger"
                ))
            })
        })
        .collect()
}

/// Read the head row of a session from the shared connection.
async fn read_head(
    conn: &Connection,
    session_id: &SessionId,
) -> Result<Option<HeadState>, SessionStoreError> {
    let mut rows = conn
        .query(
            "SELECT branch_id, epoch, head_event_id, last_event_order, last_message_seq
               FROM session_heads WHERE session_id = ?1",
            params![session_id.0.clone()],
        )
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    decode_head_row(&mut rows, session_id).await
}

/// Decode the single head row; `None` when the session has no head row.
async fn decode_head_row(
    rows: &mut turso::Rows,
    session_id: &SessionId,
) -> Result<Option<HeadState>, SessionStoreError> {
    let Some(row) = rows
        .next()
        .await
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?
    else {
        return Ok(None);
    };
    let branch_id: String = row
        .get(0)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let epoch: i64 = row
        .get(1)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let head_event_id: Option<String> = row
        .get(2)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let last_event_order: i64 = row
        .get(3)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    let last_message_seq: i64 = row
        .get(4)
        .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
    Ok(Some(HeadState {
        session_id: session_id.clone(),
        branch_id,
        epoch,
        head_event_id,
        last_event_order: u64::try_from(last_event_order)
            .map_err(|_| SessionStoreError::Internal("negative head event_order".into()))?,
        last_message_seq: u64::try_from(last_message_seq)
            .map_err(|_| SessionStoreError::Internal("negative head message_seq".into()))?,
    }))
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
        // Every session starts with a head row under `main`/epoch 1; appends
        // move it. `OR IGNORE` keeps re-creation idempotent for callers that
        // swallow `SessionAlreadyExists`.
        self.conn
            .execute(
                "INSERT OR IGNORE INTO session_heads
                     (session_id, branch_id, epoch, head_event_id, last_event_order, last_message_seq)
                 VALUES (?1, ?2, ?3, NULL, 0, 0)",
                params![id.0.clone(), MAIN_BRANCH, INITIAL_EPOCH],
            )
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
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
        // The head read and the unit inserts must be atomic so concurrent
        // appends cannot hand out the same order/message sequence: one
        // transaction on a separate connection.
        //
        // The gate serializes this transaction against the shared connection:
        // a concurrent event append would otherwise commit between BEGIN and
        // COMMIT and the transaction's snapshot would go stale.
        let _guard = self.gate.lock().await;
        let mut conn = self.tx_conn().await?;
        let tx = conn
            .transaction()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let head = ensure_head_tx(&tx, session_id).await?;
        let message_seq = head.last_message_seq + 1;
        let units = split_message(&head, message, Self::now_ms())?;
        insert_units_tx(&tx, &units).await?;
        let last = units
            .last()
            .map(|(unit, _)| unit)
            .ok_or_else(|| SessionStoreError::Internal("a message produced no units".into()))?;
        advance_head_tx(&tx, session_id, last, message_seq).await?;
        tx.commit()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;

        // Keep a warm graph in sync with what just committed. A cold cache
        // rebuilds from Turso on the next replay instead.
        self.update_graph(session_id, &units, &last.event_id);
        Ok(message_seq)
    }

    async fn load_messages(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<MessageEntry>, SessionStoreError> {
        let units = self.load_units(session_id).await?;
        assemble_messages(&units)
    }

    async fn replay_ledger(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<Message>, SessionStoreError> {
        // The graph names the retained chain (HEAD -> root); Turso supplies
        // the payloads by id. This is the ferrite split: graph = topology,
        // ledger = content.
        let (graph, cold_units, head) = self.graph_for(session_id).await?;
        let Some(head_id) = graph.head(session_id, &head.branch_id, head.epoch) else {
            // A session with units but no resolvable head is corrupt, not
            // empty: fail loudly instead of seeding an empty transcript.
            let empty = cold_units.as_ref().is_none_or(Vec::is_empty);
            return if empty {
                Ok(Vec::new())
            } else {
                Err(SessionStoreError::Internal(format!(
                    "session {} has units but no resolvable head",
                    session_id.0
                )))
            };
        };
        let ancestors = graph.ancestors(&head_id);
        if ancestors.is_empty() {
            return Err(SessionStoreError::Internal(format!(
                "session {} head {head_id} has no provable chain",
                session_id.0
            )));
        }
        let chain = match cold_units {
            Some(units) => order_by_ids(units, &ancestors)?,
            None => self.load_units_by_ids(session_id, &ancestors).await?,
        };
        let entries = assemble_messages(&chain)?;
        Ok(replay_messages(&entries))
    }

    async fn append_event(&self, event: &RunEvent) -> Result<(), SessionStoreError> {
        // The pump appends while readers (search, list, replay) query the same
        // connection, so this write must take the serialization gate like every
        // other direct statement — an ungated append races readers with a
        // "concurrent use forbidden" storage error.
        let _guard = self.gate.lock().await;
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

    async fn latest_event_for_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<RunEvent>, SessionStoreError> {
        let _guard = self.gate.lock().await;
        let mut rows = self
            .conn
            .query(
                "SELECT event_json FROM run_events
                  WHERE run_id = ?1
                  ORDER BY seq DESC
                  LIMIT 1",
                params![run_id.0.clone()],
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
        let json_str: String = row
            .get(0)
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let event: RunEvent =
            serde_json::from_str(&json_str).map_err(SessionStoreError::Serialization)?;
        Ok(Some(event))
    }

    async fn delete_session(&self, id: &SessionId) -> Result<(), SessionStoreError> {
        let _guard = self.gate.lock().await;
        // Children first, all in one transaction: a crash mid-delete must not
        // leave a visible sessions row with no ledger behind it.
        let mut conn = self.tx_conn().await?;
        let tx = conn
            .transaction()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        let exists: bool = tx
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
        for statement in [
            "DELETE FROM units WHERE session_id = ?1",
            "DELETE FROM session_heads WHERE session_id = ?1",
            "DELETE FROM run_events WHERE session_id = ?1",
            "DELETE FROM sessions WHERE session_id = ?1",
        ] {
            tx.execute(statement, params![id.0.clone()])
                .await
                .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|e| SessionStoreError::Storage(e.to_string()))?;
        // The cache is derived: evict after the commit, and a crash between
        // them self-heals on the next replay from the now-empty ledger.
        self.graphs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(id);
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
        // The query is client-controlled: bind it, never interpolate. The FTS
        // functions are ordinary scalar functions, so placeholders work as
        // their arguments; `?2 IS NULL` keeps the global/targeted scope in one
        // statement. A cap avoids handing an unbounded FTS expression to the
        // engine (its parser rejects oversized queries deep inside the scan).
        const MAX_QUERY_BYTES: usize = 16 * 1024;
        if query.len() > MAX_QUERY_BYTES {
            return Err(SessionStoreError::InvalidQuery(format!(
                "search query is {} bytes; the limit is {MAX_QUERY_BYTES}",
                query.len()
            )));
        }
        let _guard = self.gate.lock().await;
        let limit = i64::from(scope.limit.clamp(1, 1_000));
        let session = scope.session_id.as_ref().map(|id| id.0.clone());
        let sql = "SELECT run_id, session_id, seq, \
                          fts_highlight(event_json, '≫', '≪', ?1) AS snippet, \
                          fts_score(event_json, ?1) AS score \
                   FROM run_events \
                   WHERE fts_match(event_json, ?1) \
                     AND (?2 IS NULL OR session_id = ?2) \
                   ORDER BY score DESC \
                   LIMIT ?3";
        let mut rows = self
            .conn
            .query(sql, params![query.to_string(), session, limit])
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
