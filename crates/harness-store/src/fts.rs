//! The `RecallStore` implementation over the run-event FTS index.
//!
//! Private to the adapter: hosts reach it through the
//! `RecallStore` port, assembled in [`crate::open`]. The handle shares the
//! store's connection and write gate (direct statements only — see the
//! store module docs).

use std::sync::Arc;

use harness_core::run::{RunId, SessionId};
use harness_session::{RecallHit, RecallScope, RecallStore, SessionStoreError};
use tokio::sync::Mutex;

use turso::{Connection, params};

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

impl TursoFtsRecall {
    /// Assemble the handle over the store's shared connection and gate.
    pub(crate) fn new(conn: Arc<Connection>, gate: Arc<Mutex<()>>) -> Self {
        Self { conn, gate }
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
