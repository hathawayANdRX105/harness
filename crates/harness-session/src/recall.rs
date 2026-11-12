//! Recall: full-text search over persisted run events.
//!
//! [`RecallStore`] is the seam — implementations answer keyword queries over
//! whatever index they maintain. The Turso backend builds a Tantivy-backed
//! `USING fts` index on the event payload (turso 0.8 has no SQLite FTS5
//! module; the `fts` index method is the engine's own answer, enabled by the
//! `experimental_index_method` builder flag). [`NoRecall`] is the zero-cost
//! default: no index, no hits — recall is an opt-in capability, never a
//! hidden dependency.

use crate::store::SessionStoreError;
use harness_core::run::{RunId, SessionId};

/// One search hit: where the match lives and a highlighted snippet.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RecallHit {
    /// The run the matching event belongs to.
    pub run_id: RunId,
    /// The session the run belongs to.
    pub session_id: SessionId,
    /// The event's sequence number within its run.
    pub seq: u64,
    /// The matched event payload, with the query terms highlighted.
    pub snippet: String,
    /// Engine-reported relevance score (BM25-style; lower is more relevant
    /// for Tantivy's default output).
    pub score: f64,
}

/// Narrow a search to a session, or search everything.
#[derive(Debug, Clone, Default)]
pub struct RecallScope {
    /// Restrict hits to this session when set.
    pub session_id: Option<SessionId>,
    /// Maximum number of hits returned.
    pub limit: u32,
}

impl RecallScope {
    /// A bounded, session-wide search scope.
    pub fn all(limit: u32) -> Self {
        Self {
            session_id: None,
            limit,
        }
    }
}

/// The recall seam: keyword search over persisted run events.
#[async_trait::async_trait]
pub trait RecallStore: Send + Sync + 'static {
    /// Search run events for `query` within `scope`, most relevant first.
    async fn search(
        &self,
        query: &str,
        scope: &RecallScope,
    ) -> Result<Vec<RecallHit>, SessionStoreError>;
}

/// The zero-cost default: no index, every search returns no hits.
///
/// Used by hosts that did not opt into recall — the trait seam keeps the
/// capability pluggable without an `Option` dance at every call site.
pub struct NoRecall;

#[async_trait::async_trait]
impl RecallStore for NoRecall {
    async fn search(
        &self,
        _query: &str,
        _scope: &RecallScope,
    ) -> Result<Vec<RecallHit>, SessionStoreError> {
        Ok(Vec::new())
    }
}
