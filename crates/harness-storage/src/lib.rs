//! Turso-backed storage for the harness session port.
//!
//! This crate is the **adapter** half of the session persistence split:
//! [`harness_session`] declares the [`SessionStore`](harness_session::SessionStore)
//! and [`RecallStore`](harness_session::RecallStore) ports plus the unit-ledger
//! domain model; this crate implements them over Turso (libsql) with an
//! in-memory petgraph shadow and a tantivy-backed FTS index.
//!
//! # The boundary
//!
//! No storage-engine type crosses this crate's public API: callers hold
//! `Arc<dyn SessionStore>` and `Arc<dyn RecallStore>`, obtained from
//! [`open`], and never name turso, petgraph, or tantivy. The concrete
//! [`TursoSessionStore`] stays public only as the construction seam for
//! hosts and tests that need the inherent diagnostics (`load_units`,
//! `load_head`); its engine internals are private.
//!
//! # Assembly
//!
//! ```no_run
//! # async fn demo() -> Result<(), harness_session::SessionStoreError> {
//! let handles = harness_storage::open("sessions.db").await?;
//! let store: std::sync::Arc<dyn harness_session::SessionStore> = handles.session;
//! let recall: std::sync::Arc<dyn harness_session::RecallStore> = handles.recall;
//! # Ok(())
//! # }
//! ```
//!
//! [`open`] runs the versioned migrations (see `migrations/001_baseline.sql`)
//! inside one transaction per migration, enables WAL, and wires the recall
//! handle to the same connection and write gate as the store.
//!
//! # Layout
//!
//! - [`turso`] — [`TursoSessionStore`]: the connection model, the ledger
//!   appends, rewind/markers, the run-event log, and the migration runner.
//! - [`graph`] — [`GraphIndex`], the derived read model over unit ids
//!   (never persisted; ferrite's 0106 lesson).
//! - [`fts`] — the `RecallStore` implementation over the run-event FTS
//!   index (private; reached through the port).
//!
//! # Design provenance
//!
//! The connection model is ferrite's (direct shared connection + one
//! transaction connection, serialized by a gate); the committed-ledger
//! discipline is pi's `durable/src/harness/`.

mod fts;
pub mod graph;
mod turso;

pub use fts::TursoFtsRecall;
pub use graph::GraphIndex;
pub use turso::TursoSessionStore;

use std::sync::Arc;

use harness_session::{RecallStore, SessionStore};

/// The assembled storage stack, behind the port traits.
///
/// Both handles share one engine: the recall search reads the same
/// connection the ledger writes, serialized by the same gate. Callers hand
/// the fields to whatever consumes the ports (the daemon handler, tests)
/// without naming the engine.
pub struct StoreHandles {
    /// The session ledger behind the [`SessionStore`] port.
    pub session: Arc<dyn SessionStore>,
    /// The run-event search behind the [`RecallStore`] port.
    pub recall: Arc<dyn RecallStore>,
}

/// Open (or create) a session database at `path` and assemble the storage
/// stack over it: migrations, WAL, ledger store, and the recall handle.
///
/// This is the composition seam for hosts: the return value carries only
/// the port traits.
pub async fn open(
    path: impl AsRef<std::path::Path>,
) -> Result<StoreHandles, harness_session::SessionStoreError> {
    let store = TursoSessionStore::open(path).await?;
    let recall = store.recall();
    Ok(StoreHandles {
        session: Arc::new(store),
        recall: Arc::new(recall),
    })
}
