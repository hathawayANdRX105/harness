//! Session persistence for the harness: the `SessionStore` port trait and its
//! Turso (libsql) default implementation.
//!
//! # Crate structure
//!
//! - [`store`] — the [`SessionStore`] trait, [`SessionStoreError`], and the
//!   canonical [`replay_messages`] function that applies `None`-section
//!   removal to reconstruct a full transcript.
//! - [`turso`] — [`TursoSessionStore`], the default Turso-backed
//!   implementation (ferrite's proven connection model, idempotent ledger).
//! - [`unit`] — [`Unit`], [`StoredUnit`], the block ledger row, and the
//!   split/assemble functions that map a `Message` onto units and back.
//! - [`graph`] — [`GraphIndex`], the in-memory petgraph read model derived
//!   from the `units` ledger (never persisted): units are nodes, the parent
//!   chain is the unit-tree edge, and each scope has a HEAD.
//!
//! The read-model types (`Unit`, `StoredUnit`, `UnitKind`, `GraphIndex`,
//! `assemble_messages`) are public because integration tests drive them and
//! the daemon-side context assembler will consume the graph directly; the
//! store internals (head decoding, SQL helpers) stay private.
//!
//! # Three persistent stores
//!
//! 1. **Unit ledger** (`units`) — one row per conversational block (prompt,
//!    assistant text/reasoning, tool call, tool result, system section delta).
//!    The index columns (ids, branch, epoch, order, role, kind, parent) and
//!    the content (`payload_json`) live in the same Turso row; the graph holds
//!    only the index columns.
//! 2. **Session heads** (`session_heads`) — the HEAD pointer and the last
//!    order/message sequence per session scope. Appends move it in the same
//!    transaction that inserts units.
//! 3. **RunEvent log** (`run_events`) — ordered events keyed by `run_id` with
//!    a monotonic `seq`. Recovery detects gaps from missing seqs; recall's
//!    full-text index is built on it.
//!
//! # Recovery
//!
//! ```text
//! SessionStore::replay_ledger(id)
//!   -> graph HEAD -> ancestors() gives unit ids root..head
//!   -> load the payload rows for those ids from Turso
//!   -> assemble_messages() groups blocks back into messages
//!   -> replay_messages() applies system-section accumulation
//!   -> full transcript ready for the model
//!
//! SessionStore::load_messages(id)
//!   -> read every unit ordered by event_order
//!   -> assemble_messages() (the direct, graph-free path)
//!
//! SessionStore::events_for_run(id) + check_event_gaps(id)
//!   -> load events ordered by seq
//!   -> find missing seqs -> consumer re-emits them
//! ```
//!
//! # Design provenance
//!
//! pi's `durable/src/harness/` committed-ledger pattern: every mutation is
//! append-only, the ledger is the source of truth for recovery. NanoHarness's
//! `RunContext`/`RunCheckpoint` split (`core/schema.py:193-204`): identity
//! is immutable and carried by core types; the store holds only the data that
//! a crashed daemon re-reads.

pub mod graph;
pub mod recall;
pub mod store;
pub mod turso;
pub mod unit;

pub use graph::GraphIndex;
pub use recall::{NoRecall, RecallHit, RecallScope, RecallStore};
pub use store::{MessageEntry, SessionStore, SessionStoreError, replay_messages};
pub use turso::{TursoFtsRecall, TursoSessionStore};
pub use unit::{HeadState, StoredUnit, Unit, UnitKind, assemble_messages};
