//! Session persistence for the harness: the `SessionStore` port trait and its
//! SQLite default implementation.
//!
//! # Crate structure
//!
//! - [`store`] — the [`SessionStore`] trait, [`SessionStoreError`], and the
//!   canonical [`replay_messages`] function that applies `None`-section
//!   removal to reconstruct a full transcript.
//! - [`sqlite`] — [`SqliteSessionStore`], the default SQLite-backed
//!   implementation using `rusqlite` with `bundled`.
//!
//! # Two persistent objects
//!
//! 1. **Message ledger** — a `Vec<Message>` recording every turn in the
//!    conversation. System messages are a named-section map; replay
//!    accumulates them (including `None` removals) to reconstruct the prompt.
//! 2. **RunEvent log** — ordered events keyed by `run_id` with a monotonic
//!    `seq`. Recovery detects gaps from missing seqs.
//!
//! # Recovery
//!
//! ```text
//! SessionStore::replay_ledger(id)
//!   → load all messages ordered by seq
//!   → replay_messages() applies system-section accumulation
//!   → returns full transcript ready for the model
//!
//! SessionStore::events_for_run(id) + check_event_gaps(id)
//!   → load events ordered by seq
//!   → find missing seqs → consumer re-emits them
//! ```
//!
//! # Design provenance
//!
//! pi's `durable/src/harness/` committed-ledger pattern: every mutation is
//! append-only, the ledger is the source of truth for recovery. NanoHarness's
//! `RunContext`/`RunCheckpoint` split (`core/schema.py:193-204`): identity
//! is immutable and carried by core types; the store holds only the data that
//! a crashed daemon re-reads.

pub mod sqlite;
pub mod store;

pub use sqlite::SqliteSessionStore;
pub use store::{MessageEntry, SessionStore, SessionStoreError, replay_messages};
