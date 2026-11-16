//! The session persistence **port** for the harness: the `SessionStore`
//! and `RecallStore` traits plus the unit-ledger domain model. This crate
//! declares *what* the harness needs from storage — replay, rewind, run
//! provenance, steering kinds, keyword search — without naming any storage
//! engine. Concrete implementations live in adapter crates
//! (`harness-store` is the Turso-backed one).
//!
//! # Crate structure
//!
//! - [`store`] — the [`SessionStore`] trait, [`SessionStoreError`], the
//!   rewind surface ([`HistoryMarker`], [`RewindBoundary`]), and the
//!   canonical [`replay_messages`] function that applies `None`-section
//!   removal to reconstruct a full transcript.
//! - [`unit`] — [`Unit`], [`StoredUnit`], the block ledger row, and the
//!   split/assemble functions that map a `Message` onto units and back,
//!   with the [`integrity_hash`] the adapter verifies on read.
//! - [`recall`] — the [`RecallStore`] search port, [`RecallHit`]/
//!   [`RecallScope`], and [`NoRecall`], the zero-cost default.
//!
//! # Recovery
//!
//! ```text
//! SessionStore::replay_ledger(id)
//!   -> the adapter's read model names the retained unit chain
//!   -> assemble_messages() groups blocks back into messages
//!   -> replay_messages() applies system-section accumulation
//!   -> full transcript ready for the model
//!
//! SessionStore::load_messages(id)
//!   -> every unit ordered by event_order, grouped by message_seq
//! ```
//!
//! # Design provenance
//!
//! pi's `durable/src/harness/` committed-ledger pattern: every mutation is
//! append-only, the ledger is the source of truth for recovery.
//! NanoHarness's `RunContext`/`RunCheckpoint` split (`core/schema.py:193`):
//! identity is immutable and carried by core types; the store holds only the
//! data a crashed daemon re-reads.

pub mod recall;
pub mod store;
pub mod unit;

pub use recall::{NoRecall, RecallHit, RecallScope, RecallStore};
pub use store::{
    HistoryMarker, MessageEntry, RewindBoundary, SessionStore, SessionStoreError, replay_messages,
};
pub use unit::{
    HeadState, StoredUnit, Unit, UnitKind, assemble_messages, event_id, integrity_hash,
    split_message,
};
