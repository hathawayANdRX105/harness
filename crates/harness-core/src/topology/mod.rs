//! The turn dataflow vocabulary: `Processor` nodes machine a `Turn`.
//!
//! The **contract** layer: the turn is the single source of truth
//! ([`turn`]), a processor is one machining step ([`processor`]). The
//! assembly — the ordered chain of nodes and its one-pass driver — is
//! assembly-surface content, so it lives in `harness-loop` (`Chain`), not
//! here; this crate defines only what every run-side crate must agree on.
//!
//! `harness-loop` drives the run; the concrete processors live in the
//! domain crates (`harness-model`, `harness-tools`, `harness-context`) and
//! the facade (`harness-builder`).
//!
//! Design provenance: Kafka Streams contributes the vocabulary (a topology
//! of processing steps); LLVM contributes the load-bearing invariant — the
//! data is the single source of truth, passed `&mut` between nodes and
//! mutated in place, so exactly one node can write at a time by
//! construction, not by lock. `2-governance` is the naming regime this
//! module obeys.

pub mod processor;
pub mod turn;

pub use processor::{Processor, Route};
pub use turn::{Turn, TurnArtifacts};
