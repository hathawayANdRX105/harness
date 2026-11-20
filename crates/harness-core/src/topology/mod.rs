//! The turn dataflow: `Processor` nodes machine a `Turn` along a `Topology`.
//!
//! The vocabulary every run-side crate builds on: the turn is the single
//! source of truth ([`turn`]), a processor is one machining step
//! ([`processor`]), a topology is the machining order and its one-pass
//! driver ([`topology`]). `harness-loop` drives the run; the concrete nodes
//! live in the domain crates (`harness-model`, `harness-tools`,
//! `harness-context`) and the facade (`harness-builder`).
//!
//! Design provenance: Kafka Streams contributes the vocabulary (a topology
//! of processing steps); LLVM contributes the load-bearing invariant — the
//! data is the single source of truth, passed `&mut` between nodes and
//! mutated in place, so exactly one node can write at a time by
//! construction, not by lock. `2-governance` is the naming regime this
//! module obeies.

pub mod chain;
pub mod processor;
pub mod turn;

pub use chain::Topology;
pub use processor::{Processor, Route};
pub use turn::{Turn, TurnArtifacts};
