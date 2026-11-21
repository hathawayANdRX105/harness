//! Harness agent loop: the driving and assembly layer over the turn
//! dataflow.
//!
//! This crate provides the stateless, async [`run()`] that drives one agent
//! run: one `Turn` (the message ledger, tools, control and per-turn
//! artifacts — the single source of truth) machined by a [`Chain`] of
//! `Processor` nodes, assembled from domain-owned implementations.
//!
//! # Layering — this crate owns the driver and the chain, not the nodes
//!
//! | Phase | Nodes | Owner crate |
//! |---|---|---|
//! | prompt | system ledger, instruction | `harness-builder` (the facade's default nodes) |
//! | prompt | compaction | `harness-context` (owns `Budget` / `Compactor`) |
//! | response | model stream node + append gate | `harness-model` (owns the model stream) |
//! | response | tool dispatch | `harness-tools` (owns tool execution) |
//!
//! This crate is the **assembly surface** for the response phase: the driver
//! composes the default chain (model → append gate → tool dispatch) from
//! the domain-owned nodes at **run time**, over whatever sinks the run
//! carries — the scheduler and the daemon swap `event_sink` /
//! `transcript_sink` per run, and nodes capture sinks at construction, so
//! assembly must happen when the run starts, not when the host builds. The
//! prompt phase is assembled by `harness-builder` (the facade); hosts
//! extend either through `with_processor`.
//!
//! # Design provenance
//!
//! pi (TypeScript) contributes the callback shape and parallel/sequential
//! tool dispatch mode; NanoHarness (Python) the policy → approval →
//! execution seam ordering and the ordered event log; harness-rs (Rust) the
//! error-to-ToolResult mapping. See `engine.rs` for the per-component
//! provenance.

pub mod chain;
pub mod engine;

pub use chain::Chain;
pub use engine::{LoopDeps, run};
