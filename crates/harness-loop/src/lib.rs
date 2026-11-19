//! Harness agent loop: model stream → tool dispatch → turn loop.
//!
//! This crate provides the core agent loop — a stateless, async `run()`
//! function that drives one agent run as a dataflow over one [`Turn`] (see
//! `docs/topology-design.md`): a prompt-side [`Topology`] machines the
//! ledger, then a response-side topology (model → append → tool dispatch)
//! finishes the turn. Both phases share the `Processor` trait from
//! `harness-core::topology`.
//!
//! # Capability dependencies — what this crate consumes, and what it does not
//!
//! The loop depends on **contracts, not adapters**:
//!
//! - `harness-core` — the vocabulary: `Model`, `Tool`, `ToolPolicy`,
//!   `ApprovalBroker`, `EventSink`, `Message`, and the `Processor` /
//!   `Topology` types. Everything the loop drives is a core trait object.
//! - `harness-context` — **one** consumer: [`budget`], the turn-boundary
//!   budget gate (`Budget`, `Compactor`).
//! - `futures` / `tokio` (macros only) / `serde_json` — **one** consumer:
//!   [`processors::pump`], the model-stream machinery.
//!
//! Capability adapters never leak in: `harness-mcp` (retrieval / tools over
//! MCP), `harness-model` (concrete model implementations), `harness-tools`
//! are **not** dependencies of this crate. A processor that needs one is
//! implemented by the *host* and registered through the builder's
//! `with_processor`, where that capability is already available. The
//! response-side assembly is exposed as [`LoopDeps::response_topology`] for
//! the same reason: hosts composing their own driver start from the default
//! chain and append nodes instead of the loop growing adapter dependencies.
//!
//! # Design provenance
//!
//! Designed from three reference implementations (see each module's doc for
//! provenance per component):
//!
//! - **pi** (TypeScript) — the hook callback shape and parallel/sequential
//!   tool dispatch mode.
//! - **NanoHarness** (Python) — the policy → approval → execution seam
//!   ordering and the ordered event log.
//! - **harness-rs** (Rust) — the error-to-ToolResult mapping and the
//!   run-level vocabulary.
//!
//! # Crate structure
//!
//! | Module | Responsibility |
//! |---|---|
//! | [`engine`] | The `run()` driver: the turn loop, `LoopDeps`, and the `response_topology` seam. |
//! | [`processors`] | Topology nodes, one file each: prompt side (system ledger, instruction), model (+ stream pump), response side (append gate, tool dispatch). |
//! | [`budget`] | The turn-boundary budget gate — the sole `harness-context` consumer. |
//! | [`dispatch`] | Tool dispatch pipeline (policy → approval → execute → after). |
//! | [`hooks`] | Pi-style config callbacks (`LoopHooks`, `ToolExecutionMode`). |
//! | [`transcript`] | `TranscriptSink`: host persistence of appended messages. |

pub mod budget;
pub mod dispatch;
pub mod engine;
pub mod hooks;
pub mod processors;
pub mod transcript;

pub use engine::{LoopDeps, run};
pub use hooks::{LoopHooks, ToolExecutionMode};
pub use processors::{
    AppendProcessor, InstructionProcessor, ModelProcessor, SystemLedgerProcessor,
    ToolDispatchProcessor,
};
pub use transcript::TranscriptSink;
