//! Processor nodes for the agent loop's topology — one file per node,
//! mirroring gateway-gate's split (each gate in its own file, shared
//! vocabulary in the trait crate).
//!
//! Three phases of nodes:
//!
//! - prompt-side ([`system_ledger`], [`instruction`]): what the legacy
//!   `prepare_request` hook did — the system ledger leads the transcript,
//!   the instruction is appended once;
//! - the model node ([`model`] + [`pump`]): the one place that holds the
//!   model stream;
//! - response-side ([`append`], [`tool_dispatch`]): append the assistant
//!   content and gate the run, then drive the tool batch through the
//!   policy → approval → execute → after chain.
//!
//! The trait itself lives in `harness-core::topology` — implementations
//! stay here because they orchestrate loop-owned seams (`LoopHooks`,
//! dispatch). A node that needs a capability adapter (MCP retrieval,
//! a model registry) is registered by the *host* through the builder's
//! `with_processor`; this crate never depends on those adapter crates.
//!
//! Response-side deviation (slice 5, see docs/topology-design.md §6): the
//! six-step dispatch chain interleaves per tool call in sequential mode,
//! so it stays one batch-level [`ToolDispatchProcessor`]
//! rather than three stage nodes — the split follows once per-call state
//! rides `TurnArtifacts`.

mod append;
mod instruction;
mod model;
mod pump;
mod system_ledger;
mod tool_dispatch;

pub use append::AppendProcessor;
pub use instruction::InstructionProcessor;
pub use model::ModelProcessor;
pub use system_ledger::SystemLedgerProcessor;
pub use tool_dispatch::ToolDispatchProcessor;

// Shared with the driver's error backfill: the batch append belongs with
// the dispatch node.
pub(crate) use tool_dispatch::push_tool_results;
