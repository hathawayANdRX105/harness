//! Harness agent loop: model stream → tool dispatch → turn loop.
//!
//! This crate provides the core agent loop — a stateless, async `run()`
//! function that drives one agent run to completion. It consumes the contracts
//! from `harness-core` (Model, Tool, ToolPolicy, ApprovalBroker, EventSink,
//! RunControl) and the pi-style config callbacks from [`hooks::LoopHooks`].
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
//! | [`engine`] | The `run()` function and the turn loop. |
//! | [`dispatch`] | Tool dispatch pipeline (policy → approval → execute → after). |
//! | [`hooks`] | Pi-style config callbacks (`LoopHooks`, `ToolExecutionMode`). |
//! | [`transcript`] | `TranscriptSink`: host persistence of appended messages. |

pub mod dispatch;
pub mod engine;
pub mod hooks;
pub mod transcript;

pub use engine::{LoopDeps, run};
pub use hooks::{LoopHooks, ToolExecutionMode};
pub use transcript::TranscriptSink;
