//! Context management and compaction for the harness loop.
//!
//! Defines the compaction vocabulary — [`Compactor`], [`Budget`],
//! [`CompactionStage`], [`CompactionCheckpoint`] — plus the
//! [`BudgetBlocked`] stop reason that the loop produces when a round is
//! refused before the provider call.
//!
//! # Design provenance
//!
//! - **harness-rs** (`crates/harness-core/src/compactor.rs`): the
//!   `CompactionStage` threshold ladder (five levels at 60/70/80/90/95%)
//!   and the `Budget` type.
//! - **pi** (`packages/durable/src/harness/{context.ts,compaction.ts}`):
//!   compaction as a cancellable, checkpointed Task — the
//!   [`CompactionCheckpoint`] type (head marker / tail entry for resume).
//! - pi's core layer prunes via `transformContext`; actual budget lives
//!   in the coding-agent's compaction layer — the same layered split in
//!   this crate.
//!
//! # Dependency discipline
//!
//! This crate depends on `harness-core` (messages, model info) and
//! foundational crates (`serde`, `async-trait`, `futures-core`,
//! `tokio-util`). It does **not** depend on `harness-config`,
//! `harness-mcp`, `harness-tools`, or the loop/daemon.

mod budget;
mod checkpoint;
mod compactor;
mod stage;
mod stop_reason;

pub use budget::Budget;
pub use checkpoint::CompactionCheckpoint;
pub use compactor::{CompactionResult, Compactor};
pub use stage::{CompactionStage, default_stages};
pub use stop_reason::BudgetBlocked;
