//! The machining step: one `Processor`, one `Route`.
//!
//! A processor holds only the strategy — config, handles, limiters — never
//! per-run data: everything stateful arrives as the `&mut Turn` and leaves
//! with it (LLVM's pass model, not a stateful service).
//!
//! Naming: `process` is the Kafka Streams verb, kept over the
//! higher-scoring `run` in the naming audit (see `2-governance/01-expression.md`).
//! The `process` bad-signal exemption is repaid by the signature saying
//! what/who/result and by every implementation's module header stating what
//! it machines.

use async_trait::async_trait;

use crate::run::RunStopReason;
use crate::topology::Turn;

/// One machining step in the topology. Implementations hold only the
/// strategy — config, handles, limiters — never per-run data: everything
/// stateful arrives as the `&mut Turn` and leaves with it.
#[async_trait]
pub trait Processor: Send + Sync + 'static {
    /// The node's name: `Goto` targets address nodes by it, and it labels
    /// tracing spans and metric points.
    fn name(&self) -> &'static str;
    /// Machine the turn once and report where the run goes next. `Next`
    /// follows the default edge; `Goto` takes the conditional branch;
    /// `Done` ends the run-level loop.
    async fn process(&self, turn: &mut Turn) -> Route;
}

/// Where a processor sends the run next: the graph's routing outcome,
/// gateway's `StageOutcome` minus the HTTP response (this graph machines a
/// turn, not a request).
pub enum Route {
    /// Continue along the node's default edge.
    Next,
    /// Jump to a named node — the conditional branch. "No tool calls → end
    /// the turn" is one `Goto`, not a hardcoded short-circuit in the driver.
    Goto(&'static str),
    /// End the run-level loop with a reason (Completed / Cancelled / Error).
    Done(RunStopReason),
}
