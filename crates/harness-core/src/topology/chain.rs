//! The machining order: a `Topology` is a chain of `Processor` nodes plus
//! the one-pass driver (`step`).
//!
//! The run-level cycle — turn boundary, prompt pass, response pass, repeat
//! — lives in the driver (`harness_loop::run`), not here: machining an
//! agent turn is a one-directional pass, and the outer loop is what makes
//! it cyclic (Kafka topologies are acyclic for the same reason).

use std::sync::Arc;

use crate::run::RunStopReason;
use crate::topology::processor::{Processor, Route};
use crate::topology::turn::Turn;

/// The per-turn machining order: a chain of `Processor` nodes.
pub struct Topology {
    nodes: Vec<Arc<dyn Processor>>,
}

impl Topology {
    /// An empty topology; register nodes with `node`.
    #[must_use]
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Register one node. The driver resolves `Goto` targets against
    /// registered names.
    #[must_use]
    pub fn node(mut self, processor: Arc<dyn Processor>) -> Self {
        self.nodes.push(processor);
        self
    }

    /// Registered node count — the driver's sanity check and tests'
    /// unit surface.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether no node is registered yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Registered node names, in machining order — `Goto` targets resolve
    /// against this, and hosts surface it in debug output.
    #[must_use]
    pub fn names(&self) -> Vec<&'static str> {
        self.nodes.iter().map(|node| node.name()).collect()
    }

    /// Run one pass of the chain over the turn: every node in registration
    /// order, stopping at the first `Done`.
    ///
    /// `Goto(name)` jumps to the next node with that name (a forward or
    /// backward jump — the name is what matters, not the direction) and
    /// machining continues from there. A name with no registered node is a
    /// topology authoring bug, and the run fails loudly instead of
    /// silently skipping the routing node's intent.
    pub async fn step(&self, turn: &mut Turn) -> Route {
        let mut next = 0usize;
        while let Some(node) = self.nodes.get(next) {
            match node.process(turn).await {
                Route::Next => next += 1,
                Route::Goto(target) => match self.nodes.iter().position(|n| n.name() == target) {
                    Some(index) => next = index,
                    None => {
                        return Route::Done(RunStopReason::Error {
                            message: format!("topology references unknown node `{target}`"),
                        });
                    }
                },
                Route::Done(reason) => return Route::Done(reason),
            }
        }
        Route::Next
    }
}

impl std::fmt::Debug for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&'static str> = self.nodes.iter().map(|node| node.name()).collect();
        f.debug_struct("Topology").field("nodes", &names).finish()
    }
}

impl Clone for Topology {
    fn clone(&self) -> Self {
        // Nodes are `Arc`-shared: cloning a topology shares the processors
        // across runs rather than copying them (processors never mutate).
        Self {
            nodes: self.nodes.clone(),
        }
    }
}

impl Default for Topology {
    fn default() -> Self {
        Self::new()
    }
}
