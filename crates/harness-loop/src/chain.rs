//! The machining order: a `Chain` is an ordered set of `Processor` nodes
//! plus the one-pass driver (`step`).
//!
//! This is the assembly surface — the factory line. The run-level cycle
//! (turn boundary → prompt pass → response pass → repeat) lives in
//! [`crate::engine::run`], not here: machining one agent turn is a
//! one-directional pass, and the outer loop is what makes it cyclic
//! (Kafka topologies are acyclic for the same reason).
//!
//! The trait vocabulary (`Processor`, `Route`, `Turn`) lives in
//! `harness-core`; the chain itself is assembly, so it lives here with the
//! driver, not in the contract crate.

use std::sync::Arc;

use harness_core::run::RunStopReason;
use harness_core::topology::{Processor, Route, Turn};

/// The per-turn machining order: a chain of `Processor` nodes.
pub struct Chain {
    nodes: Vec<Arc<dyn Processor>>,
}

impl Chain {
    /// An empty chain; register nodes with [`Chain::node`].
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
    /// chain authoring bug, and the run fails loudly instead of silently
    /// skipping the routing node's intent. A routing *cycle* — a node that
    /// always re-jumps — is bounded too: the pass dies with an error once
    /// it clearly stopped making forward progress (see the hop budget in
    /// the loop), and cancellation is re-checked every hop.
    pub async fn step(&self, turn: &mut Turn) -> Route {
        let mut next = 0usize;
        // A `Goto` cycle (a router that always re-jumps) would spin forever
        // inside one turn: the run's cancel token is only read at turn
        // boundaries otherwise, so the pass bounds itself — a generous hop
        // budget that no honest router approaches, checked every hop
        // alongside cancellation so a hung pass still dies on cancel.
        let max_hops = self.nodes.len().max(8) * 16;
        let mut hops = 0usize;
        while let Some(node) = self.nodes.get(next) {
            hops += 1;
            if hops > max_hops {
                return Route::Done(RunStopReason::Error {
                    message: format!(
                        "chain exceeded {max_hops} hops — suspected routing cycle at node `{}`",
                        node.name()
                    ),
                });
            }
            if turn.control.is_cancelled() {
                return Route::Done(RunStopReason::Cancelled);
            }
            match node.process(turn).await {
                Route::Next => next += 1,
                Route::Goto(target) => match self.nodes.iter().position(|n| n.name() == target) {
                    Some(index) => next = index,
                    None => {
                        return Route::Done(RunStopReason::Error {
                            message: format!("chain references unknown node `{target}`"),
                        });
                    }
                },
                Route::Done(reason) => return Route::Done(reason),
            }
        }
        Route::Next
    }
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&'static str> = self.nodes.iter().map(|node| node.name()).collect();
        f.debug_struct("Chain").field("nodes", &names).finish()
    }
}

impl Clone for Chain {
    fn clone(&self) -> Self {
        // Nodes are `Arc`-shared: cloning a chain shares the processors
        // across runs rather than copying them (processors never mutate).
        Self {
            nodes: self.nodes.clone(),
        }
    }
}

impl Default for Chain {
    fn default() -> Self {
        Self::new()
    }
}
