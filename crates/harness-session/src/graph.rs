//! In-memory petgraph index derived from the `run_events` ledger.
//!
//! The graph is a **purely in-memory derived read model**: the Turso
//! `run_events` ledger is the source of truth, and the graph is rebuilt from
//! that ledger on open (one O(N) pass) and updated on each append. Nothing is
//! written to disk and nothing is persisted — dropping the index loses the
//! shadow, and a fresh one is rebuilt from the ledger. This is ferrite's
//! 0106 lesson: a persistent graph file stales silently, so the graph must
//! never be the source of truth.
//!
//! Nodes are **units**: one node per run event that carries a user prompt or
//! an agent response, keyed by `(run_id, seq)` so the event's payload is
//! looked up via the store's [`crate::SessionStore::events_for_run`].
//! Sequence edges link consecutive units within a run (append order by seq);
//! a parent edge links each unit back to the unit it continues.
//!
//! [`GraphIndex`] is the single implementation: the engine stays inside this
//! crate behind the struct, so it can be replaced without touching callers.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use petgraph::Direction;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;

use harness_core::event::{RunEvent, RunEventKind};
use harness_core::run::RunId;

/// One unit of the session ledger, as the graph index stores it.
///
/// Only the references needed for topology walks are kept: message payloads
/// never enter the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphUnit {
    /// Run that produced the unit.
    pub run_id: RunId,
    /// Monotonic sequence number within the run.
    pub seq: u64,
    /// Role of the unit: `"assistant"` for content-carrying events
    /// (`TextDelta`, `ToolCall`, …), `"system"` for lifecycle events.
    pub role: String,
}

/// One edge between units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EdgeKind {
    /// Consecutive units within a run, in append (seq) order.
    Sequence,
    /// The unit a given unit continues (its prompt or predecessor).
    Parent,
}

/// The mutable state behind [`GraphIndex`].
///
/// The graph and its lookup maps are guarded together so they can never
/// drift apart. A poisoned lock is recovered from: the graph is a rebuildable
/// derived index, so a panic in another caller must not make this instance
/// permanently unusable.
struct Inner {
    graph: StableDiGraph<GraphUnit, EdgeKind>,
    by_key: HashMap<(RunId, u64), NodeIndex>,
    last_per_run: HashMap<RunId, NodeIndex>,
}

impl Inner {
    fn new() -> Self {
        Self {
            graph: StableDiGraph::new(),
            by_key: HashMap::new(),
            last_per_run: HashMap::new(),
        }
    }
}

/// The in-memory graph index: petgraph `StableDiGraph` of units derived from
/// the `run_events` ledger.
pub struct GraphIndex {
    inner: Mutex<Inner>,
}

impl GraphIndex {
    /// Creates an empty graph index.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
        }
    }

    /// Recovers from a poisoned lock (the graph is a rebuildable shadow).
    fn inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rebuild the graph from the ledger: one O(N) pass over `events`.
    ///
    /// Clears any existing graph state, then appends each event in seq order.
    /// Callers pass events ordered by seq (the store's `events_for_run`
    /// guarantees this); events are re-sorted here so the rebuilt graph is
    /// identical to incremental appends over the same ledger.
    pub fn rebuild_from_ledger(&self, events: &[RunEvent]) {
        let mut inner = self.inner();
        inner.graph.clear();
        inner.by_key.clear();
        inner.last_per_run.clear();

        let mut ordered: Vec<&RunEvent> = events.iter().collect();
        ordered.sort_by_key(|a| a.seq);
        for event in ordered {
            inner.append_unit(event);
        }
    }

    /// Update the graph with one newly persisted event: add a unit node, a
    /// sequence edge from this run's previous unit, and a parent edge back to
    /// it.
    pub fn append_event(&self, event: &RunEvent) {
        let mut inner = self.inner();
        inner.append_unit(event);
    }

    /// Every unit of `run_id`, in ascending seq order.
    pub fn units_for_run(&self, run_id: &RunId) -> Vec<GraphUnit> {
        let inner = self.inner();
        let mut units: Vec<GraphUnit> = inner
            .graph
            .node_weights()
            .filter(|unit| unit.run_id == *run_id)
            .cloned()
            .collect();
        units.sort_by_key(|a| a.seq);
        units
    }

    /// The chain of units starting at `(run_id, seq)`, following sequence
    /// edges forward to the end of the run. Returns an empty vec when no
    /// unit exists at that seq.
    pub fn chain_from(&self, run_id: &RunId, seq: u64) -> Vec<GraphUnit> {
        let inner = self.inner();
        let Some(&start) = inner.by_key.get(&(run_id.clone(), seq)) else {
            return Vec::new();
        };
        let mut chain = Vec::new();
        let mut current = Some(start);
        while let Some(node) = current {
            let unit = inner.graph.node_weight(node).cloned();
            match unit {
                Some(u) => chain.push(u),
                None => break,
            }
            current = inner
                .graph
                .edges_directed(node, Direction::Outgoing)
                .find(|edge| matches!(*edge.weight(), EdgeKind::Sequence))
                .map(|edge| edge.target());
        }
        chain
    }
}

impl Inner {
    /// Append one unit node plus its sequence and parent edges.
    fn append_unit(&mut self, event: &RunEvent) {
        let key = (event.run_id.clone(), event.seq);
        if self.by_key.contains_key(&key) {
            return; // already indexed (idempotent, like the ledger append)
        }
        let node = self.graph.add_node(GraphUnit {
            run_id: event.run_id.clone(),
            seq: event.seq,
            role: role_of(&event.kind),
        });
        self.by_key.insert(key, node);

        // Sequence edge: previous unit of this run -> this unit.
        // Parent edge: this unit -> the unit it continues.
        if let Some(&prev) = self.last_per_run.get(&event.run_id) {
            self.graph.add_edge(prev, node, EdgeKind::Sequence);
            self.graph.add_edge(node, prev, EdgeKind::Parent);
        }
        self.last_per_run.insert(event.run_id.clone(), node);
    }
}

/// Role of a unit, derived from the run event kind.
fn role_of(kind: &RunEventKind) -> String {
    match kind {
        RunEventKind::TextDelta { .. }
        | RunEventKind::ToolCall(_)
        | RunEventKind::ToolStarted { .. }
        | RunEventKind::ToolFinished { .. } => "assistant".to_string(),
        _ => "system".to_string(),
    }
}

impl Default for GraphIndex {
    fn default() -> Self {
        Self::new()
    }
}
