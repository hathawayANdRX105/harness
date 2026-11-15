//! In-memory petgraph index derived from the session unit ledger.
//!
//! The graph is a **purely in-memory derived read model**: Turso's `units`
//! table is the source of truth, and the graph is rebuilt from it on demand
//! (one O(N) pass) and updated on each append. Nothing is written to disk —
//! dropping the index loses the shadow, and a fresh one is rebuilt from the
//! ledger. This is ferrite's lesson: a persistent graph stales silently, so it
//! must never become the source of truth.
//!
//! Nodes are [`Unit`]s (index fields only; payloads stay in Turso), one parent
//! edge per unit, and a `HEAD` pointer per `(session, branch, epoch)` scope.
//! [`GraphIndex::ancestors`] is the read the context assembler uses: walk the
//! parent chain from `HEAD` to the root and return the unit ids in root→head
//! order. Payloads are fetched by those ids afterwards.
//!
//! `petgraph` stays inside this crate: callers use [`GraphIndex`] methods, so
//! the engine can be replaced without touching the store or the daemon.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use harness_core::run::SessionId;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};

use crate::unit::Unit;

/// One edge: `child -> parent`, the unit-tree relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EdgeKind {
    Parent,
}

/// The scope a head pointer belongs to.
type Scope = (SessionId, String, i64);

struct Inner {
    graph: StableDiGraph<Unit, EdgeKind>,
    by_id: HashMap<String, NodeIndex>,
    /// Child unit id → parent unit id. The graph edge mirrors this; the map is
    /// the walk the assembler needs without a graph traversal allocation.
    parents: HashMap<String, String>,
    /// `HEAD` per scope: the newest unit of the retained chain.
    heads: HashMap<Scope, String>,
}

impl Inner {
    fn new() -> Self {
        Self {
            graph: StableDiGraph::new(),
            by_id: HashMap::new(),
            parents: HashMap::new(),
            heads: HashMap::new(),
        }
    }

    /// Upsert one unit node; returns whether it was new.
    fn upsert(&mut self, unit: &Unit) -> bool {
        if self.by_id.contains_key(&unit.event_id) {
            return false;
        }
        let node = self.graph.add_node(unit.clone());
        self.by_id.insert(unit.event_id.clone(), node);
        true
    }

    /// Link a unit to its parent when both are present.
    fn link_parent(&mut self, unit: &Unit) {
        let Some(parent_id) = unit.parent_event_id.as_deref() else {
            return;
        };
        let (Some(&child), Some(&parent)) =
            (self.by_id.get(&unit.event_id), self.by_id.get(parent_id))
        else {
            return;
        };
        self.graph.add_edge(child, parent, EdgeKind::Parent);
        self.parents
            .insert(unit.event_id.clone(), parent_id.to_string());
    }

    fn scope(unit: &Unit) -> Scope {
        (unit.session_id.clone(), unit.branch_id.clone(), unit.epoch)
    }
}

/// The in-memory unit graph.
pub struct GraphIndex {
    inner: Mutex<Inner>,
}

impl GraphIndex {
    /// A fresh, empty graph.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
        }
    }

    /// Recover from a poisoned lock: the graph is rebuildable, so a panic in
    /// another caller must not make this instance permanently unusable.
    fn inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rebuild the graph from the ledger: clears the previous state, upserts
    /// every unit, links parent edges, and derives each scope's head as its
    /// highest `event_order` unit.
    ///
    /// A caller holding an authoritative head (the store's `session_heads`
    /// row) should call [`set_head`](Self::set_head) afterwards: derivation is
    /// only a fallback for a ledger read without the head row.
    pub fn rebuild(&self, units: &[Unit]) {
        let mut inner = self.inner();
        *inner = Inner::new();
        for unit in units {
            inner.upsert(unit);
        }
        for unit in units {
            inner.link_parent(unit);
        }
        let mut newest: HashMap<Scope, &Unit> = HashMap::new();
        for unit in units {
            let scope = Inner::scope(unit);
            let entry = newest.entry(scope).or_insert(unit);
            if unit.event_order > entry.event_order {
                *entry = unit;
            }
        }
        for (scope, unit) in newest {
            inner.heads.insert(scope, unit.event_id.clone());
        }
    }

    /// Append newly persisted units and move the scope head to `head_event_id`.
    pub fn append(&self, units: &[Unit], head_event_id: &str) {
        let mut inner = self.inner();
        for unit in units {
            inner.upsert(unit);
        }
        for unit in units {
            inner.link_parent(unit);
        }
        if let Some(last) = units.last() {
            inner
                .heads
                .insert(Inner::scope(last), head_event_id.to_string());
        }
    }

    /// Point a scope's head at `event_id`.
    pub fn set_head(&self, session_id: &SessionId, branch_id: &str, epoch: i64, event_id: &str) {
        let mut inner = self.inner();
        inner.heads.insert(
            (session_id.clone(), branch_id.to_string(), epoch),
            event_id.to_string(),
        );
    }

    /// The scope's head unit id, when one is set.
    pub fn head(&self, session_id: &SessionId, branch_id: &str, epoch: i64) -> Option<String> {
        let inner = self.inner();
        inner
            .heads
            .get(&(session_id.clone(), branch_id.to_string(), epoch))
            .cloned()
    }

    /// The unit ids from the scope root to `event_id`, inclusive, in
    /// root→head order.
    ///
    /// An unknown id yields an empty chain — the same "no proof" answer the
    /// assembler treats as an empty context.
    pub fn ancestors(&self, event_id: &str) -> Vec<String> {
        let inner = self.inner();
        if !inner.by_id.contains_key(event_id) {
            return Vec::new();
        }
        let mut chain = Vec::new();
        let mut current = Some(event_id.to_string());
        while let Some(id) = current {
            if !inner.by_id.contains_key(&id) {
                break;
            }
            chain.push(id.clone());
            current = inner.parents.get(&id).cloned();
        }
        chain.reverse();
        chain
    }

    /// Every unit of `session_id`, in `event_order` order.
    pub fn units_for_session(&self, session_id: &SessionId) -> Vec<Unit> {
        let inner = self.inner();
        let mut units: Vec<Unit> = inner
            .graph
            .node_weights()
            .filter(|unit| unit.session_id == *session_id)
            .cloned()
            .collect();
        units.sort_by_key(|unit| unit.event_order);
        units
    }
}

impl Default for GraphIndex {
    fn default() -> Self {
        Self::new()
    }
}
