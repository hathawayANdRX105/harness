//! The turn dataflow: `Processor` nodes machine a `Turn` along a `Topology`.
//!
//! Slice 1 (branch `arch/pipeline-dataflow`): vocabulary only. These types
//! are the contract the next slices implement against; nothing here is
//! wired into the loop yet, and `Topology::drive` plus the ledger-guard
//! primitives are `todo!()` placeholders until their slices land. Shipping
//! the shell first lets slices 2–5 add processors without touching `run()`'s
//! legacy path.
//!
//! Design: see `docs/topology-design.md`. Kafka Streams contributes the
//! vocabulary; LLVM contributes the load-bearing invariant — the data is
//! the single source of truth, passed `&mut` between nodes and mutated in
//! place, so exactly one node can write at a time by construction, not by
//! lock. `2-governance` is the naming/discipline regime this module obeys.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use async_trait::async_trait;

use crate::control::RunControl;
use crate::message::{Content, Message, ToolCall, ToolResultMessage};
use crate::model::{Model, Usage};
use crate::run::{RunContext, RunStopReason};
use crate::tool::Tool;

/// One turn being machined: the single source of truth a `Processor` reads
/// and mutates in place.
///
/// The turn carries the per-run state the legacy `LoopDeps` scattered
/// across function parameters — ledger, tool set, run control, model — so
/// a node asks the turn for whatever it needs instead of holding a copy.
/// Cross-run handles (configs, sinks, limiters) stay in the processors.
pub struct Turn {
    /// The message ledger. Processors append and prune it; the model request
    /// is rendered from it. Mutations go through `append_message` /
    /// `retract_messages` once the guard lands, so tool-call/result pairing
    /// never breaks.
    pub ledger: Vec<Message>,
    /// The tool set injected into this turn: a prompt-side processor writes
    /// it, `ModelProcessor` renders it into the request.
    pub tools: Vec<Arc<dyn Tool>>,
    /// Cancellation plus the steering mailbox, read at every node boundary.
    pub control: RunControl,
    /// The run's identity and limits — what events and tool requests carry
    /// so a node never needs a second channel to know which run it serves.
    pub ctx: RunContext,
    /// Which turn of the run is being machined (0-based). The driver sets it
    /// before each topology pass; nodes read it for event correlation.
    pub turn_idx: u32,
    /// Per-run event sequence counter, incremented whenever a node emits.
    pub seq: AtomicU64,
    /// Response-side intermediate products, filled once the model stream
    /// pumped and the response nodes ran.
    pub artifacts: TurnArtifacts,
    /// The model this turn talks to. `ModelProcessor` is the only node that
    /// streams; the rest read `info()` for catalog data and budget math.
    pub model: Arc<dyn Model>,
}

impl Turn {
    /// Start a turn from the host's seed ledger — a session replay for a
    /// resumed run, empty for a fresh session. Seed messages are already
    /// persisted upstream, so the run never re-mirrors them.
    pub fn new(
        model: Arc<dyn Model>,
        control: RunControl,
        ctx: RunContext,
        seed: Vec<Message>,
    ) -> Self {
        Self {
            ledger: seed,
            tools: Vec::new(),
            control,
            ctx,
            turn_idx: 0,
            seq: AtomicU64::new(0),
            artifacts: TurnArtifacts::default(),
            model,
        }
    }

    /// Append one message without orphaning a tool result from its call.
    ///
    /// Slice 5 placeholder: the final guard rejects an append that would
    /// break tool-call/tool-result pairing and mirrors the message to the
    /// transcript sink when one is wired in.
    pub fn append_message(&mut self, _message: Message) {
        todo!("slice 5: pairing-aware append")
    }

    /// Drop a message range without orphaning a tool result from its call.
    pub fn retract_messages(&mut self, _range: std::ops::Range<usize>) {
        todo!("slice 5: pairing-aware retract")
    }
}

/// Response-side intermediate products: what the model stream folded into,
/// plus what the response nodes produced from it. Populated by
/// `ModelProcessor` and the tool-side processors; read at turn boundaries,
/// never per token.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TurnArtifacts {
    /// The assistant content blocks folded from the model stream.
    pub content: Vec<Content>,
    /// The tool calls parsed out of the stream.
    pub calls: Vec<ToolCall>,
    /// The tool results the response nodes produced, in dispatch order.
    pub results: Vec<ToolResultMessage>,
    /// Token accounting, whenever the provider reports it.
    pub usage: Usage,
    /// The terminal route the model stream folded into: `None` on a clean
    /// turn, `Some` when `ModelProcessor` saw a terminal event. The driver
    /// reads it to backfill partials and finish the run.
    pub stop: Option<RunStopReason>,
}

/// One machining step in the topology. Implementations hold only the
/// strategy — config, handles, limiters — never per-run data: everything
/// stateful arrives as the `&mut Turn` and leaves with it.
///
/// `process` is the Kafka Streams verb, kept over the higher-scoring `run`
/// in the naming audit (`docs/topology-design.md` §1.1). The
/// `2-governance/01-expression.md` bad-signal exemption is repaid by the
/// signature saying what/who/result and by every implementation's module
/// header stating what it machines.
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

/// The per-turn machining order: a DAG of `Processor` nodes.
///
/// Slice 2: `step` runs the registered nodes in order (the edge machinery —
/// `Route::Goto` — lands with the response-side slice). The
/// DAG-plus-outer-loop split is documented in `docs/topology-design.md`
/// §4 — Kafka topologies are acyclic, so the run-level cycle belongs to
/// `drive`, not to the graph.
pub struct Topology {
    nodes: Vec<Arc<dyn Processor>>,
}

impl Topology {
    /// An empty topology; register nodes with `node`.
    #[must_use]
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Register one node. Slices 2–5 fill this chain with real processors;
    /// the driver resolves `Goto` targets against registered names.
    #[must_use]
    pub fn node(mut self, processor: Arc<dyn Processor>) -> Self {
        self.nodes.push(processor);
        self
    }

    /// Registered node count — the driver's sanity check and the shell's
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

    /// Run one pass of the DAG over the turn: every node in registration
    /// order, stopping at the first `Done`.
    ///
    /// `Goto` (named-node routing) lands with the response-side slice
    /// (slice 5); a node that returns it today hits the documented
    /// placeholder, because the only topologies built so far are
    /// prompt-side and never route.
    pub async fn step(&self, turn: &mut Turn) -> Route {
        let mut route = Route::Next;
        for node in &self.nodes {
            route = node.process(turn).await;
            match &route {
                Route::Next => {}
                Route::Goto(_) => todo!("slice 5: named-node routing"),
                Route::Done(_) => return route,
            }
        }
        route
    }

    /// Drive the run through the topology until the loop stops, returning
    /// the run-level stop reason. The outer re-entry: each call runs one
    /// `step` pass, and the driver decides from the route whether the turn
    /// produced another round.
    ///
    /// Slice 4 placeholder: folds the per-turn outcome into the run-level
    /// loop decision (continue to the next turn or stop).
    pub async fn drive(&self, _turn: &mut Turn) -> RunStopReason {
        todo!("slice 4: run-level loop over step passes")
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
