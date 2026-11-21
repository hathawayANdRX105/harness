//! The turn: the single source of truth a `Processor` reads and mutates.
//!
//! One `Turn` per run carries the message ledger, the tool set, run control,
//! run identity and the sequence counter — everything the legacy loop
//! scattered across function parameters — so a node asks the turn for what
//! it needs instead of holding a copy. Cross-run handles (configs,
//! limiters) stay in the processors; per-run state stays here — the run's
//! `event_sink` rides the turn too, so a node that emits always reaches the
//! sink the *run* carries (the scheduler and the daemon swap it per run),
//! never a stale construction-time copy.
//!
//! Ledger writes keep pairing by order discipline: tool results are appended
//! adjacent to their calls, and *removal* — the only operation that can
//! orphan a result from its call — goes through `retract_messages`, the
//! pairing-closing primitive.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::control::RunControl;
use crate::event::EventSink;
use crate::hooks::LoopHooks;
use crate::message::{Content, Message, ToolCall, ToolResultMessage};
use crate::model::Model;
use crate::policy::{ApprovalBroker, ToolPolicy};
use crate::run::RunContext;
use crate::tool::Tool;
use crate::transcript::TranscriptSink;

/// The run-level seams one turn carries: the sinks and policy objects the
/// *run* is wired to, as opposed to the cross-run configuration processors
/// hold.
///
/// Hosts swap these per run — the scheduler registers a `BusSink` and a
/// run-scoped transcript sink, the daemon its own — so a processor that
/// captured one at construction would talk to a dead channel. Bundling them
/// keeps [`Turn::new`] callable; the turn then hands every node exactly the
/// seam it needs, and the nodes stay stateless.
pub struct RunSeams {
    /// The policy every tool call is evaluated against this run.
    pub policy: Arc<dyn ToolPolicy>,
    /// The approval broker the run's risky tools go through, when one is
    /// wired.
    pub approval_broker: Option<Arc<dyn ApprovalBroker>>,
    /// The run's tool-call hooks (deny/terminate/inject).
    pub hooks: LoopHooks,
    /// The sink this run's events go to.
    pub event_sink: Arc<dyn EventSink>,
    /// The sink this run's messages are persisted through, when the host
    /// persists the transcript per run.
    pub transcript_sink: Option<Arc<dyn TranscriptSink>>,
}

/// One turn being machined: the single source of truth a `Processor` reads
/// and mutates in place.
pub struct Turn {
    /// The message ledger. Processors append and prune it; the model request
    /// is rendered from it. Mutations go through `append_message` /
    /// `retract_messages`, so tool-call/result pairing never breaks.
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
    /// The run's seams: event sink, transcript sink, policy, approval
    /// broker, hooks. Every emitting/persisting/policy-reading node takes
    /// them from here (see [`RunSeams`]).
    pub seams: RunSeams,
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
        seams: RunSeams,
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
            seams,
        }
    }

    /// Append one message at the ledger tail.
    ///
    /// A tail append cannot orphan a tool result from its call — pairing
    /// only breaks when a *middle* of the ledger is edited — so the guard
    /// is a plain push today. Writers that append (the transcript seam,
    /// the seed processors) keep pairing by order; this primitive is the
    /// surface tests and future audits exercise.
    pub fn append_message(&mut self, message: Message) {
        self.ledger.push(message);
    }

    /// Drop a message range without orphaning a tool result from its call.
    ///
    /// The compactor's pairing invariant (`harness-context::Compactor`):
    /// a retained assistant message containing a `Content::ToolCall` must
    /// keep its matching `Message::ToolResult`. So the requested range is
    /// closed under pairing first — any pair straddling an edge is pulled
    /// in wholesale — and only then removed. A range that starts on a
    /// result whose call sits outside still drops both: keeping the call
    /// without its result is the exact orphan the guard exists to refuse.
    pub fn retract_messages(&mut self, range: std::ops::Range<usize>) {
        let drop_set = pairing_closure(&self.ledger, range);
        // Remove in reverse so earlier indices stay valid.
        for index in drop_set.into_iter().rev() {
            self.ledger.remove(index);
        }
    }
}

/// Response-side intermediate products: what the model stream folded into,
/// read by the append gate and the tool dispatch. Written by
/// `ModelProcessor` only; read at turn boundaries, never per token.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TurnArtifacts {
    /// The assistant content blocks folded from the model stream.
    pub content: Vec<Content>,
    /// The tool calls parsed out of the stream.
    pub calls: Vec<ToolCall>,
}

/// Close a message range under tool-call/tool-result pairing: the exact set
/// of indices to remove so no straddling pair is cut.
///
/// Pairs are matched by tool-call id: an assistant message carrying
/// `Content::ToolCall(id)` and the `Message::ToolResult` whose
/// `tool_call_id` is that id. When exactly one side of a pair is inside the
/// requested range, the other side joins the removal — the invariant is
/// "both or neither", never "call without result".
fn pairing_closure(ledger: &[Message], range: std::ops::Range<usize>) -> Vec<usize> {
    use std::collections::HashSet;

    // Map each tool-call id to (call index, result index), when both exist.
    let mut calls: Vec<(String, usize)> = Vec::new();
    let mut results: Vec<(String, usize)> = Vec::new();
    for (index, message) in ledger.iter().enumerate() {
        match message {
            Message::Assistant { content } => {
                for block in content {
                    if let Content::ToolCall(ToolCall { id, .. }) = block {
                        calls.push((id.clone(), index));
                    }
                }
            }
            Message::ToolResult(ToolResultMessage { tool_call_id, .. }) => {
                results.push((tool_call_id.clone(), index));
            }
            _ => {}
        }
    }

    let mut drop_set: HashSet<usize> = range.collect();
    let mut changed = true;
    while changed {
        changed = false;
        for (id, call_index) in &calls {
            let Some(result_index) = results.iter().find(|(rid, _)| rid == id).map(|(_, i)| *i)
            else {
                continue;
            };
            let call_in = drop_set.contains(call_index);
            let result_in = drop_set.contains(&result_index);
            if call_in != result_in {
                drop_set.insert(*call_index);
                drop_set.insert(result_index);
                changed = true;
            }
        }
    }

    let mut indices: Vec<usize> = drop_set.into_iter().collect();
    indices.sort_unstable();
    indices
}
