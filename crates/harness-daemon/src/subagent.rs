//! The `subagent` tool: `subagent { task, profile }` runs a task in an
//! in-process child run and returns its terminal status to the parent.
//!
//! A child is a fresh `harness_loop::run` over the shared model with the
//! tools and turn ceiling of a named profile (`[[agents.subagents]]`), the
//! parent's limits, and the daemon's policy. The child run id is
//! `{parent_run_id}--sub{n}` in the synthetic session
//! `{parent_session_id}--sub`: its own event store namespace, never its
//! parent's scheduler slot.
//!
//! Profile tools resolve against the base catalog and are intersected with
//! the parent run's active tool set; unknown names and withheld tools are
//! `ToolError::InvalidArgs` with the valid names attached. Child events go
//! to live transport subscribers (SubagentStarted, then the child's stream,
//! then SubagentReport) but are not persisted — only per-run pumps persist
//! and children are not scheduler runs.
//!
//! The `Tool` trait passes no run context, so the shared instance the
//! daemon registers stays stateless: `wire::start_run` swaps a per-run
//! instance (`for_run`) into the run's tools. Invoking the shared instance
//! directly is an error, and no parent state is shared between runs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use crate::tools::ToolCatalog;
use harness_config::SubagentProfile;
use harness_core::RunControl;
use harness_core::event::{EventSink, RunEvent, RunEventKind, SubagentStatus};
use harness_core::model::Model;
use harness_core::policy::ToolPolicy;
use harness_core::run::{RunContext, RunId, RunLimits, RunStopReason, SessionId, Task};
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use harness_loop::{LoopDeps, LoopHooks, ToolExecutionMode, run as run_child};

/// Default tool subset for a profile with no `tools` listed: read-only, so
/// a subagent can observe its environment without necessarily changing it.
/// A subagent must be strictly more restricted than its parent.
pub const DEFAULT_SUBAGENT_TOOLS: &[&str] = &["read", "glob", "grep"];

/// Unix epoch milliseconds, written locally: the loop's helper is private
/// to `wire.rs` and is not reusable here.
fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Identity of the parent run a child is spawned from, carried by the
/// per-run tool instance (`for_run`) that wraps the shared one.
#[derive(Clone)]
struct ParentCtx {
    run_id: RunId,
    session_id: SessionId,
    limits: RunLimits,
    /// The parent's own active tool names: a child may never run a tool the
    /// parent's session allowlist withholds, so a profile is intersected
    /// with this set before any child run starts.
    allowed_tools: Vec<String>,
}

/// One delegated subagent run.
///
/// Two shapes share this type: the SHARED instance the daemon registers
/// (`new`, no parent scope — invoking it is an error) and the per-run
/// instance `for_run` produces (parent scope present), which `wire::start_run`
/// swaps into a run's `LoopDeps`. The immutable shared state lives behind
/// `Arc<Shared>` so the per-run clone is cheap and carries NO mutable
/// cross-run state.
pub struct SubagentTool {
    shared: Arc<Shared>,
    /// The parent run this instance belongs to: `None` on the shared
    /// instance the daemon registers.
    parent: Option<ParentCtx>,
}

/// Immutable shared state: model, profiles, sink, policy, catalog, and the
/// child counter (per-shared-instance, so siblings get distinct ids).
struct Shared {
    model: Arc<dyn Model>,
    profiles: HashMap<String, SubagentProfile>,
    sink: Arc<dyn EventSink>,
    policy: Arc<dyn ToolPolicy>,
    /// Set by main.rs after assembly: the tool is injected into the builder
    /// before the catalog exists.
    catalog: OnceLock<Arc<ToolCatalog>>,
    child_seq: AtomicU64,
}

impl SubagentTool {
    /// The shared instance: registered as the base-list tool the catalog
    /// snapshots; a run never invokes it directly (see `for_run`).
    pub fn new(
        model: Arc<dyn Model>,
        profiles: HashMap<String, SubagentProfile>,
        sink: Arc<dyn EventSink>,
        policy: Arc<dyn ToolPolicy>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                model,
                profiles,
                sink,
                policy,
                catalog: OnceLock::new(),
                child_seq: AtomicU64::new(0),
            }),
            parent: None,
        }
    }

    /// Install the runtime catalog once it exists (main.rs calls this right
    /// after assembling the runtime, whose tool list is the catalog base).
    pub fn set_catalog(&self, catalog: Arc<ToolCatalog>) {
        let _ = self.shared.catalog.set(catalog);
    }

    /// The per-run instance the host swaps into a run's `LoopDeps`: same
    /// name, schema and risk, but carrying this run's identity so children
    /// anchor to their true parent even when sessions run concurrently.
    /// `allowed_tools` are the parent run's active tool names — the child's
    /// profile is intersected with them: delegation never amplifies the
    /// caller's tool authority.
    pub fn for_run(
        &self,
        run_id: &RunId,
        session_id: &SessionId,
        limits: RunLimits,
        allowed_tools: Vec<String>,
    ) -> Arc<dyn Tool> {
        Arc::new(SubagentTool {
            shared: Arc::clone(&self.shared),
            parent: Some(ParentCtx {
                run_id: run_id.clone(),
                session_id: session_id.clone(),
                limits,
                allowed_tools,
            }),
        })
    }

    fn invalid_args(reason: String) -> ToolError {
        ToolError::InvalidArgs {
            name: "subagent".into(),
            reason,
        }
    }
}

#[async_trait]
impl Tool for SubagentTool {
    fn name(&self) -> &str {
        "subagent"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "subagent".into(),
            description: "Delegate a task to an in-process child run restricted to a named profile's tool subset and turn ceiling.".into(),
            input: json!({
                "type": "object",
                "properties": {
                    "task": {
                        "type": "string",
                        "description": "Task description for the child run."
                    },
                    "profile": {
                        "type": "string",
                        "description": "Name of the subagent profile (defines the child's tools and max_turns)."
                    },
                },
                "required": ["task", "profile"],
                "additionalProperties": false,
            }),
        }
    }

    /// Destructive: the child inherits the parent's policy and may execute
    /// state-changing tools, so the delegation itself carries side effects.
    fn risk(&self) -> ToolRisk {
        ToolRisk::Destructive
    }

    async fn invoke(
        &self,
        args: Value,
        _cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        // 1. Args: both required, `task` non-empty.
        let task = args
            .get("task")
            .and_then(Value::as_str)
            .ok_or_else(|| Self::invalid_args("missing required string argument `task`".into()))?;
        if task.trim().is_empty() {
            return Err(Self::invalid_args("`task` must not be empty".into()));
        }
        let profile_name = args.get("profile").and_then(Value::as_str).ok_or_else(|| {
            Self::invalid_args("missing required string argument `profile`".into())
        })?;

        // 2. Profile: an unknown name is a misconfigured profile reported
        //    back to the model with the names that do exist.
        let profile = self.shared.profiles.get(profile_name).ok_or_else(|| {
            let names: Vec<&str> = self.shared.profiles.keys().map(String::as_str).collect();
            Self::invalid_args(format!(
                "unknown profile {profile_name:?}; available profiles: {names:?}"
            ))
        })?;

        // 3. The catalog must be wired, then each profile tool name must
        //    resolve against the base catalog.
        let catalog = self.shared.catalog.get().ok_or_else(|| {
            Self::invalid_args(
                "subagent tool wiring is not ready: the tool catalog has not been installed".into(),
            )
        })?;
        let tool_names: Vec<&str> = if profile.tools.is_empty() {
            DEFAULT_SUBAGENT_TOOLS.to_vec()
        } else {
            profile.tools.iter().map(String::as_str).collect()
        };
        let mut child_tools = Vec::with_capacity(tool_names.len());
        let Some(parent) = self.parent.as_ref() else {
            return Err(Self::invalid_args(
                "invoked outside of a run: this subagent instance carries no parent scope".into(),
            ));
        };
        for name in tool_names {
            // Unknown to the base catalog first: a misconfigured profile
            // names something that cannot exist at all.
            let tool = catalog.find(name).ok_or_else(|| {
                let base: Vec<&str> = catalog.names();
                Self::invalid_args(format!(
                    "profile {profile_name:?} references unknown tool {name:?}; base tools: {base:?}"
                ))
            })?;
            // Then the trust boundary: the child may only run tools the
            // parent's session itself allows — a profile restricts, never
            // amplifies, the caller's authority.
            if !parent.allowed_tools.iter().any(|allowed| allowed == name) {
                return Err(Self::invalid_args(format!(
                    "profile {profile_name:?} grants tool {name:?} that the parent run is not allowed to call"
                )));
            }
            child_tools.push(tool);
        }

        // 4. Parent run context: the child's ids and limits derive from it.
        //    Only the per-run instance (for_run) carries one.

        // 5. Child identity: its own store namespace, never contending with
        //    the parent's session slot.
        let n = self.shared.child_seq.fetch_add(1, Ordering::Relaxed);
        let child_run_id = RunId(format!("{}--sub{n}", parent.run_id));
        let child_session_id = SessionId(format!("{}--sub", parent.session_id));

        // 6. Child dependencies: shared model, profile-restricted tools,
        //    shared sink and policy, no approval broker, default hooks,
        //    sequential dispatch, no budget or compaction.
        let child_sink = ChildRunSink::new(
            Arc::clone(&self.shared.sink),
            child_run_id.clone(),
            child_session_id.clone(),
        );
        let child_sink_dyn: Arc<dyn EventSink> = Arc::<ChildRunSink>::clone(&child_sink);

        let deps = LoopDeps {
            model: Arc::clone(&self.shared.model),
            tools: child_tools,

            event_sink: child_sink_dyn,
            policy: Arc::clone(&self.shared.policy),
            approval_broker: None,
            hooks: LoopHooks::default(),
            tool_mode: ToolExecutionMode::Sequential,
            budget: None,
            compactor: None,
            seed: Vec::new(),
            transcript_sink: None,
        };
        let ctx = RunContext {
            run_id: child_run_id.clone(),
            session_id: child_session_id.clone(),
            task: Task {
                description: task.to_string(),
                source: None,
                deadline: None,
            },
            limits: RunLimits {
                max_turns: profile.max_turns,
                ..parent.limits
            },
            started_at_ms: unix_millis(),
        };

        // 7. `SubagentStarted` takes seq 0; the child loop's events continue
        //    from 1 in the child's own sequence space.
        child_sink.emit(RunEvent {
            run_id: child_run_id.clone(),
            session_id: child_session_id.clone(),
            seq: 0,
            timestamp_ms: unix_millis(),
            kind: RunEventKind::SubagentStarted {
                child_run_id: child_run_id.clone(),
                profile: profile.name.clone(),
            },
        });

        // 8. Run the child to completion.
        let reason = run_child(ctx, Arc::new(deps), RunControl::default()).await;
        let status = match &reason {
            RunStopReason::Completed => SubagentStatus::Completed,
            RunStopReason::MaxTurns => SubagentStatus::Blocked,
            RunStopReason::Error { .. } => SubagentStatus::Error,
            RunStopReason::Cancelled => SubagentStatus::DoneWithConcerns,
            // `RunStopReason` is `#[non_exhaustive]`: any future terminal
            // reason is treated as done-with-concerns so the parent never
            // mistakes an unknown child outcome for a clean completion.
            _ => SubagentStatus::DoneWithConcerns,
        };
        let text = child_sink.final_text();

        // 9. `SubagentReport` lands last in the child's sequence space.
        let mut payload = Map::new();
        payload.insert("child_run_id".into(), json!(child_run_id.0.clone()));
        payload.insert("profile".into(), json!(profile.name.clone()));
        payload.insert(
            "status".into(),
            serde_json::to_value(&status).map_err(|e| ToolError::Exec(e.to_string()))?,
        );
        payload.insert(
            "reason".into(),
            serde_json::to_value(&reason).map_err(|e| ToolError::Exec(e.to_string()))?,
        );
        if !text.is_empty() {
            payload.insert("text".into(), json!(text));
        }
        child_sink.emit(RunEvent {
            run_id: child_run_id.clone(),
            session_id: child_session_id.clone(),
            seq: 0,
            timestamp_ms: unix_millis(),
            kind: RunEventKind::SubagentReport {
                child_run_id: child_run_id.clone(),
                status,
                reason,
            },
        });

        // 10. A non-`Completed` child is a report, not a tool error.
        Ok(ToolResult {
            content: serde_json::to_string(&Value::Object(payload))
                .map_err(|e| ToolError::Exec(e.to_string()))?,
            is_error: false,
            trace: None,
        })
    }
}

/// Forwards the child run's events onto the shared broadcast sink with the
/// child's run id/session_id and a child-owned, gap-free sequence space
/// (the loop restarts its own counter at 0), and captures the child's
/// assistant text so the parent's tool result can return it.
struct ChildRunSink {
    sink: Arc<dyn EventSink>,
    run_id: RunId,
    session_id: SessionId,
    seq: AtomicU64,
    text: Mutex<String>,
}

impl ChildRunSink {
    fn new(sink: Arc<dyn EventSink>, run_id: RunId, session_id: SessionId) -> Arc<Self> {
        Arc::new(Self {
            sink,
            run_id,
            session_id,
            seq: AtomicU64::new(0),
            text: Mutex::new(String::new()),
        })
    }

    /// Overwrite `event.seq` with the next child sequence number, anchor
    /// the event at the child's ids, capture `TextDelta` fragments, then
    /// forward to the shared sink.
    fn emit(&self, event: RunEvent) {
        if let RunEventKind::TextDelta { text } = &event.kind {
            let mut captured = self
                .text
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            captured.push_str(text);
        }
        let mut event = event;
        event.seq = self.seq.fetch_add(1, Ordering::Relaxed);
        event.run_id = self.run_id.clone();
        event.session_id = self.session_id.clone();
        self.sink.emit(event);
    }

    /// The assistant text the child streamed, if any.
    fn final_text(&self) -> String {
        self.text
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl EventSink for ChildRunSink {
    fn emit(&self, event: RunEvent) {
        self.emit(event)
    }
}
