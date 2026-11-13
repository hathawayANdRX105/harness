//! The `subagent` tool: delegates a task to an in-process child run with a
//! runtime-configurable, restricted tool subset per named profile.
//!
//! # Design
//!
//! The model calls `subagent { task, profile }`. The tool spawns a child
//! run — a fresh `harness_loop::run` over the shared model, the
//! profile-restricted tool subset, this daemon's policy — and reports the
//! child's terminal status back as an event pair, `SubagentStarted` then
//! `SubagentReport`, plus a JSON tool result the parent model can read.
//!
//! ## Why a restricted profile
//!
//! A subagent must be strictly more restricted than its parent: its tool
//! subset comes from a named profile whose names resolve against the base
//! catalog at setup time. An unknown profile or tool is a `ToolError`
//! reported back to the model — a misconfigured profile, never a model
//! mistake worth retrying blindly.
//!
//! ## Child identity and limits
//!
//! The child run is `{parent_run_id}--sub{n}` in the synthetic session
//! `{parent_session_id}--sub`, so child events get their own store
//! namespace and never contend with the parent's session slot. The child
//! inherits the parent's `RunLimits` with `max_turns` overridden by the
//! profile ceiling.
//!
//! ## Policy inheritance
//!
//! The child inherits the parent's `DaemonPolicy`: one policy surface for
//! the daemon, so what the parent may do the child may do within its
//! profile — no second policy to keep in sync.
//!
//! ## Persistence
//!
//! Child events reach live transport subscribers through the shared
//! broadcast sink, but are not persisted: only per-run pumps persist, and
//! children are not scheduler runs. Subagent runs are live-observable only.
//!
//! ## Parent context
//!
//! The `Tool` trait carries no run context, so `invoke` cannot learn which
//! parent run it belongs to from its arguments. The shared tool therefore
//! stays stateless: `wire::start_run` swaps in a per-run instance
//! (`for_run`) carrying the parent's ids and limits, so concurrent sessions
//! never stamp each other's context. Invoking the shared instance directly
//! is an `InvalidArgs` error rather than a silently wrong parent.

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
const DEFAULT_SUBAGENT_TOOLS: &[&str] = &["read", "glob", "grep"];

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
                ..parent.limits.clone()
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
            RunStopReason::Cancelled | RunStopReason::Error { .. } => {
                SubagentStatus::DoneWithConcerns
            }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::stream::iter as stream_iter;
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use harness_core::event::{EventSink, RunEvent, RunEventKind, SubagentStatus};
    use harness_core::message::StopReason;
    use harness_core::model::{Model, ModelEvent, ModelInfo, ModelRequest, ModelStream};
    use harness_core::policy::{
        PolicyDecision, PolicyOutcome, PolicyStage, ToolExecution, ToolPolicy,
    };
    use harness_core::run::{RunId, RunLimits, RunStopReason, SessionId};
    use harness_core::tool::{Tool, ToolError, ToolRequest, ToolResult, ToolRisk, ToolSchema};

    use super::{DEFAULT_SUBAGENT_TOOLS, SubagentTool};
    use harness_config::SubagentProfile;

    /// A sink that records every event for inspection.
    #[derive(Default)]
    struct CollectSink {
        events: Mutex<Vec<RunEvent>>,
    }

    impl EventSink for CollectSink {
        fn emit(&self, event: RunEvent) {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event);
        }
    }

    impl CollectSink {
        fn snapshot(&self) -> Vec<RunEvent> {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    /// A tool that returns a fixed result.
    struct StubTool {
        name: String,
        result: String,
    }

    #[async_trait]
    impl Tool for StubTool {
        fn name(&self) -> &str {
            &self.name
        }

        fn schema(&self) -> ToolSchema {
            ToolSchema {
                name: self.name.clone(),
                description: "stub tool".into(),
                input: json!({"type": "object"}),
            }
        }

        fn risk(&self) -> ToolRisk {
            ToolRisk::ReadOnly
        }

        async fn invoke(
            &self,
            _args: Value,
            _cancel: &CancellationToken,
        ) -> Result<ToolResult, ToolError> {
            Ok(ToolResult {
                content: self.result.clone(),
                is_error: false,
                trace: None,
            })
        }
    }

    /// A model that streams pre-recorded turns, FIFO; when the queue is
    /// empty it repeats the last turn forever.
    struct ScriptedModel {
        info: ModelInfo,
        turns: Mutex<std::collections::VecDeque<Vec<ModelEvent>>>,
        last: Vec<ModelEvent>,
    }

    impl ScriptedModel {
        fn new(turns: Vec<Vec<ModelEvent>>) -> Self {
            let last = turns.last().cloned().unwrap_or_default();
            Self {
                info: ModelInfo {
                    handle: "mock".into(),
                    provider: "mock".into(),
                    model: "mock-1".into(),
                    context_window: 8192,
                    supports_tool_use: true,
                    supports_streaming: true,
                },
                turns: Mutex::new(turns.into()),
                last,
            }
        }
    }

    #[async_trait]
    impl Model for ScriptedModel {
        fn info(&self) -> &ModelInfo {
            &self.info
        }

        async fn stream(&self, _req: &ModelRequest) -> ModelStream<'_> {
            let events = self
                .turns
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or_else(|| self.last.clone());
            Box::pin(stream_iter(events))
        }
    }

    /// Allow-every-thing policy.
    struct AllowAll;

    impl ToolPolicy for AllowAll {
        fn decide(
            &self,
            _stage: PolicyStage,
            _req: &ToolRequest,
            _execution: Option<&ToolExecution>,
        ) -> PolicyDecision {
            PolicyDecision {
                outcome: PolicyOutcome::Allow,
                reason: String::new(),
                source: "test-allow-all".into(),
                context_injection: None,
                output_suffix: None,
            }
        }
    }

    fn parent_limits() -> RunLimits {
        RunLimits {
            max_turns: 50,
            max_input_tokens: 150_000,
            max_output_tokens: 8_000,
        }
    }

    fn base_catalog() -> std::sync::Arc<crate::tools::ToolCatalog> {
        std::sync::Arc::new(crate::tools::ToolCatalog::new(vec![
            std::sync::Arc::new(StubTool {
                name: "read".into(),
                result: "stub content".into(),
            }),
            std::sync::Arc::new(StubTool {
                name: "glob".into(),
                result: "stub content".into(),
            }),
            std::sync::Arc::new(StubTool {
                name: "grep".into(),
                result: "stub content".into(),
            }),
        ]))
    }

    fn profiles() -> HashMap<String, SubagentProfile> {
        HashMap::from([(
            "scout".into(),
            SubagentProfile {
                name: "scout".into(),
                tools: vec!["read".into(), "glob".into(), "grep".into()],
                max_turns: 5,
            },
        )])
    }

    /// Invoke the tool as a per-run instance scoped to the given parent:
    /// mirrors how `wire::start_run` installs the tool before a run.
    async fn invoke(
        tool: &SubagentTool,
        parent_run: &str,
        parent_session: &str,
        limits: RunLimits,
        task: &str,
        profile: &str,
    ) -> Result<ToolResult, ToolError> {
        let scoped = tool.for_run(
            &RunId(parent_run.into()),
            &SessionId(parent_session.into()),
            limits,
            // Tests let the parent allow every base tool: profile-level
            // restriction is exercised instead.
            base_catalog()
                .names()
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
        scoped
            .invoke(
                json!({"task": task, "profile": profile}),
                &CancellationToken::new(),
            )
            .await
    }

    /// Purpose: a completed child reports Completed, captures its final
    /// text, and emits SubagentStarted + SubagentReport on the shared sink.
    /// Scenario: profile `scout`, child model answers with text and no tool
    /// calls.
    /// Acceptance: `is_error` is false; the payload names `parent-1--sub0`,
    /// profile `scout`, status `completed`, reason `completed`, and carries
    /// the child's text; the sink shows `subagent_started` at seq 0 and
    /// `subagent_report` last.
    #[tokio::test]
    async fn completed_child_reports_completed_with_captured_text() {
        let sink = std::sync::Arc::new(CollectSink::default());
        let model: std::sync::Arc<dyn Model> = std::sync::Arc::new(ScriptedModel::new(vec![vec![
            ModelEvent::TextDelta("ok".into()),
            ModelEvent::Stop(StopReason::EndTurn),
        ]]));
        let tool = SubagentTool::new(
            std::sync::Arc::clone(&model),
            profiles(),
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        );
        tool.set_catalog(base_catalog());

        let result = invoke(
            &tool,
            "parent-1",
            "s-1",
            parent_limits(),
            "summarize",
            "scout",
        )
        .await;

        let result = result.unwrap_or_else(|e| panic!("subagent invoke failed: {e}"));
        assert_eq!(
            result.is_error, false,
            "a completed child is not a tool error"
        );

        let payload: Value = serde_json::from_str(&result.content)
            .unwrap_or_else(|e| panic!("payload is JSON: {e}"));
        assert_eq!(payload["child_run_id"], "parent-1--sub0");
        assert_eq!(payload["profile"], "scout");
        assert_eq!(payload["status"], "completed");
        assert_eq!(payload["reason"]["kind"], "completed");
        assert_eq!(payload["text"], "ok");

        let events = sink.snapshot();
        assert!(events.first().is_some(), "at least SubagentStarted emitted");
        let started = &events[0];
        assert_eq!(started.seq, 0);
        assert_eq!(
            started.kind,
            RunEventKind::SubagentStarted {
                child_run_id: RunId("parent-1--sub0".into()),
                profile: "scout".into(),
            }
        );
        assert_eq!(started.session_id, SessionId("s-1--sub".into()));
        let report = events
            .iter()
            .last()
            .unwrap_or_else(|| panic!("no events emitted"));
        assert_eq!(
            report.kind,
            RunEventKind::SubagentReport {
                child_run_id: RunId("parent-1--sub0".into()),
                status: SubagentStatus::Completed,
                reason: RunStopReason::Completed,
            }
        );
        assert!(
            report.seq > started.seq,
            "report lands after the started event"
        );
    }

    /// Purpose: a child that exhausts its turns reports Blocked.
    /// Scenario: profile with `max_turns: 1`; the child model requests a
    /// tool every turn.
    /// Acceptance: status `blocked`, reason `max_turns`, `is_error` false.
    #[tokio::test]
    async fn max_turns_child_reports_blocked() {
        let sink = std::sync::Arc::new(CollectSink::default());
        let model: std::sync::Arc<dyn Model> = std::sync::Arc::new(ScriptedModel::new(vec![vec![
            ModelEvent::ToolCallStart {
                id: "c1".into(),
                name: "read".into(),
            },
            ModelEvent::ToolCallArgs {
                id: "c1".into(),
                partial_json: "{}".into(),
            },
            ModelEvent::ToolCallEnd { id: "c1".into() },
            ModelEvent::Stop(StopReason::ToolUse),
        ]]));
        let profile = SubagentProfile {
            name: "scout".into(),
            tools: vec!["read".into()],
            max_turns: 1,
        };
        let mut profiles = HashMap::new();
        profiles.insert("scout".into(), profile);
        let tool = SubagentTool::new(
            std::sync::Arc::clone(&model),
            profiles,
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        );
        tool.set_catalog(base_catalog());
        let result = invoke(
            &tool,
            "parent-1",
            "s-1",
            parent_limits(),
            "loop forever",
            "scout",
        )
        .await;
        let result = result.unwrap_or_else(|e| panic!("subagent invoke failed: {e}"));
        assert_eq!(result.is_error, false);

        let payload: Value = serde_json::from_str(&result.content)
            .unwrap_or_else(|e| panic!("payload is JSON: {e}"));
        assert_eq!(payload["status"], "blocked");
        assert_eq!(payload["reason"]["kind"], "max_turns");
        assert_eq!(payload["child_run_id"], "parent-1--sub0");

        let events = sink.snapshot();
        let report = events
            .iter()
            .last()
            .unwrap_or_else(|| panic!("no events emitted"));
        assert_eq!(
            report.kind,
            RunEventKind::SubagentReport {
                child_run_id: RunId("parent-1--sub0".into()),
                status: SubagentStatus::Blocked,
                reason: RunStopReason::MaxTurns,
            }
        );
    }

    /// Purpose: a child whose stream errors reports DoneWithConcerns.
    /// Scenario: child model emits text then `ModelEvent::Error`.
    /// Acceptance: status `done_with_concerns`, reason carries the error
    /// message, `is_error` false.
    #[tokio::test]
    async fn error_child_reports_done_with_concerns() {
        let sink = std::sync::Arc::new(CollectSink::default());
        let model: std::sync::Arc<dyn Model> = std::sync::Arc::new(ScriptedModel::new(vec![vec![
            ModelEvent::TextDelta("partial".into()),
            ModelEvent::Error {
                message: "upstream died".into(),
            },
        ]]));
        let tool = SubagentTool::new(
            std::sync::Arc::clone(&model),
            profiles(),
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        );
        tool.set_catalog(base_catalog());
        let result = invoke(&tool, "parent-1", "s-1", parent_limits(), "fail", "scout").await;
        let result = result.unwrap_or_else(|e| panic!("subagent invoke failed: {e}"));
        assert_eq!(result.is_error, false);

        let payload: Value = serde_json::from_str(&result.content)
            .unwrap_or_else(|e| panic!("payload is JSON: {e}"));
        assert_eq!(payload["status"], "done_with_concerns");
        assert_eq!(payload["reason"]["kind"], "error");
        assert_eq!(payload["reason"]["message"], "upstream died");
        assert_eq!(payload["text"], "partial");
    }

    /// Purpose: an unknown profile is a labeled tool error.
    /// Scenario: invoke with a profile name nothing registered.
    /// Acceptance: `is_error`, content names the unknown profile and lists
    /// available profiles.
    #[tokio::test]
    async fn unknown_profile_reports_available_profiles() {
        let sink = std::sync::Arc::new(CollectSink::default());
        let tool = SubagentTool::new(
            std::sync::Arc::new(ScriptedModel::new(vec![])),
            profiles(),
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        );
        tool.set_catalog(base_catalog());
        let err = match invoke(
            &tool,
            "parent-1",
            "s-1",
            parent_limits(),
            "whatever",
            "nope",
        )
        .await
        {
            Ok(value) => panic!("unknown profile must be rejected: {value:?}"),
            Err(err) => err,
        };
        let content = err.to_string();
        assert!(
            content.contains("unknown profile"),
            "names the profile: {content}"
        );
        assert!(
            content.contains("nope"),
            "names the unknown profile: {content}"
        );
        assert!(
            content.contains("scout"),
            "lists available profiles: {content}"
        );
    }

    /// Purpose: an unknown tool in a profile is a labeled tool error.
    /// Scenario: profile lists a tool absent from the base catalog.
    /// Acceptance: `is_error`, content names the unknown tool and lists
    /// base tool names.
    #[tokio::test]
    async fn unknown_tool_in_profile_reports_base_tools() {
        let sink = std::sync::Arc::new(CollectSink::default());
        let mut bad = profiles();
        bad.insert(
            "mischief".into(),
            SubagentProfile {
                name: "mischief".into(),
                tools: vec!["nope".into()],
                max_turns: 5,
            },
        );
        let tool = SubagentTool::new(
            std::sync::Arc::new(ScriptedModel::new(vec![])),
            bad,
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        );
        tool.set_catalog(base_catalog());
        let err = match invoke(
            &tool,
            "parent-1",
            "s-1",
            parent_limits(),
            "whatever",
            "mischief",
        )
        .await
        {
            Ok(value) => panic!("unknown tool in profile must be rejected: {value:?}"),
            Err(err) => err,
        };
        let content = err.to_string();
        assert!(
            content.contains("unknown tool"),
            "names the tool problem: {content}"
        );
        assert!(
            content.contains("nope"),
            "names the unknown tool: {content}"
        );
        for expected in DEFAULT_SUBAGENT_TOOLS {
            assert!(
                content.contains(expected),
                "lists base tool {expected}: {content}"
            );
        }
    }

    /// Purpose: a profile tool the parent run itself is not allowed to call
    /// is rejected before any child run starts — delegation never amplifies
    /// the caller's tool authority.
    /// Scenario: the parent's active set is only [read, glob, grep] while
    /// the profile grants `write`.
    /// Acceptance: the invoke is an `InvalidArgs` error naming the withheld
    /// tool, and no child run is spawned.
    #[tokio::test]
    async fn profile_tool_withheld_from_parent_is_rejected() {
        let sink = std::sync::Arc::new(CollectSink::default());
        let mut profiles = profiles();
        profiles.insert(
            "writer".into(),
            SubagentProfile {
                name: "writer".into(),
                tools: vec!["write".into()],
                max_turns: 5,
            },
        );
        let tool = SubagentTool::new(
            std::sync::Arc::new(ScriptedModel::new(vec![])),
            profiles,
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        );
        tool.set_catalog(base_catalog());

        let scoped = tool.for_run(
            &RunId("parent-1".into()),
            &SessionId("s-1".into()),
            parent_limits(),
            // The parent's session withholds `write`.
            vec!["read".into(), "glob".into(), "grep".into()],
        );
        let err = match scoped
            .invoke(
                json!({"task": "edit things", "profile": "writer"}),
                &CancellationToken::new(),
            )
            .await
        {
            Ok(value) => panic!("a withheld tool must be rejected: {value:?}"),
            Err(err) => err,
        };
        let content = err.to_string();
        assert!(
            content.contains("write"),
            "names the withheld tool: {content}"
        );
        assert!(
            sink.snapshot().is_empty(),
            "no child events may be emitted for a rejected invocation"
        );
    }

    /// Purpose: child ids derive from the parent as specified.
    /// Scenario: two invokes under one parent, then one under a second
    /// tool instance.
    /// Acceptance: `parent-1--sub0`, `parent-1--sub1` under the first
    /// instance; a second instance's child starts back at `parent-2--sub0`
    /// (the child counter is per shared tool instance, not per parent);
    /// sessions `s-1--sub` / `s-2--sub`.
    #[tokio::test]
    async fn child_ids_derive_from_parent() {
        let make_tool = |sink: std::sync::Arc<CollectSink>| {
            let model: std::sync::Arc<dyn Model> =
                std::sync::Arc::new(ScriptedModel::new(vec![vec![
                    ModelEvent::TextDelta("x".into()),
                    ModelEvent::Stop(StopReason::EndTurn),
                ]]));
            SubagentTool::new(
                model,
                profiles(),
                sink as std::sync::Arc<dyn EventSink>,
                std::sync::Arc::new(AllowAll),
            )
        };

        let sink = std::sync::Arc::new(CollectSink::default());
        let tool = make_tool(std::sync::Arc::clone(&sink));
        tool.set_catalog(base_catalog());
        let r0 = invoke(&tool, "parent-1", "s-1", parent_limits(), "one", "scout")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let p0: Value = serde_json::from_str(&r0.content).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(p0["child_run_id"], "parent-1--sub0");

        let r1 = invoke(&tool, "parent-1", "s-1", parent_limits(), "two", "scout")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let p1: Value = serde_json::from_str(&r1.content).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(p1["child_run_id"], "parent-1--sub1");
        assert_eq!(sink.snapshot()[0].session_id, SessionId("s-1--sub".into()));

        let sink2 = std::sync::Arc::new(CollectSink::default());
        let tool2 = make_tool(std::sync::Arc::clone(&sink2));
        tool2.set_catalog(base_catalog());
        let r2 = invoke(&tool2, "parent-2", "s-2", parent_limits(), "three", "scout")
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let p2: Value = serde_json::from_str(&r2.content).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(p2["child_run_id"], "parent-2--sub0");
        assert_eq!(sink2.snapshot()[0].session_id, SessionId("s-2--sub".into()));
    }

    /// Purpose: an unwired catalog is a labeled error, not a crash.
    /// Scenario: invoke before `set_catalog`.
    /// Acceptance: `is_error`, content explains the wiring is not ready.
    #[tokio::test]
    async fn unset_catalog_reports_wiring_not_ready() {
        let sink = std::sync::Arc::new(CollectSink::default());
        let tool = SubagentTool::new(
            std::sync::Arc::new(ScriptedModel::new(vec![])),
            profiles(),
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        );
        let result = invoke(
            &tool,
            "parent-1",
            "s-1",
            parent_limits(),
            "whatever",
            "scout",
        )
        .await;
        let err = match result {
            Ok(value) => panic!("an unwired catalog must be rejected: {value:?}"),
            Err(err) => err,
        };
        let content = err.to_string();
        assert!(
            content.contains("not ready"),
            "explains the missing catalog: {content}"
        );
    }

    /// Purpose: an invocation outside a run is a labeled error.
    /// Scenario: catalog wired, no parent context anchored.
    /// Acceptance: `is_error`, content explains the missing parent context.
    #[tokio::test]
    async fn no_parent_context_reports_invalid_args() {
        let sink = std::sync::Arc::new(CollectSink::default());
        let tool = SubagentTool::new(
            std::sync::Arc::new(ScriptedModel::new(vec![])),
            profiles(),
            std::sync::Arc::clone(&sink) as std::sync::Arc<dyn EventSink>,
            std::sync::Arc::new(AllowAll),
        );
        tool.set_catalog(base_catalog());

        // The shared instance carries no parent scope: only the per-run
        // instance `for_run` produces can spawn a child.
        let result = tool
            .invoke(
                json!({"task": "whatever", "profile": "scout"}),
                &CancellationToken::new(),
            )
            .await;
        let err = match result {
            Ok(value) => panic!("a scope-less invoke must be rejected: {value:?}"),
            Err(err) => err,
        };
        let content = err.to_string();
        assert!(
            content.contains("outside of a run"),
            "explains the missing parent context: {content}"
        );
    }
}
