//! The tool contract: `Tool` trait, `ToolError`, `ToolCatalog` / `ToolExecutor`.
//!
//! The contract sits at the bottom of the dependency tree (in `core`, the
//! harness trait hub) instead of inside the loop crate: the loop dispatches
//! tools against this contract and every implementer — host tools, MCP
//! adapters, plan-mode and subagent seams — stands on it, so there is exactly
//! one tool trait. The wire shape the provider sees (`ToolDef`) stays in
//! `harness-model`; host tool *implementations* stay in `harness-tools`.
//! This module is types and the registry only.

use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Cooperative abort signal: a shared flag plus the adapters that hand the
/// same flag to tools that poll it.
///
/// Long-running tools poll [`Self::is_aborted`] between work units and return
/// [`ToolError::Aborted`]; the signal also bridges hosts that own their own
/// flag through [`Self::from_flag`].
#[derive(Debug, Clone, Default)]
pub struct AbortSignal {
    /// Shared flag.
    flag: Arc<AtomicBool>,
}

impl AbortSignal {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn abort(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }
    pub fn is_aborted(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
    /// Shared flag clone: lets adapters hand the same `AtomicBool` to
    /// [`Tool::execute`], which polls it directly.
    pub fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }

    /// Wrap an existing shared flag — the inverse of [`Self::flag`]. Lets a
    /// host adapter hand the loop's own `AtomicBool` to a harness tool
    /// instead of a fresh, unconnected flag.
    pub fn from_flag(flag: Arc<AtomicBool>) -> Self {
        Self { flag }
    }
}

/// A parsed tool call from the model stream: the id the provider assigned,
/// the tool name and its raw arguments. Shared by the runtime event
/// vocabulary ([`crate::event::AgentEvent`]) and the request surface
/// ([`crate::model::ModelEvent`]), so it lives here at the bottom.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallSpec {
    pub id: String,
    pub name: String,
    pub args: serde_json::Value,
}

/// One action the model requested: the tool, the call id answers arrive on,
/// and the raw arguments. The hook vocabulary (a later batch) observes
/// actions; a parsed stream call becomes one through [`ToolCallSpec`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    pub tool: String,
    pub call_id: String,
    pub args: serde_json::Value,
}

/// Tool spec: API-facing definition assembled from the three primitives a
/// [`Tool`] supplies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub params_schema: serde_json::Value,
}

/// Tool execution result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub output: String,
    pub is_error: bool,
}

/// Tool execution error.
///
/// `Aborted` is the cooperative-cancellation answer (the tool polled
/// [`AbortSignal`] and gave up); `Execute` is a tool-level refusal with a
/// human-readable reason; `Io` carries a failed filesystem/pipe operation
/// straight out of the tool body through `?`.
#[derive(Debug)]
pub enum ToolError {
    Aborted,
    Execute(String),
    Io(std::io::Error),
}

impl Display for ToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolError::Aborted => write!(f, "tool call aborted"),
            ToolError::Execute(msg) => write!(f, "tool error: {msg}"),
            ToolError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ToolError {}

impl From<std::io::Error> for ToolError {
    fn from(e: std::io::Error) -> Self {
        ToolError::Io(e)
    }
}

/// One model-callable tool.
///
/// The three primitives are what a provider-facing definition needs; the
/// loop's own dispatch only needs [`Tool::execute`]. Errors are values: the
/// loop backfills a failed execution into the context instead of unwinding.
pub trait Tool: Send + Sync {
    /// Tool name as seen by the model. Borrowed, not `&'static str`: MCP tool
    /// names arrive at runtime from the server and are owned by the tool.
    fn name(&self) -> &str;
    fn description(&self) -> String;
    fn parameters(&self) -> Value;
    fn execute(&self, args: &Value, abort: &AbortSignal) -> Result<ToolResult, ToolError>;

    /// Execute with the caller's working directory (the session's workspace).
    ///
    /// `None` means "no session context": the tool keeps its process-cwd
    /// behavior, which is what direct callers (tests, CLI-shaped uses) rely
    /// on. The loop passes the session workspace here for every call, and the
    /// built-in file/shell tools override this method to resolve relative
    /// paths / spawn `sh` under that directory. Tools with no path or process
    /// semantics keep the default, which delegates to [`Tool::execute`].
    fn execute_in(
        &self,
        args: &Value,
        abort: &AbortSignal,
        cwd: Option<&Path>,
    ) -> Result<ToolResult, ToolError> {
        let _ = cwd;
        self.execute(args, abort)
    }

    /// API-facing definition, provided so an implementor only supplies the
    /// three primitives above.
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name().to_string(),
            description: self.description(),
            params_schema: self.parameters(),
        }
    }
}

/// Extract a string argument from JSON args, or error.
pub fn arg_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::Execute(format!("missing string argument `{key}`")))
}

/// Tool output truncation limit (lines); tail is kept — errors live at the end.
pub const MAX_OUTPUT_LINES: usize = 200;

/// Spill directory for oversized tool output: the system temp dir, not a
/// hard-coded `/tmp`.
pub fn spill_dir() -> std::path::PathBuf {
    std::env::temp_dir()
}

/// Truncate to the last `MAX_OUTPUT_LINES` lines; full output spills to a
/// temp file.
///
/// The spill file is `harness-output-{pid}-{seq}.txt`. `pid` separates runs of
/// different processes — a daemon restart used to reset the counter and
/// clobber the previous run's spill — and `seq` rises monotonically within
/// this process so a second overflow no longer overwrites the first one's
/// full output.
pub fn truncate_output(content: &str) -> std::io::Result<String> {
    let line_count = content.lines().count();
    if line_count <= MAX_OUTPUT_LINES {
        return Ok(content.to_string());
    }
    let kept: Vec<&str> = content
        .lines()
        .skip(line_count - MAX_OUTPUT_LINES)
        .collect();

    // Process-wide and monotonically increasing. Relaxed is sufficient: the
    // only invariant is that no two calls hand back the same number.
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let spill_path = spill_dir().join(format!("harness-output-{}-{seq}.txt", std::process::id()));
    std::fs::write(&spill_path, content)?;
    Ok(format!(
        "[output truncated: showing last {MAX_OUTPUT_LINES} of {line_count} lines. full output: {}]\n{}",
        spill_path.display(),
        kept.join("\n")
    ))
}

/// Registry of available tools, keyed by name.
///
/// Interior mutability: tools register through the shared catalog
/// (`register` takes `&self`), matching the loop's shared-tool-families
/// pattern. The catalog speaks the same [`Tool`] trait every implementer
/// implements, so a host registers built-ins, MCP clients and capability
/// tools side by side without an adapter; a host that dispatches its own way
/// takes [`ToolCatalog::all`] and gets the same `Arc` handles back.
pub struct ToolCatalog {
    tools: Mutex<Vec<Arc<dyn Tool>>>,
}

impl ToolCatalog {
    pub fn new() -> Self {
        Self {
            tools: Mutex::new(Vec::new()),
        }
    }

    pub fn register(&self, tool: Arc<dyn Tool>) {
        self.tools.lock().push(tool);
    }

    /// Look up a tool by name.
    pub fn find(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.lock().iter().find(|t| t.name() == name).cloned()
    }

    /// All specs in registration order.
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.tools.lock().iter().map(|t| t.spec()).collect()
    }

    /// Cloned handles to every registered tool, in registration order — the
    /// shape a host merges its own shared tool families onto.
    pub fn all(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.lock().iter().cloned().collect()
    }
}

impl Default for ToolCatalog {
    fn default() -> Self {
        Self::new()
    }
}

/// Executes tool calls against a catalog.
///
/// Constraint: synchronous; `abort` is polled by long-running tools to
/// support cooperative cancellation. Non-goal: no parallel tool dispatch; no
/// schema validation here.
pub trait ToolExecutor {
    /// Tool specs advertised to the provider. Default: none, so
    /// implementors that expose a fixed tool set override this.
    fn specs(&self) -> Vec<ToolSpec> {
        Vec::new()
    }
    fn execute(
        &self,
        spec: &ToolSpec,
        args: &Value,
        abort: &AbortSignal,
    ) -> Result<ToolResult, ToolError>;
}

impl ToolExecutor for ToolCatalog {
    fn specs(&self) -> Vec<ToolSpec> {
        ToolCatalog::specs(self)
    }
    fn execute(
        &self,
        spec: &ToolSpec,
        args: &Value,
        abort: &AbortSignal,
    ) -> Result<ToolResult, ToolError> {
        self.find(&spec.name)
            .ok_or_else(|| ToolError::Execute(format!("unknown tool: {}", spec.name)))?
            .execute(args, abort)
    }
}
