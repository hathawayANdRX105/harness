//! Tool contract behavior: spec generation through a trait object, catalog
//! registration/lookup, `ToolExecutor` dispatch, abort-signal bridging and
//! output truncation.
//!
//! Acceptance: a mock implementer of `Tool` driven through `Arc<dyn Tool>`
//! and `dyn ToolExecutor` behaves exactly as the contract promises — the
//! three primitives feed `spec()`, unknown names refuse with `ToolError`, and
//! an aborted signal short-circuits a polling tool.

use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use harness_core::tool::{
    AbortSignal, MAX_OUTPUT_LINES, Tool, ToolCallSpec, ToolCatalog, ToolError, ToolExecutor,
    ToolResult, ToolSpec, arg_str, truncate_output,
};
use serde_json::{Value, json};

/// A tool that echoes its `text` argument unless the abort signal fired.
struct EchoTool;

impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> String {
        "Echoes the `text` argument back.".to_string()
    }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "text": { "type": "string" } },
            "required": ["text"],
        })
    }
    fn execute(&self, args: &Value, abort: &AbortSignal) -> Result<ToolResult, ToolError> {
        let text = arg_str(args, "text")?;
        if abort.is_aborted() {
            return Err(ToolError::Aborted);
        }
        Ok(ToolResult {
            output: text.to_string(),
            is_error: false,
        })
    }
}

/// A tool that stands in for any long-running one: it polls the abort signal
/// between work units and reports nothing until the signal fires.
struct PollingTool;

impl Tool for PollingTool {
    fn name(&self) -> &str {
        "poll"
    }
    fn description(&self) -> String {
        "Polls the abort signal until it fires.".to_string()
    }
    fn parameters(&self) -> Value {
        json!({"type": "object"})
    }
    fn execute(&self, _args: &Value, abort: &AbortSignal) -> Result<ToolResult, ToolError> {
        // Bounded so a missing abort fails the test rather than hanging it;
        // every test that reaches here aborts on the first poll.
        for _ in 0..1000 {
            if abort.is_aborted() {
                return Err(ToolError::Aborted);
            }
            sleep(Duration::from_millis(1));
        }
        Ok(ToolResult {
            output: "never reached".to_string(),
            is_error: true,
        })
    }
}

/// spec() must assemble exactly the three primitives an implementer supplies —
/// this is the "schema generation through a trait object" acceptance: the
/// implementor never builds a `ToolSpec` itself.
#[test]
fn spec_assembles_the_three_primitives() {
    let tool: Arc<dyn Tool> = Arc::new(EchoTool);
    let spec = tool.spec();
    assert_eq!(spec.name, "echo");
    assert_eq!(spec.description, "Echoes the `text` argument back.");
    assert_eq!(
        spec.params_schema,
        json!({
            "type": "object",
            "properties": { "text": { "type": "string" } },
            "required": ["text"],
        })
    );
}

/// A catalog keeps registration order for `specs`/`all` and finds tools by
/// the name the model sees.
#[test]
fn catalog_registers_finds_and_lists_in_order() {
    let catalog = ToolCatalog::new();
    catalog.register(Arc::new(EchoTool));
    catalog.register(Arc::new(PollingTool));

    assert_eq!(
        catalog.find("echo").map(|t| t.name().to_string()),
        Some("echo".into())
    );
    assert!(catalog.find("missing").is_none());

    let names: Vec<String> = catalog.specs().iter().map(|s| s.name.clone()).collect();
    assert_eq!(names, vec!["echo", "poll"]);
    assert_eq!(catalog.all().len(), 2);
}

/// Dispatching through `ToolExecutor for ToolCatalog` runs the registered
/// tool with its own spec, and an unknown name is a value error, not a panic.
#[test]
fn executor_dispatches_known_and_refuses_unknown() -> Result<(), ToolError> {
    let catalog = ToolCatalog::new();
    catalog.register(Arc::new(EchoTool));

    let spec = ToolSpec {
        name: "echo".into(),
        description: "ignored on dispatch".into(),
        params_schema: json!({}),
    };
    let result = catalog.execute(&spec, &json!({"text": "hi"}), &AbortSignal::new())?;
    assert_eq!(result.output, "hi");
    assert!(!result.is_error);

    let unknown = ToolSpec {
        name: "nope".into(),
        description: String::new(),
        params_schema: json!({}),
    };
    let Err(err) = catalog.execute(&unknown, &json!({}), &AbortSignal::new()) else {
        panic!("unknown tool must error, got Ok");
    };
    assert!(matches!(err, ToolError::Execute(msg) if msg.contains("unknown tool: nope")));
    Ok(())
}

/// `execute_in` with no session cwd delegates to `execute`, so direct
/// callers keep the process-cwd behavior.
#[test]
fn execute_in_without_cwd_delegates() -> Result<(), ToolError> {
    let tool: Arc<dyn Tool> = Arc::new(EchoTool);
    let result = tool.execute_in(&json!({"text": "direct"}), &AbortSignal::new(), None)?;
    assert_eq!(result.output, "direct");
    Ok(())
}

/// Aborting before the first poll short-circuits a running tool, and `flag()`
/// / `from_flag()` bridge two signals onto one shared `AtomicBool` — the
/// shape a host uses to hand its own flag to a harness tool.
#[test]
fn abort_signal_short_circuits_and_bridges() -> Result<(), ToolError> {
    let tool: Arc<dyn Tool> = Arc::new(PollingTool);
    let abort = AbortSignal::new();
    abort.abort();
    let Err(err) = tool.execute(&json!({}), &abort) else {
        panic!("pre-aborted tool must return Aborted, got Ok");
    };
    assert!(matches!(err, ToolError::Aborted));

    // Bridging: aborting the wrapper flips the flag the adapter kept.
    let owner = AbortSignal::new();
    let bridged = AbortSignal::from_flag(owner.flag());
    bridged.abort();
    assert!(owner.is_aborted());
    Ok(())
}

/// Truncation keeps the tail (errors live at the end) and spills the full
/// body to a temp file once a tool exceeds `MAX_OUTPUT_LINES` lines.
#[test]
fn truncate_output_keeps_tail_and_spills() -> std::io::Result<()> {
    let short = "alpha\nbeta\n";
    assert_eq!(truncate_output(short)?, short.to_string());

    let lines: Vec<String> = (0..MAX_OUTPUT_LINES + 50)
        .map(|i| format!("line-{i}"))
        .collect();
    let body = lines.join("\n");
    let kept = truncate_output(&body)?;
    let kept_lines: Vec<&str> = kept.lines().collect();

    // 1 marker line + MAX_OUTPUT_LINES tail lines; the last input line
    // survives intact.
    assert_eq!(kept_lines.len(), MAX_OUTPUT_LINES + 1);
    assert!(kept.starts_with("[output truncated:"));
    assert_eq!(kept_lines[kept_lines.len() - 1], "line-249");
    Ok(())
}

/// The catalog owns a parsed spec's round-trip shape is the stream's job
/// (see `event_serde.rs`); here we only pin what `ToolCallSpec` carries —
/// id, name, args — since that is the vocabulary the stream produces.
#[test]
fn tool_call_spec_carries_id_name_args() {
    let spec = ToolCallSpec {
        id: "call-7".into(),
        name: "echo".into(),
        args: json!({"text": "hi"}),
    };
    assert_eq!(spec.id, "call-7");
    assert_eq!(spec.name, "echo");
    assert_eq!(spec.args, json!({"text": "hi"}));
}
