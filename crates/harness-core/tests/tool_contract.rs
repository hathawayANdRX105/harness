//! Tool contract behavior: the async `Tool` trait behind a trait object,
//! risk classification, `serde_json::Value` arguments, and cancellation
//! racing a tool body.
//!
//! Acceptance: `name`/`schema`/`risk` surface an implementor's three
//! primitives, a failed invocation is a value (`is_error`) rather than an
//! unwind, and a long-running `invoke` resolves `ToolError::Cancelled`
//! once the run's token fires.

use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

/// A tool that echoes the `text` argument.
struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "echo".into(),
            description: "Echoes the `text` argument back.".into(),
            input: json!({
                "type": "object",
                "properties": { "text": { "type": "string" } },
                "required": ["text"],
            }),
        }
    }
    fn risk(&self) -> ToolRisk {
        ToolRisk::ReadOnly
    }
    async fn invoke(
        &self,
        args: Value,
        _cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        let text =
            args.get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| ToolError::InvalidArgs {
                    name: "echo".into(),
                    reason: "missing string argument `text`".into(),
                })?;
        Ok(ToolResult {
            content: text.to_string(),
            is_error: false,
            trace: None,
        })
    }
}

/// A tool that stands in for any long-running one: it polls the token
/// between work units and only ever stops because the run was cancelled.
struct SlowTool;

#[async_trait]
impl Tool for SlowTool {
    fn name(&self) -> &str {
        "slow"
    }
    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "slow".into(),
            description: "Polls the cancel token until it fires.".into(),
            input: json!({"type": "object"}),
        }
    }
    fn risk(&self) -> ToolRisk {
        ToolRisk::Idempotent
    }
    async fn invoke(
        &self,
        _args: Value,
        cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        // Bounded so a missing cancellation fails the test rather than
        // hanging it; the loop must observe the token on every poll.
        for _ in 0..10_000 {
            if cancel.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
        }
        Ok(ToolResult {
            content: String::new(),
            is_error: true,
            trace: None,
        })
    }
}

/// The three primitives feed the schema; the risk class is separate from the
/// name so a policy can decide without running anything.
#[test]
fn primitives_and_risk_surface_through_trait_object() {
    let tool: Arc<dyn Tool> = Arc::new(EchoTool);
    let schema = tool.schema();
    assert_eq!(schema.name, "echo");
    assert_eq!(schema.description, "Echoes the `text` argument back.");
    assert_eq!(schema.input["required"][0], "text");
    assert_eq!(tool.risk(), ToolRisk::ReadOnly);
}

/// `Value` arguments pass through the contract unchanged — the dynamic
/// boundary MCP and plugin tools require.
#[test]
fn value_args_round_trip_into_results() {
    let tool: Arc<dyn Tool> = Arc::new(EchoTool);
    let token = CancellationToken::new();
    let result = block_on(tool.invoke(json!({"text": "hi"}), &token));
    let result = result.unwrap_or_else(|e| panic!("echo must succeed, got {e:?}"));
    assert_eq!(result.content, "hi");
    assert!(!result.is_error);
}

/// Missing-typed arguments are a `ToolError::InvalidArgs`, not a panic —
/// validation failures are values the dispatch layer maps.
#[test]
fn invalid_args_is_a_value_error() {
    let tool: Arc<dyn Tool> = Arc::new(EchoTool);
    let token = CancellationToken::new();
    let err = block_on(tool.invoke(json!({"count": 3}), &token));
    let Err(ToolError::InvalidArgs { name, reason }) = err else {
        panic!("expected InvalidArgs, got: {err:?}");
    };
    assert_eq!(name, "echo");
    assert!(reason.contains("missing string argument"));
}

// A failing tool reports `is_error`, not an Err — the model sees the failure
// as text (the value-shaped failure contract).
#[test]
fn failed_executions_can_be_values_too() {
    struct ExplodingTool;
    #[async_trait]
    impl Tool for ExplodingTool {
        fn name(&self) -> &str {
            "explode"
        }
        fn schema(&self) -> ToolSchema {
            ToolSchema {
                name: "explode".into(),
                description: "Always fails.".into(),
                input: json!({"type": "object"}),
            }
        }
        fn risk(&self) -> ToolRisk {
            ToolRisk::Destructive
        }
        async fn invoke(
            &self,
            _args: Value,
            _cancel: &CancellationToken,
        ) -> Result<ToolResult, ToolError> {
            // A tool that *chooses* to report failure as a value rather
            // than an Err: the loop backfills it into the transcript.
            Ok(ToolResult {
                content: "command failed: exit 42".into(),
                is_error: true,
                trace: Some("stderr captured".into()),
            })
        }
    }
    let token = CancellationToken::new();
    let result =
        block_on(ExplodingTool.invoke(json!({}), &token)).unwrap_or_else(|e| panic!("{e:?}"));
    assert!(result.is_error);
    assert_eq!(result.trace.as_deref(), Some("stderr captured"));
}

/// A cancelled run folds into the tool's own decision: the token fires and
/// the tool returns `ToolError::Cancelled` instead of finishing its work.
#[test]
fn cancelled_run_resolves_invoke_as_cancelled() {
    let tool: Arc<dyn Tool> = Arc::new(SlowTool);
    let token = CancellationToken::new();
    // Cancel before the body's first poll: every poll from the first checks
    // the token, so a cancelled run never enters the work loop.
    token.cancel();
    let err = block_on(tool.invoke(json!({}), &token));
    let Err(err) = err else {
        panic!("cancelled invoke must return Err, got Ok");
    };
    assert_eq!(err, ToolError::Cancelled);
}
