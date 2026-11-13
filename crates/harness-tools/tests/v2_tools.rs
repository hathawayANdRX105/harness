//! Integration tests for the v2 tools: TerminalTool and WebFetchTool.
//!
//! Each test is self-contained: TerminalTool runs against a temp directory,
//! WebFetchTool against a local axum server on a random port. Nothing touches
//! real user files or the network. Tests return `Result` and use `?`, so no
//! `unwrap`/`expect` appears anywhere.

use std::path::PathBuf;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use harness_core::tool::{Tool, ToolError, ToolRisk};
use harness_tools::builtin::{TerminalTool, WebFetchTool};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Helper: a local axum server with text, large, error, and binary routes.
// ---------------------------------------------------------------------------

/// Purpose: give WebFetchTool a real localhost server to fetch from.
/// Scenario: bind a random port, serve /text, /large, /error, /binary, spawn
/// axum on it, return the base URL.
/// Acceptance: the caller can fetch `http://127.0.0.1:<port>/...`.
async fn spawn_text_server() -> Result<String, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let app = Router::new()
        .route("/text", get(|| async { "hello webfetch" }))
        .route("/large", get(|| async { "a".repeat(2000) }))
        .route(
            "/error",
            get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/binary",
            get(|| async {
                ([(CONTENT_TYPE, "application/octet-stream")], "not text").into_response()
            }),
        );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

// ---------------------------------------------------------------------------
// TerminalTool
// ---------------------------------------------------------------------------

/// Purpose: verify TerminalTool runs a command and returns its merged output.
/// Scenario: invoke `printf 'hello terminal'` in a temp cwd.
/// Acceptance: `is_error` is false and content contains the command output.
#[tokio::test]
async fn terminal_runs_command_with_output() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = TerminalTool::new(dir.path().to_path_buf());
    let cancel = CancellationToken::new();
    let result = tool
        .invoke(json!({"command": "printf 'hello terminal'"}), &cancel)
        .await?;
    assert!(!result.is_error);
    assert!(result.content.contains("hello terminal"));
    Ok(())
}

/// Purpose: verify TerminalTool merges stderr under a `[stderr]` label.
/// Scenario: invoke a command writing to both stdout and stderr.
/// Acceptance: content contains both streams and the `[stderr]` label.
#[tokio::test]
async fn terminal_merges_labelled_stderr() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = TerminalTool::new(dir.path().to_path_buf());
    let cancel = CancellationToken::new();
    let result = tool
        .invoke(
            json!({"command": "printf 'out'; printf 'err' >&2"}),
            &cancel,
        )
        .await?;
    assert!(!result.is_error);
    assert!(result.content.contains("out"));
    assert!(result.content.contains("[stderr]"));
    assert!(result.content.contains("err"));
    Ok(())
}

/// Purpose: verify TerminalTool reports `is_error` on a non-zero exit.
/// Scenario: invoke `exit 3`.
/// Acceptance: `is_error` is true and content carries the `[exit 3]` marker.
#[tokio::test]
async fn terminal_nonzero_exit_is_error() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = TerminalTool::new(dir.path().to_path_buf());
    let cancel = CancellationToken::new();
    let result = tool.invoke(json!({"command": "exit 3"}), &cancel).await?;
    assert!(result.is_error);
    assert!(result.content.contains("[exit 3]"));
    Ok(())
}

/// Purpose: verify TerminalTool kills the child when the timeout expires.
/// Scenario: invoke `sleep 30` with `timeout_ms: 100`.
/// Acceptance: `is_error` is true and content reports the timeout.
#[tokio::test]
async fn terminal_timeout_kills_sleep() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = TerminalTool::new(dir.path().to_path_buf());
    let cancel = CancellationToken::new();
    let result = tool
        .invoke(json!({"command": "sleep 30", "timeout_ms": 100}), &cancel)
        .await?;
    assert!(result.is_error);
    assert!(
        result
            .content
            .contains("timed out after 100ms and was killed")
    );
    Ok(())
}

/// Purpose: verify TerminalTool kills the child on cancellation.
/// Scenario: invoke `sleep 30` and cancel after 50 ms.
/// Acceptance: invoke returns `Err(ToolError::Cancelled)`.
#[tokio::test]
async fn terminal_cancel_kills_child() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let tool = TerminalTool::new(dir.path().to_path_buf());
    let token = CancellationToken::new();
    let canceller = token.clone();
    let _guard = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        canceller.cancel();
    });
    let result = tool.invoke(json!({"command": "sleep 30"}), &token).await;
    assert!(matches!(result, Err(ToolError::Cancelled)));
    Ok(())
}

/// Purpose: verify the new tools' name and risk metadata.
/// Scenario: construct TerminalTool and WebFetchTool.
/// Acceptance: names are `"terminal"`/`"webfetch"`, risks are
/// `Destructive`/`ReadOnly`.
#[test]
fn terminal_and_webfetch_metadata() {
    let tool = TerminalTool::new(PathBuf::from("."));
    assert_eq!(tool.name(), "terminal");
    assert_eq!(tool.risk(), ToolRisk::Destructive);
    let tool = WebFetchTool::new();
    assert_eq!(tool.name(), "webfetch");
    assert_eq!(tool.risk(), ToolRisk::ReadOnly);
}

// ---------------------------------------------------------------------------
// WebFetchTool
// ---------------------------------------------------------------------------

/// Purpose: verify WebFetchTool GETs text from a local server.
/// Scenario: fetch /text from the mock axum server.
/// Acceptance: `is_error` is false and content is the served body.
#[tokio::test]
async fn webfetch_gets_text() -> Result<(), Box<dyn std::error::Error>> {
    let base = spawn_text_server().await?;
    let tool = WebFetchTool::new();
    let cancel = CancellationToken::new();
    let result = tool
        .invoke(json!({"url": format!("{base}/text")}), &cancel)
        .await?;
    assert!(!result.is_error);
    assert_eq!(result.content.trim(), "hello webfetch");
    Ok(())
}

/// Purpose: verify WebFetchTool caps the body at max_bytes.
/// Scenario: fetch /large (2000 bytes) with max_bytes=100.
/// Acceptance: content starts with 100 captured bytes and carries the cap
/// notice.
#[tokio::test]
async fn webfetch_caps_body_at_max_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let base = spawn_text_server().await?;
    let tool = WebFetchTool::new();
    let cancel = CancellationToken::new();
    let result = tool
        .invoke(
            json!({"url": format!("{base}/large"), "max_bytes": 100}),
            &cancel,
        )
        .await?;
    assert!(!result.is_error);
    assert!(result.content.starts_with(&"a".repeat(100)));
    assert!(result.content.contains("(body capped at 100 bytes)"));
    Ok(())
}

/// Purpose: verify WebFetchTool reports `is_error` for a non-2xx status.
/// Scenario: fetch /error, which returns 500.
/// Acceptance: `is_error` is true and content mentions HTTP 500.
#[tokio::test]
async fn webfetch_non_2xx_is_error() -> Result<(), Box<dyn std::error::Error>> {
    let base = spawn_text_server().await?;
    let tool = WebFetchTool::new();
    let cancel = CancellationToken::new();
    let result = tool
        .invoke(json!({"url": format!("{base}/error")}), &cancel)
        .await?;
    assert!(result.is_error);
    assert!(result.content.contains("HTTP 500"));
    Ok(())
}

/// Purpose: verify WebFetchTool refuses non-text content types.
/// Scenario: fetch /binary, which serves application/octet-stream.
/// Acceptance: `is_error` is true and content names the content type.
#[tokio::test]
async fn webfetch_rejects_non_text_content_type() -> Result<(), Box<dyn std::error::Error>> {
    let base = spawn_text_server().await?;
    let tool = WebFetchTool::new();
    let cancel = CancellationToken::new();
    let result = tool
        .invoke(json!({"url": format!("{base}/binary")}), &cancel)
        .await?;
    assert!(result.is_error);
    assert!(result.content.contains("application/octet-stream"));
    Ok(())
}

/// Purpose: verify WebFetchTool rejects non-http(s) schemes.
/// Scenario: fetch ftp://example.com.
/// Acceptance: `is_error` is true and content names the scheme.
#[tokio::test]
async fn webfetch_rejects_invalid_scheme() -> Result<(), Box<dyn std::error::Error>> {
    let tool = WebFetchTool::new();
    let cancel = CancellationToken::new();
    let result = tool
        .invoke(json!({"url": "ftp://example.com"}), &cancel)
        .await?;
    assert!(result.is_error);
    assert!(result.content.contains("ftp"));
    Ok(())
}
