//! Terminal tool: run a shell command via `sh -c` with a deadline and abort.
//!
//! v1 semantics: no pty — the command runs as `sh -c` with stdout and stderr
//! captured from pipes, so interactive programs that need a terminal are not
//! a good fit here; a pty is a separate concern. Both pipes are drained by
//! their own reader tasks, so a chatty command cannot stall on a full pipe
//! or exhaust memory: each pipe is captured up to [`PIPE_CAP_BYTES`], past
//! which the reader keeps draining and drops the rest. The child is killed
//! when the timeout expires or the cancellation token fires, and
//! `kill_on_drop` covers the race where the tool value is dropped while a
//! command is still in flight (the child is then reaped on drop).

use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

use crate::args::{optional_integer, require_string};
use crate::output::{DEFAULT_MAX_OUTPUT_CHARS, truncate_output};

/// Bytes captured from one pipe before the reader drops the rest.
///
/// The merged output is truncated again by [`crate::output::truncate_output`]
/// anyway, so this only bounds memory while streaming.
const PIPE_CAP_BYTES: usize = DEFAULT_MAX_OUTPUT_CHARS;

/// Run a shell command (`sh -c`) in the tool's working directory.
pub struct TerminalTool {
    cwd: Arc<std::path::PathBuf>,
}

impl TerminalTool {
    pub fn new(cwd: std::path::PathBuf) -> Self {
        Self { cwd: Arc::new(cwd) }
    }
}

#[async_trait]
impl Tool for TerminalTool {
    fn name(&self) -> &str {
        "terminal"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "terminal".into(),
            description: "Run a shell command via `sh -c` in the tool's working directory, with an optional `timeout_ms` deadline and cancellation that kills the child process. stdout and stderr are merged (stderr under a `[stderr]` label) and truncated to the configured output cap, with the full output spilled to a temp file. A non-zero exit, a kill by timeout, or a kill by cancellation reports `is_error`; cancellation itself is reported as `ToolError::Cancelled`.".into(),
            input: json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "Shell command to run, interpreted by `sh -c`."
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "description": "Maximum milliseconds to run before the child is killed. Absent means no deadline.",
                    }
                },
                "required": ["command"],
            }),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::Destructive
    }

    async fn invoke(
        &self,
        args: Value,
        cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        let command = require_string(&args, "command")?;
        let deadline = timeout_deadline(&args)?;

        // Piped stdout/stderr so both streams are captured; the readers run
        // as their own tasks so neither pipe can block waiting on the other
        // while we wait for the child to exit. `kill_on_drop` reaps the child
        // if this tool value is dropped while a command is still in flight.
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&command)
            .current_dir(&*self.cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| ToolError::Exec(format!("failed to spawn `sh -c {command}`: {e}")))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ToolError::Exec("terminal: `sh -c` provided no stdout pipe".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ToolError::Exec("terminal: `sh -c` provided no stderr pipe".into()))?;
        let stdout_task = tokio::spawn(drain_pipe(stdout, PIPE_CAP_BYTES));
        let stderr_task = tokio::spawn(drain_pipe(stderr, PIPE_CAP_BYTES));

        // Wait for the child, losing to cancellation or the deadline: both
        // kill the child and reap it so nothing is left behind.
        let (status, timed_out) = match deadline {
            Some(duration) => wait_with_deadline(&mut child, cancel, duration).await?,
            None => {
                let status = wait_exit_or_cancel(&mut child, cancel).await?;
                (status, false)
            }
        };

        // The child is dead, so both pipes are closed and the reader tasks
        // have finished draining; collect what they captured.

        let stdout_text = stdout_task
            .await
            .map_err(|e| ToolError::Exec(format!("stdout reader failed: {e}")))?;
        let stderr_text = stderr_task
            .await
            .map_err(|e| ToolError::Exec(format!("stderr reader failed: {e}")))?;
        let mut output = merge_output(stdout_text, stderr_text);
        let mut is_error = false;
        if timed_out {
            output.push_str(&format!(
                "\ncommand timed out after {}ms and was killed",
                deadline.map_or(0, |d| d.as_millis() as u64)
            ));
            is_error = true;
        } else {
            append_exit_marker(&mut output, &status, &mut is_error);
        }

        let (content, spill) = truncate_output(&output, DEFAULT_MAX_OUTPUT_CHARS);
        let trace = spill
            .as_ref()
            .map(|path| format!("terminal `{command}`: full output at {}", path.display()));
        Ok(ToolResult {
            content,
            is_error,
            trace,
        })
    }
}

/// Wait for `child` to exit within `duration`, losing to cancellation: on
/// cancellation or deadline the child is killed and reaped.
async fn wait_with_deadline(
    child: &mut Child,
    cancel: &CancellationToken,
    duration: Duration,
) -> Result<(ExitStatus, bool), ToolError> {
    match tokio::time::timeout(duration, wait_exit_or_cancel(child, cancel)).await {
        Ok(Ok(status)) => Ok((status, false)),
        Ok(Err(error)) => Err(error),
        Err(_elapsed) => {
            let _ = child.kill().await;
            let status = child
                .wait()
                .await
                .map_err(|e| ToolError::Exec(format!("failed to reap timed-out child: {e}")))?;
            Ok((status, true))
        }
    }
}

/// Wait for `child` to exit, unless `cancel` fires first: then kill and reap
/// the child and report [`ToolError::Cancelled`].
async fn wait_exit_or_cancel(
    child: &mut Child,
    cancel: &CancellationToken,
) -> Result<ExitStatus, ToolError> {
    match tokio::select! {
        biased;
        _ = cancel.cancelled() => WaitWon::Cancelled,
        status = child.wait() => WaitWon::Exited(status),
    } {
        WaitWon::Cancelled => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(ToolError::Cancelled)
        }
        WaitWon::Exited(status) => {
            status.map_err(|e| ToolError::Exec(format!("failed to wait for child process: {e}")))
        }
    }
}

/// How the wait race ended.
enum WaitWon {
    Cancelled,
    Exited(std::io::Result<ExitStatus>),
}

/// Read `pipe` to EOF, keeping at most `cap` bytes and discarding the rest.
///
/// Draining to EOF is load-bearing: once we stop capturing we keep reading,
/// so a writer whose output we no longer keep is never blocked behind a full
/// pipe buffer.
async fn drain_pipe(
    mut pipe: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    cap: usize,
) -> String {
    let mut captured = Vec::new();
    let mut chunk = [0u8; 8192];
    while let Ok(n) = pipe.read(&mut chunk).await {
        if n == 0 {
            break;
        }
        if captured.len() < cap {
            let take = (cap - captured.len()).min(n);
            captured.extend_from_slice(&chunk[..take]);
        }
    }
    String::from_utf8_lossy(&captured).to_string()
}

/// Concatenate captured stdout with stderr under a `[stderr]` label.
fn merge_output(stdout: String, stderr: String) -> String {
    let mut output = stdout;
    if !stderr.is_empty() {
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str("[stderr]\n");
        output.push_str(&stderr);
    }
    output
}

/// Append the exit marker to `output` and flip `is_error` on failure.
fn append_exit_marker(output: &mut String, status: &ExitStatus, is_error: &mut bool) {
    match status.code() {
        Some(code) if code != 0 => {
            *is_error = true;
            output.push_str(&format!("\n[exit {code}]"));
        }
        Some(_) => {}
        None => {
            *is_error = true;
            output.push_str("\n[killed by signal]");
        }
    }
}

/// Parse `timeout_ms` into an optional `Duration`; a negative value is an
/// argument error, absent means no deadline.
fn timeout_deadline(args: &Value) -> Result<Option<Duration>, ToolError> {
    match optional_integer(args, "timeout_ms")? {
        None => Ok(None),
        Some(ms) if ms < 0 => Err(ToolError::InvalidArgs {
            name: "terminal".into(),
            reason: "argument `timeout_ms` must be a non-negative integer".into(),
        }),
        Some(ms) => Ok(Some(Duration::from_millis(ms as u64))),
    }
}
