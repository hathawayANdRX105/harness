//! Read tool: read a file from the cwd-relative path.
//!
//! Design provenance: harness-rs's `read-tool` — cwd-aware, reports file
//! not found as a value-shaped error, respects output truncation via the
//! shared `crate::output` helpers.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::{Value, json};
use tokio::fs;
use tokio_util::sync::CancellationToken;

use crate::args::require_string;
use crate::output::{DEFAULT_MAX_OUTPUT_CHARS, truncate_output};

/// Read a file from disk.
///
/// Takes a `file_path` argument (relative to the session's cwd or absolute)
/// and an optional `limit` for truncation.
pub struct ReadTool {
    cwd: Arc<std::path::PathBuf>,
}

impl ReadTool {
    pub fn new(cwd: PathBuf) -> Self {
        Self { cwd: Arc::new(cwd) }
    }
}

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &str {
        "read"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "read".into(),
            description: "Read the contents of a file. Returns the file content, truncated to the configured limit with a spill file for the full output.".into(),
            input: json!({
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the file, relative to the current working directory or absolute"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum characters to return (default 100000). Full output is spilled to a temp file.",
                        "default": DEFAULT_MAX_OUTPUT_CHARS
                    }
                },
                "required": ["file_path"]
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
        let file_path = require_string(&args, "file_path")?;
        let limit = args
            .get("limit")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_MAX_OUTPUT_CHARS, |v| v as usize);

        let path = self.cwd.join(&file_path);
        let content = fs::read_to_string(&path).await.map_err(|e| {
            let reason = if e.kind() == std::io::ErrorKind::NotFound {
                format!("file not found: {}", file_path)
            } else {
                format!("failed to read `{}`: {e}", file_path)
            };
            ToolError::Exec(reason)
        })?;

        let char_count = content.chars().count();
        let (truncated, spill) = truncate_output(&content, limit);

        let trace = spill.as_ref().map(|p| {
            format!(
                "read `{file_path}`: {char_count} chars, full output at {}",
                p.display()
            )
        });

        Ok(ToolResult {
            content: truncated,
            is_error: false,
            trace,
        })
    }
}
