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
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use crate::args::require_string;
use crate::builtin::path::{MAX_FILE_BYTES, resolve_in_cwd};
use crate::output::{DEFAULT_MAX_OUTPUT_CHARS, truncate_output};

/// Read a file from disk.
///
/// Takes a `file_path` argument (relative to the session's cwd) and an
/// optional `limit` for truncation.
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

        // Containment: reject absolute paths, `..`, and symlink escapes
        // before touching the filesystem.
        let path = resolve_in_cwd(&self.cwd, &file_path)?;

        // Read with a per-file byte cap so an oversized file cannot consume
        // unbounded memory. Read up to cap+1 bytes so truncation is
        // detectable, then report it in the result content.
        let file = File::open(&path).await.map_err(|e| {
            let reason = if e.kind() == std::io::ErrorKind::NotFound {
                format!("file not found: {file_path}")
            } else {
                format!("failed to read `{file_path}`: {e}")
            };
            ToolError::Exec(reason)
        })?;

        let mut buf = Vec::new();
        let mut take = file.take(MAX_FILE_BYTES + 1);
        take.read_to_end(&mut buf)
            .await
            .map_err(|e| ToolError::Exec(format!("failed to read `{file_path}`: {e}")))?;

        let hit_byte_cap = buf.len() > MAX_FILE_BYTES as usize;
        if hit_byte_cap {
            buf.truncate(MAX_FILE_BYTES as usize);
        }

        let content = String::from_utf8(buf).map_err(|e| {
            ToolError::Exec(format!(
                "failed to read `{file_path}`: file is not valid UTF-8 ({e})"
            ))
        })?;

        let content = if hit_byte_cap {
            format!(
                "... (file `{file_path}` is larger than {MAX_FILE_BYTES} bytes; showing the first {MAX_FILE_BYTES} bytes)\n{content}"
            )
        } else {
            content
        };

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
