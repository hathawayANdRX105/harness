//! Glob tool: list files matching a glob pattern.
//!
//! Design provenance: harness-rs's `glob-tool` — wraps the `glob` crate in
//! a blocking task since `glob` is synchronous I/O. Results are joined as
//! newline-separated paths, one per match.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::Value;
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;

use crate::args::require_string;
use crate::output::{DEFAULT_MAX_OUTPUT_CHARS, truncate_output};

/// List files matching a glob pattern.
///
/// The pattern is resolved relative to the session's cwd.
pub struct GlobTool {
    cwd: Arc<PathBuf>,
}

impl GlobTool {
    pub fn new(cwd: PathBuf) -> Self {
        Self { cwd: Arc::new(cwd) }
    }
}

#[async_trait]
impl Tool for GlobTool {
    fn name(&self) -> &str {
        "glob"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "glob".into(),
            description: "List files and directories matching a glob pattern. The pattern is relative to the current working directory.".into(),
            input: serde_json::json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern (e.g. `**/*.rs`, `src/**/*.ts`)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum characters to return (default 100000). Full list is spilled to a temp file.",
                        "default": DEFAULT_MAX_OUTPUT_CHARS
                    }
                },
                "required": ["pattern"]
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
        let pattern = require_string(&args, "pattern")?;
        let limit = args
            .get("limit")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_MAX_OUTPUT_CHARS, |v| v as usize);

        let cwd = Arc::clone(&self.cwd);
        let full_pattern = cwd.join(&pattern).to_string_lossy().to_string();

        let matches = spawn_blocking(move || -> Result<Vec<String>, String> {
            let entries: Vec<String> = glob::glob(&full_pattern)
                .map_err(|e| format!("invalid glob pattern: {e}"))?
                .filter_map(|r| r.ok())
                .map(|p| p.to_string_lossy().to_string())
                .collect();
            Ok(entries)
        })
        .await
        .map_err(|e| ToolError::Exec(format!("glob task failed: {e}")))?
        .map_err(ToolError::Exec)?;

        if matches.is_empty() {
            return Ok(ToolResult {
                content: format!("no matches for `{pattern}`"),
                is_error: false,
                trace: None,
            });
        }

        let joined = matches.join("\n");
        let char_count = joined.chars().count();
        let (truncated, spill) = truncate_output(&joined, limit);

        let trace = spill.as_ref().map(|p| {
            format!(
                "glob `{pattern}`: {} matches, {} chars, full list at {}",
                matches.len(),
                char_count,
                p.display()
            )
        });

        Ok(ToolResult {
            content: format!("{} matches for `{pattern}`:\n{}", matches.len(), truncated),
            is_error: false,
            trace,
        })
    }
}
