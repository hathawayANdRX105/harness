//! Glob tool: list files matching a glob pattern.
//!
//! Design provenance: harness-rs's `glob-tool` — wraps the `glob` crate in
//! a blocking task since `glob` is synchronous I/O. Results are joined as
//! newline-separated paths, one per match.

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;

use crate::args::require_string;
use crate::builtin::path::validate_glob_pattern;
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

        // Containment: reject absolute patterns and `..` components before
        // joining onto cwd.
        validate_glob_pattern(&pattern)?;

        // Match cap: stop the walk after this many matches instead of
        // collecting an unbounded tree, and note the truncation in output.
        const GLOB_MAX_MATCHES: usize = 1_000;

        let cwd = Arc::clone(&self.cwd);
        let full_pattern = cwd.join(&pattern).to_string_lossy().to_string();

        let (matches, truncated) =
            spawn_blocking(move || -> Result<(Vec<String>, bool), String> {
                let mut entries: Vec<String> = Vec::new();
                for result in
                    glob::glob(&full_pattern).map_err(|e| format!("invalid glob pattern: {e}"))?
                {
                    match result {
                        Ok(p) => {
                            entries.push(p.to_string_lossy().to_string());
                            if entries.len() >= GLOB_MAX_MATCHES {
                                return Ok((entries, true));
                            }
                        }
                        Err(e) => return Err(format!("glob iteration error: {e}")),
                    }
                }
                Ok((entries, false))
            })
            .await
            .map_err(|e| ToolError::Exec(format!("glob task failed: {e}")))?
            .map_err(ToolError::Exec)?;

        if matches.is_empty() && !truncated {
            return Ok(ToolResult {
                content: format!("no matches for `{pattern}`"),
                is_error: false,
                trace: None,
            });
        }

        let match_count = matches.len();
        let mut content = matches.join("\n");
        if truncated {
            content.push_str(&format!(
                "\n... ({match_count} matches shown; glob stopped at the {GLOB_MAX_MATCHES} match cap)"
            ));
        }
        let char_count = content.chars().count();
        let (truncated_text, spill) = truncate_output(&content, limit);

        let trace = spill.as_ref().map(|p| {
            format!(
                "glob `{pattern}`: {} matches, {} chars, full list at {}",
                match_count,
                char_count,
                p.display()
            )
        });

        Ok(ToolResult {
            content: format!("{match_count} matches for `{pattern}`:\n{truncated_text}"),
            is_error: false,
            trace,
        })
    }
}
