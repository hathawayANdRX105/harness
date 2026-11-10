//! Edit tool: find-and-replace edit in a file.
//!
//! Design provenance: harness-rs's `edit-tool` — the destructive risk
//! matches the fact that the edit changes file content in place. The
//! `old_text` / `new_text` pair matches the agent-coding convention:
//! the model specifies the exact text to replace and the new text, so
//! the edit is deterministic.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::Value;
use tokio::fs;
use tokio_util::sync::CancellationToken;

use crate::args::require_string;

/// Edit a file by replacing `old_text` with `new_text`.
///
/// Finds the first occurrence of `old_text` in the file and replaces it
/// with `new_text`. Reports an error when `old_text` is not found.
pub struct EditTool {
    cwd: Arc<PathBuf>,
}

impl EditTool {
    pub fn new(cwd: PathBuf) -> Self {
        Self { cwd: Arc::new(cwd) }
    }
}

#[async_trait]
impl Tool for EditTool {
    fn name(&self) -> &str {
        "edit"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "edit".into(),
            description: "Edit a file by finding `old_text` and replacing it with `new_text`. Only the first occurrence is replaced.".into(),
            input: serde_json::json!({
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the file to edit"
                    },
                    "old_text": {
                        "type": "string",
                        "description": "The exact text to find (first occurrence replaced)"
                    },
                    "new_text": {
                        "type": "string",
                        "description": "The replacement text"
                    }
                },
                "required": ["file_path", "old_text", "new_text"]
            }),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::Destructive
    }

    async fn invoke(
        &self,
        args: Value,
        _cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        let file_path = require_string(&args, "file_path")?;
        let old_text = require_string(&args, "old_text")?;
        let new_text = require_string(&args, "new_text")?;

        let path = self.cwd.join(&file_path);
        let content = fs::read_to_string(&path).await.map_err(|e| {
            let reason = if e.kind() == std::io::ErrorKind::NotFound {
                format!("file not found: {file_path}")
            } else {
                format!("failed to read `{file_path}`: {e}")
            };
            ToolError::Exec(reason)
        })?;

        let Some(pos) = content.find(&old_text) else {
            return Err(ToolError::Exec(format!(
                "`old_text` not found in `{file_path}`: {old_text:?}"
            )));
        };

        let new_content = format!(
            "{}{}{}",
            &content[..pos],
            new_text,
            &content[pos + old_text.len()..]
        );

        fs::write(&path, &new_content).await.map_err(|e| {
            ToolError::Exec(format!("failed to write `{file_path}` after edit: {e}"))
        })?;

        let char_diff = new_text.chars().count() as isize - old_text.chars().count() as isize;
        let sign = if char_diff >= 0 { "+" } else { "" };
        Ok(ToolResult {
            content: format!(
                "edited `{file_path}` ({sign}{} characters, 1 replacement)",
                char_diff
            ),
            is_error: false,
            trace: None,
        })
    }
}
