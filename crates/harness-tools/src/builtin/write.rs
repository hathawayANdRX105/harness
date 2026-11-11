//! Write tool: write content to a file (create or overwrite).
//!
//! Design provenance: harness-rs's `write-tool` — the destructive risk
//! classification matches the fact that writing a file can overwrite
//! existing content and cannot be undone by the tool layer.

use std::path::PathBuf;
use std::process::id as process_id;
use std::sync::Arc;

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::Value;
use tokio::fs;
use tokio_util::sync::CancellationToken;

use crate::args::require_string;
use crate::builtin::path::resolve_in_cwd;
/// Write content to a file (create or overwrite).
///
/// Takes `file_path` and `content` arguments. Creates parent directories
/// automatically.
pub struct WriteTool {
    cwd: Arc<PathBuf>,
}

impl WriteTool {
    pub fn new(cwd: PathBuf) -> Self {
        Self { cwd: Arc::new(cwd) }
    }
}

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &str {
        "write"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "write".into(),
            description:
                "Write content to a file. Creates parent directories if they don't exist. Overwrites existing files without confirmation.".into(),
            input: serde_json::json!({
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to write to, relative to the current working directory"
                    },
                    "content": {
                        "type": "string",
                        "description": "Content to write to the file"
                    }
                },
                "required": ["file_path", "content"]
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
        let content = require_string(&args, "content")?;

        // Containment: reject absolute paths, `..`, and symlink escapes
        // before touching the filesystem.
        let path = resolve_in_cwd(&self.cwd, &file_path)?;

        // Create parent directories if they don't exist.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                ToolError::Exec(format!(
                    "failed to create parent directories for `{file_path}`: {e}"
                ))
            })?;
        }

        // Atomic write: stage in a temp file in the same directory, then
        // rename over the target (atomic on the same filesystem).
        let pid = process_id();
        let file_name = path
            .file_name()
            .map(|name| format!("{}.tmp.{pid}", name.to_string_lossy()))
            .unwrap_or_else(|| format!("write.tmp.{pid}"));
        let mut tmp_path = path.clone();
        tmp_path.set_file_name(file_name);

        if let Err(e) = fs::write(&tmp_path, &content).await {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(ToolError::Exec(format!(
                "failed to write `{file_path}`: {e}"
            )));
        }

        if let Err(e) = fs::rename(&tmp_path, &path).await {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(ToolError::Exec(format!(
                "failed to finalize write to `{file_path}`: {e}"
            )));
        }

        let bytes = content.len();
        Ok(ToolResult {
            content: format!("wrote {} bytes to `{file_path}`", bytes),
            is_error: false,
            trace: None,
        })
    }
}
