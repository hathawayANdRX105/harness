use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use regex::Regex;
use serde_json::Value;
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;

use crate::args::{optional_string, require_string};
use crate::builtin::path::MAX_FILE_BYTES;
use crate::output::{DEFAULT_MAX_OUTPUT_CHARS, truncate_output};

/// Walk the filesystem and find lines matching a regex.
///
/// The `cwd` is used as the search root; results are returned as
/// `file_path:line_number:content` lines.
pub struct GrepTool {
    cwd: Arc<PathBuf>,
}

impl GrepTool {
    pub fn new(cwd: PathBuf) -> Self {
        Self { cwd: Arc::new(cwd) }
    }
}

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &str {
        "grep"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "grep".into(),
            description: "Search file contents using a regular expression. Returns matching lines with file paths and line numbers.".into(),
            input: serde_json::json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regular expression pattern to search for"
                    },
                    "include": {
                        "type": "string",
                        "description": "Optional glob filter for file paths (e.g. `**/*.rs`)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum characters to return (default 100000). Full output is spilled to a temp file.",
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
        let include = optional_string(&args, "include")?;
        let limit = args
            .get("limit")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_MAX_OUTPUT_CHARS, |v| v as usize);

        let cwd = Arc::clone(&self.cwd);
        let compiled = Regex::new(&pattern).map_err(|e| ToolError::InvalidArgs {
            name: "grep".into(),
            reason: format!("invalid regex `{pattern}`: {e}"),
        })?;

        let (results, truncated) = spawn_blocking(move || -> Result<(String, bool), String> {
            search_files(&cwd, &compiled, include.as_deref())
        })
        .await
        .map_err(|e| ToolError::Exec(format!("grep task failed: {e}")))?
        .map_err(ToolError::Exec)?;

        if results.is_empty() && !truncated {
            return Ok(ToolResult {
                content: format!("no matches for `{pattern}`"),
                is_error: false,
                trace: None,
            });
        }

        let mut content = results;
        if truncated {
            content.push_str(&format!(
                "\n... (grep stopped at the file-visit cap; results may be incomplete)"
            ));
        }
        let char_count = content.chars().count();
        let (truncated_text, spill) = truncate_output(&content, limit);

        let trace = spill.as_ref().map(|p| {
            format!(
                "grep `{pattern}`: {} chars, full output at {}",
                char_count,
                p.display()
            )
        });

        Ok(ToolResult {
            content: format!("matches for `{pattern}`:\n{truncated_text}"),
            is_error: false,
            trace,
        })
    }
}

/// Walk `dir` recursively, read each file that matches `include_glob`, and
/// collect lines matching `re`.
///
/// Cycle detection: symlinked directories are followed only once per
/// canonicalized path, so a symlink loop cannot cause unbounded traversal.
/// Resource caps: each file larger than `MAX_FILE_BYTES` is skipped, and the
/// walk stops after `MAX_FILE_VISITS` files, returning a truncation flag.
fn search_files(
    dir: &PathBuf,
    re: &Regex,
    include_glob: Option<&str>,
) -> Result<(String, bool), String> {
    use std::fs;

    // Build a glob matcher for the include filter if present.
    let include_matcher = include_glob
        .map(|g| glob::Pattern::new(g).map_err(|e| format!("invalid include glob: {e}")))
        .transpose()?;

    // Resource caps, documented per the MAJOR finding.
    const MAX_FILE_VISITS: u64 = 10_000;

    let mut out = Vec::new();
    let mut walk_stack = vec![dir.clone()];
    let mut visited: HashSet<PathBuf> = HashSet::new();
    let mut file_visits: u64 = 0;
    let mut truncated = false;

    while let Some(current) = walk_stack.pop() {
        let canonical = current
            .canonicalize()
            .map_err(|e| format!("cannot canonicalize {current:?}: {e}"))?;
        if !visited.insert(canonical) {
            // Already walked this directory (symlink loop or hard link).
            continue;
        }

        let Ok(entries) = fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_stack.push(path);
            } else if path.is_file() {
                // Per-file byte cap: skip oversized files instead of reading
                // them wholly into memory.
                let Ok(metadata) = path.metadata() else {
                    continue;
                };
                if metadata.len() > MAX_FILE_BYTES {
                    continue;
                }

                file_visits += 1;
                if file_visits > MAX_FILE_VISITS {
                    truncated = true;
                    break;
                }

                let Ok(s) = fs::read_to_string(&path) else {
                    continue;
                };
                // Apply the include filter.
                let rel = path.strip_prefix(dir).unwrap_or(&path).to_string_lossy();
                if let Some(matcher) = &include_matcher
                    && !matcher.matches(&rel)
                {
                    continue;
                }
                for (lineno, line) in s.lines().enumerate() {
                    if re.is_match(line) {
                        out.push(format!("{}:{}:{}", rel, lineno + 1, line));
                    }
                }
            }
        }
        if truncated {
            break;
        }
    }

    Ok((out.join("\n"), truncated))
}
