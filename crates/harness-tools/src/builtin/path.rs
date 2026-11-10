//! Path containment for cwd-relative tool arguments.
//!
//! Every file path a model passes to a builtin tool is relative to the
//! session's working directory — that is the model-facing contract. This
//! module enforces it: absolute paths are rejected outright, `..` components
//! are rejected even when they would still land inside cwd, and the resolved
//! path is checked against the canonicalized cwd so symlinks cannot be used
//! to escape the working directory.

use std::path::{Component, Path, PathBuf};

use harness_core::tool::ToolError;

/// Per-file byte cap applied by `read` and `grep` so no tool call can read
/// an unbounded file into memory.
///
/// One mebibyte: large enough for normal source files, small enough that a
/// malicious or accidental huge file cannot exhaust memory. `read` shows the
/// first `MAX_FILE_BYTES` bytes and marks the result truncated; `grep`
/// skips oversized files entirely.
pub const MAX_FILE_BYTES: u64 = 1_048_576; // 1 MiB

/// Resolve `requested` (relative to `cwd`) to a concrete path, rejecting
/// anything that escapes the working directory.
///
/// Rules:
/// 1. Absolute paths are rejected outright — the model-facing contract is
///    "paths are relative to the working directory".
/// 2. Any `..` component is rejected, even when the path would still land
///    inside cwd (`sub/../ok.txt` is refused).
/// 3. The joined path is checked against the canonicalized cwd: the deepest
///    existing ancestor is canonicalized (for not-yet-existing targets, the
///    parent directory) and must start_with the canonicalized cwd, so
///    symlinks cannot be used to escape.
///
/// # Errors
///
/// Returns [`ToolError::Exec`] with a "path escapes the working directory"
/// message on any violation.
pub fn resolve_in_cwd(cwd: &Path, requested: &str) -> Result<PathBuf, ToolError> {
    check_components(requested)?;

    let canonical_cwd = cwd
        .canonicalize()
        .map_err(|e| ToolError::Exec(format!("failed to canonicalize working directory: {e}")))?;

    let joined = cwd.join(Path::new(requested));

    // Deepest existing ancestor: the target itself if it exists, otherwise
    // the first existing parent walking up — not-yet-existing targets are
    // validated via their parent directory.
    let mut ancestor = joined.clone();
    loop {
        if ancestor.exists() {
            break;
        }
        let Some(parent) = ancestor.parent() else {
            break;
        };
        ancestor = parent.to_path_buf();
    }

    let canonical_ancestor = ancestor
        .canonicalize()
        .map_err(|e| ToolError::Exec(format!("failed to canonicalize `{requested}`: {e}")))?;

    if !canonical_ancestor.starts_with(&canonical_cwd) {
        return Err(ToolError::Exec(format!(
            "path escapes the working directory: `{requested}` resolves outside cwd"
        )));
    }

    Ok(joined)
}

/// Reject absolute paths and `..` components lexically, before any fs access.
/// Used for file paths and glob patterns alike.
fn check_components(requested: &str) -> Result<(), ToolError> {
    let p = Path::new(requested);
    if p.is_absolute() {
        return Err(ToolError::Exec(format!(
            "path escapes the working directory: `{requested}` is absolute"
        )));
    }
    for component in p.components() {
        if matches!(component, Component::ParentDir) {
            return Err(ToolError::Exec(format!(
                "path escapes the working directory: `{requested}` contains `..`"
            )));
        }
    }
    Ok(())
}

/// Validate a glob pattern the same way `resolve_in_cwd` validates a file
/// path: no absolute patterns and no `..` components, before joining onto
/// cwd.
pub fn validate_glob_pattern(pattern: &str) -> Result<(), ToolError> {
    check_components(pattern)
}
