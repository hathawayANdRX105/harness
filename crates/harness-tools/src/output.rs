//! Output truncation and spill helpers for builtin tools.
//!
//! When a tool's output exceeds the configured character limit, the content
//! keeps the *tail* (last N characters) so the model sees the end of the
//! output — the part most likely to be recent or relevant — and the full
//! output is written to a temporary file whose path is returned separately.
//! The caller (or the tool's template) appends a spill notice to the content.
//!
//! Design provenance: harness-rs's `truncate_output`
//! (`crates/harness-tools/src/output.rs`) where the tail-preserving truncation
//! strategy and the temp-file spill path originate. pi's `tools.ts` cap at
//! 100k characters is the default value.

use std::io::Write;
use std::path::PathBuf;

use tempfile::NamedTempFile;

/// Default maximum output length in characters.
///
/// Matches pi's `CAP_OUTPUT_CHARS` (100_000), enough for several pages of
/// source code or a file listing without flooding the context window.
pub const DEFAULT_MAX_OUTPUT_CHARS: usize = 100_000;

/// Truncate `content` to `max_chars`, keeping the tail, and spill the full
/// text to a temporary file.
///
/// Returns `(truncated_text, spill_path)` where:
/// - `truncated_text` is the last `max_chars` characters (or the full text
///   when shorter than `max_chars`), with a note prepended when truncated.
/// - `spill_path` is `Some(path)` when the full text was spilled, `None`
///   when the text fit within the limit.
///
/// The temporary file is created in the system temp directory with a
/// `harness-tools-` prefix and `.txt` suffix. It is *not* automatically
/// deleted — the consumer (agent loop output compaction, human review) may
/// need to reference it after the tool call completes.
pub fn truncate_output(content: &str, max_chars: usize) -> (String, Option<PathBuf>) {
    if content.chars().count() <= max_chars {
        return (content.to_string(), None);
    }

    // Spill file write failed — return what we have without truncation
    // so the model at least sees the full output, even if the spill
    // side effect failed.
    let Ok(spill_path) = write_spill_file(content) else {
        return (content.to_string(), None);
    };

    let tail: String = content
        .chars()
        .rev()
        .take(max_chars)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let truncated = format!(
        "... ({} characters, truncated to {}. Full output at {})",
        content.chars().count(),
        max_chars,
        spill_path.display(),
    );

    // Tack the tail after the notice — the model sees the end of the output.
    let result = format!("{truncated}\n{tail}");
    (result, Some(spill_path))
}

/// Write `content` to a new temporary file and return its path.
fn write_spill_file(content: &str) -> Result<PathBuf, std::io::Error> {
    let mut tmp = NamedTempFile::with_prefix_in("harness-tools-", std::env::temp_dir())?;
    tmp.write_all(content.as_bytes())?;
    let path = tmp.into_temp_path();
    // Keep the file alive — the caller references it after this function returns.
    let persisted = path.to_path_buf();
    let _ = path.keep();
    Ok(persisted)
}
