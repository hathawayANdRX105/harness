//! Config error types — three variants with file:line locations, plus IO and
//! migration wrappers.
//!
//! Design provenance: pi's `settings-schema.ts` produces structured validation
//! errors with field name and path. Here the toml deserialization error is
//! classified by message content and enriched with the source file path so the
//! user sees where exactly in their config the problem is.

use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur when loading or parsing harness configuration.
///
/// Every variant carries a human-readable message that includes the source file
/// path and line number so the user can locate the problem in their config file
/// without a second tool.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// A required field is missing from the config file.
    #[error("missing field `{field}` at {file}:{line}")]
    MissingField {
        field: String,
        file: String,
        line: usize,
    },

    /// A field has the wrong type (e.g., a string where a number is expected).
    #[error("type error for field `{field}` at {file}:{line}: {detail}")]
    TypeError {
        field: String,
        detail: String,
        file: String,
        line: usize,
    },

    /// An unknown field was present in the config (deny_unknown_fields fired).
    #[error("unknown field `{field}` at {file}:{line}")]
    UnknownField {
        field: String,
        file: String,
        line: usize,
    },

    /// The config file could not be read (does not exist, permissions, …).
    #[error("failed to read config `{path}`: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    /// A schema migration step failed — the config version could not be
    /// advanced to the current schema version.
    #[error("migration v{from_version} → v{target_version} failed: {detail}")]
    Migration {
        from_version: u32,
        target_version: u32,
        detail: String,
    },
}

/// Classify a `toml::de::Error` by message content and wrap it in a
/// `ConfigError` with the source file path and line number.
///
/// toml 0.8's deserialization errors expose a `message()` and a byte `span()`,
/// but not a structured enum. We classify by inspecting the message string and
/// compute the line number from the byte span against the source text.
pub(crate) fn classify_toml_error(
    err: &toml::de::Error,
    file: &str,
    source_text: &str,
) -> ConfigError {
    let line = err
        .span()
        .map(|s| byte_offset_to_line(source_text, s.start))
        .unwrap_or(0);
    let msg = err.message().to_string();

    // serde's `missing field` error: `missing field \`field_name\``
    if let Some(field) = extract_quoted_field(&msg, "missing field") {
        return ConfigError::MissingField {
            field,
            file: file.to_string(),
            line,
        };
    }

    // serde's `unknown field` error: `unknown field \`field_name\`, expected …`
    if let Some(field) = extract_quoted_field(&msg, "unknown field") {
        return ConfigError::UnknownField {
            field,
            file: file.to_string(),
            line,
        };
    }

    // Everything else (type mismatch, syntax error, …) is a type error.
    let field = extract_field_name_fallback(&msg).unwrap_or("<root>");
    ConfigError::TypeError {
        field: field.to_string(),
        detail: msg,
        file: file.to_string(),
        line,
    }
}

/// Convert a byte offset in source text to a 1-based line number.
fn byte_offset_to_line(source: &str, offset: usize) -> usize {
    // Count newlines up to the given offset. This is O(n) but config files
    // are small enough that the cost is negligible.
    source[..offset.min(source.len())]
        .bytes()
        .filter(|&b| b == b'\n')
        .count()
        + 1
}

/// Extract a quoted field name from a serde error message after a known
/// prefix. serde formats: `` missing field `field_name` ``
fn extract_quoted_field(msg: &str, prefix: &str) -> Option<String> {
    let rest = msg.strip_prefix(prefix)?;
    let start = rest.find('`')?;
    let after_open = &rest[start + 1..];
    let end = after_open.find('`')?;
    Some(after_open[..end].to_string())
}

/// Extract a field name from a type error that mentions a key.
/// toml type errors: `invalid type: …, for key \`field_name\``
fn extract_field_name_fallback(msg: &str) -> Option<&str> {
    let marker = "for key `";
    let start = msg.find(marker)?;
    let after_marker = &msg[start + marker.len()..];
    let end = after_marker.find('`')?;
    Some(&after_marker[..end])
}
