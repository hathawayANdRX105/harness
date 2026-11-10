//! Argument extraction helpers: typed field access from `serde_json::Value` args.
//!
//! These helpers produce clear error messages for missing or wrong-type
//! arguments so every tool can give the model a useful "what went wrong"
//! response instead of a generic parse failure.
//!
//! Design provenance: pi's `parseArgs` / `validateArgs`
//! (`packages/coding-agent/src/core/tools.ts`) — the split between required
//! and optional matches the model-facing schema: a tool declares its
//! required fields in `ToolSchema.input` and the helper enforces them.

use harness_core::tool::ToolError;
use serde_json::Value;

/// Extract a required string field from a JSON object argument map.
///
/// # Errors
///
/// Returns [`ToolError::InvalidArgs`] when the key is missing or the value
/// is not a JSON string.
pub fn require_string(args: &Value, key: &str) -> Result<String, ToolError> {
    let obj = args.as_object().ok_or_else(|| ToolError::InvalidArgs {
        name: "(arg extraction)".into(),
        reason: "args must be a JSON object".into(),
    })?;
    let val = obj.get(key).ok_or_else(|| ToolError::InvalidArgs {
        name: "(arg extraction)".into(),
        reason: format!("missing required argument `{key}`"),
    })?;
    val.as_str().map_or_else(
        || {
            Err(ToolError::InvalidArgs {
                name: "(arg extraction)".into(),
                reason: format!("argument `{key}` must be a string, got {}", val_type(val)),
            })
        },
        |s| Ok(s.to_string()),
    )
}

/// Extract an optional string field from a JSON object argument map.
///
/// Returns `None` when the key is absent or the value is JSON `null`.
/// A non-null, non-string value produces an error — silently ignoring a
/// type mismatch would hide the model's mistake.
pub fn optional_string(args: &Value, key: &str) -> Result<Option<String>, ToolError> {
    let obj = args.as_object().ok_or_else(|| ToolError::InvalidArgs {
        name: "(arg extraction)".into(),
        reason: "args must be a JSON object".into(),
    })?;
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(val) => val.as_str().map_or_else(
            || {
                Err(ToolError::InvalidArgs {
                    name: "(arg extraction)".into(),
                    reason: format!(
                        "argument `{key}` must be a string or null, got {}",
                        val_type(val)
                    ),
                })
            },
            |s| Ok(Some(s.to_string())),
        ),
    }
}

/// Extract a required integer field from a JSON object argument map.
///
/// # Errors
///
/// Returns [`ToolError::InvalidArgs`] when the key is missing or the value
/// is not a JSON integer.
pub fn require_integer(args: &Value, key: &str) -> Result<i64, ToolError> {
    let obj = args.as_object().ok_or_else(|| ToolError::InvalidArgs {
        name: "(arg extraction)".into(),
        reason: "args must be a JSON object".into(),
    })?;
    let val = obj.get(key).ok_or_else(|| ToolError::InvalidArgs {
        name: "(arg extraction)".into(),
        reason: format!("missing required argument `{key}`"),
    })?;
    val.as_i64().map_or_else(
        || {
            Err(ToolError::InvalidArgs {
                name: "(arg extraction)".into(),
                reason: format!("argument `{key}` must be an integer, got {}", val_type(val)),
            })
        },
        Ok,
    )
}

/// Human-readable type name for a JSON value — used in error messages.
fn val_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
