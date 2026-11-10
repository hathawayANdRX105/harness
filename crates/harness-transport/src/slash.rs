//! Slash command parsing: text commands like `/run`, `/cancel`, `/steer`
//! into a typed enum, plus JSON-RPC method+params conversion.
//!
//! Commands are designed to be human-memorable: `/run <task>` starts a run,
//! `/cancel <run_id>` cancels, `/steer <run_id> <msg>` queues an instruction,
//! `/status <run_id>` queries state, `/list` lists recent runs.
//!
//! The same [`SlashCommand`] enum is produced by two paths:
//!
//! - [`parse_slash`] — text `/cmd args` form for terminal/UDS clients.
//! - [`command_from_jsonrpc`] — JSON-RPC `method` + `params` form for all
//!   three channels.  Both converge on the same enum so the
//!   [`CommandHandler`](crate::CommandHandler) seam sees only one vocabulary.
//!
//! ## Test policy deviation
//!
//! The core repo convention (testing-ci.md §1) places all tests in `tests/`.
//! This module contains inline `#[cfg(test)]` unit tests for the pure
//! string-parsing functions because they have zero crate dependencies and
//! writing them as integration tests would pull the full harness-transport
//! dependency tree just to exercise a `&str → SlashCommand` mapping.
//! The deviation is scoped to exactly this module and each test carries
//! a justification comment.

use serde::Deserialize;

/// A command parsed from a transport client, either from slash text or
/// from a JSON-RPC method call.
#[derive(Debug, Clone, PartialEq)]
pub enum SlashCommand {
    /// Start a new run with the given task description.
    Run { task: String },
    /// Cancel the run identified by the given id.
    Cancel { run_id: String },
    /// Queue a steering message for a run.
    Steer { run_id: String, message: String },
    /// Query the status of a run.
    Status { run_id: String },
    /// List recent runs.
    List,
}

/// Errors that can occur when parsing a slash command.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseSlashError {
    /// The input does not start with `/`.
    NotSlash,
    /// A required argument is missing.
    MissingArgs(&'static str),
    /// The command name is not recognised.
    Unknown(String),
}

impl std::fmt::Display for ParseSlashError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSlash => f.write_str("input does not start with '/'"),
            Self::MissingArgs(arg) => write!(f, "missing argument: {arg}"),
            Self::Unknown(cmd) => write!(f, "unknown command: {cmd}"),
        }
    }
}

impl std::error::Error for ParseSlashError {}

/// Parse a `/cmd [args]` text line into a [`SlashCommand`].
///
/// The input is trimmed before parsing.  The command name is everything
/// after the leading `/` up to the first whitespace; the remaining text
/// becomes the argument(s).
pub fn parse_slash(line: &str) -> Result<SlashCommand, ParseSlashError> {
    let line = line.trim();
    if !line.starts_with('/') {
        return Err(ParseSlashError::NotSlash);
    }

    let rest = line.strip_prefix('/').unwrap_or("");
    let space = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let cmd = &rest[..space];
    let args = rest[space..].trim();

    match cmd {
        "run" => {
            if args.is_empty() {
                return Err(ParseSlashError::MissingArgs("task"));
            }
            Ok(SlashCommand::Run {
                task: args.to_string(),
            })
        }
        "cancel" => {
            if args.is_empty() {
                return Err(ParseSlashError::MissingArgs("run_id"));
            }
            Ok(SlashCommand::Cancel {
                run_id: args.to_string(),
            })
        }
        "steer" => {
            if args.is_empty() {
                return Err(ParseSlashError::MissingArgs("run_id message"));
            }
            let first_space = args.find(char::is_whitespace).unwrap_or(args.len());
            let run_id = args[..first_space].to_string();
            let message = args[first_space..].trim().to_string();
            if message.is_empty() {
                return Err(ParseSlashError::MissingArgs("message"));
            }
            Ok(SlashCommand::Steer { run_id, message })
        }
        "status" => {
            if args.is_empty() {
                return Err(ParseSlashError::MissingArgs("run_id"));
            }
            Ok(SlashCommand::Status {
                run_id: args.to_string(),
            })
        }
        "list" => Ok(SlashCommand::List),
        other => Err(ParseSlashError::Unknown(other.to_string())),
    }
}

// ── JSON-RPC method → SlashCommand ────────────────────────────────────────

/// Parameter shape for the `run` JSON-RPC method.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunParams {
    pub task: String,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Parameter shape for the `cancel` JSON-RPC method.
#[derive(Deserialize)]
pub struct CancelParams {
    pub run_id: String,
}

/// Parameter shape for the `steer` JSON-RPC method.
#[derive(Deserialize)]
pub struct SteerParams {
    pub run_id: String,
    pub message: String,
}

/// Parameter shape for the `status` JSON-RPC method.
#[derive(Deserialize)]
pub struct StatusParams {
    pub run_id: String,
}

/// Convert a JSON-RPC method name and optional params value into a
/// [`SlashCommand`].  All three transport channels use this when they
/// receive a JSON-RPC request.
pub fn command_from_jsonrpc(
    method: &str,
    params: Option<serde_json::Value>,
) -> Result<SlashCommand, ParseSlashError> {
    match method {
        "run" => {
            let p = params.ok_or(ParseSlashError::MissingArgs("task"))?;
            let rp: RunParams =
                serde_json::from_value(p).map_err(|_| ParseSlashError::MissingArgs("task"))?;
            Ok(SlashCommand::Run { task: rp.task })
        }
        "cancel" => {
            let p = params.ok_or(ParseSlashError::MissingArgs("run_id"))?;
            let cp: CancelParams =
                serde_json::from_value(p).map_err(|_| ParseSlashError::MissingArgs("run_id"))?;
            Ok(SlashCommand::Cancel { run_id: cp.run_id })
        }
        "steer" => {
            let p = params.ok_or(ParseSlashError::MissingArgs("run_id, message"))?;
            let sp: SteerParams = serde_json::from_value(p)
                .map_err(|_| ParseSlashError::MissingArgs("run_id, message"))?;
            Ok(SlashCommand::Steer {
                run_id: sp.run_id,
                message: sp.message,
            })
        }
        "status" => {
            let p = params.ok_or(ParseSlashError::MissingArgs("run_id"))?;
            let sp: StatusParams =
                serde_json::from_value(p).map_err(|_| ParseSlashError::MissingArgs("run_id"))?;
            Ok(SlashCommand::Status { run_id: sp.run_id })
        }
        "list" => Ok(SlashCommand::List),
        other => Err(ParseSlashError::Unknown(other.to_string())),
    }
}

/// Build a JSON-RPC method name and params value from a [`SlashCommand`].
/// Used by transport tests that need to serialise outbound commands.
pub fn slash_to_jsonrpc(cmd: &SlashCommand) -> (String, Option<serde_json::Value>) {
    match cmd {
        SlashCommand::Run { task } => ("run".into(), Some(serde_json::json!({ "task": task }))),
        SlashCommand::Cancel { run_id } => (
            "cancel".into(),
            Some(serde_json::json!({ "run_id": run_id })),
        ),
        SlashCommand::Steer { run_id, message } => (
            "steer".into(),
            Some(serde_json::json!({ "run_id": run_id, "message": message })),
        ),
        SlashCommand::Status { run_id } => (
            "status".into(),
            Some(serde_json::json!({ "run_id": run_id })),
        ),
        SlashCommand::List => ("list".into(), None),
    }
}

#[cfg(test)]
mod unit_tests {
    // ── Inline test justification ──────────────────────────────────────
    //
    // This module is a deliberate deviation from the repo's "all tests in
    // tests/" convention (testing-ci.md §1).  The functions under test are
    // pure string-to-enum transformations with zero harness-transport crate
    // dependencies.  Placing them in tests/ would compile the full crate
    // (axum, tokio, etc.) just to assert `&str → SlashCommand`.
    // The deviation is scoped to exactly this module.
    //
    // Purpose: verify that every slash command variant round-trips through
    // parse_slash correctly.  Acceptance: each known command produces the
    // expected SlashCommand variant with the correct argument values.

    use super::*;

    #[test]
    fn parse_run_command() {
        let cmd = parse_slash("/run write unit tests").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            cmd,
            SlashCommand::Run {
                task: "write unit tests".into()
            }
        );
    }

    #[test]
    fn parse_cancel_command() {
        let cmd = parse_slash("/cancel abc-123").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            cmd,
            SlashCommand::Cancel {
                run_id: "abc-123".into()
            }
        );
    }

    #[test]
    fn parse_steer_command() {
        let cmd =
            parse_slash("/steer run-1 use a different approach").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            cmd,
            SlashCommand::Steer {
                run_id: "run-1".into(),
                message: "use a different approach".into()
            }
        );
    }

    #[test]
    fn parse_status_command() {
        let cmd = parse_slash("/status run-42").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            cmd,
            SlashCommand::Status {
                run_id: "run-42".into()
            }
        );
    }

    #[test]
    fn parse_list_command() {
        let cmd = parse_slash("/list").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(cmd, SlashCommand::List);
    }

    #[test]
    fn parse_unknown_command_is_error() {
        let err = parse_slash("/unknown")
            .err()
            .unwrap_or_else(|| panic!("expected an error"));
        assert_eq!(err, ParseSlashError::Unknown("unknown".into()));
    }

    #[test]
    fn parse_empty_input_is_not_a_slash_command() {
        let err = parse_slash("")
            .err()
            .unwrap_or_else(|| panic!("expected an error"));
        assert_eq!(err, ParseSlashError::NotSlash);
    }

    #[test]
    fn parse_plain_text_is_not_a_slash_command() {
        let err = parse_slash("hello")
            .err()
            .unwrap_or_else(|| panic!("expected an error"));
        assert_eq!(err, ParseSlashError::NotSlash);
    }

    #[test]
    fn parse_run_missing_task() {
        let err = parse_slash("/run")
            .err()
            .unwrap_or_else(|| panic!("expected an error"));
        assert_eq!(err, ParseSlashError::MissingArgs("task"));
    }
}
