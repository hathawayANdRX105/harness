//! Slash command parsing: text commands like `/run`, `/cancel`, `/steer`
//! into a typed enum, plus JSON-RPC method+params conversion.
//!
//! Commands are designed to be human-memorable: `/run <task>` starts a run,
//! `/cancel <run_id>` cancels, `/steer <run_id> <msg>` queues an instruction,
//! `/status <run_id>` queries state, `/list` lists recent runs.
//! `/search <query>` searches persisted run events.
//!
//! The same [`SlashCommand`] enum is produced by two paths:
//!
//! - [`parse_slash`] — text `/cmd args` form for terminal/UDS clients.
//! - [`command_from_jsonrpc`] — JSON-RPC `method` + `params` form for all
//!   three channels.  Both converge on the same enum so the
//!   [`CommandHandler`](crate::CommandHandler) seam sees only one vocabulary.
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
    /// Search persisted run events for `query`.
    Search { query: String },
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

impl ParseSlashError {
    /// The JSON-RPC error code this parse failure maps to.
    ///
    /// An unrecognised *method* is `METHOD_NOT_FOUND` (-32601), not a malformed
    /// request: the client used a name the server does not implement. Missing
    /// or ill-typed params are `INVALID_PARAMS` (-32602).
    pub fn code(&self) -> i32 {
        match self {
            Self::Unknown(_) => crate::frame::error_code::METHOD_NOT_FOUND,
            Self::MissingArgs(_) | Self::NotSlash => crate::frame::error_code::INVALID_PARAMS,
        }
    }
}

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

/// Parameter shape for the `search` JSON-RPC method.
#[derive(Deserialize)]
pub struct SearchParams {
    pub query: String,
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
        "search" => {
            let p = params.ok_or(ParseSlashError::MissingArgs("query"))?;
            let sp: SearchParams =
                serde_json::from_value(p).map_err(|_| ParseSlashError::MissingArgs("query"))?;
            Ok(SlashCommand::Search { query: sp.query })
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
        SlashCommand::Search { query } => {
            ("search".into(), Some(serde_json::json!({ "query": query })))
        }
        SlashCommand::List => ("list".into(), None),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Purpose: the `search` JSON-RPC method parses into [`SlashCommand::Search`]
    /// and serialises back out unchanged.
    /// Scenario: `command_from_jsonrpc("search", {"query": "fox"})`, then
    /// `slash_to_jsonrpc` on the parsed command.
    /// Acceptance: the parsed command carries `query == "fox"` and the round
    /// trip yields method `search` with params `{"query": "fox"}`.
    #[test]
    fn search_command_parses_and_round_trips() {
        let cmd = match command_from_jsonrpc("search", Some(serde_json::json!({ "query": "fox" })))
        {
            Ok(cmd) => cmd,
            Err(err) => panic!("search params must parse: {err}"),
        };
        assert!(
            matches!(cmd, SlashCommand::Search { ref query } if query == "fox"),
            "parsed search command must carry the query"
        );
        let (method, params) = slash_to_jsonrpc(&cmd);
        assert_eq!(method, "search");
        assert_eq!(
            params,
            Some(serde_json::json!({ "query": "fox" })),
            "round-tripped params must match the input"
        );
    }

    /// Purpose: `search` without `query` is a typed invalid-params error.
    /// Scenario: `command_from_jsonrpc("search", None)`.
    /// Acceptance: the error is `ParseSlashError::MissingArgs("query")` and its
    /// JSON-RPC code is `INVALID_PARAMS` (-32602).
    #[test]
    fn search_command_missing_query_is_invalid_params() {
        let err = match command_from_jsonrpc("search", None) {
            Err(err) => err,
            Ok(cmd) => panic!("expected search without query to fail: {cmd:?}"),
        };
        assert_eq!(err, ParseSlashError::MissingArgs("query"));
        assert_eq!(
            err.code(),
            crate::frame::error_code::INVALID_PARAMS,
            "missing query must map to INVALID_PARAMS"
        );
    }
}
