//! Integration tests for the slash/JSON-RPC command surface.

use harness_transport::slash::{
    ParseSlashError, SlashCommand, command_from_jsonrpc, slash_to_jsonrpc,
};

/// Purpose: the `search` JSON-RPC method parses into [`SlashCommand::Search`]
/// and serialises back out unchanged.
/// Scenario: `command_from_jsonrpc("search", {"query": "fox"})`, then
/// `slash_to_jsonrpc` on the parsed command.
/// Acceptance: the parsed command carries `query == "fox"` and the round
/// trip yields method `search` with params `{"query": "fox"}`.
#[test]
fn search_command_parses_and_round_trips() {
    let cmd = match command_from_jsonrpc("search", Some(serde_json::json!({ "query": "fox" }))) {
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
        harness_transport::frame::error_code::INVALID_PARAMS,
        "missing query must map to INVALID_PARAMS"
    );
}
/// Purpose: the `run` JSON-RPC method threads `session_id` into the parsed
/// command and preserves it when serialised back out.
/// Scenario: `command_from_jsonrpc("run", {"task": "session routing smoke alpha", "session_id": "sess-smoke-alpha"})`,
/// then `slash_to_jsonrpc` on the parsed command.
/// Acceptance: the parsed command carries `task == "session routing smoke alpha"` and
/// `session_id == Some("sess-smoke-alpha")`, and the round trip yields method `run`
/// with params `{"task": "session routing smoke alpha", "session_id": "sess-smoke-alpha"}`.
#[test]
fn run_command_with_session_id_parses_and_round_trips() {
    let cmd = match command_from_jsonrpc(
        "run",
        Some(serde_json::json!({
            "task": "session routing smoke alpha",
            "session_id": "sess-smoke-alpha",
        })),
    ) {
        Ok(cmd) => cmd,
        Err(err) => panic!("run params with session_id must parse: {err}"),
    };
    assert!(
        matches!(
            cmd,
            SlashCommand::Run { ref task, ref session_id }
                if task == "session routing smoke alpha"
                    && session_id.as_deref() == Some("sess-smoke-alpha")
        ),
        "parsed run command must carry task and session_id"
    );
    let (method, params) = slash_to_jsonrpc(&cmd);
    assert_eq!(method, "run");
    assert_eq!(
        params,
        Some(serde_json::json!({
            "task": "session routing smoke alpha",
            "session_id": "sess-smoke-alpha",
        })),
        "round-tripped params must preserve session_id"
    );
}

/// Purpose: `run` without `session_id` parses to `None` and serialises
/// back without the key, so existing payloads stay byte-identical.
/// Scenario: `command_from_jsonrpc("run", {"task": "beta"})`, then
/// `slash_to_jsonrpc` on the parsed command.
/// Acceptance: the parsed command carries `session_id: None` and the round
/// trip yields method `run` with params `{"task": "beta"}` — no
/// `session_id` key.
#[test]
fn run_command_without_session_id_defaults_to_none() {
    let cmd = match command_from_jsonrpc("run", Some(serde_json::json!({ "task": "beta" }))) {
        Ok(cmd) => cmd,
        Err(err) => panic!("run params without session_id must parse: {err}"),
    };
    assert!(
        matches!(
            cmd,
            SlashCommand::Run { ref task, ref session_id }
                if task == "beta" && session_id.is_none()
        ),
        "parsed run command without session_id must default session_id to None"
    );
    let (method, params) = slash_to_jsonrpc(&cmd);
    assert_eq!(method, "run");
    assert_eq!(
        params,
        Some(serde_json::json!({ "task": "beta" })),
        "round-tripped params must omit session_id when it is None"
    );
}
