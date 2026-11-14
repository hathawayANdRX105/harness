//! Integration tests for the slash/JSON-RPC command surface.

use harness_transport::slash::{
    ParseSlashError, SlashCommand, command_from_jsonrpc, parse_slash, slash_to_jsonrpc,
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

/// Purpose: `tools.list` parses with and without `session_id` and
/// round-trips, omitting `session_id` when it is `None`.
/// Scenario: `command_from_jsonrpc("tools.list", None)` and
/// `command_from_jsonrpc("tools.list", {"session_id": "sess-tools"})`,
/// then `slash_to_jsonrpc` on each parsed command.
/// Acceptance: the parsed commands carry `session_id: None` and
/// `Some("sess-tools")`; the round trips yield method `tools.list` with
/// params `None` and `{"session_id": "sess-tools"}` respectively.
#[test]
fn tools_list_parses_and_round_trips() {
    let cmd = match command_from_jsonrpc("tools.list", None) {
        Ok(cmd) => cmd,
        Err(err) => panic!("tools.list without params must parse: {err}"),
    };
    assert!(
        matches!(cmd, SlashCommand::ToolsList { ref session_id } if session_id.is_none()),
        "tools.list without session_id must default to None"
    );
    let (method, params) = slash_to_jsonrpc(&cmd);
    assert_eq!(method, "tools.list");
    assert_eq!(
        params, None,
        "round-tripped params must omit session_id when None"
    );

    let cmd = match command_from_jsonrpc(
        "tools.list",
        Some(serde_json::json!({ "session_id": "sess-tools" })),
    ) {
        Ok(cmd) => cmd,
        Err(err) => panic!("tools.list with session_id must parse: {err}"),
    };
    assert!(
        matches!(
            cmd,
            SlashCommand::ToolsList { ref session_id }
                if session_id.as_deref() == Some("sess-tools")
        ),
        "parsed tools.list must carry session_id"
    );
    let (method, params) = slash_to_jsonrpc(&cmd);
    assert_eq!(method, "tools.list");
    assert_eq!(
        params,
        Some(serde_json::json!({ "session_id": "sess-tools" })),
        "round-tripped params must preserve session_id"
    );
}

/// Purpose: `tools.set_active` parses `names` and an optional
/// `session_id` and round-trips.
/// Scenario: `command_from_jsonrpc("tools.set_active",
/// {"session_id": "sess-tools", "names": ["read", "write"]})`,
/// then `slash_to_jsonrpc` on the parsed command.
/// Acceptance: the parsed command carries both names and the session id,
/// and the round trip yields method `tools.set_active` with params
/// `{"session_id": "sess-tools", "names": ["read", "write"]}`.
#[test]
fn tools_set_active_parses_and_round_trips() {
    let cmd = match command_from_jsonrpc(
        "tools.set_active",
        Some(serde_json::json!({
            "session_id": "sess-tools",
            "names": ["read", "write"],
        })),
    ) {
        Ok(cmd) => cmd,
        Err(err) => panic!("tools.set_active params must parse: {err}"),
    };
    assert!(
        matches!(
            cmd,
            SlashCommand::ToolsSetActive { ref session_id, ref names }
                if session_id.as_deref() == Some("sess-tools")
                    && names == &vec!["read".to_string(), "write".to_string()]
        ),
        "parsed tools.set_active must carry session_id and names"
    );
    let (method, params) = slash_to_jsonrpc(&cmd);
    assert_eq!(method, "tools.set_active");
    assert_eq!(
        params,
        Some(serde_json::json!({
            "session_id": "sess-tools",
            "names": ["read", "write"],
        })),
        "round-tripped params must preserve session_id and names"
    );
}

/// Purpose: `tools.set_active` without `names` is a typed
/// invalid-params error.
/// Scenario: `command_from_jsonrpc("tools.set_active", None)`.
/// Acceptance: the error is `ParseSlashError::MissingArgs("names")` and
/// its JSON-RPC code is `INVALID_PARAMS` (-32602).
#[test]
fn tools_set_active_missing_names_is_invalid_params() {
    let err = match command_from_jsonrpc("tools.set_active", None) {
        Err(err) => err,
        Ok(cmd) => panic!("expected tools.set_active without names to fail: {cmd:?}"),
    };
    assert_eq!(err, ParseSlashError::MissingArgs("names"));
    assert_eq!(
        err.code(),
        harness_transport::frame::error_code::INVALID_PARAMS,
        "missing names must map to INVALID_PARAMS"
    );
}

/// Purpose: text `/tools` forms parse as documented.
/// Scenario: `parse_slash` on `/tools`, `/tools set a,,b`,
/// `/tools set` and `/tools frob`.
/// Acceptance: `/tools` parses to `ToolsList { session_id: None }`;
/// `/tools set a,,b` parses to `ToolsSetActive { names: ["a", "b"] }`
/// (empty segments skipped); `/tools set` with no names is
/// `MissingArgs("names")`; `/tools frob` is `Unknown("frob")`.
#[test]
fn parse_slash_tools_forms() {
    let cmd = match parse_slash("/tools") {
        Ok(cmd) => cmd,
        Err(err) => panic!("/tools must parse: {err}"),
    };
    assert!(
        matches!(cmd, SlashCommand::ToolsList { ref session_id } if session_id.is_none()),
        "/tools with no args must parse to tools list"
    );
    let cmd = match parse_slash("/tools set a,,b") {
        Ok(cmd) => cmd,
        Err(err) => panic!("/tools set must parse: {err}"),
    };
    assert!(
        matches!(
            cmd,
            SlashCommand::ToolsSetActive { ref session_id, ref names }
                if session_id.is_none() && names == &vec!["a".to_string(), "b".to_string()]
        ),
        "empty segments must be skipped"
    );
    let err = match parse_slash("/tools set") {
        Ok(cmd) => panic!("/tools set without names must fail: {cmd:?}"),
        Err(err) => err,
    };
    assert_eq!(err, ParseSlashError::MissingArgs("names"));
    let err = match parse_slash("/tools frob") {
        Ok(cmd) => panic!("/tools frob must fail: {cmd:?}"),
        Err(err) => err,
    };
    assert_eq!(err, ParseSlashError::Unknown("frob".to_string()));
}
