//! Tests for the harness-mcp crate.
//!
//! All tests use a fake in-process transport to assert protocol behaviour
//! without spawning child processes or HTTP servers. The fake transport
//! records sent frames and replays canned responses.
//!
//! # Test strategy
//!
//! - **Frame roundtrip**: request_line / parse_response / request function
//!   are exercised against a fake transport that verifies the wire format.
//! - **Initialize handshake**: the fake transport returns a valid server
//!   `ServerCapabilities` response and the test checks that the client
//!   accepted it and sent the `notifications/initialized` ack.
//! - **tools/list → Tool adapter**: fake transport returns a tool list
//!   and the test verifies ToolMeta construction and the McpProxyTool
//!   adapter (name, schema, risk defaults).
//! - **Error frames**: fake transport returns a JSON-RPC error object
//!   and the test verifies it maps to McpError::Server.
//! - **Pagination**: fake transport returns multiple pages via `nextCursor`.
//! - **Reconnect backoff**: test the backoff delay calculation directly.
//! - **McpProxyTool build**: construct McpProxyTool from ToolMeta and verify
//!   Tool trait methods.
