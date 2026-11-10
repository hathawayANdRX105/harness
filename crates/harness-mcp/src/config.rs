//! Configuration types for MCP server connections.
//!
//! These types are owned by this crate (not by `harness-config`) so the builder
//! (#10) can construct them without a circular dependency. The builder maps
//! from the application config model to these structures.

use std::collections::HashMap;
use std::fmt;

/// Description of one MCP server the daemon connects to at startup.
///
/// For stdio transport, `command` / `args` / `env` drive a child process. For
/// HTTP transport, `url` points to a running server and `command` is ignored.
#[derive(Clone)]
pub struct McpServerConfig {
    /// Human-readable name for error messages and tool namespacing.
    pub name: String,
    /// Command to spawn (stdio transport). `None` means HTTP-only.
    pub command: Option<String>,
    /// Arguments to the command.
    pub args: Vec<String>,
    /// Environment variables to inject.
    pub env: HashMap<String, String>,
    /// Server URL (HTTP transport). `None` means stdio-only.
    pub url: Option<String>,
    /// Per-call timeout in milliseconds. `None` falls back to
    /// [`crate::transport::MCP_TIMEOUT`].
    pub tool_call_timeout_ms: Option<u64>,
}
impl fmt::Debug for McpServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `env` may hold secret values — mask them while keeping the keys
        // and every other field visible, matching harness-config's masked
        // Debug style. `args` are configuration, not secrets, and stay visible.
        let masked_env: HashMap<&String, &str> = self.env.keys().map(|key| (key, "***")).collect();
        f.debug_struct("McpServerConfig")
            .field("name", &self.name)
            .field("command", &self.command)
            .field("args", &self.args)
            .field("env", &masked_env)
            .field("url", &self.url)
            .field("tool_call_timeout_ms", &self.tool_call_timeout_ms)
            .finish()
    }
}
