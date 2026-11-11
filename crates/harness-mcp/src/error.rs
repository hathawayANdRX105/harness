//! MCP error taxonomy: everything that can go wrong talking to an MCP server.
//!
//! Variants distinguish spawn failures from handshake failures from protocol
//! violations, so callers can decide what to retry and what to surface as a
//! broken configuration. The `named` helper attaches the server's config name
//! to the message while preserving the variant discriminant.

use harness_core::tool::ToolError;

/// Everything that can go wrong talking to an MCP server.
#[derive(Debug)]
pub enum McpError {
    /// The child process could not be started (binary not found, bad cwd, …).
    Spawn(String),
    /// Pipe broken, channel closed, or HTTP connection failed mid-session.
    Transport(String),
    /// Well-formed transport delivered malformed MCP (bad JSON, wrong id, …).
    Protocol(String),
    /// JSON-RPC `error` object from the server — a non-retryable server
    /// decision (unknown method, bad params, …).
    Server {
        /// JSON-RPC error code.
        code: i64,
        /// Human-readable error message from the server.
        message: String,
    },
    /// The tool ran and reported failure (`isError: true` in the response).
    Tool(String),
    /// No response within the configured timeout.
    Timeout,
    /// The caller's abort signal fired before the call completed.
    Aborted,
}

impl McpError {
    /// Prefix the message with the config entry name the error came from.
    ///
    /// The variant is preserved so callers can distinguish a spawn failure from
    /// a protocol violation without pattern-matching on the message text.
    pub fn named(self, server: &str) -> Self {
        match self {
            Self::Spawn(m) => Self::Spawn(format!("server `{server}`: {m}")),
            Self::Transport(m) => Self::Transport(format!("server `{server}`: {m}")),
            Self::Protocol(m) => Self::Protocol(format!("server `{server}`: {m}")),
            Self::Server { code, message } => Self::Server {
                code,
                message: format!("server `{server}`: {message}"),
            },
            Self::Tool(m) => Self::Tool(format!("server `{server}`: {m}")),
            // Unit variants carry no message to prefix; adding a "which server"
            // field is a public-API change.
            Self::Timeout => Self::Timeout,
            Self::Aborted => Self::Aborted,
        }
    }
}

impl std::fmt::Display for McpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(m) => write!(f, "mcp: failed to start server: {m}"),
            Self::Transport(m) => write!(f, "mcp: transport error: {m}"),
            Self::Protocol(m) => write!(f, "mcp: protocol error: {m}"),
            Self::Server { code, message } => write!(f, "mcp: server error {code}: {message}"),
            Self::Tool(m) => write!(f, "{m}"),
            Self::Timeout => write!(f, "mcp: server did not respond in time"),
            Self::Aborted => write!(f, "aborted"),
        }
    }
}

impl std::error::Error for McpError {}

impl From<McpError> for ToolError {
    fn from(e: McpError) -> Self {
        ToolError::Exec(e.to_string())
    }
}
