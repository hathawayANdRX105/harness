//! A live MCP server connection: handshake, cached tool list, tool invocation.
//!
//! [`Mcp`] owns one transport and the [`ToolMeta`] list the server advertised
//! at registration. After construction callers consume it via
//! [`Mcp::into_tools`] to get registrable [`crate::proxy::McpProxyTool`]
//! instances.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::config::McpServerConfig;
use crate::error::McpError;
use crate::http::HttpTransport;
use crate::proxy::McpProxyTool;
use crate::reconnect::{McpReconnect, ReconnectPolicy};
use crate::stdio::StdioTransport;
use crate::transport::MCP_TIMEOUT;
use crate::transport::{McpTransport, ToolMeta, initialize, list_tools};

/// A live MCP server plus the tools it advertised at registration.
pub struct Mcp {
    name: String,
    transport: Arc<dyn McpTransport>,
    tools: Vec<ToolMeta>,
}

impl Mcp {
    /// Spawn a server, handshake, and cache its tool list.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Spawn`] when the child process cannot be started,
    /// [`McpError::Protocol`] when the handshake fails, or forwards transport
    /// errors from the underlying protocol calls.
    pub fn spawn(cfg: &McpServerConfig, signal: &AtomicBool) -> Result<Self, McpError> {
        Self::connect(cfg, Arc::new(StdioTransport::spawn(cfg)?), signal)
    }

    /// Handshake over an existing transport.
    ///
    /// Split out so tests can drive the protocol without a child process.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Protocol`] if the handshake or tool listing fails,
    /// or forwards transport errors.
    pub fn connect(
        cfg: &McpServerConfig,
        transport: Arc<dyn McpTransport>,
        signal: &AtomicBool,
    ) -> Result<Self, McpError> {
        initialize(transport.as_ref(), signal)?;
        let tools = list_tools(transport.as_ref(), &cfg.name, signal)?;
        Ok(Self {
            name: cfg.name.clone(),
            transport,
            tools,
        })
    }

    /// Connect over HTTP with an optional reconnect policy.
    ///
    /// The server must already be running — this transport spawns nothing.
    /// `cfg.url` is required; `cfg.command` is ignored.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Spawn`] when no URL is configured, or forwards
    /// errors from connection initialization and tool listing.
    pub fn connect_http(cfg: &McpServerConfig, signal: &AtomicBool) -> Result<Self, McpError> {
        let url = cfg
            .url
            .clone()
            .ok_or_else(|| McpError::Spawn("no url for http transport".into()))?;
        let timeout_ms = cfg
            .tool_call_timeout_ms
            .unwrap_or_else(|| MCP_TIMEOUT.as_millis() as u64);
        let transport: Arc<dyn McpTransport> = Arc::new(HttpTransport::new(url, timeout_ms));
        let policy = ReconnectPolicy::default();
        let wrapped = McpReconnect::new(transport, policy);
        Self::connect(cfg, Arc::new(wrapped), signal)
    }

    /// Human-readable server name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Metadata for the advertised tools.
    pub fn list_tools(&self) -> &[ToolMeta] {
        &self.tools
    }

    /// Consume the connection into registrable tools.
    ///
    /// Each tool holds a shared reference to the transport — the same
    /// transport is shared among all tools from this server.
    pub fn into_tools(self) -> Vec<Arc<dyn harness_core::tool::Tool>> {
        self.tools
            .into_iter()
            .map(|meta| {
                Arc::new(McpProxyTool::new(meta, Arc::clone(&self.transport)))
                    as Arc<dyn harness_core::tool::Tool>
            })
            .collect()
    }
}
