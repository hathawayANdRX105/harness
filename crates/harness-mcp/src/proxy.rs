//! Adapter: one remote MCP tool presented as a built-in [`harness_core::tool::Tool`].
//!
//! [`McpProxyTool`] bridges the sync [`McpTransport`] world to the async
//! [`Tool`] trait by forwarding `invoke` to a blocking task. Each remote tool
//! from `tools/list` becomes one `McpProxyTool` instance that holds a shared
//! reference to the transport — the same transport is shared among all tools
//! from the same MCP server.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::McpError;
use crate::transport::{McpTransport, ToolMeta, call_tool};

/// A remote MCP tool. `invoke` bridges to the sync transport via
/// `tokio::task::spawn_blocking`.
pub struct McpProxyTool {
    meta: ToolMeta,
    transport: Arc<dyn McpTransport>,
}

impl McpProxyTool {
    pub fn new(meta: ToolMeta, transport: Arc<dyn McpTransport>) -> Self {
        Self { meta, transport }
    }

    /// Name the server knows this tool by.
    pub fn remote(&self) -> &str {
        &self.meta.remote
    }
}

#[async_trait::async_trait]
impl Tool for McpProxyTool {
    fn name(&self) -> &str {
        &self.meta.name
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: self.meta.name.clone(),
            description: self.meta.description.clone(),
            input: self.meta.parameters.clone(),
        }
    }

    fn risk(&self) -> ToolRisk {
        // MCP tools can be anything; the server advertises hints in the tool
        // definition, and a sandbox layer can downgrade risk based on policy.
        // By default, assume the most restrictive classification: the tool may
        // modify state.
        ToolRisk::Destructive
    }

    async fn invoke(
        &self,
        args: Value,
        cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        let transport = Arc::clone(&self.transport);
        let remote = self.meta.remote.clone();
        let signal = Arc::new(AtomicBool::new(false));
        let sig = Arc::clone(&signal);

        // Forward the CancellationToken to the AtomicBool the sync transport polls.
        let cancel_watch = cancel.clone();
        tokio::task::spawn(async move {
            cancel_watch.cancelled().await;
            sig.store(true, Ordering::Relaxed);
        });

        let result = tokio::task::spawn_blocking(move || {
            call_tool(transport.as_ref(), &remote, &args, &signal)
        })
        .await
        .map_err(|join_err| ToolError::Exec(format!("blocking task join failed: {join_err}")))?;

        match result {
            Ok(text) => Ok(ToolResult {
                content: text,
                is_error: false,
                trace: None,
            }),
            Err(McpError::Aborted) => Err(ToolError::Cancelled),
            Err(e) => Err(ToolError::Exec(e.to_string())),
        }
    }
}
