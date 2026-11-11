//! Streamable-HTTP MCP transport: JSON-RPC over a single HTTP POST per request.
//!
//! The wire protocol is the same line-delimited JSON-RPC 2.0 that
//! [`crate::transport::request_line`] and [`crate::transport::parse_response`]
//! handle. Each request is a `POST` to the server's base URL; the response
//! body is a single JSON line. This is not SSE streaming — it is one
//! request/response pair, matching how the stdio transport works.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::error::McpError;
use crate::transport::McpTransport;

/// How many characters of a 4xx error response body to carry in the
/// [`McpError::Server`] message — enough to diagnose (HTML error pages, JSON
/// error responses) without dumping a large body into logs.
const ERROR_BODY_EXCERPT_CHARS: usize = 200;

/// HTTP transport: one blocking `POST` per request, response body is one JSON line.
pub struct HttpTransport {
    url: String,
    next: AtomicU64,
    timeout_ms: u64,
}

impl HttpTransport {
    /// New transport posting to `url` with a per-call timeout in ms.
    pub fn new(url: String, timeout_ms: u64) -> Self {
        Self {
            url,
            next: AtomicU64::new(1),
            timeout_ms,
        }
    }

    /// The server's base URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// A ureq agent with this transport's per-call timeouts.
    ///
    /// The global `timeout` bounds the whole request (connect + write + read);
    /// `timeout_read` adds a separate budget for a stalled body read.
    fn client(&self) -> ureq::Agent {
        ureq::AgentBuilder::new()
            .timeout(Duration::from_millis(self.timeout_ms))
            .timeout_read(Duration::from_millis(self.timeout_ms))
            .build()
    }

    /// POST `line`, mapping transport errors to [`McpError`].
    ///
    /// Returns the response body (the JSON-RPC response line) on any 2xx.
    ///
    /// Status classification: a 4xx (except 408/429) is the *server's*
    /// answer — bad URL, auth failure, unknown endpoint — so retrying the
    /// identical request cannot help; it surfaces as a non-retryable
    /// [`McpError::Server`] with the first bytes of the response body.
    /// 5xx, 408, and 429 stay retryable [`McpError::Transport`].
    fn post(&self, line: &str) -> Result<String, McpError> {
        let body = self
            .client()
            .post(&self.url)
            .set("content-type", "application/json")
            .send_string(line)
            .map_err(|e| match e {
                ureq::Error::Status(code, resp)
                    if (400..500).contains(&code) && code != 408 && code != 429 =>
                {
                    let excerpt = resp
                        .into_string()
                        .map(|b| b.chars().take(ERROR_BODY_EXCERPT_CHARS).collect())
                        .unwrap_or_else(|_| "<unreadable>".to_string());
                    McpError::Server {
                        code: code as i64,
                        message: excerpt,
                    }
                }
                ureq::Error::Status(code, _) => McpError::Transport(format!("http {code}")),
                other => McpError::Transport(format!("http {other}")),
            })?;

        let body = body
            .into_string()
            .map_err(|e| McpError::Transport(format!("reading response body: {e}")))?;
        if body.trim().is_empty() {
            return Err(McpError::Transport("empty response body".into()));
        }
        Ok(body)
    }
}

impl McpTransport for HttpTransport {
    fn next_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    fn notify(&self, line: &str) -> Result<(), McpError> {
        // Notifications carry no `id`; the server answers 2xx with the
        // response body. Fire and discard the reply.
        let _ = self.post(line)?;
        Ok(())
    }

    fn roundtrip(&self, _id: u64, line: &str, signal: &AtomicBool) -> Result<String, McpError> {
        if signal.load(Ordering::Relaxed) {
            return Err(McpError::Aborted);
        }
        // Check on both sides of the POST: a successful transport call that
        // raced an abort must not hand a reply back, and a failed call must
        // report the abort (caller intent), not the transport error.
        let body = match self.post(line) {
            Ok(body) => body,
            Err(e) => {
                if signal.load(Ordering::Relaxed) {
                    return Err(McpError::Aborted);
                }
                return Err(e);
            }
        };
        if signal.load(Ordering::Relaxed) {
            return Err(McpError::Aborted);
        }
        // Same contract as the stdio transport: hand back the raw response
        // line so the caller can `parse_response` it. An empty body is not
        // valid JSON-RPC — say so explicitly instead of a bare parse error.
        if body.trim().is_empty() {
            return Err(McpError::Transport("empty response body".into()));
        }
        Ok(body)
    }
}
