//! Web fetch tool: GET a URL and return its text body.
//!
//! v1 semantics: a plain GET over http/https only — no JavaScript rendering,
//! and no DNS-level SSRF mitigations beyond the scheme allow-list (a
//! separate concern). Only `text/*` content types are accepted; the body is
//! capped at `max_bytes` and the result is truncated through the shared
//! spill path. Cancellation aborts the in-flight request and the body read.

use async_trait::async_trait;
use harness_core::tool::{Tool, ToolError, ToolResult, ToolRisk, ToolSchema};
use reqwest::header::CONTENT_TYPE;
use serde_json::{Value, json};

use crate::args::{optional_integer, require_string};
use crate::output::{DEFAULT_MAX_OUTPUT_CHARS, truncate_output};
use tokio_util::sync::CancellationToken;
/// Bytes of body to download at most when `max_bytes` is absent.
const DEFAULT_MAX_BODY_BYTES: usize = DEFAULT_MAX_OUTPUT_CHARS;

/// Fetch one URL with a GET.
#[derive(Default)]
pub struct WebFetchTool;

impl WebFetchTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for WebFetchTool {
    fn name(&self) -> &str {
        "webfetch"
    }

    fn schema(&self) -> ToolSchema {
        ToolSchema {
            name: "webfetch".into(),
            description: "GET a URL and return its text body. Only `http` and `https` URLs are allowed (SSRF guardrail) and only `text/*` content types are accepted. `max_bytes` caps the download; the result is truncated to the configured output cap with the full body spilled to a temp file. Cancellation aborts the request. Non-2xx responses report `is_error` with the status.".into(),
            input: json!({
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to fetch, `http` or `https` scheme only."
                    },
                    "max_bytes": {
                        "type": "integer",
                        "description": "Maximum body bytes to download (default 100000).",
                    }
                },
                "required": ["url"],
            }),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::ReadOnly
    }

    async fn invoke(
        &self,
        args: Value,
        cancel: &CancellationToken,
    ) -> Result<ToolResult, ToolError> {
        let url = require_string(&args, "url")?;
        // SSRF guardrail, v1: only http and https schemes are allowed. DNS
        // restrictions and allow-listed hosts are deliberately out of scope.
        let scheme = url
            .split_once(':')
            .map(|(s, _)| s.to_lowercase())
            .unwrap_or_default();
        if scheme != "http" && scheme != "https" {
            return Ok(ToolResult {
                content: format!(
                    "unsupported url scheme `{scheme}`: only `http` and `https` are allowed"
                ),
                is_error: true,
                trace: None,
            });
        }
        let max_bytes = max_body_bytes(&args)?;
        let parsed = reqwest::Url::parse(&url)
            .map_err(|e| ToolError::Exec(format!("invalid url `{url}`: {e}")))?;
        let request = reqwest::Client::new().get(parsed);

        // Send, cancel-aware: aborting before the response arrives is
        // reported as Cancelled so the loop backfills a cancelled result.
        let mut response = match tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = request.send() => result,
        } {
            Ok(response) => response,
            Err(error) => return Err(ToolError::Exec(format!("GET {url} failed: {error}"))),
        };

        let status = response.status();
        if !status.is_success() {
            return Ok(ToolResult {
                content: format!("GET {url} failed: HTTP {status}"),
                is_error: true,
                trace: None,
            });
        }

        // Text-only: refuse anything whose declared content type is not text.
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let mime = content_type
            .split_once(';')
            .map(|(t, _)| t)
            .unwrap_or(content_type)
            .trim()
            .to_lowercase();
        if !mime.is_empty() && !mime.starts_with("text/") {
            return Ok(ToolResult {
                content: format!("GET {url} refused: content type `{content_type}` is not text"),
                is_error: true,
                trace: None,
            });
        }

        // Read the body in chunks, capped at `max_bytes`, aborting on cancel.
        let mut body = Vec::new();
        let mut capped = false;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ToolError::Cancelled),
                chunk = response.chunk() => match chunk {
                    Ok(Some(chunk)) => {
                        if body.len() + chunk.len() > max_bytes {
                            let take = max_bytes.saturating_sub(body.len());
                            body.extend_from_slice(&chunk[..take]);
                            capped = true;
                            break;
                        }
                        body.extend_from_slice(&chunk);
                    }
                    Ok(None) => break,
                    Err(error) => {
                        return Err(ToolError::Exec(format!(
                            "reading body for {url}: {error}"
                        )))
                    }
                }
            }
        }

        let mut content = String::from_utf8_lossy(&body).to_string();
        if capped {
            content.push_str(&format!("\n... (body capped at {max_bytes} bytes)"));
        }
        let (truncated, spill) = truncate_output(&content, DEFAULT_MAX_OUTPUT_CHARS);
        let trace = spill
            .as_ref()
            .map(|path| format!("webfetch {url}: full body at {}", path.display()));
        Ok(ToolResult {
            content: truncated,
            is_error: false,
            trace,
        })
    }
}

/// Parse `max_bytes` into a download cap; absent means the default cap.
fn max_body_bytes(args: &Value) -> Result<usize, ToolError> {
    match optional_integer(args, "max_bytes")? {
        None => Ok(DEFAULT_MAX_BODY_BYTES),
        Some(bytes) if bytes < 0 => Err(ToolError::InvalidArgs {
            name: "webfetch".into(),
            reason: "argument `max_bytes` must be a non-negative integer".into(),
        }),
        Some(bytes) => Ok(bytes as usize),
    }
}
