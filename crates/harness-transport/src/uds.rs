//! Unix-domain-socket transport: local control-plane JSON-RPC over NDJSON.
//!
//! # Protocol
//!
//! Each connection is a sequence of **newline-delimited JSON (NDJSON)**
//! messages.  The client sends one JSON-RPC 2.0 request per line; the
//! server replies with one JSON-RPC 2.0 response per line.  Concurrent
//! event push is not supported over UDS in this version — the client
//! polls via `/status` or reconnects for a new command.
//!
//! # Authentication
//!
//! - The socket file is created with mode `0o700` (owner-only access).
//! - An optional bearer token can be required: the first message on a
//!   connection must be `{"jsonrpc":"2.0","method":"auth","params":{"token":"..."},"id":1}`.
//!   If the token does not match, the server sends an error and closes.
//! - Token auth is disabled by default (`None`).  Enable it by setting
//!   [`UdsConfig::auth_token`].
//!
//! # Design
//!
//! - Uses [`tokio::net::UnixListener`] for async I/O.
//! - Each connection is handled in its own spawned task; a connection that
//!   sends malformed JSON is replied to with a parse error frame and then
//!   closed.
//! - The socket path is cleaned up on drop via a guard file or by the
//!   caller — the server does not `unlink` the path on its own (another
//!   process may be watching it).

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

use crate::frame::{JsonRpcRequest, JsonRpcResponse, error_code};
use crate::slash::command_from_jsonrpc;
use crate::{TransportState, dispatch_command};

/// Configuration for the UDS transport.
///
/// The socket is bound before its permissions are tightened to `0o700`
/// (`bind` then `chmod` is the only possible ordering: `chmod` needs the
/// path to exist).  There is therefore a tiny window between `bind` and
/// `chmod` during which the socket file is reachable with the umask's
/// default permissions.  For strictly local control-plane use this is
/// acceptable; enable [`auth_token`](#structfield.auth_token) if the
/// socket may be visible to other local users.
pub struct UdsConfig {
    /// Path to the Unix domain socket.
    pub socket_path: String,
    /// Optional bearer token.  When set, every connection must authenticate
    /// with a matching `auth` method call before issuing commands.
    /// Token comparison is constant-time.
    pub auth_token: Option<String>,
}

/// Serve the UDS transport forever (or until a fatal I/O error).
///
/// Binds to the configured socket path, sets permissions to `0o700`, and
/// accepts connections in a loop.  Each connection gets a spawned task.
///
/// # Errors
///
/// Returns an error if the socket cannot be bound or the listener cannot
/// be created.
pub async fn serve_uds(
    config: UdsConfig,
    state: Arc<TransportState>,
) -> Result<(), std::io::Error> {
    // Remove any existing socket file at the path.
    let _ = tokio::fs::remove_file(&config.socket_path).await;

    let listener = UnixListener::bind(&config.socket_path)?;

    // Set permissions to owner-only (async, so the bind→chmod window is
    // as small as the runtime allows).
    set_permissions(&config.socket_path).await?;

    loop {
        let (stream, _peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                // Log and continue on transient accept errors.
                eprintln!("uds accept error: {e}");
                continue;
            }
        };

        let state = state.clone();
        let token = config.auth_token.clone();
        tokio::spawn(async move {
            handle_connection(stream, state, token.as_deref()).await;
        });
    }
}

/// Handle one UDS connection: read NDJSON lines, dispatch, write responses.
async fn handle_connection(
    stream: tokio::net::UnixStream,
    state: Arc<TransportState>,
    auth_token: Option<&str>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut authenticated = auth_token.is_none();

    loop {
        line.clear();
        let _bytes_read = match reader.read_line(&mut line).await {
            Ok(0) => break, // EOF
            Ok(n) => n,
            Err(_) => break,
        };

        // Skip empty lines (keepalive whitespace).
        if line.trim().is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(line.trim()) {
            Ok(req) => req,
            Err(_) => {
                let err = JsonRpcResponse::error(0, error_code::PARSE_ERROR, "invalid JSON");
                let _ = write_response(&mut writer, &err).await;
                break;
            }
        };

        // If auth is required and not yet authenticated, check the method.
        if !authenticated {
            if request.method == "auth" {
                let token_ok = auth_token.is_some_and(|expected| {
                    request
                        .params
                        .as_ref()
                        .and_then(|p| p.get("token"))
                        .and_then(|v| v.as_str())
                        .is_some_and(|t| constant_time_eq(t, expected))
                });

                if token_ok {
                    authenticated = true;
                    let ok = JsonRpcResponse::success(request.id, serde_json::json!({"ok": true}));
                    let _ = write_response(&mut writer, &ok).await;
                    continue;
                }
            }

            let err = JsonRpcResponse::error(
                request.id,
                error_code::INVALID_REQUEST,
                "authentication required",
            );
            let _ = write_response(&mut writer, &err).await;
            break;
        }

        // Dispatch the command.
        let response = match command_from_jsonrpc(&request.method, request.params) {
            Ok(cmd) => dispatch_command(&*state.handler, cmd, request.id),
            Err(parse_err) => {
                JsonRpcResponse::error(request.id, parse_err.code(), parse_err.to_string())
            }
        };

        if write_response(&mut writer, &response).await.is_err() {
            break;
        }
    }
}

/// Write a JSON-RPC response as a single NDJSON line.
async fn write_response<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    response: &JsonRpcResponse,
) -> Result<(), std::io::Error> {
    let json = serde_json::to_string(response).unwrap_or_else(|_| "{}".into());
    let mut line = json;
    line.push('\n');
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await
}

/// Set Unix socket permissions to `0o700` (owner-only) asynchronously.
///
/// Performed after [`UnixListener::bind`] so there is a brief window
/// (documented on [`UdsConfig`]) where the socket is reachable before
/// permissions are tightened.
#[cfg(unix)]
async fn set_permissions(path: &str) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    let perm = std::fs::Permissions::from_mode(0o700);
    tokio::fs::set_permissions(path, perm).await
}

#[cfg(not(unix))]
async fn set_permissions(_path: &str) -> Result<(), std::io::Error> {
    // No-op on non-Unix platforms.
    Ok(())
}
/// Constant-time comparison of two strings.
///
/// Returns `false` when the lengths differ (without revealing which is
/// longer) and otherwise XOR-folds the byte slices so comparison time does
/// not depend on the position of a mismatch.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.len() != b_bytes.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a_bytes.iter().zip(b_bytes.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
