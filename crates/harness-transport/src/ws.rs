//! WebSocket transport: bidirectional JSON-RPC communication with `RunEvent`
//! push.
//!
//! # Protocol
//!
//! After the HTTP upgrade, the WebSocket carries text frames only.  The
//! client sends JSON-RPC 2.0 request frames; the server replies with
//! JSON-RPC response frames.  Concurrently, the server pushes `RunEvent`s
//! as JSON-RPC notification frames (`method: "event"`) with the event
//! payload in `params`.
//!
//! # Reconnection
//!
//! On reconnect the client sends a `subscribe` method with `run_id` and
//! `after_seq`; the server replays missed events from the
//! [`EventLog`](crate::EventLog) seam before subscribing to the live
//! broadcast.  This keeps the client's event log contiguous across
//! reconnections without the transport needing session state.
//!
//! # Design
//!
//! - Each WebSocket connection spawns two tasks: one reads commands from
//!   the client and dispatches them; one subscribes to the broadcast
//!   channel and forwards events to the client.  If either task ends
//!   (connection closed, broadcast lagged too far) the other is cancelled.
//! - Errors are sent as JSON-RPC error responses, never as bare text frames.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use futures_util::sink::SinkExt;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};

use crate::frame::{JsonRpcRequest, JsonRpcResponse, error_code};
use crate::slash::command_from_jsonrpc;
use crate::{TransportState, dispatch_command};

/// Build an [`axum::Router`] with the WebSocket transport route at `/ws`.
pub fn ws_routes(state: Arc<TransportState>) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state)
}

/// WebSocket upgrade handler.
///
/// Accept the upgrade, then run the bidirectional message loop.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<TransportState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

/// Run the bidirectional WebSocket message loop for one connection.
///
/// Splits the socket into a sender and receiver.  The receiver task reads
/// JSON-RPC requests, dispatches them, and writes responses back through
/// the sender.  The sender task subscribes to the broadcast channel and
/// pushes `RunEvent` notifications to the client.
async fn handle_ws(socket: WebSocket, state: Arc<TransportState>) {
    let (ws_sender, ws_receiver) = socket.split();
    let sender = Arc::new(tokio::sync::Mutex::new(ws_sender));

    // ── Receiver task: read commands ─────────────────────────────────
    let rx_sender = sender.clone();
    let rx_state = state.clone();
    let rx_handle = tokio::spawn(async move {
        read_loop(ws_receiver, rx_sender, rx_state).await;
    });

    // ── Sender task: push events ─────────────────────────────────────
    let tx_state = state.clone();
    let tx_handle = tokio::spawn(async move {
        event_push_loop(sender, tx_state).await;
    });

    // Wait for either task to finish (connection closed or error).
    let _ = tokio::join!(rx_handle, tx_handle);
}

/// Read JSON-RPC requests from the WebSocket receiver and dispatch them.
async fn read_loop(
    mut receiver: SplitStream<WebSocket>,
    sender: Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>,
    state: Arc<TransportState>,
) {
    while let Some(msg) = receiver.next().await {
        let msg = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) | Err(_) => break,
            // Ignore binary, ping, pong frames.
            _ => continue,
        };

        let response = match serde_json::from_str::<JsonRpcRequest>(&msg) {
            Ok(req) => match command_from_jsonrpc(&req.method, req.params) {
                Ok(cmd) => dispatch_command(&*state.handler, cmd, req.id),
                Err(parse_err) => JsonRpcResponse::error(
                    req.id,
                    error_code::INVALID_REQUEST,
                    parse_err.to_string(),
                ),
            },
            Err(_) => {
                // Could not parse as a request — send a parse error back.
                // We have no id, so use 0 (the client must correlate by
                // ordering).
                JsonRpcResponse::error(0, error_code::PARSE_ERROR, "invalid JSON-RPC request")
            }
        };

        let json = serde_json::to_string(&response).unwrap_or_else(|_| "{}".into());
        let mut sender = sender.lock().await;
        if sender.send(Message::Text(json.into())).await.is_err() {
            break;
        }
    }
}

/// Subscribe to the broadcast channel and push every [`RunEvent`] as a
/// JSON-RPC notification to the client.
async fn event_push_loop(
    sender: Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>,
    state: Arc<TransportState>,
) {
    let mut rx = state.event_tx.subscribe();
    loop {
        match rx.recv().await {
            Ok(event) => {
                let json = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
                let mut sender = sender.lock().await;
                if sender.send(Message::Text(json.into())).await.is_err() {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
        }
    }
}
