//! WebSocket transport: bidirectional JSON-RPC communication with `RunEvent`
//! push.
//!
//! # Protocol
//!
//! After the HTTP upgrade, the WebSocket carries text frames only.  The
//! client sends JSON-RPC 2.0 request frames; the server replies with
//! JSON-RPC response frames.  Concurrently, the server pushes `RunEvent`s
//! as JSON text frames.
//!
//! # Reconnection
//!
//! On reconnect the client sends a `subscribe` method with `run_id`,
//! optional `session_id`, and `after_seq`; the server replies with a
//! `{"subscribed": run_id, "session_id"}` success frame, replays missed
//! events from the [`EventLog`](crate::EventLog) seam before forwarding live
//! events, and then forwards only events for that run.  This keeps the
//! client's event log contiguous across reconnections without the transport
//! needing session state.
//!
//! # Design
//!
//! - Each WebSocket connection spawns two tasks: one reads commands from
//!   the client and dispatches them; one subscribes to the broadcast
//!   channel and forwards events to the client.  If either task ends
//!   (connection closed, broadcast closed) [`tokio::select!`] cancels the
//!   other, so no task outlives its connection.
//! - Connections that never subscribe receive no events.
//! - Errors are sent as JSON-RPC error responses, never as bare text frames.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use futures_util::sink::SinkExt;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use harness_core::{RunEvent, RunId};
use serde_json::json;

use crate::frame::{JsonRpcRequest, JsonRpcResponse, error_code};
use crate::slash::command_from_jsonrpc;
use crate::{TransportState, dispatch_command};

// ── Router ────────────────────────────────────────────────────────────────

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

// ── Connection loop ───────────────────────────────────────────────────────

/// Run the bidirectional WebSocket message loop for one connection.
///
/// Splits the socket into a sender and receiver.  The receiver task reads
/// JSON-RPC requests, dispatches them, and writes responses back through
/// the sender.  The sender task subscribes to the broadcast channel and
/// pushes `RunEvent`s to the client.
async fn handle_ws(socket: WebSocket, state: Arc<TransportState>) {
    let (sender, receiver) = socket.split();
    let sender = Arc::new(tokio::sync::Mutex::new(sender));
    let subscription: Arc<tokio::sync::Mutex<Option<SubscribeState>>> =
        Arc::new(tokio::sync::Mutex::new(None));

    let read_task = read_loop(
        receiver,
        sender.clone(),
        state.clone(),
        subscription.clone(),
    );
    let write_task = event_push_loop(sender, state, subscription);

    tokio::select! {
        _ = read_task => {},
        _ = write_task => {},
    }
}

/// Per-connection subscription state.  `None` means the client has not
/// subscribed yet and receives no events.
///
/// `replayed` flips to `true` once the historical replay has been sent, so
/// the replay runs exactly once and always before the first live forward.
#[derive(Clone, Debug)]
struct SubscribeState {
    run_id: RunId,
    after_seq: u64,
    /// Optional session the client is re-attaching to (#9).  v1 semantics:
    /// the server echoes it in the subscribe reply; replay stays run-scoped
    /// via the `EventLog` seam (`run_id` + `after_seq`), so the client names
    /// the run it re-attaches to.  Resolving the session's latest run
    /// server-side is intentionally out of scope for v1.
    session_id: Option<String>,
    replayed: bool,
}

/// Read JSON-RPC requests from the WebSocket receiver and dispatch them.
async fn read_loop(
    mut receiver: SplitStream<WebSocket>,
    sender: Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>,
    state: Arc<TransportState>,
    subscription: Arc<tokio::sync::Mutex<Option<SubscribeState>>>,
) {
    while let Some(msg) = receiver.next().await {
        let msg = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) | Err(_) => break,
            // Ignore binary, ping, pong frames.
            _ => continue,
        };

        let Ok(req) = serde_json::from_str::<JsonRpcRequest>(&msg) else {
            // Could not parse as a request — send a parse error back.  We
            // have no id, so use 0 (the client must correlate by ordering).
            let response =
                JsonRpcResponse::error(0, error_code::PARSE_ERROR, "invalid JSON-RPC request");
            let _ = send_response(&sender, response).await;
            continue;
        };

        let response = match req.method.as_str() {
            "subscribe" => handle_subscribe(&req, &subscription).await,
            _ => dispatch_jsonrpc(&req, &state),
        };
        if !send_response(&sender, response).await {
            break;
        }
    }
}

/// Handle the documented `subscribe` method: record this connection's run
/// subscription and acknowledge with `{"subscribed": run_id, "session_id"}`.
async fn handle_subscribe(
    req: &JsonRpcRequest,
    subscription: &tokio::sync::Mutex<Option<SubscribeState>>,
) -> JsonRpcResponse {
    let Some(params) = req.params.as_ref().and_then(|params| params.as_object()) else {
        return JsonRpcResponse::error(
            req.id,
            error_code::INVALID_PARAMS,
            "subscribe params must be an object",
        );
    };
    let Some(run_id) = params.get("run_id").and_then(|value| value.as_str()) else {
        return JsonRpcResponse::error(
            req.id,
            error_code::INVALID_PARAMS,
            "subscribe requires a string run_id",
        );
    };
    let run_id = run_id.to_string();
    let after_seq = params
        .get("after_seq")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let session_id = params
        .get("session_id")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());

    let subscription_state = SubscribeState {
        run_id: RunId(run_id.clone()),
        after_seq,
        session_id: session_id.clone(),
        replayed: false,
    };
    let bound_session = subscription_state.session_id.clone();
    *subscription.lock().await = Some(subscription_state);
    JsonRpcResponse::success(
        req.id,
        json!({ "subscribed": run_id, "session_id": bound_session }),
    )
}

/// Dispatch one non-`subscribe` JSON-RPC request into a response.
fn dispatch_jsonrpc(req: &JsonRpcRequest, state: &TransportState) -> JsonRpcResponse {
    match command_from_jsonrpc(&req.method, req.params.clone()) {
        Ok(cmd) => dispatch_command(&*state.handler, cmd, req.id),
        Err(parse_err) => JsonRpcResponse::error(req.id, parse_err.code(), parse_err.to_string()),
    }
}

/// Serialize one JSON-RPC response and send it as a text frame.
///
/// Returns `false` when the socket is closed, so the caller can exit its
/// loop instead of spinning on sends that will never land.
async fn send_response(
    sender: &Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>,
    response: JsonRpcResponse,
) -> bool {
    let json = serde_json::to_string(&response).unwrap_or_else(|_| "{}".into());
    let mut sender = sender.lock().await;
    sender.send(Message::Text(json.into())).await.is_ok()
}

/// Subscribe to the broadcast channel and push [`RunEvent`]s to the
/// client.
///
/// The push loop subscribes to the broadcast channel exactly once.  Before
/// the first live forward, a subscribed connection replays its run's
/// persisted events since `after_seq`; afterwards only events whose
/// `run_id` matches the subscription are forwarded.  Connections without a
/// subscription receive no events.
async fn event_push_loop(
    sender: Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>,
    state: Arc<TransportState>,
    subscription: Arc<tokio::sync::Mutex<Option<SubscribeState>>>,
) {
    let mut rx = state.event_tx.subscribe();
    loop {
        match rx.recv().await {
            Ok(event) => {
                maybe_replay(&sender, &state, &subscription).await;
                if !is_subscribed_to(&subscription, &event.run_id).await {
                    continue;
                }
                if !push_event(&sender, &event).await {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
        }
    }
}

/// Replay the subscribed run's persisted events once, before any live
/// event is forwarded.  v1 session binding (#9): when the subscription
/// carries a `session_id`, the server echoes it in the subscribe reply;
/// replay itself stays run-scoped (`run_id` + `after_seq`) through the
/// `EventLog` seam, so the client names the run it re-attaches to.  No-op
/// when the connection is unsubscribed or the replay already happened.
async fn maybe_replay(
    sender: &Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>,
    state: &TransportState,
    subscription: &Arc<tokio::sync::Mutex<Option<SubscribeState>>>,
) {
    let (run_id, after_seq) = {
        let sub = subscription.lock().await;
        let Some(sub) = sub.as_ref() else { return };
        if sub.replayed {
            return;
        }
        (sub.run_id.clone(), sub.after_seq)
    };

    for event in state.event_log.read_events_since(&run_id, after_seq).await {
        if !push_event(sender, &event).await {
            return; // socket closed — the outer loop exits on the next recv
        }
    }

    let mut sub = subscription.lock().await;
    if let Some(sub) = sub.as_mut() {
        sub.replayed = true;
    }
}

/// True when `subscription` names `run_id`.
async fn is_subscribed_to(
    subscription: &Arc<tokio::sync::Mutex<Option<SubscribeState>>>,
    run_id: &RunId,
) -> bool {
    let sub = subscription.lock().await;
    sub.as_ref().is_some_and(|sub| &sub.run_id == run_id)
}

/// Send one `RunEvent` as a text frame.  `false` means the socket closed.
async fn push_event(
    sender: &Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>,
    event: &RunEvent,
) -> bool {
    let json = serde_json::to_string(event).unwrap_or_else(|_| "{}".into());
    let mut sender = sender.lock().await;
    sender.send(Message::Text(json.into())).await.is_ok()
}
