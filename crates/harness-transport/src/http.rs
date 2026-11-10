//! HTTP+SSE transport: JSON-RPC command dispatch over `POST /command` and
//! `RunEvent` streaming over `GET /events` (Server-Sent Events).
//!
//! # Routes
//!
//! | Method | Path | Description |
//! |---|---|---|
//! | POST | `/command` | Accept a JSON-RPC 2.0 request body, dispatch through the
//!   [`CommandHandler`](crate::CommandHandler), return a JSON-RPC response. |
//! | GET | `/events` | SSE stream.  Query params: `run_id` (required) and
//!   `after_seq` (optional, default 0).  Replays persisted events since
//!   `after_seq` via [`EventLog`](crate::EventLog), then subscribes to the
//!   live broadcast channel. |
//!
//! # Design
//!
//! - The POST handler is a pure request-response: no server-sent frames
//!   arrive on this connection.
//! - The SSE handler sends an initial `event: connected` frame with the
//!   last replayed event's seq (or `after_seq` when nothing was replayed)
//!   so the client can detect gaps, then pushes every [`RunEvent`] for
//!   `run_id` as `event: run_event` with JSON data.  Errors from the
//!   transport itself are sent as `event: error` frames (harness-rs design).
//! - All errors are typed [`RpcError`](crate::RpcError) frames — never bare
//!   strings.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::sse::{Event, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream::{self, Stream, StreamExt};
use harness_core::{RunEvent, RunId};

use crate::frame::{JsonRpcRequest, JsonRpcResponse, run_event_to_sse};
use crate::slash::command_from_jsonrpc;
use crate::{TransportState, dispatch_command};

/// Query parameters for the SSE events endpoint.
#[derive(serde::Deserialize)]
pub struct EventQuery {
    pub run_id: String,
    #[serde(default)]
    pub after_seq: u64,
}

/// Build an [`axum::Router`] with the HTTP+SSE transport routes.
///
/// Mount this router under any path prefix (typically `/v1/transport` or
/// directly at `/`).  Example:
///
/// ```ignore
/// let app = http::http_routes(state.clone());
/// let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
/// axum::serve(listener, app).await?;
/// ```
pub fn http_routes(state: Arc<TransportState>) -> Router {
    Router::new()
        .route("/command", post(handle_command))
        .route("/events", get(handle_events))
        .with_state(state)
}

/// POST /command — dispatch a JSON-RPC request.
async fn handle_command(
    State(state): State<Arc<TransportState>>,
    Json(req): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    let response = match command_from_jsonrpc(&req.method, req.params) {
        Ok(cmd) => dispatch_command(&*state.handler, cmd, req.id),
        Err(parse_err) => JsonRpcResponse::error(req.id, parse_err.code(), parse_err.to_string()),
    };
    Json(response)
}

/// GET /events — SSE stream of [`RunEvent`]s.
async fn handle_events(
    State(state): State<Arc<TransportState>>,
    Query(query): Query<EventQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let run_id = RunId(query.run_id.clone());
    let after_seq = query.after_seq;

    // Replay persisted events since after_seq.
    let replayed: Vec<RunEvent> = state.event_log.read_events_since(&run_id, after_seq);

    // Subscribe to the live broadcast channel.
    let rx = state.event_tx.subscribe();

    // Build a stream: first replay, then forward live events for this run.
    let stream = event_stream(replayed, after_seq, rx, run_id);

    Sse::new(stream)
}

// Build an SSE event stream that replays historical events then forwards
// live events for one `run_id`.
//
// The stream emits replayed events first (already filtered to `run_id` by
// [`EventLog::read_events_since`]), then emits an `event: connected`
// frame carrying the last replayed seq (or `after_seq` when nothing was
// replayed) so the client can detect gaps.  Live events are filtered to
// `run_id` so a client never sees another run's events.
fn event_stream(
    replayed: Vec<RunEvent>,
    after_seq: u64,
    rx: tokio::sync::broadcast::Receiver<RunEvent>,
    run_id: RunId,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let last_seq = replayed.last().map_or(after_seq, |event| event.seq);

    // Emit replayed events first, then switch to live broadcast.
    let replay_stream = stream::iter(
        replayed
            .into_iter()
            .map(|event| Ok::<_, Infallible>(run_event_to_sse(&event))),
    );

    // Signal the last replayed seq so clients can detect gaps after
    // reconnecting.
    let connected_stream = futures_util::stream::once(async move {
        Ok::<_, Infallible>(
            Event::default()
                .event("connected")
                .data(last_seq.to_string()),
        )
    });

    // Forward only this run's live events.  `run_id` round-trips through
    // unfold's state so each future owns it (no borrowed value escapes the
    // `FnMut` closure across an await point).
    let run_id_for_live = run_id.clone();
    let live_stream = stream::unfold((rx, run_id_for_live), |(mut rx, run_id)| async move {
        loop {
            match rx.recv().await {
                Ok(event) if event.run_id == run_id => {
                    let sse = run_event_to_sse(&event);
                    return Some((Ok(sse), (rx, run_id)));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                // Lagged means we missed some messages — skip and continue.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                // Other runs' events are dropped silently.
                Ok(_) => continue,
            }
        }
    });

    replay_stream.chain(connected_stream).chain(live_stream)
}
