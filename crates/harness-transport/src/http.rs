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
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::frame::{JsonRpcRequest, JsonRpcResponse};
use crate::slash::command_from_jsonrpc;
use crate::{TransportState, dispatch_command};

// ── Query parameters ──────────────────────────────────────────────────────

/// Query parameters for the SSE events endpoint.
#[derive(Deserialize)]
pub struct EventQuery {
    /// Run whose events to stream.
    pub run_id: String,
    /// Replay events with seq strictly greater than this (default 0).
    #[serde(default)]
    pub after_seq: u64,
}

// ── Routes ────────────────────────────────────────────────────────────────

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

// ── Handlers ──────────────────────────────────────────────────────────────

/// POST /command — dispatch a JSON-RPC request.
async fn handle_command(
    State(state): State<Arc<TransportState>>,
    Json(req): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    let cmd = match command_from_jsonrpc(&req.method, req.params.clone()) {
        Ok(cmd) => cmd,
        Err(parse_err) => {
            return Json(JsonRpcResponse::error(
                req.id,
                parse_err.code(),
                parse_err.to_string(),
            ));
        }
    };
    Json(dispatch_command(&*state.handler, cmd, req.id))
}

/// GET /events — SSE stream of [`RunEvent`]s.
async fn handle_events(
    State(state): State<Arc<TransportState>>,
    Query(query): Query<EventQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let run_id = RunId(query.run_id.clone());
    let after_seq = query.after_seq;

    // Replay persisted events since after_seq (async through the EventLog seam).
    let replayed = state.event_log.read_events_since(&run_id, after_seq).await;

    // Subscribe to the live broadcast channel.
    let rx = state.event_tx.subscribe();

    // Build a stream: first replay, then forward live events for this run.
    let stream = event_stream(replayed, after_seq, rx, run_id);

    Sse::new(stream)
}

// ── Stream construction ───────────────────────────────────────────────────

/// Build an SSE event stream that replays historical events then forwards
/// live events for one `run_id`.
///
/// The stream emits replayed events first (already filtered to `run_id` by
/// [`EventLog::read_events_since`]), then emits an `event: connected`
/// frame carrying the last replayed seq (or `after_seq` when nothing was
/// replayed) so the client can detect gaps.  Live events are filtered to
/// `run_id` so a client never sees another run's events.
fn event_stream(
    replayed: Vec<RunEvent>,
    after_seq: u64,
    rx: tokio::sync::broadcast::Receiver<RunEvent>,
    run_id: RunId,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let replay_seq = replayed.last().map(|e| e.seq).unwrap_or(after_seq);
    let live_run_id = run_id.clone();
    let replay = stream::iter(replayed.into_iter().map(move |event| {
        Ok(Event::default().data(serde_json::to_string(&event).unwrap_or_else(|_| "{}".into())))
    }));
    let connected = stream::iter([Ok(Event::default()
        .event("connected")
        .data(replay_seq.to_string()))]);
    let live = BroadcastStream::new(rx).filter_map(move |event| {
        let value = live_run_id.clone();
        async move {
            let event = event.ok()?;
            (event.run_id == value).then(|| {
                Ok(Event::default()
                    .data(serde_json::to_string(&event).unwrap_or_else(|_| "{}".into())))
            })
        }
    });

    replay.chain(connected).chain(live)
}
