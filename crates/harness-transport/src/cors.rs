//! CORS layer for browser-origin transport access.
//!
//! Applies [`tower_http::cors::CorsLayer`] to the HTTP+SSE and WebSocket
//! transport routers so a browser page can reach `POST /command`, `GET
//! /events` and `GET /ws` from `http://127.0.0.1:*` and
//! `http://localhost:*` (dev ports are kernel-picked, so any port is
//! accepted).
//!
//! # Origin matching
//!
//! Only exact dev origins are reflected back as
//! `Access-Control-Allow-Origin`.  The request `Origin` is parsed as a URI
//! and accepted only when:
//!
//! - the scheme is `http`, and
//! - the host is exactly `127.0.0.1` or `localhost` (any port).
//!
//! This rejects `https://`, `file://`, `null` and the prefix-impersonation
//! trick `http://localhost.evil.com` (its host is `localhost.evil.com`,
//! not `localhost`).  Requests without an `Origin` header are left
//! untouched — no `Access-Control-Allow-Origin` header is injected — so
//! the dev CLI, `curl`, same-origin pages and health probes keep their
//! original behaviour.
//!
//! # Preflight status
//!
//! [`tower_http::cors::Cors`] answers `OPTIONS` preflight requests itself
//! with `200 OK`.  The harness contract pins preflight to `204 No
//! Content`, so [`options_no_content`] normalises the status for `OPTIONS`
//! responses without touching any other response.

use axum::Router;
use axum::extract::Request;
use axum::middleware;
use axum::middleware::Next;
use axum::response::Response;
use http::request::Parts as RequestParts;
use http::{HeaderValue, Method, StatusCode, header};
use tower_http::cors::{AllowOrigin, CorsLayer};

/// Build the CORS layer applied to every transport route.
pub fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        // Only exact dev origins are reflected back as
        // `Access-Control-Allow-Origin`.
        .allow_origin(AllowOrigin::predicate(is_allowed_origin))
        // Methods the transport actually consumes.
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        // Headers the transport actually consumes.
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
}

/// Return `true` when `origin` is an allowed dev origin.
///
/// Accepted: `http://127.0.0.1:*` and `http://localhost:*` (any port).
/// Rejected: everything else, including `http://localhost.evil.com:*`.
fn is_allowed_origin(origin: &HeaderValue, _parts: &RequestParts) -> bool {
    let Ok(origin_str) = origin.to_str() else {
        return false;
    };
    let Ok(uri) = origin_str.parse::<http::Uri>() else {
        return false;
    };
    let Some(scheme) = uri.scheme() else {
        return false;
    };
    if scheme != "http" {
        return false;
    }
    let Some(host) = uri.host() else {
        return false;
    };
    host == "127.0.0.1" || host == "localhost"
}

/// Apply CORS plus preflight-204 normalisation to a transport router.
///
/// [`cors_layer`] is applied first so it sees the request method and adds
/// `Access-Control-Allow-Origin`; [`options_no_content`] sits outside it
/// and rewrites `OPTIONS` preflight responses to `204 No Content`.
pub fn with_cors(router: Router) -> Router {
    router
        .layer(cors_layer())
        .layer(middleware::from_fn(options_no_content))
}

/// Middleware that rewrites `OPTIONS` preflight responses to `204 No
// Content`.
///
/// [`tower_http::cors::Cors`] answers preflight requests itself with
/// `200 OK`.  Browsers accept 200, but the harness contract pins preflight
/// to `204 No Content`, so this normalises the status for `OPTIONS`
/// responses without touching any other response.
pub async fn options_no_content(req: Request, next: Next) -> Response {
    let is_options = req.method() == Method::OPTIONS;
    let mut res = next.run(req).await;
    if is_options {
        *res.status_mut() = StatusCode::NO_CONTENT;
    }
    res
}
