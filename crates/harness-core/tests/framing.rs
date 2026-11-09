//! JSON-RPC framing helpers: construction, line encode/decode, classification.
//!
//! Acceptance: the four frame kinds classify from a decoded value exactly as
//! the wire promises (including the non-numeric-id rule), and a frame written
//! through `write_json_line` reads back unchanged through `read_line`.

use harness_core::framing::{
    FrameKind, classify, decode_line, encode_line, notification_value, read_line, request_value,
    write_json_line,
};
use serde_json::{Value, json};
use std::io::{BufReader, Cursor};

#[test]
fn request_and_notification_constructs() {
    let req = request_value(7, "session.create", json!({"title": "demo"}));
    assert_eq!(req["jsonrpc"], "2.0");
    assert_eq!(req["id"], 7);
    assert_eq!(req["method"], "session.create");
    assert_eq!(req["params"]["title"], "demo");

    let note = notification_value("event.push", json!({"k": 1}));
    assert_eq!(note["jsonrpc"], "2.0");
    assert!(note.get("id").is_none());
    assert_eq!(note["method"], "event.push");
}

#[test]
fn classify_all_four_kinds() {
    let req = request_value(3, "tool.call", json!({}));
    match classify(&req) {
        FrameKind::Request { id, method, .. } => {
            assert_eq!(id, 3);
            assert_eq!(method, "tool.call");
        }
        kind => panic!("expected Request, got {kind:?}"),
    }

    let resp = json!({"jsonrpc": "2.0", "id": 3, "result": {"ok": true}});
    match classify(&resp) {
        FrameKind::Response { id, .. } => assert_eq!(id, 3),
        kind => panic!("expected Response, got {kind:?}"),
    }

    let note = notification_value("ping", json!({}));
    match classify(&note) {
        FrameKind::Notification { method, .. } => assert_eq!(method, "ping"),
        kind => panic!("expected Notification, got {kind:?}"),
    }

    let not_rpc = json!({"hello": "world"});
    assert!(matches!(classify(&not_rpc), FrameKind::Ignored));

    // Non-numeric ids are not JSON-RPC calls: classified as Ignored rather
    // than misread as a Request.
    let string_id = json!({"jsonrpc": "2.0", "id": "abc", "method": "m"});
    assert!(matches!(classify(&string_id), FrameKind::Ignored));
}

#[test]
fn line_round_trip() -> std::io::Result<()> {
    let frame = request_value(1, "ping", json!({}));
    let line = encode_line(&frame);
    assert!(
        line.lines().count() == 1,
        "encode_line must stay single-line"
    );

    let decoded = decode_line(&line);
    assert_eq!(decoded, Some(frame));

    assert!(decode_line("   ").is_none(), "blank lines are no frame");

    let mut writer = Cursor::new(Vec::new());
    write_json_line(&mut writer, &line)?;
    let mut reader = BufReader::new(Cursor::new(writer.into_inner()));
    let mut got = String::new();
    assert!(read_line(&mut reader, &mut got)?);
    assert_eq!(got, line);
    assert!(!read_line(&mut reader, &mut got)?, "EOF reports Ok(false)");
    Ok(())
}

#[test]
fn malformed_line_is_not_a_frame() {
    let malformed: Option<Value> = decode_line("{ not json");
    assert!(malformed.is_none());
}
