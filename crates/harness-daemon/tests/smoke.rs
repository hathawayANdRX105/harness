//! Smoke test: start the daemon binary, round-trip a UDS command, shut down.
//!
//! Purpose: prove the daemon's process wiring end to end — pid lock, config
//! load, builder assembly, transport listening, JSON-RPC dispatch, graceful
//! shutdown — without any model traffic.
//! Scenario: spawn `harness-daemon` with a temp config, wait for its UDS
//! socket, send a `status` command for an unknown run, then SIGTERM it.
//! Acceptance: the UDS request gets a typed `NO_ACTIVE_RUN` JSON-RPC error
//! back (a completed round trip), and the daemon exits with status 0 after
//! SIGTERM.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How long to wait for the socket file to appear after spawn.
const SOCKET_WAIT: Duration = Duration::from_secs(10);
/// How long to wait for the daemon to exit after SIGTERM.
const EXIT_WAIT: Duration = Duration::from_secs(30);

/// The offline config: the upstream URL is never dialed at startup (the model
/// adapter is lazy), so the smoke test needs no API backend.
const SMOKE_CONFIG: &str = r#"
version = 1

[upstream]
base_url = "http://127.0.0.1:9/v1"
api_key_var = "HARNESS_SMOKE_KEY"

[[models.aliases]]
name = "smoke"
model_id = "smoke-1"
"#;

/// Remove the temp dir best-effort (test temp output, not user data).
fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Wait (bounded) for the child to exit, so a hung daemon fails the test
/// instead of stalling CI until the job timeout.
fn wait_bounded(child: &mut Child) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + EXIT_WAIT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            return Err("daemon did not exit within 30s of SIGTERM".into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn daemon_round_trips_a_uds_command_and_shuts_down() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("harness-daemon-smoke-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let config_path = dir.join("config.toml");
    std::fs::write(&config_path, SMOKE_CONFIG)?;
    let socket_path = dir.join("daemon.sock");

    let mut child = Command::new(env!("CARGO_BIN_EXE_harness-daemon"))
        .arg("--config")
        .arg(&config_path)
        .arg("--socket")
        .arg(&socket_path)
        .arg("--http")
        .arg("127.0.0.1:0")
        .env("HARNESS_SMOKE_KEY", "smoke-key")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;

    let result = smoke_body(&socket_path, &mut child);
    let _ = child.kill();
    cleanup(&dir);
    result
}

/// Connect, round-trip one command, SIGTERM, and verify a clean exit.
fn smoke_body(socket_path: &Path, child: &mut Child) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + SOCKET_WAIT;
    let mut stream = None;
    while Instant::now() < deadline {
        match UnixStream::connect(socket_path) {
            Ok(conn) => {
                stream = Some(conn);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    let mut stream = stream.ok_or("daemon socket never appeared")?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    // One JSON-RPC command round trip: an unknown run must come back as the
    // typed NO_ACTIVE_RUN error frame, proving parse → dispatch → respond.
    let request = r#"{"jsonrpc":"2.0","method":"status","params":{"run_id":"nope"},"id":1}"#;
    stream.write_all(request.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let response: serde_json::Value = serde_json::from_str(line.trim())?;
    assert_eq!(response["id"], 1, "response id must echo the request");
    let code = response["error"]["code"]
        .as_i64()
        .ok_or("response carries no error code")?;
    assert_eq!(code, -32000, "unknown run must yield NO_ACTIVE_RUN");

    // Graceful shutdown: SIGTERM, then a bounded wait for exit status 0.
    let sent = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(sent, 0, "SIGTERM delivery failed");
    let status = wait_bounded(child)?;
    assert!(
        status.success(),
        "daemon must exit 0 after SIGTERM, got {status}"
    );
    Ok(())
}
