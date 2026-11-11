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

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How long to wait for the socket file to appear after spawn.
const SOCKET_WAIT: Duration = Duration::from_secs(10);
/// How long to wait for the daemon to exit after SIGTERM.
const EXIT_WAIT: Duration = Duration::from_secs(30);
/// How long to wait for a second daemon to refuse to start.
const LOCK_WAIT: Duration = Duration::from_secs(10);

/// The offline config: the upstream URL is never dialed at startup (the model
/// adapter is lazy), so the smoke test needs no API backend. Each test gets
/// its own `[session]` directory — two daemons otherwise race on the default
/// `~/.local/share/harness/sessions/sessions.sqlite` file lock.
fn smoke_config(session_dir: &Path) -> String {
    format!(
        r#"version = 1

[upstream]
base_url = "http://127.0.0.1:9/v1"
api_key_var = "HARNESS_SMOKE_KEY"

[[models.aliases]]
name = "smoke"
model_id = "smoke-1"

[session]
directory = "{}"
"#,
        session_dir.display()
    )
}

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

/// Wait (bounded by `timeout`) for the child to exit, killing it on timeout.
fn wait_bounded_deadline(
    child: &mut Child,
    timeout: Duration,
) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            return Err(format!("daemon did not exit within {timeout:?}").into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn daemon_round_trips_a_uds_command_and_shuts_down() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("harness-daemon-smoke-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let config_path = dir.join("config.toml");
    std::fs::write(&config_path, smoke_config(&dir.join("sessions")))?;
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

/// A second daemon on the same socket is refused by the pid lock.
///
/// Scenario:
/// 1. Spawn daemon #1 with a temp config and socket; wait until its UDS
///    socket accepts connections (by then #1 holds the pid lock).
/// 2. Spawn daemon #2 with the SAME `--socket` path and a fresh
///    `127.0.0.1:0` HTTP address: `PidLock::acquire` fails because #1 holds
///    the lock, before the config or upstream are even touched.
/// 3. Daemon #2 exits within `LOCK_WAIT` with a non-zero status and stderr
///    naming the lock ("another daemon instance is already running").
/// 4. SIGTERM daemon #1; it exits 0.
///
/// Acceptance: #2 refuses to start while the lock is held; #1 shuts down
/// cleanly; children are killed on every path.
#[test]
fn second_instance_is_rejected_by_pid_lock() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("harness-daemon-pidlock-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let config_path = dir.join("config.toml");
    std::fs::write(&config_path, smoke_config(&dir.join("sessions")))?;
    let socket_path = dir.join("daemon.sock");

    let spawn_daemon = || -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_harness-daemon"));
        cmd.arg("--config")
            .arg(&config_path)
            .arg("--socket")
            .arg(&socket_path)
            .arg("--http")
            .arg("127.0.0.1:0")
            .env("HARNESS_SMOKE_KEY", "smoke-key")
            .stdout(Stdio::null());
        cmd
    };

    // Daemon #1: acquires the pid lock and serves the socket.
    let mut child1 = spawn_daemon().stderr(Stdio::inherit()).spawn()?;

    let socket_ready = (|| {
        let deadline = Instant::now() + SOCKET_WAIT;
        loop {
            if UnixStream::connect(&socket_path).is_ok() {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    })();
    if !socket_ready {
        let _ = child1.kill();
        cleanup(&dir);
        return Err("daemon #1 socket never appeared".into());
    }

    // Daemon #2: same socket -- the lock is held, so it must refuse to start.
    let mut child2 = spawn_daemon()
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn daemon #2: {e}"));
    let status2 = wait_bounded_deadline(&mut child2, LOCK_WAIT)?;
    let stderr_text = {
        let mut stderr = child2.stderr.take().ok_or("daemon #2 has no stderr pipe")?;
        let mut text = String::new();
        stderr.read_to_string(&mut text)?;
        text
    };

    // Shut down #1 before asserting, so no child outlives this test on any path.
    let sent = unsafe { libc::kill(child1.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(sent, 0, "SIGTERM delivery to daemon #1 failed");
    let status1 = wait_bounded(&mut child1)?;
    let _ = child2.kill(); // best effort: it should already be dead

    assert!(
        !status2.success(),
        "daemon #2 must refuse to start while the lock is held, got {status2}"
    );
    assert!(
        stderr_text.contains("another daemon instance is already running"),
        "daemon #2 stderr must name the held pid lock, got: {stderr_text:?}",
    );
    assert!(
        status1.success(),
        "daemon #1 must exit 0 after SIGTERM, got {status1}"
    );

    cleanup(&dir);
    Ok(())
}

/// Purpose: RunEvents survive a daemon restart via the SQLite/Turso session
/// store (issue #12's runtime bookkeeping).
/// Scenario: daemon #1 starts with a temp session dir; a run is submitted
/// over UDS against the dead upstream (it terminates with an error but still
/// produces events); the daemon is SIGTERMed; daemon #2 restarts on the SAME
/// session dir and the SAME socket; `list` is queried over UDS.
/// Acceptance: `list` on daemon #2 contains the historical run with a
/// `finished:` status — the event log survived the restart; both daemons
/// exit 0 on SIGTERM; every child is killed on every path.
#[test]
fn daemon_persists_run_events_across_restart() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("harness-daemon-restart-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let config_path = dir.join("config.toml");
    std::fs::write(&config_path, smoke_config(&dir.join("sessions")))?;
    let socket_path = dir.join("daemon.sock");

    let spawn_daemon = || -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_harness-daemon"));
        cmd.arg("--config")
            .arg(&config_path)
            .arg("--socket")
            .arg(&socket_path)
            .arg("--http")
            .arg("127.0.0.1:0")
            .env("HARNESS_SMOKE_KEY", "smoke-key")
            .stdout(Stdio::null());
        cmd
    };

    let mut child1 = spawn_daemon().stderr(Stdio::inherit()).spawn()?;
    let result = restart_body(&socket_path, &spawn_daemon, &mut child1);
    let _ = child1.kill();
    cleanup(&dir);
    result
}

/// Submit a failing run, wait for it to reach a terminal status, restart the
/// daemon, and assert `list` still reports the run from the persisted store.
fn restart_body(
    socket_path: &Path,
    spawn_daemon: &dyn Fn() -> Command,
    child1: &mut Child,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut uds = wait_socket(socket_path)?;
    let run_id = submit_run(&mut uds, socket_path)?;
    drop(uds);

    // The upstream refuses every connection, so the run terminates quickly
    // (transient retries exhausted -> terminal error events are persisted).
    let terminal = wait_terminal(socket_path, &run_id)?;
    assert!(
        terminal.starts_with("finished:"),
        "run must reach a terminal status, got {terminal}"
    );

    // Graceful restart on the same session directory.
    let sent = unsafe { libc::kill(child1.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(sent, 0, "SIGTERM delivery to daemon #1 failed");
    let status = wait_bounded_deadline(child1, EXIT_WAIT)?;
    assert!(status.success(), "daemon #1 must exit 0, got {status}");

    let mut child2 = spawn_daemon().stderr(Stdio::inherit()).spawn()?;
    let result = assert_history_after_restart(socket_path, &run_id);
    let _ = child2.kill();
    result
}

/// Wait (bounded) for the socket, submit a run, return its id.
fn wait_socket(socket_path: &Path) -> Result<UnixStream, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + SOCKET_WAIT;
    loop {
        match UnixStream::connect(socket_path) {
            Ok(conn) => return Ok(conn),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
            Err(err) => return Err(err.into()),
        }
    }
}

/// Send one NDJSON request and read one NDJSON reply.
fn round_trip(stream: &mut UnixStream, request: &str) -> Result<String, std::io::Error> {
    use std::io::Write;
    stream.write_all(request.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line)
}

/// Submit a run over UDS and return the run id from the reply.
fn submit_run(
    stream: &mut UnixStream,
    socket_path: &Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let reply = round_trip(
        stream,
        r#"{"jsonrpc":"2.0","method":"run","params":{"task":"smoke persistence run"},"id":1}"#,
    )
    .map_err(|err| format!("run submit failed on {socket_path:?}: {err}"))?;
    let response: serde_json::Value = serde_json::from_str(reply.trim())?;
    response["result"]["run_id"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| format!("no run_id in reply: {reply}").into())
}

/// Poll `status` (bounded) until the run reaches a terminal status.
fn wait_terminal(socket_path: &Path, run_id: &str) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut stream = UnixStream::connect(socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let request = format!(
            r#"{{"jsonrpc":"2.0","method":"status","params":{{"run_id":"{run_id}"}},"id":2}}"#
        );
        let reply = round_trip(&mut stream, &request)?;
        let response: serde_json::Value = serde_json::from_str(reply.trim())?;
        if let Some(status) = response["result"]["status"].as_str()
            && status.starts_with("finished:")
        {
            return Ok(status.to_string());
        }
        if Instant::now() > deadline {
            return Err(format!("run {run_id} never reached a terminal status").into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// After the restart, `list` must report the historical run from the store.
fn assert_history_after_restart(
    socket_path: &Path,
    run_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + SOCKET_WAIT;
    loop {
        match UnixStream::connect(socket_path) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                let reply = round_trip(&mut stream, r#"{"jsonrpc":"2.0","method":"list","id":3}"#)?;
                let response: serde_json::Value = serde_json::from_str(reply.trim())?;
                let runs = response["result"]["runs"]
                    .as_array()
                    .ok_or("no runs array")?;
                let hit = runs.iter().any(|run| {
                    run["run_id"].as_str() == Some(run_id)
                        && run["status"]
                            .as_str()
                            .is_some_and(|s| s.starts_with("finished:"))
                });
                assert!(
                    hit,
                    "historical run {run_id} must survive the restart, got {runs:?}"
                );
                return Ok(());
            }
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
            Err(_) => return Err("daemon #2 socket never appeared".into()),
        }
    }
}
