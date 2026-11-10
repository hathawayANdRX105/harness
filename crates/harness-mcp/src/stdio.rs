//! Stdio transport: a child process spoken to over line-delimited JSON-RPC.
//!
//! stderr is inherited so server diagnostics land in the host's log instead of
//! filling an unread pipe. stdout is drained by a reader thread into a channel
//! so a chatty server can never fill the pipe and deadlock. The write half
//! plus the receiver sit behind one mutex: MCP allows pipelining, this
//! implementation does not use it, and one lock keeps request and reply from
//! interleaving across threads.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Instant;

use crate::config::McpServerConfig;
use crate::error::McpError;
use crate::transport::{MCP_TIMEOUT, McpTransport, POLL_INTERVAL};

/// Stdio transport: one child process, one reader thread, one mutex.
pub struct StdioTransport {
    io: Mutex<Io>,
    child: Mutex<Child>,
    next: AtomicU64,
}

struct Io {
    stdin: ChildStdin,
    replies: Receiver<String>,
}

// SAFETY: the reader thread does not hold any reference to `StdioTransport`
// after construction — it owns the `Receiver` end of the channel, which is
// `Send`. The `Mutex<Io>` serialises the write + reply-receive path. See the
// module-level comment for the threading design.
unsafe impl Send for StdioTransport {}
unsafe impl Sync for StdioTransport {}

impl StdioTransport {
    /// Spawn the server process and wire up pipes.
    ///
    /// The server gets its own process group so [`Drop`] can reap the server's
    /// children too. The working directory defaults to the daemon's cwd; set
    /// one via [`McpServerConfig`] when the command resolves relative paths.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Spawn`] when the command is missing, the binary
    /// cannot be found, or the pipes cannot be created.
    pub fn spawn(cfg: &McpServerConfig) -> Result<Self, McpError> {
        let command = cfg
            .command
            .as_deref()
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| McpError::Spawn("no command for stdio transport".into()))?;
        let mut cmd = Command::new(command);
        cmd.args(&cfg.args)
            .envs(&cfg.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            // Own process group: Drop can then reap the server's children too.
            .process_group(0);

        let mut child = cmd
            .spawn()
            .map_err(|e| McpError::Spawn(format!("{command}: {e}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| McpError::Spawn("stdin pipe missing".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::Spawn("stdout pipe missing".into()))?;

        let (tx, replies) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line.trim().is_empty() {
                    continue;
                }
                if tx.send(line).is_err() {
                    break; // transport dropped
                }
            }
        });

        Ok(Self {
            io: Mutex::new(Io { stdin, replies }),
            child: Mutex::new(child),
            next: AtomicU64::new(1),
        })
    }
}

impl McpTransport for StdioTransport {
    fn next_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    fn notify(&self, line: &str) -> Result<(), McpError> {
        let mut io = lock(&self.io);
        write_line(&mut io.stdin, line)
    }

    fn roundtrip(&self, _id: u64, line: &str, signal: &AtomicBool) -> Result<String, McpError> {
        let mut io = lock(&self.io);
        write_line(&mut io.stdin, line)?;

        let deadline = Instant::now() + MCP_TIMEOUT;
        loop {
            if signal.load(Ordering::Relaxed) {
                return Err(McpError::Aborted);
            }
            if Instant::now() >= deadline {
                return Err(McpError::Timeout);
            }
            match io.replies.recv_timeout(POLL_INTERVAL) {
                Ok(reply) => {
                    // Accept any JSON-RPC 2.0 response — parse_response validates
                    // the `id` and any error object.
                    return Ok(reply);
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Loop back to check signal and deadline.
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(McpError::Transport("server closed stdout".into()));
                }
            }
        }
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        let mut child = lock(&self.child);
        // The child gets its own process group at spawn, so signal the group
        // to take any helper processes it forked.
        let rc = kill_process_group(child.id() as i32);
        if rc != 0 {
            // killpg failed; fall back to child.kill().
            let _ = child.kill();
        }
        let _ = child.wait();
    }
}

// SIGKILL the whole process group led by `pgrp` (SIGKILL = 9). Returns the
// `killpg` return value: 0 = signalled, anything else = fallback to kill().
fn kill_process_group(pgrp: i32) -> i32 {
    // SAFETY: the child is spawned with `.process_group(0)`, so it is its own
    // process-group leader and the group id equals its pid; pid_max keeps pids
    // within i32::MAX, so the cast cannot change sign and killpg targets
    // exactly that group (on failure the caller falls back to child.kill()).
    libc_killpg(pgrp, 9)
}

unsafe extern "C" {
    fn killpg(pgrp: i32, sig: i32) -> i32;
}

fn libc_killpg(pgrp: i32, sig: i32) -> i32 {
    // SAFETY: thin wrapper around the C library's killpg.
    unsafe { killpg(pgrp, sig) }
}

/// Lock a `Mutex`, tolerating poisoning.
///
/// A panic while holding the lock only ever leaves protected data mid-write on
/// a connection we are about to error out of, so recovering the guard beats
/// propagating a poison panic into the agent loop.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Write one newline-delimited JSON-RPC message and flush it.
fn write_line(stdin: &mut ChildStdin, line: &str) -> Result<(), McpError> {
    writeln!(stdin, "{line}")
        .and_then(|_| stdin.flush())
        .map_err(|e| McpError::Transport(format!("write to server stdin failed: {e}")))
}
