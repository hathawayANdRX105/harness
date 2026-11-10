//! Single-instance guard: an exclusive file lock held for the daemon's
//! lifetime.
//!
//! The lock file sits next to the UDS socket (`<socket>.lock`). A second
//! daemon process fails to take the lock and exits with a clear error instead
//! of silently sharing the socket and stealing commands.
//!
//! The lock is advisory (`flock` on unix): it guards against honest double
//! starts, not against a process that deliberately opens the file. Releasing
//! happens on drop or process exit — no explicit unlock exists, so a crashed
//! daemon never leaves a stale lock behind.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::Path;

/// Why a pid lock could not be taken.
#[derive(Debug, thiserror::Error)]
pub enum PidLockError {
    /// Another process holds the lock — a daemon is already running.
    #[error("another daemon instance is already running (lock held: {path})")]
    AlreadyRunning { path: String },
    /// The lock file could not be opened or locked.
    #[error("pid lock at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// A held single-instance lock.
pub struct PidLock {
    // The lock lives as long as the file handle does; the field is unused but
    // must stay resident for the process lifetime.
    _file: File,
}

impl PidLock {
    /// Take the exclusive lock at `path` and write the current pid into it.
    ///
    /// The file is opened without truncation so a concurrent holder's pid is
    /// never erased before the lock attempt fails.
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self, PidLockError> {
        let path = path.as_ref();
        let display = path.display().to_string();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .map_err(|source| PidLockError::Io {
                path: display.clone(),
                source,
            })?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(PidLockError::AlreadyRunning { path: display });
            }
            Err(TryLockError::Error(source)) => {
                return Err(PidLockError::Io {
                    path: display,
                    source,
                });
            }
        }
        file.set_len(0).map_err(|source| PidLockError::Io {
            path: display.clone(),
            source,
        })?;
        writeln!(file, "{}", std::process::id()).map_err(|source| PidLockError::Io {
            path: display.clone(),
            source,
        })?;
        file.flush().map_err(|source| PidLockError::Io {
            path: display,
            source,
        })?;
        Ok(Self { _file: file })
    }
}
