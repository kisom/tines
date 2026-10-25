//! Shared helpers for the tines-bus integration tests.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A fresh, unique scratch directory under the system temp dir.
pub fn unique_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("tines-bus-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Spawn the bus daemon on a thread and return its socket path once bound.
pub fn spawn_daemon(dir: &Path) -> PathBuf {
    let sock = dir.join("bus.sock");
    let s = sock.clone();
    std::thread::spawn(move || {
        if let Err(e) = tines_bus::daemon::serve(&s) {
            panic!("daemon failed: {e}");
        }
    });
    // Wait for the daemon to bind, so the first connect cannot race it.
    for _ in 0..400 {
        if sock.exists() {
            return sock;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    panic!("daemon did not bind in time");
}
