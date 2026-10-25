//! Shared test setup for the integration suites. Compiled per test binary;
//! only declare `mod common;` in tests that use these helpers.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::OnceLock;

/// One shared scratch setup per test binary: a temp dir that becomes both
/// XDG_STATE_HOME (fleet dirs) and a PATH dir holding a fake `tines-busd`.
/// Env is process-global, so it is set exactly once before any test touches
/// `fleet_dir` — a per-test `set_var` from parallel test threads would race.
pub fn env_with_fake_busd() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("tines-gui-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Fake tines-busd: create the socket named by argv[1], then stay
        // alive so the spawning Fleet keeps a live child.
        let fake = dir.join("tines-busd");
        std::fs::write(&fake, "#!/bin/sh\ntouch \"$1\"\nsleep 60\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let path = format!(
            "{}:{}",
            dir.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        // SAFETY: set once, before any fleet_dir call; this process is a
        // test binary that exits afterwards, so nothing else observes it.
        unsafe {
            std::env::set_var("XDG_STATE_HOME", &dir);
            std::env::set_var("PATH", path);
        }
        dir
    })
}
