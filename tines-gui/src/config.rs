//! Fleet/node layout on disk and the service configs the launcher writes.
//!
//! The launcher owns per-fleet state under
//! `$XDG_STATE_HOME|~/.local/state/tines-console/`, and each node's
//! `config.json` is exactly the service's [`tines::config::Config`] schema —
//! the service rejects unknown keys, so the generated config must match it
//! byte-for-byte (pinned by `tests/config.rs`).

use std::fs::{self, Permissions};
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

/// The per-fleet state dir, created `0700` on first use:
/// `$XDG_STATE_HOME|~/.local/state` + `tines-console/fleets/<fleet>`.
pub fn fleet_dir(fleet: &str) -> PathBuf {
    let base = state_home();
    let dir = base.join("tines-console").join("fleets").join(fleet);
    if let Err(e) = fs::create_dir_all(&dir) {
        eprintln!("tines-gui: cannot create fleet dir {}: {e}", dir.display());
    }
    // create_dir_all honors the umask; the fleets carry node sockets and
    // logs, so they are private by design.
    if let Err(e) = fs::set_permissions(&dir, Permissions::from_mode(0o700)) {
        eprintln!("tines-gui: cannot chmod fleet dir {}: {e}", dir.display());
    }
    dir
}

/// A node's dir inside a fleet: `<fleet_dir>/<node>`. Not created here —
/// [`NodeConfig::write`] creates it when a config lands.
pub fn node_dir(fleet: &str, node: &str) -> PathBuf {
    fleet_dir(fleet).join(node)
}

/// A fresh random node id: 32 lowercase hex chars from 16 bytes of
/// `/dev/urandom`.
pub fn generate_node_id() -> String {
    let mut buf = [0u8; 16];
    let mut f =
        std::fs::File::open("/dev/urandom").expect("/dev/urandom is available on this platform");
    f.read_exact(&mut buf)
        .expect("reading 16 bytes from /dev/urandom");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// A node's service config, serializing to exactly the service schema.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeConfig {
    /// This node's id (32 lowercase hex chars).
    pub node_id: String,
    /// bt: the peer; bus: the allowlist of one, `None` for any peer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer: Option<String>,
    /// Absolute path to the LogStore log inside the container.
    pub log_path: String,
    /// Absolute path to the IPC socket inside the container.
    pub socket_path: String,
    /// `"bt"` or `"bus"`.
    pub transport: String,
    /// bus only: the bus daemon's socket inside the container.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bus_socket: Option<String>,
    /// `"system"` when the host clock is trustworthy (the container's is).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trusted_clock: Option<String>,
}

impl NodeConfig {
    /// A bt-mode config: one peer over the BLE transport.
    pub fn bt(
        node_id: String,
        peer: Option<String>,
        log_path: String,
        socket_path: String,
    ) -> Self {
        NodeConfig {
            node_id,
            peer,
            log_path,
            socket_path,
            transport: "bt".into(),
            bus_socket: None,
            trusted_clock: Some("system".into()),
        }
    }

    /// A bus-mode config: peers are every node on the bus daemon (or the
    /// allowlisted one).
    pub fn bus(
        node_id: String,
        peer: Option<String>,
        log_path: String,
        socket_path: String,
        bus_socket: String,
    ) -> Self {
        NodeConfig {
            node_id,
            peer,
            log_path,
            socket_path,
            transport: "bus".into(),
            bus_socket: Some(bus_socket),
            trusted_clock: Some("system".into()),
        }
    }

    /// Write this config to `<node_dir(fleet, node)>/config.json` (creating
    /// the node dir, `0700`) and return the path.
    pub fn write(&self, fleet: &str, node: &str) -> io::Result<PathBuf> {
        let dir = node_dir(fleet, node);
        fs::create_dir_all(&dir)?;
        fs::set_permissions(&dir, Permissions::from_mode(0o700))?;
        let path = dir.join("config.json");
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(&path, json)?;
        Ok(path)
    }
}

/// `$XDG_STATE_HOME` when set to an absolute path, else `~/.local/state`.
fn state_home() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME") {
        let p = PathBuf::from(dir);
        if p.is_absolute() {
            return p;
        }
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(".local/state")
}
