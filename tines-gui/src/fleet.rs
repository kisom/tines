//! A fleet: the on-disk layout plus the bus daemon that serves it.
//!
//! [`Fleet::create`] makes the fleet dir (`0700`, via
//! [`crate::config::fleet_dir`]) and spawns `tines-busd
//! <fleet_dir>/bus.sock` as a supervised child — the daemon is what lets
//! every sim-mode node on the fleet exchange frames over the host IPC bus.
//! Node configs land under `<fleet_dir>/<name>/config.json` and mount into
//! the containers at `/data`, so all container-side paths are `/data/...`.

use std::io;
use std::path::PathBuf;
use std::process::{Child, Command};

use crate::config::{NodeConfig, fleet_dir, generate_node_id};

/// How a node connects to its peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// A real Bluetooth radio (privileged container, host adapter).
    Bt,
    /// The host IPC bus (simulated fleet).
    Sim,
}

/// What `add_node` produced: the node's name and its config dir.
#[derive(Debug, Clone)]
pub struct NodeSpec {
    /// The node name (also the dir and container-name stem).
    pub name: String,
    /// `<fleet_dir>/<name>` — holds `config.json`.
    pub dir: PathBuf,
}

/// A live fleet with a supervised bus daemon.
pub struct Fleet {
    /// The fleet name.
    fleet: String,
    /// `<fleet_dir>`.
    dir: PathBuf,
    /// The daemon child; `None` after `stop_bus`.
    busd: Option<Child>,
}

impl Fleet {
    /// Create the fleet dir (`0700`) and spawn the bus daemon on
    /// `<fleet_dir>/bus.sock`. `tines-busd` must be on PATH.
    pub fn create(fleet: &str) -> io::Result<Fleet> {
        let dir = fleet_dir(fleet);
        let child = Command::new("tines-busd")
            .arg(dir.join("bus.sock"))
            .spawn()
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("cannot spawn tines-busd (is it on PATH?): {e}"),
                )
            })?;
        Ok(Fleet {
            fleet: fleet.to_string(),
            dir,
            busd: Some(child),
        })
    }

    /// The bus daemon's socket: `<fleet_dir>/bus.sock`.
    pub fn bus_socket(&self) -> PathBuf {
        self.dir.join("bus.sock")
    }

    /// Write a node's service config and return its spec. A missing
    /// `node_id` gets a fresh generated one. Bus mode points `bus_socket`
    /// at the in-container `/data/bus.sock` (the fleet dir is mounted at
    /// `/data`); log and IPC paths are `/data/<name>/log` and
    /// `/data/<name>/sock`.
    pub fn add_node(
        &self,
        name: &str,
        node_id: Option<&str>,
        mode: Mode,
        peer: Option<&str>,
    ) -> io::Result<NodeSpec> {
        let node_id = match node_id {
            Some(id) => id.to_string(),
            None => generate_node_id(),
        };
        let peer = peer.map(str::to_string);
        let log_path = format!("/data/{name}/log");
        let socket_path = format!("/data/{name}/sock");
        let config = match mode {
            Mode::Bt => NodeConfig::bt(node_id, peer, log_path, socket_path),
            Mode::Sim => NodeConfig::bus(
                node_id,
                peer,
                log_path,
                socket_path,
                "/data/bus.sock".into(),
            ),
        };
        config.write(&self.fleet, name)?;
        Ok(NodeSpec {
            name: name.to_string(),
            dir: self.dir.join(name),
        })
    }

    /// Stop the bus daemon.
    pub fn stop_bus(&mut self) {
        if let Some(mut child) = self.busd.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        self.stop_bus();
    }
}
