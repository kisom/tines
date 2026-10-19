//! Standalone `tines` service binary.
//!
//! Loads a JSON config, builds a `Node<LogStore, BtTransport>` (BLE over
//! `bluer`), and serves the IPC ([`tines::serve`]) over a Unix-domain socket.
//! One instance per device — the typical deployment on robots without ROS2.
//!
//! The node/peer ids are 32 lowercase hex chars (the 16-byte `NodeId`), and
//! The tines sequence numbers are seeded from the LogStore's boot counter, so
//! they persist across reboots without a separate config field.

use std::error::Error;
use std::path::Path;

use tines::{serve, HostClock};
use tines_bt::{BluerRadio, BtTransport};
use tines_core::{Node, NodeId};
use tines_store::LogStore;

/// On-disk configuration for a running service.
#[derive(serde::Deserialize)]
struct Config {
    /// This device's advertised `NodeId`, 32 lowercase hex chars.
    node_id: String,
    /// The peer's advertised `NodeId`, 32 lowercase hex chars.
    peer: String,
    /// Absolute path to the LogStore append-only log.
    log_path: String,
    /// Unix-domain socket path the IPC listens on.
    socket_path: String,
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cfg_path = std::env::args()
        .nth(1)
        .ok_or("usage: tines <config.json>")?;
    let config: Config = serde_json::from_str(&std::fs::read_to_string(cfg_path)?)?;

    let me = parse_node_id(&config.node_id)?;
    let peer = parse_node_id(&config.peer)?;

    // LogStore is the Store (S); BtTransport wraps it as the Transport (T).
    let clock = HostClock::start();
    let store = LogStore::open(Path::new(&config.log_path), clock.now())
        .map_err(|e| format!("failed to open log store: {e:?}"))?;
    let radio = BluerRadio::new(me, peer);
    let transport = BtTransport::new(me, peer, radio);
    let node = Node::new(me, store.first_seq(), store, transport);

    println!(
        "tines: listening on {} (node {})",
        config.socket_path,
        node_hex(me)
    );
    serve(node, Path::new(&config.socket_path), clock)?;
    Ok(())
}

fn parse_node_id(s: &str) -> Result<NodeId, Box<dyn Error + Send + Sync>> {
    if s.len() != 32 {
        return Err(format!("node id must be 32 hex chars, got {}", s.len()).into());
    }
    let mut id = [0u8; 16];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = hex_val(chunk[0])?;
        let lo = hex_val(chunk[1])?;
        id[i] = (hi << 4) | lo;
    }
    Ok(id)
}

fn hex_val(c: u8) -> Result<u8, Box<dyn Error + Send + Sync>> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        other => Err(format!("invalid hex char: {other:02x}").into()),
    }
}

fn node_hex(id: NodeId) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(32);
    for b in id {
        let _ = write!(s, "{b:02x}");
    }
    s
}
