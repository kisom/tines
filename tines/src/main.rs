//! Standalone `tines` service binary.
//!
//! Loads a JSON config, builds a `Node<LogStore, BtTransport>` (BLE over
//! `bluer`), and serves the IPC ([`tines::serve`]) over a Unix-domain socket.
//! One instance per device — the typical deployment on robots without ROS2.
//!
//! The node and peer ids are 32 lowercase hex chars (the 16-byte `NodeId`).
//! Sequence numbers are seeded from the LogStore's boot counter, so they don't
//! repeat across reboots and need no config field.

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
    /// Exchange poll interval in milliseconds. The real default belongs to the
    /// builder; 20 ms keeps the link responsive without burning idle cycles.
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    /// Trust the system clock for the host's wall time, instead of deriving it
    /// from a peer's observation. Set to `"system"` when the machine's clock is
    /// reliable — an RTC, or a GPS/NTP-disciplined clock — the operator asserts
    /// it rather than tines detecting it. Left unset, tines falls back to a
    /// peer observation, and, with none available, stays quarantined.
    #[serde(default)]
    trusted_clock: Option<String>,
    /// The `unknown_downtime_age_ms` broadcast floor passed to LogStore while
    /// the wall clock is unknown (the fold's unknown-downtime branch). Optional;
    /// leave the LogStore default (`24h`) when unset.
    ///
    /// The floor must stay below the host's trim `max_age_ms`. That bound waits
    /// for host hardening — the trim defaults are still empty — so this check is
    /// not validated until then.
    #[serde(default)]
    unknown_downtime_age_ms: Option<u64>,
}

fn default_poll_interval_ms() -> u64 {
    20
}

/// The current system time, since the Unix epoch, in milliseconds — the source
/// of the wall clock when `trusted_clock = "system"`. Returns 0 if the system
/// clock is unset (never, on any real machine).
fn system_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cfg_path = std::env::args()
        .nth(1)
        .ok_or("usage: tines <config.json>")?;
    let config: Config = serde_json::from_str(&std::fs::read_to_string(cfg_path)?)?;

    let me = parse_node_id(&config.node_id)?;
    let peer = parse_node_id(&config.peer)?;

    // LogStore is the Store (S); BtTransport wraps it as the Transport (T).
    // unknown_downtime_age_ms (the broadcast floor while the wall clock is
    // unknown) is optional; leave the LogStore default when unset.
    let builder = match config.unknown_downtime_age_ms {
        Some(ms) => LogStore::builder(Path::new(&config.log_path)).set_unknown_downtime_age_ms(ms),
        None => LogStore::builder(Path::new(&config.log_path)),
    };
    let mut clock = HostClock::start();
    let mut store = builder
        .open(clock.now())
        .map_err(|e| format!("failed to open log store: {e:?}"))?;

    // Trusted wall clock, in order: (1) a system clock the operator trusts
    // (`trusted_clock = "system"`), (2) a peer's observation handled by the
    // exchange's poll loop, or (3) stay quarantined until one is observed. This
    // is the system-clock branch: anchor the wall once to the current system
    // (Unix) time, so every observation then carries an observed_at.
    if config.trusted_clock.as_deref() == Some("system") {
        let wall_now = system_time_ms();
        let now = clock.now();
        let report = store.set_wall_clock(wall_now, now);
        clock.anchor_wall(wall_now, now);
        match report.downtime_ms {
            Some(ms) => println!("tines: trusted system clock, folded {ms} ms downtime"),
            None => {
                println!("tines: trusted system clock, downtime unknown, held data quarantined");
            }
        }
    }
    let radio = BluerRadio::new(me, peer);
    let transport = BtTransport::new(me, peer, radio);
    // `serve` runs the exchange inline (a poll loop fed by the socket read
    // timeouts) on this same thread, so observations and beliefs flow over the
    // transport, not just over IPC. The node isn't `Send` (the BLE transport
    // carries a `dyn Radio` with no `Send` bound), so it never crosses a thread.
    let node = Node::new(me, store.first_seq(), store, transport);

    println!(
        "tines: listening on {} (node {}, exchange poll {} ms)",
        config.socket_path,
        node_hex(me),
        config.poll_interval_ms,
    );
    serve(
        node,
        Path::new(&config.socket_path),
        clock,
        config.poll_interval_ms,
    )?;
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
