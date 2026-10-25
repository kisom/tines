//! Standalone `tines` service binary.
//!
//! Loads a JSON config, builds a `Node<LogStore, HostTransport>` (BLE over
//! `bluer`, or the host bus — see [`tines::config::TransportKind`]), and
//! serves the IPC ([`tines::serve`]) over a Unix-domain socket.
//! One instance per device — the typical deployment on robots without ROS2.
//!
//! The node and peer ids are 32 lowercase hex chars (the 16-byte `NodeId`).
//! Sequence numbers are seeded from the LogStore's boot counter, so they don't
//! repeat across reboots and need no config field.
//!
//! # Config
//!
//! A JSON object; an unknown key is an error, so a typo can't silently fall
//! back to a default. The full contract lives in [`tines::config::Config`].
//!
//! | key | required | meaning |
//! |---|---|---|
//! | `node_id` | yes | this device's `NodeId`, 32 hex chars |
//! | `peer` | bt only | the peer's `NodeId`, 32 hex chars; on bus, the allowlist of one (`None` = any bus peer) |
//! | `log_path` | yes | the LogStore log file |
//! | `socket_path` | yes | the IPC Unix socket |
//! | `transport` | no (`bt`) | `"bt"` or `"bus"` |
//! | `bus_socket` | bus only | the bus daemon's Unix socket |
//! | `poll_interval_ms` | no (20) | exchange poll cadence |
//! | `trusted_clock` | no (unset) | `"system"` to trust the system clock; the only accepted value |
//! | `peer_time_window_ms` | no (10000) | how long to collect peer time estimates before adopting the latest |
//! | `unknown_downtime_age_ms` | no (LogStore default, 24 h) | broadcast age floor when downtime is unknown |
//!
//! # Trusted time
//!
//! With `trusted_clock = "system"` the service adopts the system clock at
//! startup — if it reads a plausible time (2026 to 2126, see
//! [`tines::trust::WALL_FLOOR_MS`]). A clock outside that range, such as a
//! board that booted before NTP synced and reads 1970, is logged loudly and
//! ignored; the service then takes its time from peers as if the option were
//! unset. Peer time is collected for `peer_time_window_ms` from the first
//! plausible estimate, and the latest is adopted, once per run.
//!
//! The system clock is read once, at startup. Under systemd, order the unit
//! after clock sync so the first read is already disciplined:
//!
//! ```ini
//! [Unit]
//! Wants=time-sync.target
//! After=time-sync.target
//! ```
//!
//! `time-sync.target` is only reached once the clock is synced when a sync
//! service supports it (`systemd-timesyncd` with
//! `systemd-time-wait-sync.service` enabled, or chrony's equivalent).

use std::error::Error;
use std::path::Path;

use tines::config::{Config, TransportKind, TrustedClock};
use tines::transport::HostTransport;
use tines::trust::{adopt_wall_clock, system_wall_ms};
use tines::{serve, HostClock};
use tines_bt::{BluerRadio, BtTransport};
use tines_bus::BusTransport;
use tines_core::{Node, NodeId};
use tines_store::LogStore;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cfg_path = std::env::args()
        .nth(1)
        .ok_or("usage: tines <config.json>")?;
    let config: Config = serde_json::from_str(&std::fs::read_to_string(cfg_path)?)
        .map_err(|e| format!("invalid config: {e}"))?;
    config
        .validate()
        .map_err(|e| format!("invalid config: {e}"))?;

    let me = parse_node_id(&config.node_id)?;

    // LogStore is the Store (S); HostTransport wraps BLE or the bus as the
    // Transport (T). unknown_downtime_age_ms (the broadcast floor while the
    // wall clock is unknown) is optional; leave the LogStore default when
    // unset. Peer estimates outside the plausible range don't count
    // (decision J).
    let builder = LogStore::builder(Path::new(&config.log_path))
        .plausible_wall_ms(tines::trust::PLAUSIBLE_WALL_MS);
    let builder = match config.unknown_downtime_age_ms {
        Some(ms) => builder.set_unknown_downtime_age_ms(ms),
        None => builder,
    };
    let mut clock = HostClock::start();
    let store = builder
        .open(clock.now())
        .map_err(|e| format!("failed to open log store: {e:?}"))?;

    // bt: the single configured peer (validation guarantees it). bus: the
    // configured peer is the allowlist of one; None means any bus peer.
    let transport = match config.transport.unwrap_or(TransportKind::Bt) {
        TransportKind::Bt => {
            let peer = parse_node_id(config.peer.as_deref().expect("validated"))?;
            let radio = BluerRadio::new(me, peer);
            HostTransport::Bt(BtTransport::new(me, peer, radio))
        }
        TransportKind::Bus => {
            let allowlist = match &config.peer {
                Some(p) => Some(parse_node_id(p)?),
                None => None,
            };
            HostTransport::Bus(BusTransport::new(
                me,
                allowlist,
                config.bus_socket.as_deref().expect("validated"),
            )?)
        }
    };
    // `serve` runs the exchange inline (a poll loop fed by the socket read
    // timeouts) on this same thread, so observations and beliefs flow over the
    // transport, not just over IPC. The node isn't `Send` (the BLE transport
    // carries a `dyn Radio` with no `Send` bound), so it never crosses a thread.
    let mut node = Node::new(me, store.first_seq(), store, transport);

    // Trusted wall clock, in order: (1) a system clock the operator trusts
    // (`trusted_clock = "system"`), (2) peers' observations, handled by
    // `serve`'s poll loop, or (3) stay quarantined until one is observed. This
    // is the system-clock branch: adopt the current system time once, so every
    // observation then carries an observed_at. An implausible system clock is
    // never adopted (review S6): log it and leave it to the peer path.
    if config.trusted_clock == Some(TrustedClock::System) {
        let now = clock.now();
        match system_wall_ms().and_then(|wall| adopt_wall_clock(&mut node, &mut clock, wall, now)) {
            Some(report) => match report.downtime_ms {
                Some(ms) => println!("tines: trusted system clock, folded {ms} ms downtime"),
                None => println!("tines: trusted system clock, downtime unknown"),
            },
            None => eprintln!(
                "tines: WARNING: trusted_clock = \"system\" but the system clock is not \
                 plausible (before 2026 or after 2126); NOT trusting it. Falling back to \
                 peer time; held data stays quarantined until then. Order the service \
                 after time-sync.target."
            ),
        }
    }

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
        config.peer_time_window_ms,
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
