//! On-disk service configuration, shared by the `tines` binary and its
//! clients (the fleet console generates these configs and drives the service
//! it configures).
//!
//! The config is JSON with `deny_unknown_fields`; every key except the core
//! `node_id` / `log_path` / `socket_path` is optional. See [`Config`] for the
//! per-key contract, and [`Config::validate`] for the transport constraints.

use crate::trust::DEFAULT_PEER_TIME_WINDOW_MS;

/// On-disk configuration for a running service. Unknown keys are rejected.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// This device's advertised `NodeId`, 32 lowercase hex chars.
    pub node_id: String,
    /// The peer's advertised `NodeId`, 32 lowercase hex chars. Required for
    /// the bt transport (its single peer); for the bus transport it is the
    /// allowlist of one — `None` means any bus peer.
    pub peer: Option<String>,
    /// Absolute path to the LogStore append-only log.
    pub log_path: String,
    /// Unix-domain socket path the IPC listens on.
    pub socket_path: String,
    /// Exchange poll interval in milliseconds. The real default belongs to the
    /// builder; 20 ms keeps the link responsive without burning idle cycles.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Trust the system clock for the host's wall time, instead of deriving it
    /// from a peer's observation. Set to `"system"` when the machine's clock is
    /// reliable — an RTC, or a GPS/NTP-disciplined clock — the operator asserts
    /// it rather than tines detecting it. Left unset, tines falls back to a
    /// peer observation, and, with none available, stays quarantined. Any
    /// other value is a config error.
    #[serde(default)]
    pub trusted_clock: Option<TrustedClock>,
    /// How long, from the first plausible peer estimate, to keep collecting
    /// estimates before adopting the latest (spec decision J).
    #[serde(default = "default_peer_time_window_ms")]
    pub peer_time_window_ms: u64,
    /// The `unknown_downtime_age_ms` broadcast floor passed to LogStore while
    /// the wall clock is unknown (the fold's unknown-downtime branch). Optional;
    /// leave the LogStore default (`24h`) when unset.
    ///
    /// The floor must stay below the host's trim `max_age_ms`. That bound waits
    /// for host hardening — the trim defaults are still empty — so this check is
    /// not validated until then.
    #[serde(default)]
    pub unknown_downtime_age_ms: Option<u64>,
    /// Which transport the service runs: `bt` (default) or `bus`.
    #[serde(default)]
    pub transport: Option<TransportKind>,
    /// Absolute path to the bus daemon's Unix-domain socket. Required when
    /// `transport` is `bus`.
    #[serde(default)]
    pub bus_socket: Option<String>,
}

impl Config {
    /// Check the transport constraints. Called by `main` before anything
    /// else touches the config.
    ///
    /// - `bt` (the default) requires `peer`.
    /// - `bus` requires `bus_socket`; `peer` is the allowlist of one, and
    ///   `None` means any peer.
    pub fn validate(&self) -> Result<(), String> {
        match self.transport.unwrap_or(TransportKind::Bt) {
            TransportKind::Bt if self.peer.is_none() => Err("transport bt requires peer".into()),
            TransportKind::Bus if self.bus_socket.is_none() => {
                Err("transport bus requires bus_socket".into())
            }
            _ => Ok(()),
        }
    }
}

/// The accepted values of `trusted_clock`.
#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrustedClock {
    /// Trust the system clock (`"system"`).
    System,
}

/// The transport the service runs over.
#[derive(Debug, PartialEq, Eq, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportKind {
    /// The BLE transport (the default), with the single configured peer.
    Bt,
    /// The host bus transport, peers = every node on the bus daemon (or the
    /// allowlisted one).
    Bus,
}

fn default_poll_interval_ms() -> u64 {
    20
}

fn default_peer_time_window_ms() -> u64 {
    DEFAULT_PEER_TIME_WINDOW_MS
}

#[cfg(test)]
mod tests {
    use super::{Config, TransportKind, TrustedClock};

    const BASE: &str = r#""node_id": "00000000000000000000000000000001",
        "peer": "00000000000000000000000000000002",
        "log_path": "/var/lib/tines/log",
        "socket_path": "/run/tines.sock""#;

    fn parse(extra: &str) -> Result<Config, serde_json::Error> {
        serde_json::from_str(&format!("{{{BASE}{extra}}}"))
    }

    #[test]
    fn defaults() {
        let c = parse("").expect("minimal config");
        assert_eq!(c.poll_interval_ms, 20);
        assert_eq!(c.peer_time_window_ms, 10_000);
        assert_eq!(c.trusted_clock, None);
        assert_eq!(c.unknown_downtime_age_ms, None);
    }

    #[test]
    fn trusted_clock_system_is_accepted() {
        let c = parse(r#", "trusted_clock": "system""#).expect("system");
        assert_eq!(c.trusted_clock, Some(TrustedClock::System));
    }

    #[test]
    fn trusted_clock_unknown_value_is_rejected() {
        assert!(parse(r#", "trusted_clock": "System""#).is_err());
        assert!(parse(r#", "trusted_clock": "rtc""#).is_err());
        assert!(parse(r#", "trusted_clock": "systme""#).is_err());
    }

    #[test]
    fn unknown_field_is_rejected() {
        assert!(parse(r#", "trusted_clok": "system""#).is_err());
    }

    #[test]
    fn peer_time_window_is_configurable() {
        let c = parse(r#", "peer_time_window_ms": 2500"#).expect("window");
        assert_eq!(c.peer_time_window_ms, 2_500);
    }

    const BUS_BASE: &str = r#""node_id": "00000000000000000000000000000001",
        "log_path": "/var/lib/tines/log",
        "socket_path": "/run/tines.sock""#;

    fn parse_bus(extra: &str) -> Result<Config, serde_json::Error> {
        serde_json::from_str(&format!("{{{BUS_BASE}{extra}}}"))
    }

    #[test]
    fn bus_config_parses() {
        let c = parse_bus(r#", "transport": "bus", "bus_socket": "/run/tines-bus.sock""#)
            .expect("bus config");
        assert_eq!(c.transport, Some(TransportKind::Bus));
        assert_eq!(c.bus_socket.as_deref(), Some("/run/tines-bus.sock"));
        assert_eq!(c.peer, None);
        assert_eq!(c.validate(), Ok(()));
    }

    #[test]
    fn bt_without_peer_is_invalid() {
        let c = parse_bus("").expect("bt config without peer parses");
        assert_eq!(c.validate(), Err("transport bt requires peer".to_string()));
    }

    #[test]
    fn bus_without_socket_is_invalid() {
        let c = parse_bus(r#", "transport": "bus""#).expect("parses");
        assert_eq!(
            c.validate(),
            Err("transport bus requires bus_socket".to_string())
        );
    }

    #[test]
    fn unknown_transport_is_rejected() {
        assert!(parse_bus(r#", "transport": "wifi""#).is_err());
    }

    #[test]
    fn transport_defaults_to_bt() {
        let c = parse("").expect("minimal config");
        assert_eq!(c.transport, None);
        assert_eq!(c.validate(), Ok(()));
    }
}
