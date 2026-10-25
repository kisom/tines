//! The GUI's generated configs must deserialize as the service's `Config`
//! byte-for-byte: unknown keys are fatal on the service side.

use tines_gui::config::{NodeConfig, generate_node_id};

#[test]
fn generated_config_parses_as_service_config() {
    // bt: transport bt, a peer, no bus socket.
    let bt = NodeConfig::bt(
        generate_node_id(),
        Some(generate_node_id()),
        "/tmp/fleet/log".into(),
        "/tmp/fleet/node.sock".into(),
    );
    let json = serde_json::to_string(&bt).unwrap();
    let parsed: tines::config::Config =
        serde_json::from_str(&json).expect("bt config must parse as the service config");
    assert_eq!(parsed.node_id, bt.node_id);
    assert_eq!(parsed.peer, bt.peer);
    assert_eq!(parsed.bus_socket, None);
    assert_eq!(parsed.validate(), Ok(()));

    // bus: transport bus, no peer, a bus socket.
    let bus = NodeConfig::bus(
        generate_node_id(),
        None,
        "/tmp/fleet/log".into(),
        "/tmp/fleet/node.sock".into(),
        "/tmp/fleet/bus.sock".into(),
    );
    let json = serde_json::to_string(&bus).unwrap();
    let parsed: tines::config::Config =
        serde_json::from_str(&json).expect("bus config must parse as the service config");
    assert_eq!(parsed.node_id, bus.node_id);
    assert_eq!(parsed.peer, None);
    assert_eq!(parsed.bus_socket, Some("/tmp/fleet/bus.sock".to_string()));
    assert_eq!(parsed.validate(), Ok(()));
}

#[test]
fn node_id_is_32_hex_chars() {
    let id = generate_node_id();
    assert_eq!(id.len(), 32, "node id must be 32 chars, got {id:?}");
    assert!(
        id.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "node id must be lowercase hex, got {id:?}"
    );
}

#[test]
fn fleet_dir_is_0700() {
    use std::os::unix::fs::PermissionsExt;

    // Point XDG_STATE_HOME at a scratch dir so the test never touches the
    // real ~/.local/state, and fleet_dir's creation is what we inspect.
    let scratch = std::env::temp_dir().join(format!("tines-gui-state-{}", std::process::id()));
    std::fs::remove_dir_all(&scratch).ok();
    std::fs::create_dir_all(&scratch).unwrap();
    // SAFETY: this is the only test in this binary touching fleet_dir, and
    // the test binary dies right after; the env change cannot leak to other
    // tests in other binaries.
    unsafe { std::env::set_var("XDG_STATE_HOME", &scratch) };

    let dir = tines_gui::config::fleet_dir("fleet-a");
    let meta = std::fs::metadata(&dir).expect("fleet dir must be created");
    assert!(meta.is_dir());
    assert_eq!(
        meta.permissions().mode() & 0o777,
        0o700,
        "fleet dir must be 0700"
    );

    std::fs::remove_dir_all(&scratch).ok();
}
