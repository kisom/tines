//! Fleet layout and node-config tests. The bus daemon is a fake `tines-busd`
//! seeded onto PATH (the real binary isn't on PATH for unit tests): it
//! records its argv and creates the socket file, so the fleet's spawn
//! protocol is what's under test — the real daemon is covered by tines-bus's
//! own suite and Task 4's e2e.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tines_gui::fleet::{Fleet, Mode};

/// One shared scratch setup for every test in this binary: a temp dir that
/// becomes both XDG_STATE_HOME (fleet dirs) and a PATH dir holding the fake
/// `tines-busd`. Env is process-global, so it is set exactly once before any
/// test touches fleet_dir.
fn setup() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("tines-gui-fleet-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Fake tines-busd: touch the socket named by argv[1], then stay alive.
        let fake = dir.join("tines-busd");
        std::fs::write(&fake, "#!/bin/sh\ntouch \"$1\"\nsleep 60\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
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

fn fleet_name(tag: &str) -> String {
    format!(
        "test-{tag}-{}",
        std::thread::current().name().unwrap_or("t")
    )
}

#[test]
fn fleet_dir_layout() {
    setup();
    let fleet = Fleet::create(&fleet_name("layout")).expect("fleet creates");
    let dir = tines_gui::config::fleet_dir(&fleet_name("layout"));
    let meta = std::fs::metadata(&dir).expect("fleet dir exists");
    assert!(meta.is_dir());
    assert_eq!(meta.permissions().mode() & 0o777, 0o700);

    // The fake daemon creates the bus socket the fleet asked it to bind.
    let socket = fleet.bus_socket();
    assert_eq!(socket, dir.join("bus.sock"));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() {
        assert!(
            Instant::now() < deadline,
            "bus.sock never appeared: {}",
            socket.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn add_node_bus_config_mounts_data() {
    setup();
    let fleet_name = fleet_name("bus");
    let fleet = Fleet::create(&fleet_name).expect("fleet creates");
    let spec = fleet
        .add_node("alpha", Some(&"11".repeat(16)), Mode::Sim, None)
        .expect("node added");

    let json = std::fs::read_to_string(spec.dir.join("config.json")).unwrap();
    let parsed: tines::config::Config =
        serde_json::from_str(&json).expect("config parses as the service config");
    assert_eq!(parsed.transport, Some(tines::config::TransportKind::Bus));
    assert_eq!(parsed.bus_socket, Some("/data/bus.sock".to_string()));
    assert_eq!(parsed.socket_path, "/data/alpha/sock".to_string());
    assert_eq!(parsed.log_path, "/data/alpha/log".to_string());
    assert_eq!(parsed.node_id, "11".repeat(16));
    assert_eq!(parsed.validate(), Ok(()));
}

#[test]
fn add_node_bt_config_has_peer() {
    setup();
    let fleet_name = fleet_name("bt");
    let fleet = Fleet::create(&fleet_name).expect("fleet creates");
    let peer = "22".repeat(16);
    let spec = fleet
        .add_node("bravo", None, Mode::Bt, Some(&peer))
        .expect("node added");

    let json = std::fs::read_to_string(spec.dir.join("config.json")).unwrap();
    let parsed: tines::config::Config =
        serde_json::from_str(&json).expect("config parses as the service config");
    assert_eq!(parsed.transport, Some(tines::config::TransportKind::Bt));
    assert_eq!(parsed.peer, Some(peer));
    assert_eq!(parsed.bus_socket, None);
    assert_eq!(parsed.validate(), Ok(()));
}

#[test]
fn generated_id_roundtrips() {
    setup();
    let fleet_name = fleet_name("gen");
    let fleet = Fleet::create(&fleet_name).expect("fleet creates");
    let spec = fleet
        .add_node("charlie", None, Mode::Sim, None)
        .expect("node added");

    let json = std::fs::read_to_string(spec.dir.join("config.json")).unwrap();
    let parsed: tines::config::Config =
        serde_json::from_str(&json).expect("config parses as the service config");
    assert_eq!(parsed.node_id.len(), 32);
    assert!(
        parsed
            .node_id
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "generated id must be lowercase hex"
    );
}
