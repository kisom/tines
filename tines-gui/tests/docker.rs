//! Command-building tests for the docker module: the exact argv vectors,
//! no real docker involved.

use tines_gui::config::fleet_dir;
use tines_gui::docker::{ContainerId, Docker};

#[test]
fn start_bt_args_match_pinned_flags() {
    let docker = Docker;
    let name = "scout";
    let fleet = "yard";
    let mounted = fleet_dir(fleet).join(name);
    let mounted = mounted.display().to_string();

    let argv = docker.start_bt_argv(name, fleet);
    let expected: Vec<String> = vec![
        "docker".into(),
        "run".into(),
        "-d".into(),
        "--name".into(),
        format!("tines-bt-{name}"),
        "--privileged".into(),
        "--net=host".into(),
        "-v".into(),
        "/run/dbus/system_bus_socket:/run/dbus/system_bus_socket".into(),
        "-v".into(),
        format!("{mounted}:/data"),
        "tines-stack:latest".into(),
        "tines".into(),
        "/data/config.json".into(),
    ];
    assert_eq!(argv, expected);
}

#[test]
fn start_sim_args_match_pinned_flags() {
    let docker = Docker;
    let name = "cargo";
    let fleet = "yard";
    let mounted = fleet_dir(fleet).join(name);
    let mounted = mounted.display().to_string();

    let argv = docker.start_sim_argv(name, fleet);
    let expected: Vec<String> = vec![
        "docker".into(),
        "run".into(),
        "-d".into(),
        "--name".into(),
        format!("tines-sim-{name}"),
        "-v".into(),
        format!("{mounted}:/data"),
        "tines-stack:latest".into(),
        "tines".into(),
        "/data/config.json".into(),
    ];
    assert_eq!(argv, expected);
}

#[test]
fn stop_and_remove_args() {
    let docker = Docker;
    let id = ContainerId("abc123".into());
    assert_eq!(docker.stop_argv(&id), vec!["docker", "stop", "abc123"]);
    assert_eq!(docker.remove_argv(&id), vec!["docker", "rm", "abc123"]);
    assert_eq!(
        docker.logs_argv(&id, 25),
        vec!["docker", "logs", "--tail", "25", "abc123"]
    );
}

#[test]
fn image_name_is_pinned() {
    assert_eq!(Docker::image(), "tines-stack:latest");
}
