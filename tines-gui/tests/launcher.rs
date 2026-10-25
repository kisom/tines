//! Launcher UI tests (egui_kittest, headless). No real docker or daemon:
//! the fake `tines-busd` is seeded on PATH (see `common`), and the
//! containers are injected directly into the launcher's state.

mod common;

use egui::accesskit;
use egui_kittest::Harness;
use egui_kittest::kittest::Queryable;
use tines_gui::config::fleet_dir;
use tines_gui::docker::ContainerId;
use tines_gui::fleet::Mode;
use tines_gui::launcher::{ContainerEntry, Launcher, LauncherAction};

/// The test-side action drain: peek at what the UI queued (main.rs owns the
/// real drain with the subprocess spawns).
fn drain(launcher: &mut Launcher) -> Vec<LauncherAction> {
    launcher.actions.drain(..).collect()
}

#[test]
fn create_fleet_action_queues_spawn() {
    common::env_with_fake_busd();
    let mut harness = Harness::new_eframe(|_cc| Launcher::new());
    harness.run();

    // Type the fleet name into the first text field, then click Create.
    harness
        .get_all_by_role(accesskit::Role::TextInput)
        .next()
        .expect("fleet name field")
        .focus();
    harness.run();
    harness
        .get_all_by_role(accesskit::Role::TextInput)
        .next()
        .expect("fleet name field")
        .type_text("yard");
    harness.run();
    harness.get_by_label("Create").click();
    harness.run();

    let launcher = harness.state();
    assert!(
        launcher.fleet.is_some(),
        "Create must set the fleet (spawning the bus daemon)"
    );
    // The action queue must be drainable; fleet creation itself is not an
    // action (it is the fleet field).
    let launcher = harness.state_mut();
    assert!(drain(launcher).is_empty());
}

#[test]
fn open_window_queues_spawn() {
    common::env_with_fake_busd();
    let mut harness = Harness::new_eframe(|_cc| {
        let mut launcher = Launcher::new();
        launcher.fleet_name = "yard".to_string();
        launcher.containers.push(ContainerEntry {
            name: "alpha".to_string(),
            id: ContainerId("deadbeef".to_string()),
            mode: Mode::Sim,
        });
        launcher
    });
    harness.run();

    harness.get_by_label("Open").click();
    harness.run();

    let actions = drain(harness.state_mut());
    assert_eq!(
        actions,
        vec![LauncherAction::OpenWindow {
            name: "alpha".to_string(),
            dir: fleet_dir("yard").join("alpha"),
            id: ContainerId("deadbeef".to_string()),
            mode: Mode::Sim,
        }]
    );
}

#[test]
fn stop_queues_stop_action() {
    common::env_with_fake_busd();
    let mut harness = Harness::new_eframe(|_cc| {
        let mut launcher = Launcher::new();
        launcher.containers.push(ContainerEntry {
            name: "bravo".to_string(),
            id: ContainerId("cafebabe".to_string()),
            mode: Mode::Bt,
        });
        launcher
    });
    harness.run();

    harness.get_by_label("Stop").click();
    harness.run();

    let actions = drain(harness.state_mut());
    assert_eq!(
        actions,
        vec![LauncherAction::StopContainer(ContainerId(
            "cafebabe".to_string()
        ))]
    );
}
