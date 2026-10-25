//! The launcher window: fleet creation, node/container controls, and
//! per-node window spawns.
//!
//! [`Launcher::update`] renders and queues intents — it never spawns a
//! process (except the one-time bus daemon inside `Fleet::create`, which is
//! a non-blocking `spawn`). Everything else drains through
//! [`Launcher::drain_actions`], called by `main.rs` after each frame; the
//! headless tests drive `update` and assert on the queued actions.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::process::Child;

use egui::Context;

use crate::config::fleet_dir;
use crate::docker::{ContainerId, Docker};
use crate::fleet::{Fleet, Mode};

/// One node container the launcher knows about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerEntry {
    /// The node name (also the dir and container-name stem).
    pub name: String,
    /// The container id `docker run -d` returned.
    pub id: ContainerId,
    /// How the node connects to its peers.
    pub mode: Mode,
}

/// An intent the UI queued; `main.rs` executes these by draining the queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LauncherAction {
    /// Add the node to the fleet and start its container.
    StartContainer {
        /// The node name.
        name: String,
        /// How the node connects to its peers.
        mode: Mode,
    },
    /// Stop a running container.
    StopContainer(ContainerId),
    /// Remove a stopped container.
    RemoveContainer(ContainerId),
    /// Spawn a dedicated window process for this node.
    OpenWindow {
        /// The node name.
        name: String,
        /// The node's config dir (the `--node` arg).
        dir: PathBuf,
        /// The container the window controls.
        id: ContainerId,
        /// How the node connects to its peers.
        mode: Mode,
    },
    /// Build the runtime image.
    BuildImage,
}

/// The launcher window's full state.
pub struct Launcher {
    /// The created fleet; `None` until Create succeeds.
    pub fleet: Option<Fleet>,
    /// The docker frontend.
    pub docker: Docker,
    /// Known node containers (only successful starts land here).
    pub containers: Vec<ContainerEntry>,
    /// Spawned node-window processes.
    pub children: Vec<Child>,
    /// Intents queued by [`Launcher::update`], drained by `main.rs`.
    pub actions: VecDeque<LauncherAction>,

    /// The fleet-name field.
    pub fleet_name: String,
    /// The node-name field for Add.
    pub node_name: String,
    /// The bt peer field (bt mode only; the pinned `StartContainer` action
    /// carries no peer, so it is read from here at drain time).
    pub node_peer: String,
    /// The mode picker.
    pub mode: Mode,
    /// The selected row's name (drives the log pane).
    pub selected: Option<String>,
    /// The selected container's last-fetched log tail.
    pub log_tail: Option<String>,
    /// The latest error to surface.
    pub error: Option<String>,

    /// `docker inspect` results, refreshed at most once per second.
    running: HashMap<ContainerId, bool>,
    /// When the running map was last refreshed.
    last_running_check: f64,
}

impl Default for Launcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Launcher {
    /// An empty launcher: no fleet, no containers, sim mode.
    pub fn new() -> Launcher {
        Launcher {
            fleet: None,
            docker: Docker,
            containers: Vec::new(),
            children: Vec::new(),
            actions: VecDeque::new(),
            fleet_name: String::new(),
            node_name: String::new(),
            node_peer: String::new(),
            mode: Mode::Sim,
            selected: None,
            log_tail: None,
            error: None,
            running: HashMap::new(),
            last_running_check: f64::NEG_INFINITY,
        }
    }

    /// Render the launcher and queue intents. Side effects are limited to
    /// the fleet creation itself (a non-blocking daemon spawn) and one
    /// `docker logs` fetch when a row is selected.
    pub fn update(&mut self, ctx: &Context) {
        let now = ctx.input(|i| i.time);
        if now - self.last_running_check > 1.0 {
            self.last_running_check = now;
            self.running = self
                .containers
                .iter()
                .map(|c| (c.id.clone(), self.docker.is_running(&c.id)))
                .collect();
        }

        // egui 0.36 is Ui-based: rebuild the root Ui the eframe App would
        // have passed, so the panels have a parent.
        let mut ui = egui::Ui::new(
            ctx.clone(),
            egui::Id::new("tines-gui-root"),
            egui::UiBuilder::new().max_rect(ctx.content_rect()),
        );

        egui::Panel::top("controls").show(&mut ui, |ui| {
            ui.horizontal(|ui| {
                ui.label("Mode:");
                ui.selectable_value(&mut self.mode, Mode::Bt, "Bt");
                ui.selectable_value(&mut self.mode, Mode::Sim, "Sim");
            });
            ui.horizontal(|ui| {
                ui.label("Fleet name:");
                ui.text_edit_singleline(&mut self.fleet_name);
                if ui.button("Create").clicked() {
                    match Fleet::create(self.fleet_name.trim()) {
                        Ok(fleet) => {
                            self.fleet = Some(fleet);
                            self.error = None;
                        }
                        Err(e) => self.error = Some(format!("cannot create fleet: {e}")),
                    }
                }
            });
            if self.mode == Mode::Bt {
                ui.horizontal(|ui| {
                    ui.label("Peer (bt):");
                    ui.text_edit_singleline(&mut self.node_peer);
                });
            }
            ui.horizontal(|ui| {
                ui.label("Node name:");
                ui.text_edit_singleline(&mut self.node_name);
                if ui.button("Add").clicked() {
                    self.actions.push_back(LauncherAction::StartContainer {
                        name: self.node_name.trim().to_string(),
                        mode: self.mode,
                    });
                }
            });
            if ui.button("Build image").clicked() {
                self.actions.push_back(LauncherAction::BuildImage);
            }
            if let Some(error) = &self.error {
                ui.colored_label(egui::Color32::from_rgb(0xff, 0x55, 0x55), error);
            }
        });

        egui::CentralPanel::default().show(&mut ui, |ui| {
            egui::Grid::new("nodes")
                .striped(true)
                .min_col_width(80.0)
                .show(ui, |ui| {
                    ui.label("node");
                    ui.label("mode");
                    ui.label("state");
                    ui.label("");
                    ui.end_row();
                    for entry in &self.containers {
                        let running = self.running.get(&entry.id).copied().unwrap_or(false);
                        let name = entry.name.clone();
                        if ui
                            .selectable_label(self.selected.as_deref() == Some(&name), &entry.name)
                            .clicked()
                        {
                            self.selected = Some(name.clone());
                            self.log_tail = self.docker.logs(&entry.id, 50).ok();
                        }
                        ui.label(match entry.mode {
                            Mode::Bt => "bt",
                            Mode::Sim => "sim",
                        });
                        ui.label(if running { "running" } else { "stopped" });
                        ui.horizontal(|ui| {
                            if ui.button("Open").clicked() {
                                self.actions.push_back(LauncherAction::OpenWindow {
                                    name: name.clone(),
                                    dir: fleet_dir(self.fleet_name.trim()).join(&name),
                                    id: entry.id.clone(),
                                    mode: entry.mode,
                                });
                            }
                            if ui.button("Stop").clicked() {
                                self.actions
                                    .push_back(LauncherAction::StopContainer(entry.id.clone()));
                            }
                            if ui.button("Remove").clicked() {
                                self.actions
                                    .push_back(LauncherAction::RemoveContainer(entry.id.clone()));
                            }
                        });
                        ui.end_row();
                    }
                });
        });

        egui::Panel::bottom("logs").show(&mut ui, |ui| {
            ui.heading(match &self.selected {
                Some(name) => format!("logs — {name}"),
                None => "logs — select a container".to_string(),
            });
            if let Some(tail) = &self.log_tail {
                egui::ScrollArea::vertical()
                    .max_height(120.0)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.monospace(tail);
                    });
            }
        });
    }

    /// Execute every queued intent. This is the only place containers are
    /// started/stopped/removed and node windows spawned; `main.rs` calls it
    /// after each frame.
    pub fn drain_actions(&mut self) {
        while let Some(action) = self.actions.pop_front() {
            match action {
                LauncherAction::StartContainer { name, mode } => {
                    let Some(fleet) = self.fleet.as_ref() else {
                        self.error = Some("create a fleet first".to_string());
                        continue;
                    };
                    let peer = (mode == Mode::Bt && !self.node_peer.trim().is_empty())
                        .then(|| self.node_peer.trim().to_string());
                    if let Err(e) = fleet.add_node(&name, None, mode, peer.as_deref()) {
                        self.error = Some(format!("cannot add node {name}: {e}"));
                        continue;
                    }
                    let started = match mode {
                        Mode::Bt => self.docker.start_bt(&name, self.fleet_name.trim()),
                        Mode::Sim => self.docker.start_sim(&name, self.fleet_name.trim()),
                    };
                    match started {
                        Ok(id) => self.containers.push(ContainerEntry { name, id, mode }),
                        Err(e) => self.error = Some(format!("cannot start {name}: {e}")),
                    }
                }
                LauncherAction::StopContainer(id) => {
                    if let Err(e) = self.docker.stop(&id) {
                        self.error = Some(format!("cannot stop {}: {e}", id.0));
                    }
                }
                LauncherAction::RemoveContainer(id) => {
                    if let Err(e) = self.docker.remove(&id) {
                        self.error = Some(format!("cannot remove {}: {e}", id.0));
                    }
                    self.containers.retain(|c| c.id != id);
                    if self.selected.as_ref() == Some(&id.0) {
                        self.selected = None;
                        self.log_tail = None;
                    }
                }
                LauncherAction::OpenWindow {
                    name,
                    dir,
                    id,
                    mode,
                } => {
                    let mode_arg = match mode {
                        Mode::Bt => "bt",
                        Mode::Sim => "sim",
                    };
                    match std::env::current_exe().and_then(|exe| {
                        std::process::Command::new(exe)
                            .arg("--node")
                            .arg(&dir)
                            .arg(&id.0)
                            .arg(mode_arg)
                            .spawn()
                    }) {
                        Ok(child) => self.children.push(child),
                        Err(e) => {
                            self.error = Some(format!("cannot open window for {name}: {e}"));
                        }
                    }
                }
                LauncherAction::BuildImage => {
                    if let Err(e) = self.docker.build_image() {
                        self.error = Some(format!("image build failed: {e}"));
                    }
                }
            }
        }
    }
}

impl eframe::App for Launcher {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        Launcher::update(self, ui.ctx());
    }
}
