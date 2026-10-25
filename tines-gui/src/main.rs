//! tines-gui entry point: no args opens the launcher window;
//! `--node <dir> <container-id> <mode>` opens a node window.

use eframe::egui;
use tines_gui::docker::ContainerId;
use tines_gui::fleet::Mode;
use tines_gui::launcher::Launcher;
use tines_gui::window::{NodeAction, NodeWin};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    match args.as_slice() {
        [_, flag, dir, id, mode] if flag == "--node" => {
            let mode = match mode.as_str() {
                "bt" => Mode::Bt,
                "sim" => Mode::Sim,
                other => {
                    eprintln!("tines-gui: unknown mode {other:?} (want bt|sim)");
                    std::process::exit(2);
                }
            };
            run_node_window(dir.into(), ContainerId(id.clone()), mode)?;
        }
        [_] => run_launcher()?,
        _ => {
            eprintln!("usage: tines-gui [--node <dir> <container-id> <mode>]");
            std::process::exit(2);
        }
    }
    Ok(())
}

/// The launcher window: renders via `Launcher::update`, then drains the
/// action queue — the only place subprocess spawns happen.
struct LauncherApp {
    launcher: Launcher,
    theme: tines_gui::theme::OmarchyTheme,
    watching: bool,
}

impl eframe::App for LauncherApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.theme = tines_gui::theme::OmarchyTheme::load();
        self.theme.apply(&ctx);
        if !self.watching {
            let ctx = ctx.clone();
            if tines_gui::theme::OmarchyTheme::watch(move || ctx.request_repaint()).is_ok() {
                self.watching = true;
            }
        }
        self.launcher.update(&ctx);
        self.launcher.drain_actions();
    }
}

/// The node window: renders via `NodeWin::update`, then drains the
/// container actions (stop/remove/logs).
struct NodeWinApp {
    node: NodeWin,
    theme: tines_gui::theme::OmarchyTheme,
    watching: bool,
}

impl eframe::App for NodeWinApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.theme = tines_gui::theme::OmarchyTheme::load();
        self.theme.apply(&ctx);
        if !self.watching {
            let ctx = ctx.clone();
            if tines_gui::theme::OmarchyTheme::watch(move || ctx.request_repaint()).is_ok() {
                self.watching = true;
            }
        }
        self.node.update(&ctx);
        while let Some(action) = self.node.actions.pop_front() {
            match action {
                NodeAction::Stop => {
                    if let Err(e) = self.node.docker.stop(&self.node.container_id) {
                        self.node.error = Some(format!("cannot stop: {e}"));
                    }
                }
                NodeAction::Remove => {
                    if let Err(e) = self.node.docker.remove(&self.node.container_id) {
                        self.node.error = Some(format!("cannot remove: {e}"));
                    }
                }
                NodeAction::FetchLogs => match self.node.docker.logs(&self.node.container_id, 50) {
                    Ok(tail) => self.node.apply_log_tail(tail),
                    Err(e) => self.node.error = Some(format!("cannot fetch logs: {e}")),
                },
            }
        }
    }
}

fn options(title: &str, size: [f32; 2]) -> eframe::NativeOptions {
    let viewport = egui::ViewportBuilder::default()
        .with_app_id("tines-gui")
        .with_title(title)
        .with_inner_size(size);
    eframe::NativeOptions {
        viewport,
        glow_options: eframe::egui_glow::GlowConfiguration {
            vsync: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn run_launcher() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    eframe::run_native(
        "tines-gui",
        options("tines — fleet console", [760.0, 520.0]),
        Box::new(|_cc| {
            Ok(Box::new(LauncherApp {
                launcher: Launcher::new(),
                theme: tines_gui::theme::OmarchyTheme::load(),
                watching: false,
            }))
        }),
    )
    .map_err(|e| format!("eframe: {e}").into())
}

fn run_node_window(
    dir: std::path::PathBuf,
    container_id: ContainerId,
    mode: Mode,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let node = NodeWin::new(dir, container_id, mode);
    let title = format!("tines — {}", node.name);
    eframe::run_native(
        "tines-gui-node",
        options(&title, [680.0, 480.0]),
        Box::new(move |_cc| {
            Ok(Box::new(NodeWinApp {
                node,
                theme: tines_gui::theme::OmarchyTheme::load(),
                watching: false,
            }))
        }),
    )
    .map_err(|e| format!("eframe: {e}").into())
}
