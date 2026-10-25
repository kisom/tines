//! tines-gui entry point: no args opens the launcher window;
//! `--node <dir> <container-id> <mode>` opens a node window (Task 8b).

use eframe::egui;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    match args.as_slice() {
        [_, flag, dir, id, mode] if flag == "--node" => {
            let _ = (dir, id, mode);
            eprintln!("tines-gui: node window is Task 8b — not wired yet");
            std::process::exit(1);
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
    launcher: tines_gui::launcher::Launcher,
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

fn run_launcher() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let viewport = egui::ViewportBuilder::default()
        .with_app_id("tines-gui")
        .with_title("tines — fleet console")
        .with_inner_size([760.0, 520.0]);
    let options = eframe::NativeOptions {
        viewport,
        glow_options: eframe::egui_glow::GlowConfiguration {
            vsync: false,
            ..Default::default()
        },
        ..Default::default()
    };
    eframe::run_native(
        "tines-gui",
        options,
        Box::new(|_cc| {
            Ok(Box::new(LauncherApp {
                launcher: tines_gui::launcher::Launcher::new(),
                theme: tines_gui::theme::OmarchyTheme::load(),
                watching: false,
            }))
        }),
    )
    .map_err(|e| format!("eframe: {e}").into())
}
