//! The omarchy theme: the live palette plus a watch that repaints on change.
//!
//! Reads `~/.local/state/omarchy/current/theme/colors.toml` for the three
//! keys the console uses — `background`, `foreground`, `accent`. Omarchy
//! rewrites the `current` directory in place on theme change, so the watch
//! is on the directory (recursive), not the file.

use std::path::PathBuf;

use egui::Color32;
use notify::Watcher;

/// The three palette colors the console paints with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OmarchyTheme {
    /// The window background.
    pub background: Color32,
    /// The window foreground (text).
    pub foreground: Color32,
    /// The accent (highlights, focus).
    pub accent: Color32,
}

/// The app's own named fallback — used only when the live theme is missing
/// or unreadable. Deliberately not the omarchy palette: the console must be
/// visibly on fallback, not approximating the real theme.
pub const FALLBACK: OmarchyTheme = OmarchyTheme {
    background: Color32::from_rgb(0x16, 0x18, 0x1d),
    foreground: Color32::from_rgb(0xd7, 0xdb, 0xe2),
    accent: Color32::from_rgb(0x7a, 0xa2, 0xf7),
};

impl OmarchyTheme {
    /// Load the live theme; [`FALLBACK`] when it is missing or malformed.
    pub fn load() -> OmarchyTheme {
        match read_theme_file() {
            Some(t) => t,
            None => FALLBACK,
        }
    }

    /// Apply the palette: egui visuals plus an opaque background fill so
    /// the window is never transparent on Wayland. Call every frame.
    pub fn apply(&self, ctx: &egui::Context) {
        let mut visuals = egui::Visuals::dark();
        visuals.panel_fill = self.background;
        visuals.window_fill = self.background;
        visuals.window_stroke = egui::Stroke::NONE;
        visuals.override_text_color = Some(self.foreground);
        visuals.selection.bg_fill = self.accent;
        visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, self.accent);
        visuals.widgets.active.bg_stroke = egui::Stroke::new(1.0, self.accent);
        visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0, self.accent);
        ctx.set_visuals(visuals);
        let painter = ctx.layer_painter(egui::LayerId::new(
            egui::Order::Background,
            egui::Id::new("tines-gui-bg"),
        ));
        painter.rect_filled(ctx.content_rect(), 0.0, self.background);
    }

    /// Watch the omarchy `current` theme dir (recursively) and call
    /// `on_change` whenever it changes — the caller repaints from there.
    /// Returns an error only if the watcher cannot be installed.
    pub fn watch<F>(on_change: F) -> notify::Result<()>
    where
        F: FnMut() + Send + 'static,
    {
        // notify's callback is Fn, ours is FnMut: share it through a mutex.
        let on_change = std::sync::Mutex::new(on_change);
        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                if res.is_ok()
                    && let Ok(mut f) = on_change.lock()
                {
                    f();
                }
            })?;
        watcher.watch(&current_dir(), notify::RecursiveMode::Recursive)?;
        // The watcher must outlive the call: park it on a thread.
        std::thread::spawn(move || {
            std::thread::park();
            let _ = watcher;
        });
        Ok(())
    }
}

/// `~/.local/state/omarchy/current` — the live-theme indirection.
fn current_dir() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    home.join(".local/state/omarchy/current")
}

fn read_theme_file() -> Option<OmarchyTheme> {
    let text = std::fs::read_to_string(current_dir().join("theme/colors.toml")).ok()?;
    let mut background = None;
    let mut foreground = None;
    let mut accent = None;
    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue; // blank lines and comments carry no key
        };
        let parsed = parse_hex(value);
        match key.trim() {
            "background" => background = parsed,
            "foreground" => foreground = parsed,
            "accent" => accent = parsed,
            _ => {}
        }
    }
    Some(OmarchyTheme {
        background: background?,
        foreground: foreground?,
        accent: accent?,
    })
}

fn parse_hex(s: &str) -> Option<Color32> {
    let s = s.trim().trim_matches('"').strip_prefix('#')?;
    if s.len() != 6 {
        return None;
    }
    let r = u8::from_str_radix(&s[0..2], 16).ok()?;
    let g = u8::from_str_radix(&s[2..4], 16).ok()?;
    let b = u8::from_str_radix(&s[4..6], 16).ok()?;
    Some(Color32::from_rgb(r, g, b))
}
