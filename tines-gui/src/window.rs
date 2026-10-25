//! The node window: the whole service control surface for one container.
//!
//! All node I/O goes through the [`IpcClient`] — [`NodeWin::update`] queues
//! requests (Status/List once, Events every 500 ms, Observe/Belief on
//! clicks) and drains responses each frame; the UI thread never blocks.
//! Container operations (stop/remove/logs) are queued as [`NodeAction`]s
//! and drained by `main.rs`, like the launcher's action queue.

use std::collections::VecDeque;
use std::path::PathBuf;

use egui::Context;
use tines::{BeliefOut, ObservationOut, Request, Response, TransportKind};

use crate::docker::{ContainerId, Docker};
use crate::fleet::Mode;
use crate::ipc_client::IpcClient;

/// The status strip's data, from `Response::Status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusData {
    /// Hex of the node's own `NodeId`.
    pub id: String,
    /// Next tines sequence number the node will use.
    pub next_seq: u64,
    /// Number of observations in the store.
    pub stored: u32,
}

/// One events-feed row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedEntry {
    /// A peer joined or was lost.
    Contact {
        /// Hex of the peer's `NodeId`.
        peer: String,
        /// `true` for a new contact, `false` for a loss.
        joined: bool,
    },
    /// A confirmed delivery.
    Delivered {
        /// Hex of the peer the observation reached.
        peer: String,
        /// Hex of the delivered observation's id.
        id: String,
    },
    /// Counted drops drained from the exchange.
    Dropped {
        /// Hex of the peer the dropped frame came from, or `None` for the
        /// overflow slot.
        peer: Option<String>,
        /// The drop reason, debug-formatted.
        reason: String,
        /// The number of frames folded into the counter.
        count: u32,
    },
}

/// An intent the container panel queued; `main.rs` drains these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeAction {
    /// `docker stop` the container.
    Stop,
    /// `docker rm` the container.
    Remove,
    /// Re-fetch the log tail.
    FetchLogs,
}

/// One node window's full state.
pub struct NodeWin {
    /// The node's IPC client (dialed at the node's service socket).
    pub client: IpcClient,
    /// The node name (the config dir's leaf).
    pub name: String,
    /// How the node connects to its peers.
    pub mode: Mode,
    /// The transport the service is configured for, derived from `mode`.
    pub transport: TransportKind,
    /// The latest `Status` answer.
    pub status: Option<StatusData>,
    /// Latest beliefs, most recent first.
    pub beliefs: VecDeque<BeliefOut>,
    /// The events feed, oldest first.
    pub feed: VecDeque<FeedEntry>,
    /// The store browser's rows, from the latest `List`.
    pub store_rows: VecDeque<ObservationOut>,
    /// The id of the most recently inserted observation.
    pub last_id: Option<String>,

    /// The docker frontend (container panel).
    pub docker: Docker,
    /// The container this window controls.
    pub container_id: ContainerId,
    /// Intents queued by [`NodeWin::update`], drained by `main.rs`.
    pub actions: VecDeque<NodeAction>,

    /// The observe panel's topic field.
    pub topic: String,
    /// The observe panel's payload field.
    pub payload: String,
    /// The observe panel's recipient field (hex-encoded on send).
    pub recipient: String,
    /// The observe panel's lifetime field (ms).
    pub lifetime: String,
    /// When on, the topic/payload/recipient inputs are already hex.
    pub hex_input: bool,
    /// The belief panel's topic field.
    pub belief_topic: String,
    /// The container's last-fetched log tail.
    pub log_tail: Option<String>,
    /// The latest error to surface.
    pub error: Option<String>,

    /// Outstanding requests (responses not yet drained).
    pub outstanding: usize,
    /// When the Events poll last fired (input time).
    pub last_events_poll: f64,
    /// Whether the initial Status request went out.
    status_requested: bool,
    /// Whether the initial List request went out.
    list_requested: bool,
}

/// Encode text as lowercase hex (the service's wire form for topics,
/// payloads, and node ids).
fn to_hex(text: &str) -> String {
    let mut out = String::with_capacity(text.len() * 2);
    for byte in text.as_bytes() {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

impl NodeWin {
    /// Build the window: dial the service at `<dir>/sock`, name from the
    /// dir's leaf, transport from the mode.
    pub fn new(dir: PathBuf, container_id: ContainerId, mode: Mode) -> NodeWin {
        let name = dir
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "node".to_string());
        let transport = match mode {
            Mode::Bt => TransportKind::Bt,
            Mode::Sim => TransportKind::Bus,
        };
        let client = IpcClient::spawn(dir.join("sock")).expect("ipc worker spawns");
        NodeWin {
            client,
            name,
            mode,
            transport,
            status: None,
            beliefs: VecDeque::new(),
            feed: VecDeque::new(),
            store_rows: VecDeque::new(),
            last_id: None,
            docker: Docker,
            container_id,
            actions: VecDeque::new(),
            topic: String::new(),
            payload: String::new(),
            recipient: String::new(),
            lifetime: String::new(),
            hex_input: false,
            belief_topic: String::new(),
            log_tail: None,
            error: None,
            outstanding: 0,
            last_events_poll: f64::NEG_INFINITY,
            status_requested: false,
            list_requested: false,
        }
    }

    /// Queue a request and count it outstanding.
    fn send(&mut self, req: Request) {
        match self.client.request(req) {
            Ok(_) => self.outstanding += 1,
            Err(e) => self.error = Some(e.to_string()),
        }
    }

    /// Apply one drained response.
    fn apply(&mut self, resp: Response) {
        match resp {
            Response::Status {
                id,
                next_seq,
                stored,
            } => {
                self.status = Some(StatusData {
                    id,
                    next_seq,
                    stored,
                });
            }
            Response::List {
                observations,
                truncated: _,
            } => self.store_rows = observations.into(),
            Response::Belief { obs } => {
                if let Some(obs) = obs {
                    self.beliefs.push_front(obs);
                }
            }
            Response::Observe { id } => self.last_id = Some(id),
            Response::Events {
                events,
                contacts,
                drops,
            } => {
                for contact in contacts {
                    self.feed.push_back(FeedEntry::Contact {
                        peer: contact.peer,
                        joined: contact.joined,
                    });
                }
                for event in events {
                    self.feed.push_back(FeedEntry::Delivered {
                        peer: event.peer,
                        id: event.id,
                    });
                }
                for drop in drops {
                    self.feed.push_back(FeedEntry::Dropped {
                        peer: drop.peer,
                        reason: drop.reason,
                        count: drop.count,
                    });
                }
            }
            Response::Error { message } => self.error = Some(message),
        }
    }

    /// Render the node window and queue intents. Side effects are limited
    /// to queueing requests on the [`IpcClient`] (responses drain here
    /// too); container intents wait in `actions` for `main.rs`.
    pub fn update(&mut self, ctx: &Context) {
        while let Some((_id, resp)) = self.client.try_recv() {
            self.outstanding = self.outstanding.saturating_sub(1);
            self.apply(resp);
        }

        if !self.status_requested {
            self.status_requested = true;
            self.send(Request::Status);
        }
        if !self.list_requested {
            self.list_requested = true;
            self.send(Request::List);
        }
        let now = ctx.input(|i| i.time);
        if self.last_events_poll.is_nan() || now - self.last_events_poll >= 0.5 {
            self.last_events_poll = now;
            self.send(Request::Events);
        }

        // egui 0.36 is Ui-based: rebuild the root Ui, like the launcher.
        let mut ui = egui::Ui::new(
            ctx.clone(),
            egui::Id::new("tines-node-root"),
            egui::UiBuilder::new().max_rect(ctx.content_rect()),
        );

        egui::Panel::top("strip").show(&mut ui, |ui| {
            let (id, next_seq, stored) = match &self.status {
                Some(s) => (s.id.clone(), s.next_seq.to_string(), s.stored.to_string()),
                None => ("—".to_string(), "—".to_string(), "—".to_string()),
            };
            ui.horizontal(|ui| {
                ui.label(format!(
                    "node {id} · seq {next_seq} · stored {stored} · transport {}",
                    match self.transport {
                        TransportKind::Bt => "bt",
                        TransportKind::Bus => "bus",
                    }
                ));
                if self.mode == Mode::Bt {
                    ui.separator();
                    ui.label("bt: single peer — addressed observes beyond the configured peer have no route");
                }
            });
        });

        egui::Panel::left("controls")
            .resizable(true)
            .show(&mut ui, |ui| {
                ui.heading("Observe");
                ui.label("topic:");
                ui.text_edit_singleline(&mut self.topic);
                ui.label("payload:");
                ui.text_edit_singleline(&mut self.payload);
                ui.label("recipient (optional):");
                ui.text_edit_singleline(&mut self.recipient);
                ui.label("lifetime ms (optional):");
                ui.text_edit_singleline(&mut self.lifetime);
                ui.checkbox(&mut self.hex_input, "input is hex");
                if ui.button("Send").clicked() {
                    let encode = |s: &str| {
                        if self.hex_input {
                            s.to_string()
                        } else {
                            to_hex(s)
                        }
                    };
                    let to = (!self.recipient.is_empty()).then(|| encode(&self.recipient));
                    let lifetime_ms = (!self.lifetime.is_empty())
                        .then(|| self.lifetime.trim().parse::<u64>().ok())
                        .flatten();
                    self.send(Request::Observe {
                        topic: encode(&self.topic),
                        payload: encode(&self.payload),
                        to,
                        lifetime_ms,
                    });
                }
                if let Some(id) = &self.last_id {
                    ui.label(format!("last: {id}"));
                }

                ui.separator();
                ui.heading("Belief");
                ui.label("topic:");
                ui.text_edit_singleline(&mut self.belief_topic);
                if ui.button("Fetch").clicked() {
                    let topic = if self.hex_input {
                        self.belief_topic.clone()
                    } else {
                        to_hex(&self.belief_topic)
                    };
                    self.send(Request::Belief { topic });
                }
                for belief in &self.beliefs {
                    ui.label(format!("{}: {}", belief.topic, belief.payload));
                }

                ui.separator();
                ui.heading("Container");
                ui.horizontal(|ui| {
                    if ui.button("Refresh logs").clicked() {
                        self.actions.push_back(NodeAction::FetchLogs);
                    }
                    if ui.button("Stop").clicked() {
                        self.actions.push_back(NodeAction::Stop);
                    }
                    if ui.button("Remove").clicked() {
                        self.actions.push_back(NodeAction::Remove);
                    }
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

        egui::CentralPanel::default().show(&mut ui, |ui| {
            ui.heading("Store");
            if ui.button("Refresh").clicked() {
                self.send(Request::List);
            }
            egui::ScrollArea::vertical().show(ui, |ui| {
                egui::Grid::new("store").striped(true).show(ui, |ui| {
                    ui.label("id");
                    ui.label("topic");
                    ui.label("payload");
                    ui.label("age ms");
                    ui.end_row();
                    for obs in &self.store_rows {
                        ui.monospace(&obs.id);
                        ui.monospace(&obs.topic);
                        ui.monospace(&obs.payload);
                        ui.label(obs.age_ms.to_string());
                        ui.end_row();
                    }
                });
            });
        });

        egui::Panel::right("feed")
            .resizable(true)
            .show(&mut ui, |ui| {
                ui.heading("Events");
                egui::ScrollArea::vertical().show(ui, |ui| {
                    for entry in &self.feed {
                        match entry {
                            FeedEntry::Contact { peer, joined } => {
                                ui.label(format!(
                                    "contact {peer} {}",
                                    if *joined { "joined" } else { "lost" }
                                ));
                            }
                            FeedEntry::Delivered { peer, id } => {
                                ui.label(format!("delivered {id} to {peer}"));
                            }
                            FeedEntry::Dropped {
                                peer,
                                reason,
                                count,
                            } => {
                                let peer = peer.as_deref().unwrap_or("overflow");
                                ui.label(format!("dropped {count} from {peer}: {reason}"));
                            }
                        }
                    }
                });
            });

        if let Some(error) = &self.error {
            egui::Panel::bottom("error").show(&mut ui, |ui| {
                ui.colored_label(egui::Color32::from_rgb(0xff, 0x55, 0x55), error);
            });
        }

        // Keep frames flowing while responses are in flight (the harness
        // settles when this stops).
        if self.outstanding > 0 {
            ctx.request_repaint();
        }
    }

    /// The container panel's latest log tail (set by `main.rs`'s drain).
    pub fn apply_log_tail(&mut self, tail: String) {
        self.log_tail = Some(tail);
    }
}

impl eframe::App for NodeWin {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        NodeWin::update(self, ui.ctx());
    }
}
