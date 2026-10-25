//! Node window tests (egui_kittest, headless). The fake service speaks the
//! real wire framing and answers scripted responses; the window is driven
//! through `NodeWin::update` and asserted via the harness tree and the
//! fake's received-request log.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use egui::accesskit;
use egui_kittest::Harness;
use egui_kittest::kittest::Queryable;
use tines::{ContactOut, Request, Response};
use tines_gui::docker::ContainerId;
use tines_gui::fleet::Mode;
use tines_gui::window::NodeWin;

/// One scratch dir per test: the fake service binds `<dir>/sock`, which is
/// exactly where the node window dials.
fn scratch_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "tines-gui-window-{tag}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn new_window(dir: &std::path::Path) -> NodeWin {
    NodeWin::new(
        dir.to_path_buf(),
        ContainerId("deadbeef".to_string()),
        Mode::Sim,
    )
}

/// The harness: generous max steps so async IPC round trips can land
/// (the default 4 panics on any slow dial).
fn harness_for(dir: &std::path::Path) -> Harness<'static, NodeWin> {
    let dir = dir.to_path_buf();
    Harness::builder()
        .with_max_steps(400)
        .build_eframe(move |_cc| new_window(&dir))
}

/// A service that answers Status/List/Belief/Observe and returns one
/// contact from the first Events drain.
fn default_handler() -> impl Fn(&Request) -> Response + Send + Sync + 'static {
    let contact_sent = Arc::new(AtomicBool::new(false));
    move |req: &Request| match req {
        Request::Status => Response::Status {
            id: "0a0b".to_string(),
            next_seq: 7,
            stored: 3,
        },
        Request::Events => {
            if contact_sent.swap(true, Ordering::Relaxed) {
                Response::Events {
                    events: vec![],
                    contacts: vec![],
                    drops: vec![],
                }
            } else {
                Response::Events {
                    events: vec![],
                    contacts: vec![ContactOut {
                        peer: "bb".to_string(),
                        joined: true,
                    }],
                    drops: vec![],
                }
            }
        }
        Request::List => Response::List {
            observations: vec![],
            truncated: false,
        },
        Request::Belief { .. } => Response::Belief { obs: None },
        Request::Observe { .. } => Response::Observe {
            id: "00".to_string(),
        },
    }
}

/// The response drain is a background worker: poll the fake's log until
/// the predicate holds or the deadline passes.
fn wait_for<F: Fn(&Request) -> bool>(fake: &common::FakeService, pred: F) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if fake.any_request(&pred) {
            return;
        }
        assert!(Instant::now() < deadline, "request never arrived");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Focus the `idx`-th text input and type `text` into it. The iterator
/// borrows the harness, so it must drop before each `run()`.
fn type_into(harness: &mut Harness<'_, NodeWin>, idx: usize, text: &str) {
    {
        let mut inputs = harness.get_all_by_role(accesskit::Role::TextInput);
        inputs.nth(idx).unwrap().focus();
    }
    settle(harness);
    {
        let mut inputs = harness.get_all_by_role(accesskit::Role::TextInput);
        inputs.nth(idx).unwrap().type_text(text);
    }
    settle(harness);
}

/// Step the app until every outstanding request has been answered (real
/// time between steps: the IPC round trip runs on a background thread, and
/// a tight `run()` loop burns hundreds of frames before the first response
/// lands — while the 500ms events poll keeps queueing, so `run()` never
/// settles and panics at max_steps).
fn settle(harness: &mut Harness<'_, NodeWin>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        harness.step();
        if harness.state().outstanding == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "service never answered the outstanding requests"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn status_strip_shows_node_id() {
    let dir = scratch_dir("status");
    let fake = common::FakeService::start(dir.join("sock"), default_handler());
    let mut harness = harness_for(&dir);
    settle(&mut harness);

    // The strip is one label; the node id hex must appear in it.
    assert!(
        harness.get_all_by_label_contains("0a0b").count() >= 1,
        "status strip must show the node id answered by Status"
    );
    drop(fake);
}

#[test]
fn observe_button_queues_observe() {
    let dir = scratch_dir("observe");
    let fake = common::FakeService::start(dir.join("sock"), default_handler());
    let mut harness = harness_for(&dir);
    settle(&mut harness);

    // Text inputs render in panel order: topic, payload, recipient,
    // lifetime. Focus-then-type lands text in each field.
    type_into(&mut harness, 0, "hi");
    type_into(&mut harness, 1, "yo");

    harness.get_by_label("Send").click();
    settle(&mut harness);

    wait_for(&fake, |req| {
        matches!(
            req,
            Request::Observe {
                topic,
                payload,
                to: None,
                lifetime_ms: None,
            } if topic == "6869" && payload == "796f"
        )
    });
}

#[test]
fn addressed_fields_queue_observe_to() {
    let dir = scratch_dir("addressed");
    let fake = common::FakeService::start(dir.join("sock"), default_handler());
    let mut harness = harness_for(&dir);
    settle(&mut harness);

    // topic (0), payload (1), recipient (2), lifetime (3).
    for (idx, text) in [(0usize, "hi"), (1, "yo"), (2, "aa"), (3, "5000")] {
        type_into(&mut harness, idx, text);
    }

    harness.get_by_label("Send").click();
    settle(&mut harness);

    wait_for(&fake, |req| {
        matches!(
            req,
            Request::Observe {
                topic,
                payload,
                to: Some(to),
                lifetime_ms: Some(5000),
            } if topic == "6869" && payload == "796f" && to == "6161"
        )
    });
}

#[test]
fn feed_shows_contact() {
    let dir = scratch_dir("feed");
    let fake = common::FakeService::start(dir.join("sock"), default_handler());
    let mut harness = harness_for(&dir);
    settle(&mut harness);

    assert!(
        harness.get_all_by_label_contains("bb").count() >= 1,
        "the events feed must show the contact's peer hex"
    );
    drop(fake);
}
