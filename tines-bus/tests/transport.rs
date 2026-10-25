//! End-to-end tests for the bus transport: real `BusTransport`s on both ends
//! of a live daemon, exchanging core wire frames.
mod common;

use common::{spawn_daemon, unique_dir};
use std::path::Path;
use std::time::{Duration, Instant};
use tines_bus::BusTransport;
use tines_core::{consts, Event, SendError, Transport};

type NodeId = [u8; 16];

/// A minimally valid wire frame declaring `from` as its sender:
/// `WIRE_VERSION`, a variant tag byte, then the 16-byte sender id
/// (`frame_sender` reads exactly those bytes).
fn frame_from(from: &NodeId) -> Vec<u8> {
    let mut f = Vec::with_capacity(18);
    f.push(consts::WIRE_VERSION);
    f.push(0);
    f.extend_from_slice(from);
    f
}

/// Poll `t` until `pred` accepts an event, or panic after `timeout`.
fn poll_until(
    t: &mut BusTransport,
    mut pred: impl FnMut(&Event<NodeId>) -> bool,
    timeout: Duration,
) -> Event<NodeId> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(ev) = t.poll() {
            if pred(&ev) {
                return ev;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a bus event"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Assert `t` yields no events for `window`, then give up.
fn expect_quiet(t: &mut BusTransport, window: Duration) {
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        assert!(t.poll().is_none(), "expected no events, got one");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn daemon_for(dir: &Path) -> std::path::PathBuf {
    spawn_daemon(dir)
}

#[test]
fn two_transports_exchange_frames() {
    let dir = unique_dir("xfer");
    let sock = daemon_for(&dir);
    let mut a = BusTransport::new([1; 16], None, &sock).unwrap();
    let mut b = BusTransport::new([2; 16], None, &sock).unwrap();

    // A learns about B when B joins.
    match poll_until(
        &mut a,
        |ev| matches!(ev, Event::Contact(_)),
        Duration::from_secs(3),
    ) {
        Event::Contact(id) => assert_eq!(id, [2; 16]),
        other => panic!("expected Contact, got {other:?}"),
    }

    let frame = frame_from(&[1; 16]);
    a.send(&[2; 16], &frame).unwrap();

    // B receives the frame from A with identical bytes.
    match poll_until(
        &mut b,
        |ev| matches!(ev, Event::Frame(_, _)),
        Duration::from_secs(3),
    ) {
        Event::Frame(peer, fbuf) => {
            assert_eq!(peer, [1; 16]);
            assert_eq!(fbuf.as_slice(), frame.as_slice());
        }
        other => panic!("expected Frame, got {other:?}"),
    }
}

#[test]
fn spoofed_frame_is_dropped() {
    let dir = unique_dir("spoof");
    let sock = daemon_for(&dir);
    let mut a = BusTransport::new([1; 16], None, &sock).unwrap();
    let mut b = BusTransport::new([2; 16], None, &sock).unwrap();

    poll_until(
        &mut a,
        |ev| matches!(ev, Event::Contact(_)),
        Duration::from_secs(3),
    );

    // A (complicit) sends a frame that claims to be from C: the bus stamps
    // A as the envelope sender, so B must never surface it.
    let spoofed = frame_from(&[9; 16]);
    a.send(&[2; 16], &spoofed).unwrap();
    expect_quiet(&mut b, Duration::from_millis(400));

    // The link is still healthy: a genuine frame from A arrives intact.
    let real = frame_from(&[1; 16]);
    a.send(&[2; 16], &real).unwrap();
    match poll_until(
        &mut b,
        |ev| matches!(ev, Event::Frame(_, _)),
        Duration::from_secs(3),
    ) {
        Event::Frame(peer, fbuf) => {
            assert_eq!(peer, [1; 16]);
            assert_eq!(fbuf.as_slice(), real.as_slice());
        }
        other => panic!("expected Frame, got {other:?}"),
    }
}

#[test]
fn join_and_leave_become_contact_and_lost() {
    let dir = unique_dir("joinleave");
    let sock = daemon_for(&dir);
    let mut a = BusTransport::new([1; 16], None, &sock).unwrap();
    let b = BusTransport::new([2; 16], None, &sock).unwrap();

    match poll_until(
        &mut a,
        |ev| matches!(ev, Event::Contact(_)),
        Duration::from_secs(3),
    ) {
        Event::Contact(id) => assert_eq!(id, [2; 16]),
        other => panic!("expected Contact, got {other:?}"),
    }

    drop(b);
    match poll_until(
        &mut a,
        |ev| matches!(ev, Event::Lost(_)),
        Duration::from_secs(3),
    ) {
        Event::Lost(id) => assert_eq!(id, [2; 16]),
        other => panic!("expected Lost, got {other:?}"),
    }
}

#[test]
fn allowlist_filters() {
    let dir = unique_dir("allow");
    let sock = daemon_for(&dir);
    let mut a = BusTransport::new([1; 16], Some([2; 16]), &sock).unwrap();
    let _b = BusTransport::new([2; 16], None, &sock).unwrap();
    let _c = BusTransport::new([3; 16], None, &sock).unwrap();

    // Only the allowlisted peer surfaces.
    match poll_until(
        &mut a,
        |ev| matches!(ev, Event::Contact(_)),
        Duration::from_secs(3),
    ) {
        Event::Contact(id) => assert_eq!(id, [2; 16]),
        other => panic!("expected Contact, got {other:?}"),
    }

    // C's join is filtered out: nothing else surfaces.
    expect_quiet(&mut a, Duration::from_millis(400));

    // And sending to a non-allowlisted peer is refused.
    let frame = frame_from(&[1; 16]);
    match a.send(&[3; 16], &frame) {
        Err(SendError::Unreachable) => {}
        other => panic!("expected Unreachable, got {other:?}"),
    }
}
