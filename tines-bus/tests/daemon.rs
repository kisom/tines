//! Protocol-level tests driving raw Unix sockets straight at the daemon.
mod common;

use common::{spawn_daemon, unique_dir};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};
use tines_core::consts::MAX_FRAME;

type NodeId = [u8; 16];

fn connect(sock: &std::path::Path) -> UnixStream {
    let s = UnixStream::connect(sock).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    s
}

fn send_envelope(s: &mut UnixStream, payload: &[u8]) {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    s.write_all(&buf).unwrap();
}

fn hello(s: &mut UnixStream, id: &NodeId) {
    send_envelope(s, id);
}

fn data(s: &mut UnixStream, from: &NodeId, to: &NodeId, frame: &[u8]) {
    let mut p = Vec::with_capacity(32 + frame.len());
    p.extend_from_slice(from);
    p.extend_from_slice(to);
    p.extend_from_slice(frame);
    send_envelope(s, &p);
}

/// Read one envelope's payload; `Ok(None)` means EOF.
fn read_envelope(s: &mut UnixStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    let n = s.read(&mut len_buf)?;
    if n == 0 {
        return Ok(None);
    }
    assert_eq!(n, 4, "short length header");
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload)?;
    Ok(Some(payload))
}

fn expect_control(s: &mut UnixStream, kind: u8, id: &NodeId) {
    let p = read_envelope(s)
        .unwrap()
        .expect("connection closed unexpectedly");
    assert_eq!(p.len(), 17, "control payload is kind + 16-byte id");
    assert_eq!(p[0], kind);
    assert_eq!(&p[1..17], id);
}

/// Assert the socket stays silent for `window_ms`, then return.
fn expect_no_data(s: &mut UnixStream, window_ms: u64) {
    s.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let deadline = Instant::now() + Duration::from_millis(window_ms);
    let mut buf = [0u8; 1];
    loop {
        match s.read(&mut buf) {
            Ok(0) => panic!("connection closed, expected silence"),
            Ok(_) => panic!("expected no data, got some"),
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                if Instant::now() >= deadline {
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("unexpected read error: {e}"),
        }
    }
}

#[test]
fn newcomer_learns_existing_peers() {
    let dir = unique_dir("newcomer");
    let sock = spawn_daemon(&dir);

    let mut a = connect(&sock);
    hello(&mut a, &[1; 16]);

    // B joins after A: B must be told A is already there.
    let mut b = connect(&sock);
    hello(&mut b, &[2; 16]);
    expect_control(&mut a, 0, &[2; 16]);
    expect_control(&mut b, 0, &[1; 16]);
}

#[test]
fn duplicate_hello_replaces() {
    let dir = unique_dir("dup");
    let sock = spawn_daemon(&dir);

    let mut s1 = connect(&sock);
    hello(&mut s1, &[1; 16]);
    let mut s2 = connect(&sock);
    hello(&mut s2, &[1; 16]);

    // The first connection is closed by the daemon...
    let mut buf = [0u8; 1];
    assert_eq!(
        s1.read(&mut buf).unwrap(),
        0,
        "first connection must see EOF after replacement"
    );

    // ...and the daemon stays up, serving the replacement: a third peer's
    // join reaches s2 as a control message.
    let mut s3 = connect(&sock);
    hello(&mut s3, &[2; 16]);
    expect_control(&mut s2, 0, &[2; 16]);
}

#[test]
fn mismatched_from_is_dropped() {
    let dir = unique_dir("mismatch");
    let sock = spawn_daemon(&dir);

    let mut a = connect(&sock);
    hello(&mut a, &[1; 16]);
    let mut b = connect(&sock);
    hello(&mut b, &[2; 16]);

    // B learns about A on join (the newcomer announcement).
    expect_control(&mut b, 0, &[1; 16]);

    // A (registered as [1;16]) claims to be [2;16]: nothing may reach B.
    data(&mut a, &[2; 16], &[2; 16], &[0xAB; 10]);
    expect_no_data(&mut b, 300);

    // The daemon survives and A is still connected: a well-formed message
    // is forwarded verbatim.
    data(&mut a, &[1; 16], &[2; 16], &[0xCD; 10]);
    let p = read_envelope(&mut b).unwrap().expect("connection closed");
    assert_eq!(p.len(), 42);
    assert_eq!(&p[0..16], &[1; 16]);
    assert_eq!(&p[16..32], &[2; 16]);
    assert_eq!(&p[32..42], &[0xCD; 10]);
}

#[test]
fn unknown_dest_is_dropped() {
    let dir = unique_dir("unknown");
    let sock = spawn_daemon(&dir);

    let mut a = connect(&sock);
    hello(&mut a, &[1; 16]);

    // No [9;16] registered: dropped, and A stays connected.
    data(&mut a, &[1; 16], &[9; 16], &[0xAB; 10]);

    let mut b = connect(&sock);
    hello(&mut b, &[2; 16]);
    // Wait until the daemon has registered B: A sees the join control.
    expect_control(&mut a, 0, &[2; 16]);
    // B's side of the same join: the newcomer learns A is already here.
    expect_control(&mut b, 0, &[1; 16]);
    data(&mut a, &[1; 16], &[2; 16], &[0xCD; 10]);
    let p = read_envelope(&mut b).unwrap().expect("connection closed");
    assert_eq!(p.len(), 42);
    assert_eq!(&p[32..42], &[0xCD; 10]);
}

#[test]
fn oversized_frame_closes_connection() {
    let dir = unique_dir("oversize");
    let sock = spawn_daemon(&dir);

    let mut a = connect(&sock);
    hello(&mut a, &[1; 16]);
    let mut b = connect(&sock);
    hello(&mut b, &[2; 16]);

    // Handshake: once A has seen B join, both hellos are registered, and
    // B has learned A on its side too.
    expect_control(&mut a, 0, &[2; 16]);
    expect_control(&mut b, 0, &[1; 16]);

    // A frame one byte over MAX_FRAME: that connection is closed...
    data(&mut a, &[1; 16], &[2; 16], &vec![0u8; MAX_FRAME + 1]);
    let mut buf = [0u8; 1];
    assert_eq!(
        a.read(&mut buf).unwrap(),
        0,
        "oversized frame must close the connection"
    );

    // ...and B learns A left; the daemon stays up for everyone else.
    expect_control(&mut b, 1, &[1; 16]);
}
