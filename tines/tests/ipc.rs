//! Integration tests for the `tines` IPC.
//!
//! These drive `handle`/`serve` with a mock transport, so no real BLE radio is
//! needed: the mock surfaces a single `Contact`, records outbound frames, and
//! auto-acks any observation broadcast to it (simulating a well-behaved peer).

use std::collections::{HashSet, VecDeque};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::Duration;

use heapless::Vec as HeaplessVec;
use std::fmt::Write as _;
use tines::{handle, serve, HostClock, Request, Response};
use tines_core::{
    consts, Event, Frame, FrameBuf, Node, NodeId, ObservationId, SendError, Transport,
};
use tines_store::LogStore;

/// A no-op [`Transport`] for tests: it never surfaces inbound frames except
/// the ones injected by the test, and it records outbound frames.
struct MockTransport {
    peer: NodeId,
    contacted: bool,
    pending: VecDeque<Event<NodeId>>,
    sent: Vec<Vec<u8>>,
    acked: HashSet<ObservationId>,
}

/// A well-behaved peer: auto-acks each distinct observation it is sent.
impl MockTransport {
    fn new(peer: NodeId) -> Self {
        Self {
            peer,
            contacted: false,
            pending: VecDeque::new(),
            sent: Vec::new(),
            acked: HashSet::new(),
        }
    }
}

impl Transport for MockTransport {
    type Addr = NodeId;

    fn poll(&mut self) -> Option<Event<NodeId>> {
        if !self.contacted {
            self.contacted = true;
            return Some(Event::Contact(self.peer));
        }
        self.pending.pop_front()
    }

    fn send(&mut self, to: &NodeId, frame: &[u8]) -> Result<(), SendError> {
        self.sent.push(frame.to_vec());
        match Frame::decode(frame) {
            Ok(Frame::Obs { obs, .. }) => {
                // A well-behaved peer acks each distinct observed broadcast.
                if self.acked.insert(obs.id()) {
                    let ack = ack_frame(*to, obs.id());
                    self.pending.push_back(frame_event(*to, &ack));
                }
            }
            Ok(Frame::Summary { salt, .. }) => {
                // Echo back an empty, full-size Summary so the exchange learns
                // the peer holds nothing and broadcasts its diff (which we then
                // ack, firing a ConfidenceEvent).
                let raw: [u8; consts::SUMMARY_BYTES] = [0u8; consts::SUMMARY_BYTES];
                let bits: HeaplessVec<u8, { consts::SUMMARY_BYTES }> =
                    HeaplessVec::from_slice(&raw).expect("512 fits SUMMARY_BYTES");
                let reply = Frame::Summary {
                    from: *to,
                    salt,
                    bits,
                };
                self.pending.push_back(frame_event(*to, &reply));
            }
            _ => {}
        }
        Ok(())
    }

    fn mtu(&self) -> usize {
        consts::MAX_FRAME
    }
}

fn ack_frame(from: NodeId, id: ObservationId) -> Frame {
    let ids: HeaplessVec<ObservationId, { consts::ACK_IDS_CAP }> =
        HeaplessVec::from_slice(&[id]).expect("one id fits ACK_IDS_CAP");
    Frame::Ack { from, ids }
}

fn frame_event(from: NodeId, frame: &Frame) -> Event<NodeId> {
    let mut buf = [0u8; consts::MAX_FRAME];
    let n = frame.encode(&mut buf).expect("frame fits MAX_FRAME");
    Event::Frame(
        from,
        FrameBuf::from_slice(&buf[..n]).expect("frame fits FrameBuf"),
    )
}

fn topic() -> Vec<u8> {
    b"tines/test".to_vec()
}

fn payload() -> Vec<u8> {
    b"hello".to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tines-ipc-{}-{}", tag, process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn build_node(peer: NodeId, dir: &Path) -> Node<LogStore, MockTransport> {
    let store_path = dir.join("store.log");
    let store = LogStore::new(&store_path, 0, 1).expect("store open");
    Node::new(peer, 1, store, MockTransport::new(peer))
}

fn connect(path: &Path) -> UnixStream {
    let mut delay = 5;
    for _ in 0..500 {
        if let Ok(s) = UnixStream::connect(path) {
            return s;
        }
        thread::sleep(Duration::from_millis(delay));
        delay = delay.min(50);
    }
    panic!("failed to connect to {}", path.display());
}

fn rpc<S: Read + Write>(stream: &mut S, req: &Request) {
    let body = serde_json::to_vec(req).expect("serialize request");
    let len = (body.len() as u32).to_le_bytes();
    stream.write_all(&len).unwrap();
    stream.write_all(&body).unwrap();
    stream.flush().unwrap();
}

fn read_response<S: Read>(stream: &mut S) -> Response {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).unwrap();
    serde_json::from_slice(&buf).expect("valid response")
}

#[test]
fn handle_status_and_belief_direct() {
    let dir = unique_dir("direct");
    let mut node = build_node([3; 16], &dir);
    let x = node.observe(&topic(), &payload(), None, 0).unwrap();

    let status = handle(&mut node, Request::Status, 0);
    match status {
        Response::Status {
            id,
            next_seq,
            stored,
        } => {
            assert_eq!(id, hex_encode(&node.id()));
            assert_eq!(next_seq, 2);
            assert_eq!(stored, 1);
        }
        other => panic!("expected status, got {other:?}"),
    }

    let belief = handle(
        &mut node,
        Request::Belief {
            topic: hex_encode(&topic()),
        },
        0,
    );
    match belief {
        Response::Belief { obs: Some(b) } => {
            assert_eq!(b.id, hex_encode(&x));
            assert_eq!(b.topic, hex_encode(&topic()));
            assert_eq!(b.payload, hex_encode(&payload()));
        }
        other => panic!("expected belief, got {other:?}"),
    }
}

#[test]
fn handle_unknown_obs_is_belief_none() {
    let dir = unique_dir("unknown");
    let mut node = build_node([3; 16], &dir);
    let resp = handle(
        &mut node,
        Request::Belief {
            topic: hex_encode(&topic()),
        },
        0,
    );
    match resp {
        Response::Belief { obs: None } => {}
        other => panic!("expected empty belief, got {other:?}"),
    }
}

#[test]
fn observe_and_belief_over_socket() {
    let dir = unique_dir("roundtrip");
    let socket = dir.join("ipc.sock");
    let node = build_node([9; 16], &dir);
    let serve_path = socket.clone();
    let _handle = thread::spawn(move || serve(node, serve_path, HostClock::start()).unwrap());

    let mut stream = connect(&socket);

    rpc(
        &mut stream,
        &Request::Observe {
            topic: hex_encode(&topic()),
            payload: hex_encode(&payload()),
        },
    );
    let id = match read_response(&mut stream) {
        Response::Observe { id } => id,
        other => panic!("expected Observe, got {other:?}"),
    };
    assert_eq!(id.len(), 32);

    rpc(
        &mut stream,
        &Request::Belief {
            topic: hex_encode(&topic()),
        },
    );
    match read_response(&mut stream) {
        Response::Belief { obs: Some(b) } => {
            assert_eq!(b.id, id);
            assert_eq!(b.topic, hex_encode(&topic()));
            assert_eq!(b.payload, hex_encode(&payload()));
        }
        other => panic!("expected Belief, got {other:?}"),
    }
}

#[test]
fn events_over_socket() {
    let dir = unique_dir("events");
    let socket = dir.join("ipc.sock");
    let peer = [7; 16];
    let mut node = build_node(peer, &dir);

    // Drive a confidence event into the node directly: observe (broadcast),
    // let the mock auto-ack it, and poll it out of the exchange.
    let x = node.observe(&topic(), &payload(), None, 0).unwrap();
    node.poll(0); // contact -> node emits its summary
    node.poll(0); // node broadcasts the observed topic; mock injects the ack
    node.poll(0); // ack processed -> confidence event fires

    let serve_path = socket.clone();
    let _handle = thread::spawn(move || serve(node, serve_path, HostClock::start()).unwrap());
    let mut stream = connect(&socket);

    rpc(&mut stream, &Request::Events);
    match read_response(&mut stream) {
        Response::Events { events } => {
            assert_eq!(events.len(), 1, "expected one confidence event");
            assert_eq!(events[0].id, hex_encode(&x), "event id matches observed id");
            assert_eq!(events[0].peer, hex_encode(&peer), "event came from peer");
        }
        other => panic!("expected Events, got {other:?}"),
    }
}

#[test]
fn handle_bad_hex_is_error() {
    let dir = unique_dir("hex");
    let mut node = build_node([3; 16], &dir);
    let resp = handle(
        &mut node,
        Request::Observe {
            topic: "z".into(),
            payload: "ff".into(),
        },
        0,
    );
    match resp {
        Response::Error { .. } => {}
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn confidence_event_direct_probe() {
    let dir = unique_dir("ev");
    let mut node = build_node([7; 16], &dir);
    let x = node.observe(&topic(), &payload(), None, 0).unwrap();
    node.poll(0);
    node.poll(0);
    node.poll(0);
    let evs = node.drain_events();
    assert_eq!(evs.len(), 1, "one confidence event after observe+ack");
    assert_eq!(evs[0].id, x);
    assert_eq!(evs[0].peer, [7; 16]);
}

/// A malformed JSON frame must yield an `Error` response and leave the
/// connection alive, so a following valid request is still served.
#[test]
fn malformed_frame_gets_error_and_connection_survives() {
    let dir = unique_dir("malformed");
    let socket = dir.join("ipc.sock");
    let node = build_node([3; 16], &dir);
    let serve_path = socket.clone();
    let _handle = thread::spawn(move || serve(node, serve_path, HostClock::start()).unwrap());
    let mut stream = connect(&socket);

    // A length-prefixed frame whose body is not valid JSON.
    let body = b"{ this is not valid json";
    let len = (body.len() as u32).to_le_bytes();
    stream.write_all(&len).unwrap();
    stream.write_all(body).unwrap();
    stream.flush().unwrap();

    // The server must reply with an Error, not drop the socket.
    match read_response(&mut stream) {
        Response::Error { .. } => {}
        other => panic!("expected Error for malformed frame, got {other:?}"),
    }

    // The connection is still alive: a valid request now succeeds.
    rpc(&mut stream, &Request::Status);
    match read_response(&mut stream) {
        Response::Status { .. } => {}
        other => panic!("expected Status after a malformed frame, got {other:?}"),
    }

    let _ = std::fs::remove_file(&socket);
}
