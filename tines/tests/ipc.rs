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
    consts, Event, FrameBuf, Node, NodeEvent, NodeId, Observation, ObservationId, SendError,
    Transport,
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
        match frame_kind(frame) {
            Some(OBS_TAG) => {
                // A well-behaved peer acks each distinct observed broadcast.
                if let Ok((_, obs)) = postcard::from_bytes::<(NodeId, Observation)>(&frame[2..]) {
                    if self.acked.insert(obs.id()) {
                        self.pending.push_back(ack_event(*to, obs.id()));
                    }
                }
            }
            Some(SUMMARY_TAG) => {
                // Echo back an empty, full-size Summary so the exchange learns
                // the peer holds nothing and broadcasts its diff (which we then
                // ack, firing a NodeEvent::Delivered).
                if let Ok((from, salt, _)) = postcard::from_bytes::<(
                    NodeId,
                    u32,
                    HeaplessVec<u8, { consts::SUMMARY_BYTES }>,
                )>(&frame[2..])
                {
                    self.pending.push_back(summary_event(from, salt));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn mtu(&self) -> usize {
        consts::MAX_FRAME
    }
}

// Wire-variant tags, mirroring tines-core's private `Frame` enum order
// (Summary=0, Obs=1, Frag=2, Ack=3). tines-core keeps `Frame` internal, so the
// mock peer re-derives these here to classify inbound frames and synthesize the
// acks/summaries the exchange expects from a well-behaved peer.
const SUMMARY_TAG: u8 = 0;
const OBS_TAG: u8 = 1;
const ACK_TAG: u8 = 3;

/// Returns a wire frame's postcard variant tag, or `None` if `frame` is not a
/// well-formed tines frame (too short, or a wrong leading version byte).
fn frame_kind(frame: &[u8]) -> Option<u8> {
    (frame.len() >= 2 && frame[0] == consts::WIRE_VERSION).then_some(frame[1])
}

/// Encode a `Frame`-variant-shaped tuple as wire bytes: the WIRE_VERSION byte,
/// the variant tag, then the postcard payload — mirroring tines-core's
/// internal `Frame::encode`, using only `tines_core`'s public types.
fn encode_frame<T: serde::Serialize>(from: NodeId, tag: u8, payload: &T) -> Event<NodeId> {
    let mut buf = [0u8; consts::MAX_FRAME];
    buf[0] = consts::WIRE_VERSION;
    buf[1] = tag;
    let n = postcard::to_slice(payload, &mut buf[2..])
        .expect("frame fits MAX_FRAME")
        .len();
    Event::Frame(
        from,
        FrameBuf::from_slice(&buf[..2 + n]).expect("frame fits FrameBuf"),
    )
}

/// Synthesize the Ack a well-behaved peer sends for one observed id.
fn ack_event(from: NodeId, id: ObservationId) -> Event<NodeId> {
    let ids: HeaplessVec<ObservationId, { consts::ACK_IDS_CAP }> =
        HeaplessVec::from_slice(&[id]).expect("one id fits ACK_IDS_CAP");
    encode_frame(from, ACK_TAG, &(from, ids))
}

/// Synthesize the full-size, all-zero Summary a well-behaved peer echoes for an
/// incoming Summary (telling the exchange the peer holds nothing).
fn summary_event(from: NodeId, salt: u32) -> Event<NodeId> {
    let bits: HeaplessVec<u8, { consts::SUMMARY_BYTES }> =
        HeaplessVec::from_slice(&[0u8; consts::SUMMARY_BYTES]).expect("512 fits SUMMARY_BYTES");
    encode_frame(from, SUMMARY_TAG, &(from, salt, bits))
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
    let store = LogStore::open(&store_path, 0).expect("store open");
    Node::new(peer, store.first_seq(), store, MockTransport::new(peer))
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
            // Seeded from the boot counter (first_seq = boot_count << 32 = 1<<32
            // on a fresh store); after one observe next_seq is that + 1.
            assert_eq!(next_seq, (1u64 << 32) + 1);
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
    let _handle = thread::spawn(move || serve(node, serve_path, HostClock::start(), 20).unwrap());

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
    let _handle = thread::spawn(move || serve(node, serve_path, HostClock::start(), 20).unwrap());
    let mut stream = connect(&socket);

    rpc(&mut stream, &Request::Events);
    match read_response(&mut stream) {
        Response::Events { events } => {
            assert_eq!(events.len(), 1, "expected one confirmed delivery");
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
fn confirmed_delivery_direct_probe() {
    let dir = unique_dir("ev");
    let mut node = build_node([7; 16], &dir);
    let x = node.observe(&topic(), &payload(), None, 0).unwrap();
    node.poll(0);
    node.poll(0);
    node.poll(0);
    let evs = node.drain_events();
    assert_eq!(evs.len(), 1, "one confirmed delivery after observe+ack");
    match &evs[0] {
        NodeEvent::Delivered { peer, id } => {
            assert_eq!(*id, x);
            assert_eq!(*peer, [7; 16]);
        }
        other => panic!("expected a Delivered event, got {other:?}"),
    }
}

/// A malformed JSON frame must yield an `Error` response and leave the
/// connection alive, so a following valid request is still served.
#[test]
fn malformed_frame_gets_error_and_connection_survives() {
    let dir = unique_dir("malformed");
    let socket = dir.join("ipc.sock");
    let node = build_node([3; 16], &dir);
    let serve_path = socket.clone();
    let _handle = thread::spawn(move || serve(node, serve_path, HostClock::start(), 20).unwrap());
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
