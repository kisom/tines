//! Integration tests for the `tines` IPC.
//!
//! These drive `handle`/`serve` with a mock transport, so no real BLE radio is
//! needed: the mock surfaces a single `Contact`, records outbound frames, and
//! auto-acks any observation broadcast to it (simulating a well-behaved peer).

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use heapless::Vec as HeaplessVec;
use std::fmt::Write as _;
use tines::{handle, serve, HostClock, Request, Response};
use tines_core::{
    consts, Event, FrameBuf, Node, NodeEvent, NodeId, Observation, ObservationId, SendError, Store,
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

    let status = handle(&mut node, Request::Status, None, 0);
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
        None,
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
        None,
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
    let _handle =
        thread::spawn(move || serve(node, serve_path, HostClock::start(), 20, WINDOW_MS).unwrap());

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
    let _handle =
        thread::spawn(move || serve(node, serve_path, HostClock::start(), 20, WINDOW_MS).unwrap());
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
        None,
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
    let _handle =
        thread::spawn(move || serve(node, serve_path, HostClock::start(), 20, WINDOW_MS).unwrap());
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

// --- In-memory transport + propagation / trusted-time tests ------------------
//
// These drive two `Node`s over a shared in-memory transport (no real BLE
// radio). `SharedInMemory` binds two Nodes to one world: a frame sent by one is
// delivered to the other's `Transport::poll`.

/// A minimal shared in-memory transport: two [`Node`]s bound to the same world
/// share mailbox pairs, so a frame sent by one is delivered to the other's
/// [`Transport::poll`] — with no real BLE radio. This is the "in-memory
/// transport" the trusted-time tests run over.
struct SharedInMemory {
    me: NodeId,
    their: NodeId,
    world: Arc<Mutex<SharedInMemoryWorld>>,
    /// Set to `true` once this endpoint's first `poll` reports contact, so the
    /// `Contact` event fires exactly once, per endpoint.
    contacted: bool,
}

struct SharedInMemoryWorld {
    /// Per-node mailboxes: a frame sent to `to` lands in that node's inbox and
    /// `poll` drains the local node's inbox. A single shared mailbox would let
    /// either endpoint consume a frame destined for the other.
    inboxes: HashMap<NodeId, VecDeque<Event<NodeId>>>,
}

impl SharedInMemoryWorld {
    fn new() -> Self {
        Self {
            inboxes: HashMap::new(),
        }
    }
}

impl Transport for SharedInMemory {
    type Addr = NodeId;

    fn poll(&mut self) -> Option<Event<NodeId>> {
        if !self.contacted {
            self.contacted = true;
            return Some(Event::Contact(self.their));
        }
        self.world
            .lock()
            .unwrap()
            .inboxes
            .get_mut(&self.me)?
            .pop_front()
    }

    fn send(&mut self, to: &NodeId, frame: &[u8]) -> Result<(), SendError> {
        // Deliver only to our peer; a frame to an unknown Addr is dropped.
        if *to != self.their {
            return Ok(());
        }
        let payload = FrameBuf::from_slice(frame).map_err(|_| SendError::TooLarge)?;
        self.world
            .lock()
            .unwrap()
            .inboxes
            .entry(*to)
            .or_default()
            .push_back(Event::Frame(self.me, payload));
        Ok(())
    }

    fn mtu(&self) -> usize {
        consts::MAX_FRAME
    }
}

/// Nodes A and B over one shared in-memory world, and the world.
type Pair = (
    Node<LogStore, SharedInMemory>,
    Node<LogStore, SharedInMemory>,
    Arc<Mutex<SharedInMemoryWorld>>,
);

/// Two [`Node`]s over a shared [`SharedInMemory`] world, each with its own
/// store, bound to ids `[1;16]` (A) and `[2;16]` (B), and the world itself (so a
/// test can inject frames into either inbox).
fn build_pair(dir: &Path) -> Pair {
    let world = Arc::new(Mutex::new(SharedInMemoryWorld::new()));
    let (a_id, b_id) = ([1; 16], [2; 16]);
    let a_store = LogStore::open(&dir.join("a.store.log"), 0).expect("a store open");
    let b_store = LogStore::builder(&dir.join("b.store.log"))
        .plausible_wall_ms(tines::trust::PLAUSIBLE_WALL_MS)
        .open(0)
        .expect("b store open");
    let ta = SharedInMemory {
        me: a_id,
        their: b_id,
        world: world.clone(),
        contacted: false,
    };
    let tb = SharedInMemory {
        me: b_id,
        their: a_id,
        world: world.clone(),
        contacted: false,
    };
    (
        Node::new(a_id, a_store.first_seq(), a_store, ta),
        Node::new(b_id, b_store.first_seq(), b_store, tb),
        world,
    )
}

/// Same as [`build_pair`], but B's store opens with one already-held
/// (addressed) observation behind the unknown-wall-clock quarantine — as if B
/// had resumed with held data. The first open inserts the held observation and
/// is dropped so the record lands on disk; reopening loads it quarantined.
fn build_pair_with_held(dir: &Path) -> Pair {
    let world = Arc::new(Mutex::new(SharedInMemoryWorld::new()));
    let (a_id, b_id) = ([1; 16], [2; 16]);
    let held_store0 = LogStore::open(&dir.join("b.store.log"), 0).expect("b store open0");
    {
        let held_ta = SharedInMemory {
            me: b_id,
            their: a_id,
            world: world.clone(),
            contacted: false,
        };
        let mut held_node = Node::new(b_id, held_store0.first_seq(), held_store0, held_ta);
        held_node
            .observe_to(a_id, 3_600_000, &topic(), b"held", None, 0)
            .unwrap();
    }
    let b_store = LogStore::open(&dir.join("b.store.log"), 0).expect("b store open1");
    let a_store = LogStore::open(&dir.join("a.store.log"), 0).expect("a store open");
    let ta = SharedInMemory {
        me: a_id,
        their: b_id,
        world: world.clone(),
        contacted: false,
    };
    let tb = SharedInMemory {
        me: b_id,
        their: a_id,
        world: world.clone(),
        contacted: false,
    };
    (
        Node::new(a_id, a_store.first_seq(), a_store, ta),
        Node::new(b_id, b_store.first_seq(), b_store, tb),
        world,
    )
}

/// F2 propagation over a shared in-memory transport.
#[test]
fn f2_direct_propagation() {
    let dir = unique_dir("probe");
    let (mut node_a, mut node_b, _) = build_pair(&dir);
    // Observe FIRST so the broadcast is already in A's store when the summary
    // exchange rebuilds the send queue (mirrors pair.rs).
    let observed = node_a.observe(&topic(), &payload(), None, 0).unwrap();
    let mut t = 0u64;
    let mut ok = false;
    for _ in 0..1000 {
        let _ = node_a.poll(t);
        let _ = node_b.poll(t);
        t += 10;
        if node_b.store().contains(&observed) {
            ok = true;
            break;
        }
    }
    assert!(ok, "direct propagation failed after 1000 rounds");
}

/// F2 regression: two service instances over an in-memory transport — an
/// observation made through one instance's IPC becomes a belief readable through
/// the other's. Both instances drive their exchange inline in [`serve`]'s poll
/// loop (the node isn't `Send`, so there's no separate exchange thread).
#[test]
fn f2_propagation_between_instances() {
    let dir = unique_dir("propagate");
    let (node_a, node_b, _) = build_pair(&dir);

    let socket_a = dir.join("a.sock");
    let socket_b = dir.join("b.sock");
    let (clock_a, clock_b) = (HostClock::start(), HostClock::start());

    let socket_a_thread = socket_a.clone();
    let _a = thread::spawn(move || serve(node_a, socket_a_thread, clock_a, 20, WINDOW_MS).unwrap());
    let socket_b_thread = socket_b.clone();
    let _b = thread::spawn(move || serve(node_b, socket_b_thread, clock_b, 20, WINDOW_MS).unwrap());

    // A observes on its socket.
    let mut stream_a = connect(&socket_a);
    rpc(
        &mut stream_a,
        &Request::Observe {
            topic: hex_encode(&topic()),
            payload: hex_encode(&payload()),
        },
    );
    let obs = match read_response(&mut stream_a) {
        Response::Observe { id } => id,
        other => panic!("expected Observe, got {other:?}"),
    };

    // B queries the belief; retry until its inline exchange has polled A's
    // broadcast off the shared transport and stored it.
    for _ in 0..1000 {
        let maybe = {
            let mut stream_b = connect(&socket_b);
            rpc(
                &mut stream_b,
                &Request::Belief {
                    topic: hex_encode(&topic()),
                },
            );
            read_response(&mut stream_b)
        };
        if let Response::Belief { obs: Some(b) } = maybe {
            assert_eq!(b.id, obs);
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("observation through A never became a belief through B");
}

// Trusted wall time (spec §9, decisions G and J; 1.0 review B3).
//
// These run the host's own trust logic: B is a served instance (its node moves
// into `serve`), A is a bare node the test pumps. What B decided is read off
// the `observed_at` of an observation B makes over IPC once it reaches A —
// `serve` stamps it from B's `HostClock`, so it is `None` until B adopts a
// wall time and `adopted + elapsed` after.

/// The peer-time window the served instances use: short, to keep tests fast.
const WINDOW_MS: u64 = 300;

/// A plausible wall time (2027-01-15), inside the hosts' bounds.
const W: u64 = 1_800_000_000_000;

/// An observation from A with a forged or chosen `observed_at` and `age_ms`.
/// `Observation` is non-exhaustive, so build it and then set the age.
fn crafted(seq: u64, observed_at: u64, age_ms: u64) -> Observation {
    let mut obs =
        Observation::broadcast([1; 16], seq, &topic(), b"crafted").with_observed_at(observed_at);
    obs.age_ms = age_ms;
    obs
}

/// Deliver `obs` to B's inbox as a broadcast from A.
fn inject(world: &Arc<Mutex<SharedInMemoryWorld>>, obs: &Observation) {
    let (a_id, b_id) = ([1u8; 16], [2u8; 16]);
    let ev = encode_frame(a_id, OBS_TAG, &(a_id, obs.clone()));
    world
        .lock()
        .unwrap()
        .inboxes
        .entry(b_id)
        .or_default()
        .push_back(ev);
}

/// Serve B on its own thread; returns the socket path.
fn serve_b(node_b: Node<LogStore, SharedInMemory>, clock: HostClock, dir: &Path) -> PathBuf {
    let socket = dir.join("b.sock");
    let path = socket.clone();
    let _b = thread::spawn(move || serve(node_b, path, clock, 5, WINDOW_MS).unwrap());
    socket
}

/// Pump A until `done(A, t)` holds, or panic after ~20 s.
fn pump_a_until(
    node_a: &mut Node<LogStore, SharedInMemory>,
    t: &mut u64,
    what: &str,
    mut done: impl FnMut(&Node<LogStore, SharedInMemory>, u64) -> bool,
) {
    for _ in 0..10_000 {
        let _ = node_a.poll(*t);
        if done(node_a, *t) {
            return;
        }
        *t += 2;
        thread::sleep(Duration::from_millis(2));
    }
    panic!("timed out waiting for {what}");
}

/// Make an observation on B over IPC, wait for it to reach A, and return the
/// `observed_at` B stamped it with.
fn b_stamp(
    socket_b: &Path,
    node_a: &mut Node<LogStore, SharedInMemory>,
    t: &mut u64,
    n: u32,
) -> Option<u64> {
    let mut stream = connect(socket_b);
    rpc(
        &mut stream,
        &Request::Observe {
            topic: hex_encode(&topic()),
            payload: hex_encode(format!("probe-{n}").as_bytes()),
        },
    );
    let id = match read_response(&mut stream) {
        Response::Observe { id } => id,
        other => panic!("expected Observe, got {other:?}"),
    };
    let mut found = None;
    pump_a_until(node_a, t, "B's observation to reach A", |a, now| {
        a.store().for_each_topic(&topic(), now, &mut |obs| {
            if hex_encode(&obs.id()) == id {
                found = Some(obs.observed_at);
            }
        });
        found.is_some()
    });
    found.expect("found")
}

/// Probe B until its observations carry a wall time; return the first one.
fn b_adopted_stamp(
    socket_b: &Path,
    node_a: &mut Node<LogStore, SharedInMemory>,
    t: &mut u64,
) -> u64 {
    for n in 0..50 {
        if let Some(at) = b_stamp(socket_b, node_a, t, n) {
            return at;
        }
        thread::sleep(Duration::from_millis(WINDOW_MS / 2));
    }
    panic!("B never adopted a wall time");
}

/// With a trusted clock, an IPC `Observe` through `serve` produces an
/// observation carrying the host's wall time (R4: once trusted, every
/// observation is stamped).
#[test]
fn observe_has_observed_at_after_trusted_time() {
    let dir = unique_dir("after");
    let (mut node_a, node_b, _) = build_pair(&dir);
    let mut clock_b = HostClock::start();
    clock_b.anchor_wall(W, clock_b.now());
    let socket_b = serve_b(node_b, clock_b, &dir);

    let mut t = 0;
    let at = b_stamp(&socket_b, &mut node_a, &mut t, 0).expect("stamped once trusted");
    assert!(
        (W..W + 60_000).contains(&at),
        "stamp is the anchored wall plus elapsed ticks: {at}"
    );
}

/// Without a trusted clock (and no peer time to adopt), an IPC `Observe`
/// through `serve` carries `observed_at = None` (R4: an observation made while
/// quarantined is un-stamped).
#[test]
fn observe_has_no_observed_at_before_trusted_time() {
    let dir = unique_dir("before");
    let (mut node_a, node_b, _) = build_pair(&dir);
    let socket_b = serve_b(node_b, HostClock::start(), &dir);

    let mut t = 0;
    assert_eq!(b_stamp(&socket_b, &mut node_a, &mut t, 0), None);
}

/// Instance B has no trusted time and a log that already holds one
/// quarantined (held) observation addressed to A. A broadcasts an observation
/// carrying a wall time. B's own host logic — not the test — derives an
/// estimate from it, adopts it once the window closes, and stamps its later
/// observations with it. Adopting runs the unknown-downtime fold, which
/// expires B's held addressed observation, so it never reaches A.
#[test]
fn time_from_peer() {
    let dir = unique_dir("peer");
    let (mut node_a, node_b, _) = build_pair_with_held(&dir);
    let socket_b = serve_b(node_b, HostClock::start(), &dir);

    // A observes at tick 0 with a trusted wall time; the exchange carries it
    // to B.
    node_a
        .observe(&topic(), &payload(), Some(W), 0)
        .expect("observe");
    let mut t = 0;
    let at = b_adopted_stamp(&socket_b, &mut node_a, &mut t);
    assert!(
        (W..W + 60_000).contains(&at),
        "B's stamp follows A's wall time: {at}"
    );
    // The held observation was addressed to A; the fold expired it, so it is
    // never delivered.
    let mut held_seen = false;
    node_a.store().for_each_topic(&topic(), t, &mut |obs| {
        held_seen |= obs.payload.as_slice() == b"held";
    });
    assert!(!held_seen, "B's held addressed observation reached A");
}

/// Decision J's window: the host doesn't adopt the first plausible estimate.
/// A first observation gives an estimate of about `W`; a second, arriving
/// inside the window, gives about `W + 6000`. The host adopts the later,
/// larger one.
#[test]
fn window_adopts_a_later_larger_estimate() {
    let dir = unique_dir("window");
    let (mut node_a, node_b, world) = build_pair(&dir);
    let socket_b = serve_b(node_b, HostClock::start(), &dir);

    inject(&world, &crafted(1, W - 1_000, 1_000)); // estimate ~ W
    thread::sleep(Duration::from_millis(WINDOW_MS / 3));
    inject(&world, &crafted(2, W + 6_000, 0)); // estimate ~ W + 6000

    let mut t = 0;
    let at = b_adopted_stamp(&socket_b, &mut node_a, &mut t);
    assert!(
        (W + 6_000..W + 66_000).contains(&at),
        "B adopted the larger estimate: stamp {at}, expected >= {}",
        W + 6_000
    );
}

/// Decision J's selection: of two observations arriving together, the one
/// with the smaller `age_ms` gives the *smaller* estimate (`W` against
/// `W + 6000`). Smallest-age selection would adopt `W`; the host adopts the
/// latest estimate.
#[test]
fn latest_estimate_wins_over_smallest_age() {
    let dir = unique_dir("latest");
    let (mut node_a, node_b, world) = build_pair(&dir);
    let socket_b = serve_b(node_b, HostClock::start(), &dir);

    inject(&world, &crafted(1, W, 0)); // smaller age, estimate ~ W
    inject(&world, &crafted(2, W + 5_000, 1_000)); // estimate ~ W + 6000

    let mut t = 0;
    let at = b_adopted_stamp(&socket_b, &mut node_a, &mut t);
    assert!(
        (W + 6_000..W + 66_000).contains(&at),
        "B adopted the latest estimate: stamp {at}, expected >= {}",
        W + 6_000
    );
}

/// B stores an observation with the given `observed_at`, keeps serving, and
/// never adopts a wall time from it.
fn assert_not_adopted(tag: &str, observed_at: u64) {
    let dir = unique_dir(tag);
    let (mut node_a, node_b, world) = build_pair(&dir);
    let socket_b = serve_b(node_b, HostClock::start(), &dir);

    let bad = crafted(1, observed_at, 0);
    inject(&world, &bad);
    // B stored it: it forwards it back to A, which doesn't hold it.
    let mut t = 0;
    pump_a_until(
        &mut node_a,
        &mut t,
        "B to forward the observation",
        |a, _| a.store().contains(&bad.id()),
    );
    // Give the window ample time to have closed had B accepted it.
    thread::sleep(Duration::from_millis(3 * WINDOW_MS));
    assert_eq!(
        b_stamp(&socket_b, &mut node_a, &mut t, 0),
        None,
        "B adopted an implausible wall time from observed_at = {observed_at}"
    );
}

/// B3: a forged `observed_at = u64::MAX` neither panics the host nor gets
/// adopted (the estimate saturates to `u64::MAX`, above the ceiling). The
/// host still answers IPC afterwards, which `b_stamp` exercises.
#[test]
fn forged_max_observed_at_is_not_adopted() {
    assert_not_adopted("forged", u64::MAX);
}

/// Decision J with B3's bounds: a forged `u64::MAX` alongside an honest
/// estimate doesn't hide it. The store drops the forged one before choosing
/// the latest, so B adopts the honest time.
#[test]
fn forged_estimate_does_not_hide_an_honest_one() {
    let dir = unique_dir("forged-honest");
    let (mut node_a, node_b, world) = build_pair(&dir);
    let socket_b = serve_b(node_b, HostClock::start(), &dir);

    inject(&world, &crafted(1, u64::MAX, 0));
    inject(&world, &crafted(2, W, 0));

    let mut t = 0;
    let at = b_adopted_stamp(&socket_b, &mut node_a, &mut t);
    assert!(
        (W..W + 60_000).contains(&at),
        "B adopted the honest estimate: stamp {at}, expected ~{W}"
    );
}

/// B3: an estimate below the floor (here 2001) is not adopted.
#[test]
fn below_floor_observed_at_is_not_adopted() {
    assert_not_adopted("floor", 1_000_000_000_000);
}
