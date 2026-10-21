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

/// Two [`Node`]s over a shared [`SharedInMemory`] world, each with its own
/// store, bound to ids `[1;16]` (A) and `[2;16]` (B).
fn build_pair(
    dir: &Path,
) -> (
    Node<LogStore, SharedInMemory>,
    Node<LogStore, SharedInMemory>,
) {
    let world = Arc::new(Mutex::new(SharedInMemoryWorld::new()));
    let (a_id, b_id) = ([1; 16], [2; 16]);
    let a_store = LogStore::open(&dir.join("a.store.log"), 0).expect("a store open");
    let b_store = LogStore::open(&dir.join("b.store.log"), 0).expect("b store open");
    let ta = SharedInMemory {
        me: a_id,
        their: b_id,
        world: world.clone(),
        contacted: false,
    };
    let tb = SharedInMemory {
        me: b_id,
        their: a_id,
        world,
        contacted: false,
    };
    (
        Node::new(a_id, a_store.first_seq(), a_store, ta),
        Node::new(b_id, b_store.first_seq(), b_store, tb),
    )
}

/// Same as [`build_pair`], but B's store opens with one already-held
/// (addressed) observation behind the unknown-wall-clock quarantine — as if B
/// had resumed with held data. The first open inserts the held observation and
/// is dropped so the record lands on disk; reopening loads it quarantined.
fn build_pair_with_held(
    dir: &Path,
) -> (
    Node<LogStore, SharedInMemory>,
    Node<LogStore, SharedInMemory>,
) {
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
        world,
        contacted: false,
    };
    (
        Node::new(a_id, a_store.first_seq(), a_store, ta),
        Node::new(b_id, b_store.first_seq(), b_store, tb),
    )
}

/// F2 propagation over a shared in-memory transport.
#[test]
fn f2_direct_propagation() {
    let dir = unique_dir("probe");
    let (mut node_a, mut node_b) = build_pair(&dir);
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
    let (node_a, node_b) = build_pair(&dir);

    let socket_a = dir.join("a.sock");
    let socket_b = dir.join("b.sock");
    let (clock_a, clock_b) = (HostClock::start(), HostClock::start());

    let socket_a_thread = socket_a.clone();
    let _a = thread::spawn(move || serve(node_a, socket_a_thread, clock_a, 20).unwrap());
    let socket_b_thread = socket_b.clone();
    let _b = thread::spawn(move || serve(node_b, socket_b_thread, clock_b, 20).unwrap());

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

// Trusted wall time (Task 10b): each test pins the brief's requirement verbatim.

/// With a trusted clock, an IPC `Observe` produces an observation carrying
/// `observed_at = Some(_)` (R4: once trusted, every observation is stamped).
#[test]
fn observe_has_observed_at_after_trusted_time() {
    let dir = unique_dir("after");
    let (mut node_a, _) = build_pair(&dir);

    // A is trusted (a "system clock"); wall_ms is Some once anchored.
    let mut clock_a = HostClock::start();
    let wall = 1_700_000_000_000u64;
    clock_a.anchor_wall(wall, 0);
    let observed_at = clock_a.wall_ms(0).expect("trusted");

    let id = node_a
        .observe(&topic(), &payload(), Some(observed_at), 0)
        .unwrap();
    let stored = node_a.store().get(&id, 0).expect("stored");
    assert_eq!(stored.observed_at, Some(wall));
}

/// Without a trusted clock, an IPC `Observe` carries `observed_at = None` (R4:
/// a observation made while quarantined is un-stamped).
#[test]
fn observe_has_no_observed_at_before_trusted_time() {
    let dir = unique_dir("before");
    let (mut node_a, _) = build_pair(&dir);

    // A has no trusted clock yet: wall_ms is None, so observed_at is None.
    let clock_a = HostClock::start();
    let observed_at = clock_a.wall_ms(0);
    assert_eq!(observed_at, None);

    let id = node_a
        .observe(&topic(), &payload(), observed_at, 0)
        .unwrap();
    let stored = node_a.store().get(&id, 0).expect("stored");
    assert_eq!(stored.observed_at, None);
}

/// Of two received observations with `observed_at`, the peer estimate comes from
/// the one with the smallest `age_ms` (the youngest), so the wall is pulled
/// toward the newest broadcast.
#[test]
fn smallest_age_wins() {
    let dir = unique_dir("young");
    let (mut node_a, mut node_b) = build_pair(&dir);

    // A observes two broadcasts on the same topic: an old one and one 5000 ms
    // newer. The younger one has the smaller age_ms, so the peer estimate must
    // be drawn from it.
    let mut clock_a = HostClock::start();
    let wall = 1_700_000_000_000u64;
    clock_a.anchor_wall(wall, 0);

    let old = node_a
        .observe(&topic(), b"old", clock_a.wall_ms(0), 0)
        .unwrap();
    let young = node_a
        .observe(&topic(), b"young", clock_a.wall_ms(5000), 5000)
        .unwrap();

    let poll_interval = 10u64;
    let mut t = 0u64;
    let mut both = false;
    for _ in 0..1000 {
        let _ = node_a.poll(t);
        let _ = node_b.poll(t);
        t += poll_interval;
        if node_b.store().contains(&old) && node_b.store().contains(&young) {
            both = true;
            break;
        }
    }
    assert!(both, "both broadcasts never reached B after 1000 rounds");

    let now = t;
    let est = node_b
        .store_mut()
        .peer_time_estimate(now)
        .expect("peer_time_estimate should find both observations");
    // The young observation's observed_at (wall + 5000) is newer, so the
    // estimate must sit near it -- well ahead of the old observation's wall.
    assert!(
        est > wall + 4000,
        "estimate must come from the younger observation: got {est}, expected > {}",
        wall + 4000
    );
}

/// Instance A has trusted time and observes; instance B has none and a log that
/// already holds one quarantined (held) observation. After an exchange B's
/// `set_wall_clock` has run with an estimate within one poll interval of A's
/// wall time, and B's held observation becomes visible.
#[test]
fn time_from_peer() {
    let dir = unique_dir("peer");
    let (mut node_a, mut node_b) = build_pair_with_held(&dir);

    // A is trusted (system-clock equivalent); anchor the wall at tick 0.
    let mut clock_a = HostClock::start();
    let wall = 1_700_000_000_000u64;
    clock_a.anchor_wall(wall, 0);

    // A observes a broadcast (carrying trusted wall time) at tick 0.
    let observed_at = clock_a.wall_ms(0);
    let held = node_a
        .observe(&topic(), &payload(), observed_at, 0)
        .unwrap();

    // Drive the exchange until A's broadcast lands in B's store. B's first poll
    // returns Contact, so it receives on the next round.
    let poll_interval = 10u64;
    let mut t = 0u64;
    let mut received = false;
    for _ in 0..1000 {
        let _ = node_a.poll(t);
        let _ = node_b.poll(t);
        if node_b.store().contains(&held) {
            received = true;
            break;
        }
        t += poll_interval;
    }
    assert!(received, "A's broadcast never reached B");

    // Estimate the true wall from the youngest non-quarantined observed_at-
    // bearing observation this boot (B's held obs is quarantined -> invisible),
    // which is A's broadcast.
    let now_b = t;
    let est = node_b
        .store_mut()
        .peer_time_estimate(now_b)
        .expect("peer_time_estimate should find A's broadcast");

    let a_wall_now = clock_a.wall_ms(now_b).expect("A trusted");
    assert!(
        est <= a_wall_now,
        "estimate never later than true time: {est} > {a_wall_now}"
    );
    assert!(
        a_wall_now.saturating_sub(est) <= poll_interval,
        "estimate within one poll interval: diff {} ms",
        a_wall_now - est
    );

    // The first trusted time anchors the wall once and runs the fold. Under the
    // (unknown) downtime B is in, the fold ages its held observation past
    // lifetime and lifts its quarantine (the "becomes visible" event): the fold
    // report counts exactly that one addressed observation expiring.
    let report = node_b.store_mut().set_wall_clock(est, now_b);
    assert_eq!(
        report.downtime_ms, None,
        "unknown downtime for a fresh store"
    );
    assert_eq!(
        report.expired_addressed, 1,
        "the held observation is aged past lifetime on trust"
    );
}
