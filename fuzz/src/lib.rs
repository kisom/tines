//! Shared fuzz + proptest harness that drives `tines-core`'s [`Node`] (over a
//! [`MemStore`] and a scripted [`Transport`]) through an ordered list of
//! [`Step`]s, checking the spec §7 properties after every step.
//!
//! The same module is compiled into two crates:
//!   * the `cargo-fuzz` target (`fuzz/fuzz_targets/node.rs`) via this package's
//!     lib, through [`run_input`]; and
//!   * the stable proptest twin (`tines-core/tests/fuzz_twin.rs`) via a
//!     `#[path]` include, through [`run_input`] on random bytes and through
//!     [`Harness`] on generated [`Step`]s.
//!
//! Both entry points converge on [`Harness::run_step`], so a property checked
//! here is checked by both — that is what "same harness" means.
//!
//! # Input format ([`run_input`])
//!
//! `[mode, mtu_lo, mtu_hi, steps...]`: the low bit of `mode` picks
//! [`Mode::Basic`] or [`Mode::Adversarial`]; the little-endian `u16` maps into
//! `MIN_MTU..=MAX_FRAME` (core's sweep bound up to the wire maximum); the rest
//! decodes into [`Step`]s with [`decode_steps`].
//!
//! # What is checked after every step
//!
//! * No panic (a panic on untrusted input is itself the failure).
//! * Every frame the node *attempts* to send is no larger than `mtu()` —
//!   recorded before the transport can refuse it, so the check can fail.
//! * No addressed observation is stored past its lifetime: each newly stored
//!   addressed id has `age_ms <= lifetime_ms`, and every addressed `Obs`
//!   frame the node sends does too.
//! * Every rejected frame is counted: per address, the drained
//!   `VersionMismatch`/`Malformed` drops equal the frames from that address
//!   that fail decode (exactly, unless the overflow counter was used; then
//!   the overflow makes up the difference), `IdentityChanged` drops never
//!   exceed the frames whose `from` differs from one the address sent
//!   before, and nothing is counted beyond the frames an address sent.
//! * Stored signatures round-trip: an observation stored from a whole `Obs`
//!   frame (or `observe`) carries exactly the signature it arrived with, and
//!   a stored observation's signature never changes afterwards.
//! * Acks only for stored observations (fleet-fixes F1): every id in an Ack
//!   the node sends is in its store.
//! * Bounded output per received summary (§10, F9): between two summaries
//!   from an address, the node offers it at most `SEND_QUEUE_CAP` distinct
//!   broadcast ids.
//!
//! [`Harness::check_peer_table_recovers`] adds the §10 peer-table-recovery
//! probe; the fuzz target runs it after every adversarial-mode input.
// `dead_code`: the twin includes this file as a module and uses only part of
// its API (the fuzz target uses the rest).
#![allow(dead_code, clippy::new_without_default)]

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use tines_core::conformance;
use tines_core::transport::FrameBuf;
use tines_core::{
    consts::{ACK_IDS_CAP, MAX_FRAME, MAX_PAYLOAD, MAX_SIG, MAX_TOPIC, SUMMARY_BYTES},
    DropReason, Event, MemStore, Node, NodeEvent, NodeId, Observation, ObservationId, SendError,
    SigAlgo, Signature, Store, Tick, Transport,
};

/// Wire address of a peer on the scripted transport. The exchange pins each
/// `Addr` to the `from` of its first frame (F6), so an attacker who controls
/// addresses can present many identities — exactly what adversarial mode tests.
pub type Addr = u64;

/// The node under test's id.
pub const ME: NodeId = [0u8; 16];

/// Polls run after each step, at most: each `Node::poll` sends at most one
/// frame, and the harness stops early once a poll sends nothing.
const POLLS_PER_STEP: usize = 8;

/// Which input vocabulary a run uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Contacts, losses, raw byte frames (mostly garbage), clock steps that
    /// may go backwards, and the node's own observations, from a handful of
    /// addresses.
    Basic,
    /// Well-formed frames built from the input — summaries with chosen bits,
    /// Obs and Frag frames with chosen ids, observers (including [`ME`]),
    /// seqs, ages, lifetimes, addressing and signatures, structured fragment
    /// streams that conflict, repeat or arrive out of order, Acks naming ids
    /// the node really sent — from more addresses than `MAX_PEERS`, with
    /// `from` values that change within a contact.
    Adversarial,
}

/// How a step names a node id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Who {
    /// The sending address's identity: the `from` of the first frame the
    /// harness sent from it, or a default derived from the address.
    Peer,
    /// The node under test.
    Me,
    /// An explicit id.
    Id(NodeId),
}

/// A signature to put on an observation, not necessarily valid: version `0`,
/// algorithm `0`, or an Ed25519 signature that isn't 64 bytes all fail
/// decode.
#[derive(Clone, Debug)]
pub struct SigSpec {
    /// Signature format version.
    pub version: u8,
    /// Raw algorithm byte (`1` is Ed25519).
    pub algo: u8,
    /// Signature bytes (truncated to `MAX_SIG`).
    pub bytes: Vec<u8>,
}

/// A well-formed observation sent from `addr`.
#[derive(Clone, Debug)]
pub struct ObsSpec {
    /// The sending address.
    pub addr: Addr,
    /// The frame's `from`.
    pub from: Who,
    /// The observation's `observer`.
    pub observer: Who,
    /// Observation sequence number.
    pub seq: u64,
    /// Age the observation arrives with.
    pub age_ms: u64,
    /// Observer wall-clock time.
    pub observed_at: Option<u64>,
    /// Topic bytes (truncated to `MAX_TOPIC`).
    pub topic: Vec<u8>,
    /// Payload bytes (truncated to `MAX_PAYLOAD`).
    pub payload: Vec<u8>,
    /// `None` = broadcast; `Some((to, lifetime_ms))` = addressed.
    pub dest: Option<(Who, u64)>,
    /// Signature, if any.
    pub sig: Option<SigSpec>,
}

/// Fragment order for a [`FragSpec`].
#[derive(Clone, Debug)]
pub enum FragOrder {
    /// Index 0, 1, ... count-1.
    InOrder,
    /// Index count-1 down to 0.
    Reverse,
    /// Exactly these indices (may repeat, skip, or exceed the count).
    Explicit(Vec<u8>),
}

/// A fragment stream of one observation, cut like a sender would cut it,
/// with optional lies.
#[derive(Clone, Debug)]
pub struct FragSpec {
    /// The observation (its `addr`/`from` are the stream's).
    pub obs: ObsSpec,
    /// Data bytes per fragment (at least 1).
    pub chunk: usize,
    /// Fragment order.
    pub order: FragOrder,
    /// Claim this id instead of the observation's real one.
    pub id: Option<ObservationId>,
    /// Claim this fragment count instead of the real one.
    pub count: Option<u8>,
}

/// A well-formed `Summary` frame.
#[derive(Clone, Debug)]
pub struct SpecSummary {
    /// The sending address.
    pub addr: Addr,
    /// The frame's `from`.
    pub from: Who,
    /// Bloom salt.
    pub salt: u32,
    /// Bloom bits (truncated to `SUMMARY_BYTES`).
    pub bits: Vec<u8>,
    /// How many identical copies to send back to back (replay).
    pub repeat: u8,
}

/// A well-formed `Ack` frame.
#[derive(Clone, Debug)]
pub struct AckSpec {
    /// The sending address.
    pub addr: Addr,
    /// The frame's `from`.
    pub from: Who,
    /// Indices into the ids the node has sent to `addr` so far (taken
    /// modulo their number; ignored while there are none).
    pub sent: Vec<u16>,
    /// Ids named outright (sent or not).
    pub raw: Vec<ObservationId>,
}

/// A single scripted action.
#[derive(Clone, Debug)]
pub enum Step {
    /// A `Contact` event for `addr`.
    Contact(Addr),
    /// A `Lost` event for `addr`.
    Lost(Addr),
    /// Raw bytes from `addr` (anything; usually not a valid frame).
    Frame(Addr, Vec<u8>),
    /// One well-formed `Obs` frame.
    Obs(ObsSpec),
    /// A stream of well-formed `Frag` frames.
    Frags(FragSpec),
    /// One or more identical well-formed `Summary` frames.
    Summarize(SpecSummary),
    /// One well-formed `Ack` frame.
    Ack(AckSpec),
    /// Move the clock by `delta` ms (negative goes backwards; saturates at 0).
    Advance(i64),
    /// The node observes a broadcast with a `size`-byte payload (mod
    /// `MAX_PAYLOAD + 1`).
    Ourselves {
        /// Payload length.
        size: u16,
    },
    /// The node observes an addressed message.
    OurselvesTo {
        /// Recipient.
        to: Who,
        /// Lifetime in ms.
        lifetime_ms: u64,
        /// Payload length (mod `MAX_PAYLOAD + 1`).
        size: u16,
    },
}

// ---------------------------------------------------------------------------
// Scripted transport. Its queues are shared with the harness through
// `Rc<RefCell>`, so the harness pushes inbound events and reads every
// attempted send while `Node` owns the transport.
// ---------------------------------------------------------------------------

struct Queues {
    inbound: RefCell<VecDeque<Event<Addr>>>,
    /// Every frame `send` was called with, oversized ones included.
    attempted: RefCell<Vec<(Addr, Vec<u8>)>>,
}

/// A [`Transport`] whose queues the harness fills and drains directly.
pub struct ScriptedTransport {
    mtu: usize,
    q: Rc<Queues>,
}

impl Transport for ScriptedTransport {
    type Addr = Addr;

    fn poll(&mut self) -> Option<Event<Self::Addr>> {
        self.q.inbound.borrow_mut().pop_front()
    }

    /// Records the frame *before* checking it, so the harness's MTU check
    /// sees an oversized frame even though the send is refused.
    fn send(&mut self, to: &Self::Addr, frame: &[u8]) -> Result<(), SendError> {
        self.q.attempted.borrow_mut().push((*to, frame.to_vec()));
        if frame.len() > self.mtu {
            return Err(SendError::TooLarge);
        }
        Ok(())
    }

    fn mtu(&self) -> usize {
        self.mtu
    }
}

// ---------------------------------------------------------------------------
// Frame builders (through core's hidden `conformance` encoders).
// ---------------------------------------------------------------------------

fn encode_with(enc: impl FnOnce(&mut [u8]) -> Option<usize>) -> Option<Vec<u8>> {
    let mut buf = [0u8; MAX_FRAME];
    enc(&mut buf).map(|n| buf[..n].to_vec())
}

fn sig_algo(raw: u8) -> SigAlgo {
    if raw == 1 {
        SigAlgo::Ed25519
    } else {
        SigAlgo::Unknown(raw)
    }
}

/// Per-address counters and memory the checks reason about.
#[derive(Default)]
struct PeerBook {
    /// Frames pushed from this address (all reach the exchange).
    frames: u64,
    /// Of those, frames that fail decode.
    expect_decode_drops: u64,
    /// Of those, decodable frames whose `from` differs from one this address
    /// sent earlier — an upper bound on `IdentityChanged` drops.
    identity_candidates: u64,
    /// Distinct decoded `from`s seen from this address.
    froms: Vec<NodeId>,
    /// Drained `VersionMismatch`/`Malformed` counts.
    seen_decode_drops: u64,
    /// Drained `IdentityChanged` counts.
    seen_identity_drops: u64,
    /// Drained counts for any other reason.
    seen_other_drops: u64,
    /// Ids the node has sent this address (Obs or Frag), in first-send order.
    sent: Vec<ObservationId>,
    /// Broadcast ids offered since this address's last summary.
    offered: BTreeSet<ObservationId>,
}

/// Drives a single [`Node`] and checks it after every step.
pub struct Harness {
    node: Node<MemStore, ScriptedTransport>,
    mode: Mode,
    mtu: usize,
    q: Rc<Queues>,
    now: Tick,
    /// The latest tick the node has been given. Steps can move `now`
    /// backwards, but the node's idle clock counts from the latest tick.
    max_now: Tick,
    peers: BTreeMap<Addr, PeerBook>,
    /// Drops counted in the overflow slot (`addr: None`).
    overflow_drops: u64,
    /// Ids in the store after the last step, with the signature each was
    /// first stored with.
    stored: BTreeMap<ObservationId, Option<Signature>>,
    /// Observations this step injected whole (one `Obs` frame or an
    /// `observe` call): if one of these ids is newly stored, it must carry
    /// exactly this signature.
    whole: Vec<(ObservationId, Option<Signature>)>,
    /// `Delivered` events drained so far.
    delivered: u64,
}

impl Harness {
    /// A harness for node [`ME`] at `mtu`, clocked at 0.
    pub fn new(mode: Mode, mtu: usize) -> Self {
        let q = Rc::new(Queues {
            inbound: RefCell::new(VecDeque::new()),
            attempted: RefCell::new(Vec::new()),
        });
        Harness {
            node: Node::new(
                ME,
                0,
                MemStore::new(),
                ScriptedTransport { mtu, q: q.clone() },
            ),
            mode,
            mtu,
            q,
            now: 0,
            max_now: 0,
            peers: BTreeMap::new(),
            overflow_drops: 0,
            stored: BTreeMap::new(),
            whole: Vec::new(),
            delivered: 0,
        }
    }

    /// The input mode this harness was built for.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Current simulated time in ms.
    pub fn now(&self) -> Tick {
        self.now
    }

    /// The backing store.
    pub fn store(&self) -> &MemStore {
        self.node.store()
    }

    /// `Delivered` events drained so far.
    pub fn delivered(&self) -> u64 {
        self.delivered
    }

    /// Distinct broadcast ids the node has offered `addr` since `addr`'s
    /// last summary.
    pub fn offered_since_summary(&self, addr: Addr) -> usize {
        self.peers.get(&addr).map_or(0, |p| p.offered.len())
    }

    /// The identity a [`Who::Peer`] from `addr` resolves to.
    pub fn identity(&self, addr: Addr) -> NodeId {
        if let Some(first) = self.peers.get(&addr).and_then(|p| p.froms.first()) {
            return *first;
        }
        let mut id = [0xA0u8; 16];
        id[..8].copy_from_slice(&addr.to_le_bytes());
        id
    }

    fn who(&self, addr: Addr, who: Who) -> NodeId {
        match who {
            Who::Peer => self.identity(addr),
            Who::Me => ME,
            Who::Id(id) => id,
        }
    }

    fn build_observation(&self, spec: &ObsSpec) -> Observation {
        let observer = self.who(spec.addr, spec.observer);
        let topic = &spec.topic[..spec.topic.len().min(MAX_TOPIC)];
        let payload = &spec.payload[..spec.payload.len().min(MAX_PAYLOAD)];
        let mut obs = match spec.dest {
            None => Observation::broadcast(observer, spec.seq, topic, payload),
            Some((to, lifetime_ms)) => Observation::addressed(
                observer,
                spec.seq,
                topic,
                payload,
                self.who(spec.addr, to),
                lifetime_ms,
            ),
        };
        obs.age_ms = spec.age_ms;
        obs.observed_at = spec.observed_at;
        obs.sig = spec.sig.as_ref().and_then(|s| {
            let bytes = &s.bytes[..s.bytes.len().min(MAX_SIG)];
            conformance::signature(s.version, sig_algo(s.algo), bytes)
        });
        obs
    }

    /// Queue `bytes` from `addr` and record what the exchange must do with
    /// it. Frames over `MAX_FRAME` can't be a transport event, so they are
    /// skipped.
    fn push(&mut self, addr: Addr, bytes: Vec<u8>) {
        let Ok(buf) = FrameBuf::from_slice(&bytes) else {
            return;
        };
        let book = self.peers.entry(addr).or_default();
        book.frames += 1;
        match conformance::decoded_sender(&bytes) {
            None => book.expect_decode_drops += 1,
            Some(from) => {
                if book.froms.iter().any(|f| *f != from) {
                    book.identity_candidates += 1;
                }
                if !book.froms.contains(&from) {
                    book.froms.push(from);
                }
                if conformance::is_summary(&bytes) {
                    book.offered.clear();
                }
            }
        }
        self.q
            .inbound
            .borrow_mut()
            .push_back(Event::Frame(addr, buf));
    }

    /// Queue one step's input (without polling the node).
    pub fn step(&mut self, step: &Step) {
        match step {
            Step::Contact(addr) => self.q.inbound.borrow_mut().push_back(Event::Contact(*addr)),
            Step::Lost(addr) => self.q.inbound.borrow_mut().push_back(Event::Lost(*addr)),
            Step::Frame(addr, raw) => {
                if let Some(obs) = conformance::obs_of(raw) {
                    self.whole.push((obs.id(), obs.sig.clone()));
                }
                self.push(*addr, raw.clone());
            }
            Step::Obs(spec) => {
                let from = self.who(spec.addr, spec.from);
                let obs = self.build_observation(spec);
                if let Some(bytes) = encode_with(|out| conformance::encode_obs(from, &obs, out)) {
                    if conformance::decoded_sender(&bytes).is_some() {
                        self.whole.push((obs.id(), obs.sig.clone()));
                    }
                    self.push(spec.addr, bytes);
                }
            }
            Step::Frags(spec) => self.push_frags(spec),
            Step::Summarize(spec) => {
                let from = self.who(spec.addr, spec.from);
                let bits = &spec.bits[..spec.bits.len().min(SUMMARY_BYTES)];
                if let Some(bytes) =
                    encode_with(|out| conformance::encode_summary(from, spec.salt, bits, out))
                {
                    for _ in 0..spec.repeat.max(1) {
                        self.push(spec.addr, bytes.clone());
                    }
                }
            }
            Step::Ack(spec) => {
                let from = self.who(spec.addr, spec.from);
                let sent = self
                    .peers
                    .get(&spec.addr)
                    .map(|p| p.sent.clone())
                    .unwrap_or_default();
                let mut ids: Vec<ObservationId> = Vec::new();
                if !sent.is_empty() {
                    ids.extend(spec.sent.iter().map(|i| sent[*i as usize % sent.len()]));
                }
                ids.extend(spec.raw.iter().copied());
                ids.truncate(ACK_IDS_CAP);
                if let Some(bytes) = encode_with(|out| conformance::encode_ack(from, &ids, out)) {
                    self.push(spec.addr, bytes);
                }
            }
            Step::Advance(delta) => {
                self.now = if *delta >= 0 {
                    self.now.saturating_add(delta.unsigned_abs())
                } else {
                    self.now.saturating_sub(delta.unsigned_abs())
                };
                self.max_now = self.max_now.max(self.now);
            }
            Step::Ourselves { size } => {
                let payload = vec![0x5A; *size as usize % (MAX_PAYLOAD + 1)];
                if let Ok(id) = self.node.observe(b"self", &payload, None, self.now) {
                    self.whole.push((id, None));
                }
            }
            Step::OurselvesTo {
                to,
                lifetime_ms,
                size,
            } => {
                let to = match to {
                    Who::Peer => [0xA0; 16],
                    Who::Me => ME,
                    Who::Id(id) => *id,
                };
                let payload = vec![0xA5; *size as usize % (MAX_PAYLOAD + 1)];
                if let Ok(id) =
                    self.node
                        .observe_to(to, *lifetime_ms, b"self/to", &payload, None, self.now)
                {
                    self.whole.push((id, None));
                }
            }
        }
    }

    fn push_frags(&mut self, spec: &FragSpec) {
        let from = self.who(spec.obs.addr, spec.obs.from);
        let obs = self.build_observation(&spec.obs);
        let mut buf = vec![0u8; MAX_FRAME * 2];
        let Some(len) = conformance::encode_observation(&obs, &mut buf) else {
            return;
        };
        let bytes = &buf[..len];
        let chunk = spec.chunk.clamp(1, MAX_FRAME);
        let real_count = len.div_ceil(chunk).max(1);
        let count = spec.count.unwrap_or(real_count.min(u8::MAX as usize) as u8);
        let id = spec.id.unwrap_or_else(|| obs.id());
        let order: Vec<u8> = match &spec.order {
            FragOrder::InOrder => (0..count).collect(),
            FragOrder::Reverse => (0..count).rev().collect(),
            FragOrder::Explicit(v) => v.clone(),
        };
        for index in order {
            let start = (index as usize).saturating_mul(chunk).min(len);
            let end = start.saturating_add(chunk).min(len);
            let data = &bytes[start..end];
            if let Some(frame) =
                encode_with(|out| conformance::encode_frag(from, id, index, count, data, out))
            {
                self.push(spec.obs.addr, frame);
            }
        }
    }

    /// Poll the node until it stops sending (at most `POLLS_PER_STEP`
    /// times), folding drained events into the counters and checking every
    /// attempted send.
    pub fn pump(&mut self) {
        for _ in 0..POLLS_PER_STEP {
            let before = self.q.attempted.borrow().len();
            let _ = self.node.poll(self.now);
            self.drain_events();
            if self.q.attempted.borrow().len() == before && self.q.inbound.borrow().is_empty() {
                break;
            }
        }
        self.check_sends();
    }

    fn drain_events(&mut self) {
        for ev in self.node.drain_events() {
            match ev {
                NodeEvent::Dropped {
                    addr: Some(addr),
                    reason,
                    count,
                } => {
                    let book = self.peers.entry(addr).or_default();
                    let count = u64::from(count);
                    match reason {
                        DropReason::VersionMismatch { .. } | DropReason::Malformed => {
                            book.seen_decode_drops += count
                        }
                        DropReason::IdentityChanged => book.seen_identity_drops += count,
                        _ => book.seen_other_drops += count,
                    }
                }
                NodeEvent::Dropped {
                    addr: None, count, ..
                } => self.overflow_drops += u64::from(count),
                NodeEvent::Delivered { .. } => self.delivered += 1,
                _ => {}
            }
        }
    }

    /// The MTU, Ack-only-for-stored, forwarded-lifetime and per-summary
    /// output checks over every frame attempted since the last call.
    fn check_sends(&mut self) {
        let attempted: Vec<(Addr, Vec<u8>)> = self.q.attempted.borrow_mut().drain(..).collect();
        for (to, frame) in attempted {
            assert!(
                frame.len() <= self.mtu,
                "sent a {}-byte frame over mtu {}",
                frame.len(),
                self.mtu
            );
            let store = self.node.store();
            let mut acked_unstored = None;
            if conformance::ack_of(&frame, &mut |id| {
                if !store.contains(id) {
                    acked_unstored = Some(*id);
                }
            }) {
                assert_eq!(
                    acked_unstored, None,
                    "acked an id the store does not hold (F1)"
                );
                continue;
            }
            let offer = if let Some(obs) = conformance::obs_of(&frame) {
                if let Some(dest) = &obs.dest {
                    assert!(
                        obs.age_ms <= dest.lifetime_ms,
                        "forwarded an addressed observation past its lifetime \
                         (age {} > lifetime {})",
                        obs.age_ms,
                        dest.lifetime_ms
                    );
                }
                Some((obs.id(), obs.dest.is_none()))
            } else if let Some((id, index, _)) = conformance::frag_of(&frame) {
                let broadcast =
                    index == 0 && store.get(&id, self.now).is_some_and(|o| o.dest.is_none());
                Some((id, broadcast))
            } else {
                None
            };
            if let Some((id, broadcast)) = offer {
                let book = self.peers.entry(to).or_default();
                if !book.sent.contains(&id) {
                    book.sent.push(id);
                }
                if broadcast {
                    book.offered.insert(id);
                    assert!(
                        book.offered.len() <= conformance::SEND_QUEUE_CAP,
                        "offered {} broadcast ids to {to} on one summary (F9 bound {})",
                        book.offered.len(),
                        conformance::SEND_QUEUE_CAP
                    );
                }
            }
        }
    }

    /// Every rejected frame is counted (§7), per address.
    fn check_drops(&self) {
        let mut expect_decode = 0u64;
        let mut seen_decode = 0u64;
        let mut seen_all = 0u64;
        let mut frames = 0u64;
        for (addr, b) in &self.peers {
            assert!(
                b.seen_decode_drops <= b.expect_decode_drops,
                "{addr}: {} decode drops counted, only {} frames failed decode",
                b.seen_decode_drops,
                b.expect_decode_drops
            );
            assert!(
                b.seen_identity_drops <= b.identity_candidates,
                "{addr}: {} IdentityChanged drops counted, only {} frames changed `from`",
                b.seen_identity_drops,
                b.identity_candidates
            );
            let all = b.seen_decode_drops + b.seen_identity_drops + b.seen_other_drops;
            assert!(
                all <= b.frames,
                "{addr}: {all} drops counted for {} frames",
                b.frames
            );
            if self.overflow_drops == 0 {
                assert_eq!(
                    b.seen_decode_drops, b.expect_decode_drops,
                    "{addr}: a frame that failed decode was not counted"
                );
            }
            expect_decode += b.expect_decode_drops;
            seen_decode += b.seen_decode_drops;
            seen_all += all;
            frames += b.frames;
        }
        assert!(
            expect_decode <= seen_decode + self.overflow_drops,
            "{expect_decode} frames failed decode, only {seen_decode} counted plus {} overflow",
            self.overflow_drops
        );
        assert!(
            seen_all + self.overflow_drops <= frames,
            "more drops counted than frames received"
        );
    }

    /// Lifetime and signature checks over the store.
    fn check_store(&mut self) {
        let mut ids = Vec::new();
        self.node.store().for_each_id(&mut |id| ids.push(*id));
        let whole = std::mem::take(&mut self.whole);
        for id in ids {
            let obs = self
                .node
                .store()
                .get(&id, self.now)
                .expect("for_each_id yields stored ids");
            match self.stored.get(&id) {
                Some(first) => {
                    assert_eq!(&obs.sig, first, "a stored observation's signature changed")
                }
                None => {
                    if let Some(dest) = &obs.dest {
                        assert!(
                            obs.age_ms <= dest.lifetime_ms,
                            "stored an addressed observation past its lifetime \
                             (age {} > lifetime {})",
                            obs.age_ms,
                            dest.lifetime_ms
                        );
                    }
                    if let Some((_, sig)) = whole.iter().find(|(w, _)| *w == id) {
                        assert_eq!(
                            &obs.sig, sig,
                            "a stored observation's signature did not round-trip"
                        );
                    }
                    self.stored.insert(id, obs.sig.clone());
                }
            }
        }
    }

    /// Run one step and every check.
    pub fn run_step(&mut self, step: &Step) {
        self.step(step);
        self.pump();
        self.check_drops();
        self.check_store();
    }

    /// Run every step, checking after each.
    pub fn run(&mut self, steps: &[Step]) {
        for step in steps {
            self.run_step(step);
        }
    }

    /// §10 peer-table recovery (F8): after `PEER_IDLE_MS` of silence from
    /// every peer, a frame from a never-seen address still gets a slot, so
    /// the node stores the observation it carries.
    pub fn check_peer_table_recovers(&mut self) {
        // Idle time counts from each peer's last frame, so measure from the
        // latest tick, not the current one (a step may have moved `now`
        // backwards; ages saturate rather than go negative). Ticks this close
        // to u64::MAX can't advance by PEER_IDLE_MS, so there's nothing to
        // probe.
        let Some(now) = self.max_now.checked_add(conformance::PEER_IDLE_MS + 1) else {
            return;
        };
        self.now = now;
        self.max_now = now;
        self.pump();
        let addr = Addr::MAX;
        let obs = Observation::broadcast([0xEE; 16], u64::MAX, b"recovery", b"probe");
        let id = obs.id();
        let already = self.node.store().contains(&id);
        let bytes = encode_with(|out| conformance::encode_obs([0xEE; 16], &obs, out))
            .expect("a small Obs frame fits MAX_FRAME");
        self.push(addr, bytes);
        self.pump();
        assert!(
            already || self.node.store().contains(&id),
            "after {} ms of silence a new address still got no peer slot (F8)",
            conformance::PEER_IDLE_MS + 1
        );
    }
}

// ---------------------------------------------------------------------------
// Input decoding.
// ---------------------------------------------------------------------------

/// Bounds-checked reader over fuzz input; every read returns `None` once the
/// input runs out, so decoding stops instead of panicking.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let out = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|b| u16::from_le_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Option<u32> {
        let b = self.take(4)?;
        Some(u32::from_le_bytes(b.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        let b = self.take(8)?;
        Some(u64::from_le_bytes(b.try_into().ok()?))
    }

    fn id(&mut self) -> Option<[u8; 16]> {
        let b = self.take(16)?;
        b.try_into().ok()
    }

    /// `Who`: 0 = the peer, 1 = the node under test, else 16 explicit bytes.
    fn who(&mut self) -> Option<Who> {
        Some(match self.u8()? % 3 {
            0 => Who::Peer,
            1 => Who::Me,
            _ => Who::Id(self.id()?),
        })
    }

    /// A `len`-byte run of one fill byte: cheap max-size fields.
    fn filled(&mut self, len: usize) -> Option<Vec<u8>> {
        let fill = self.u8()?;
        Some(vec![fill; len])
    }
}

/// How many addresses each mode draws from: basic stays small so contacts
/// interact; adversarial goes well past `MAX_PEERS`.
fn addr_of(mode: Mode, byte: u8) -> Addr {
    match mode {
        Mode::Basic => Addr::from(byte % 4),
        Mode::Adversarial => Addr::from(byte) % (3 * conformance::MAX_PEERS as Addr),
    }
}

fn read_obs(c: &mut Cursor, mode: Mode) -> Option<ObsSpec> {
    let addr = addr_of(mode, c.u8()?);
    let from = c.who()?;
    let observer = c.who()?;
    let seq = c.u64()?;
    let age_ms = u64::from(c.u32()?);
    let observed_at = match c.u8()? % 2 {
        0 => None,
        _ => Some(c.u64()?),
    };
    let topic_len = usize::from(c.u8()?) % (MAX_TOPIC + 1);
    let topic = c.filled(topic_len)?;
    let payload_len = usize::from(c.u16()?) % (MAX_PAYLOAD + 1);
    let payload = c.filled(payload_len)?;
    let dest = match c.u8()? % 2 {
        0 => None,
        _ => Some((c.who()?, u64::from(c.u32()?))),
    };
    let sig = match c.u8()? % 2 {
        0 => None,
        _ => {
            let version = c.u8()?;
            let algo = c.u8()?;
            let len = usize::from(c.u8()?) % (MAX_SIG + 1);
            Some(SigSpec {
                version,
                algo,
                bytes: c.filled(len)?,
            })
        }
    };
    Some(ObsSpec {
        addr,
        from,
        observer,
        seq,
        age_ms,
        observed_at,
        topic,
        payload,
        dest,
        sig,
    })
}

fn read_step(c: &mut Cursor, mode: Mode) -> Option<Step> {
    let common = 7u8;
    let tags = match mode {
        Mode::Basic => common,
        Mode::Adversarial => common + 4,
    };
    Some(match c.u8()? % tags {
        0 => Step::Contact(addr_of(mode, c.u8()?)),
        1 => Step::Lost(addr_of(mode, c.u8()?)),
        // Small clock steps (±~524 s, in 16 ms units), either direction.
        2 => Step::Advance(i64::from(c.u16()? as i16) * 16),
        3 => Step::Advance(c.u64()? as i64),
        4 => Step::Ourselves { size: c.u16()? },
        5 => Step::OurselvesTo {
            to: c.who()?,
            lifetime_ms: u64::from(c.u32()?),
            size: c.u16()?,
        },
        6 => {
            let addr = addr_of(mode, c.u8()?);
            let len = usize::from(c.u16()?) % (MAX_FRAME + 1);
            let len = len.min(c.data.len() - c.pos);
            Step::Frame(addr, c.take(len)?.to_vec())
        }
        7 => Step::Obs(read_obs(c, mode)?),
        8 => {
            let obs = read_obs(c, mode)?;
            let chunk = usize::from(c.u16()?);
            let order = match c.u8()? % 3 {
                0 => FragOrder::InOrder,
                1 => FragOrder::Reverse,
                _ => {
                    let n = usize::from(c.u8()? % 16);
                    FragOrder::Explicit(c.take(n)?.to_vec())
                }
            };
            let id = match c.u8()? % 4 {
                0 => Some(c.id()?),
                _ => None,
            };
            let count = match c.u8()? {
                0 => None,
                n => Some(n),
            };
            Step::Frags(FragSpec {
                obs,
                chunk,
                order,
                id,
                count,
            })
        }
        9 => {
            let addr = addr_of(mode, c.u8()?);
            let from = c.who()?;
            let salt = c.u32()?;
            let len = usize::from(c.u16()?) % (SUMMARY_BYTES + 1);
            let bits = match c.u8()? % 3 {
                0 => vec![0x00; len],
                1 => vec![0xFF; len],
                _ => c.filled(len)?,
            };
            Step::Summarize(SpecSummary {
                addr,
                from,
                salt,
                bits,
                repeat: c.u8()? % 4 + 1,
            })
        }
        _ => {
            let addr = addr_of(mode, c.u8()?);
            let from = c.who()?;
            let n = c.u8()? % 16;
            let sent = (0..n).map(|_| c.u16()).collect::<Option<Vec<_>>>()?;
            let r = c.u8()? % 4;
            let raw = (0..r).map(|_| c.id()).collect::<Option<Vec<_>>>()?;
            Step::Ack(AckSpec {
                addr,
                from,
                sent,
                raw,
            })
        }
    })
}

/// Decode `data` into steps for `mode`, stopping at the first step the input
/// can't complete. Never panics.
pub fn decode_steps(mode: Mode, data: &[u8]) -> Vec<Step> {
    let mut c = Cursor { data, pos: 0 };
    let mut steps = Vec::new();
    while c.pos < data.len() {
        match read_step(&mut c, mode) {
            Some(step) => steps.push(step),
            None => break,
        }
    }
    steps
}

/// Decode a whole fuzz input (see the module docs for the format) and run it,
/// returning the harness for further checks. `None` if the input is too short
/// to name a mode and MTU.
pub fn run_input(data: &[u8]) -> Option<Harness> {
    let (&mode, rest) = data.split_first()?;
    let (mtu, rest) = rest.split_first_chunk::<2>()?;
    let mode = if mode & 1 == 0 {
        Mode::Basic
    } else {
        Mode::Adversarial
    };
    let span = MAX_FRAME - conformance::MIN_MTU + 1;
    let mtu = conformance::MIN_MTU + usize::from(u16::from_le_bytes(*mtu)) % span;
    let mut h = Harness::new(mode, mtu);
    h.run(&decode_steps(mode, rest));
    Some(h)
}
