//! Exchange state machine for tines-core.
//!
//! [`Exchange`] tracks the peers this node knows about and drives the
//! content-defined gossip protocol: acknowledging received observations,
//! summarizing what we hold, requesting what peers are missing, and delivering
//! observations (fragmenting them across the wire when they exceed the MTU).
//!
//! Everything here is `#![no_std]`-clean and touches **no allocation**: peers
//! live in a fixed [`heapless`] array, observation send queues are
//! [`heapless`] vectors, and fragment buffers are stack arrays. No path reads
//! untrusted wire bytes without first going through [`Frame::decode`], which
//! turns any malformed input into an `Err` that the caller drops.

use crate::consts;
use crate::{Bloom, Event, Frame, NodeId, Observation, ObservationId, Reassembler, Store, Tick};
use heapless::Vec;

/// The maximum number of fragments an observation may be split into.
///
/// Matches [`crate::fragment`]: the reassembler tracks up to 64 indices via a
/// bitset, so `count` is capped at 64 on both ends.
const MAX_FRAGMENTS: usize = 64;

/// A single peer the [`Exchange`] is tracking.
pub struct Peer<A> {
    /// Wire address of the peer.
    pub addr: A,
    /// Node id learned from the first frame received from this peer.
    pub id: Option<NodeId>,
    /// The peer's last-sent summary, if any. Used to queue only what they lack.
    pub their_summary: Option<Bloom>,
    /// Observations to send to this peer, youngest (lowest current age) first;
    /// the front (`index 0`) is the next id to emit.
    pub send_queue: Vec<ObservationId, { consts::SEND_QUEUE_CAP }>,
    /// Tick of the last summary we sent to this peer, if any.
    pub last_summary_sent: Option<Tick>,
    /// Reassembler for inbound fragmented streams from this peer.
    pub reassembler: Reassembler,
    /// An observation currently mid-fragmentation: `(id, next fragment index)`.
    pub in_flight: Option<(ObservationId, u8)>,
}

impl<A> Peer<A> {
    /// Creates a peer with no known id, no summary, empty queue, and a fresh
    /// reassembler — `last_summary_sent` is `None` so a summary is emitted on
    /// the next [`Exchange::next_frame`].
    fn new(addr: A) -> Self {
        Peer {
            addr,
            id: None,
            their_summary: None,
            send_queue: Vec::new(),
            last_summary_sent: None,
            reassembler: Reassembler::new(),
            in_flight: None,
        }
    }
}

/// The exchange state machine, tracking up to [`consts::MAX_PEERS`] peers.
pub struct Exchange<A> {
    me: NodeId,
    salt: u32,
    peers: Vec<Peer<A>, { consts::MAX_PEERS }>,
    rr: usize,
}

impl<A: Clone + PartialEq + core::fmt::Debug> Exchange<A> {
    /// Creates an empty exchange for local node `me`. The salt starts at 0.
    pub fn new(me: NodeId) -> Self {
        Exchange {
            me,
            salt: 0,
            peers: Vec::new(),
            rr: 0,
        }
    }

    /// Handles one transport event.
    ///
    /// Returns `Some(obs)` when an inbound observation is newly stored (the
    /// caller may react); otherwise `None`.
    pub fn on_event<S: Store>(
        &mut self,
        ev: Event<A>,
        store: &mut S,
        now: Tick,
    ) -> Option<Observation> {
        match ev {
            Event::Contact(addr) => {
                self.add_peer(addr);
                None
            }
            Event::Lost(addr) => {
                self.remove_peer(&addr);
                None
            }
            Event::Frame(addr, bytes) => self.on_frame(addr, &bytes, store, now),
        }
    }

    /// Produces at most one outbound frame, writing its bytes into `out` and
    /// returning the destination address and byte count.
    ///
    /// Priority: (1) send a Summary to any peer due for one, then (2) emit one
    /// Obs/Frag frame from a peer via round-robin.
    pub fn next_frame<S: Store>(
        &mut self,
        store: &S,
        mtu: usize,
        now: Tick,
        out: &mut [u8],
    ) -> Option<(A, usize)> {
        let me = self.me;
        // Priority 1: a summary is due for some peer.
        for peer in self.peers.iter_mut() {
            if !summary_due(peer, now) {
                continue;
            }
            let mut bloom = Bloom::new(self.salt);
            store.for_each_broadcast_id(&mut |id| bloom.insert(id));
            let bits = match Vec::<u8, { consts::SUMMARY_BYTES }>::from_slice(bloom.bits()) {
                Ok(bits) => bits,
                Err(_) => return None,
            };
            let frame = Frame::Summary {
                from: me,
                salt: self.salt,
                bits,
            };
            let n = frame.encode(out).ok()?;
            peer.last_summary_sent = Some(now);
            self.salt = self.salt.wrapping_add(1);
            return Some((peer.addr.clone(), n));
        }

        // Priority 2: round-robin, emit one frame from a single peer.
        let len = self.peers.len();
        if len == 0 {
            return None;
        }
        for _ in 0..len {
            let idx = self.rr % len;
            self.rr = self.rr.wrapping_add(1);
            let peer = &mut self.peers[idx];
            if let Some(res) = emit_one(me, peer, store, mtu, now, out) {
                return Some(res);
            }
        }
        None
    }

    /// Returns the tracked peers.
    pub fn peers(&self) -> &[Peer<A>] {
        &self.peers
    }

    fn add_peer(&mut self, addr: A) {
        if self.peers.iter().any(|p| p.addr == addr) {
            return;
        }
        if self.peers.len() < consts::MAX_PEERS {
            let _ = self.peers.push(Peer::new(addr));
        }
    }

    fn remove_peer(&mut self, addr: &A) {
        if let Some(idx) = self.peers.iter().position(|p| p.addr == *addr) {
            self.peers.remove(idx);
        }
    }

    /// Returns a mutable reference to the peer at `addr`, adding it as an
    /// implicit contact if unknown and the table is not full.
    fn get_peer_mut(&mut self, addr: &A) -> Option<&mut Peer<A>> {
        if let Some(idx) = self.peers.iter().position(|p| &p.addr == addr) {
            return self.peers.get_mut(idx);
        }
        if self.peers.len() < consts::MAX_PEERS {
            let _ = self.peers.push(Peer::new(addr.clone()));
            let idx = self.peers.len() - 1;
            return self.peers.get_mut(idx);
        }
        None
    }

    fn on_frame<S: Store>(
        &mut self,
        addr: A,
        bytes: &[u8],
        store: &mut S,
        now: Tick,
    ) -> Option<Observation> {
        let frame = match Frame::decode(bytes) {
            Ok(frame) => frame,
            Err(_) => return None,
        };
        let peer = self.get_peer_mut(&addr)?;
        peer.id = Some(frame.sender());
        match frame {
            Frame::Summary { salt, bits, .. } => {
                if let Some(bloom) = Bloom::from_parts(salt, &bits) {
                    peer.their_summary = Some(bloom);
                    rebuild_send_queue(peer, &*store, now);
                }
            }
            Frame::Obs { from: _, obs } => {
                if reject(&obs) {
                    return None;
                }
                let returned = obs.clone();
                return match store.insert(obs, now) {
                    Ok(true) => Some(returned),
                    Ok(false) | Err(_) => None,
                };
            }
            Frame::Frag {
                from: _,
                id,
                index,
                count,
                data,
            } => {
                if let Some(obs) = peer.reassembler.feed(id, index, count, &data, now) {
                    if reject(&obs) {
                        return None;
                    }
                    let returned = obs.clone();
                    return match store.insert(obs, now) {
                        Ok(true) => Some(returned),
                        Ok(false) | Err(_) => None,
                    };
                }
            }
        }
        None
    }
}

/// Returns `true` when a peer is due for a summary: none sent yet, or the
/// summary interval has elapsed since the last one.
fn summary_due<A>(peer: &Peer<A>, now: Tick) -> bool {
    match peer.last_summary_sent {
        None => true,
        Some(sent) => now.saturating_sub(sent) >= consts::SUMMARY_INTERVAL_MS,
    }
}

/// Returns `true` when a received [`Observation`] must be dropped before it is
/// stored: either its addressing is malformed, or it is an addressed message
/// whose age has passed its lifetime.
///
/// An addressed message (`to = Some`) must carry a `lifetime_ms`; a mismatched
/// pair — `to = Some` with no lifetime, or a lifetime with no recipient —
/// violates the addressing invariant and is dropped in either direction. A
/// well-formed addressed message is rejected once its age exceeds its
/// lifetime. Broadcasts (`to = None`, `lifetime_ms = None`) are never rejected.
fn reject(obs: &Observation) -> bool {
    if obs.to.is_some() != obs.lifetime_ms.is_some() {
        return true;
    }
    obs.to.is_some() && obs.age_ms > obs.lifetime_ms.unwrap()
}

/// Emits at most one frame for `peer`: continuing an in-flight fragment if one
/// is pending, otherwise draining the send queue (skipping ids no longer
/// stored).
fn emit_one<A: Clone, S: Store>(
    me: NodeId,
    peer: &mut Peer<A>,
    store: &S,
    mtu: usize,
    now: Tick,
    out: &mut [u8],
) -> Option<(A, usize)> {
    // in_flight takes precedence: continue the current fragmentation.
    if let Some((id, index)) = peer.in_flight {
        match store.get(&id, now) {
            Some(obs) => return emit_fragment(me, peer, id, index, obs, mtu, out),
            None => {
                peer.in_flight = None;
                return None;
            }
        }
    }

    while let Some(&front) = peer.send_queue.first() {
        // A single `get` both checks presence and returns the age-adjusted
        // observation; a missing id (trimmed elsewhere) is skipped.
        let Some(obs) = store.get(&front, now) else {
            peer.send_queue.remove(0);
            continue;
        };
        if let Some(res) = emit_fragment(me, peer, front, 0, obs, mtu, out) {
            return Some(res);
        }
        peer.send_queue.remove(0);
    }
    None
}

/// Emits exactly one frame for `id`: a single Obs frame if it fits in `mtu`,
/// otherwise one fragment of the postcard-encoded observation. Updates
/// `in_flight`/`send_queue` state accordingly.
fn emit_fragment<A: Clone>(
    me: NodeId,
    peer: &mut Peer<A>,
    id: ObservationId,
    index: u8,
    obs: Observation,
    mtu: usize,
    out: &mut [u8],
) -> Option<(A, usize)> {
    // 1. Try the whole Observation as a single Obs frame.
    let frame = Frame::Obs {
        from: me,
        obs: obs.clone(),
    };
    if let Ok(n) = frame.encode(out) {
        if n <= mtu {
            peer.send_queue.retain(|&x| x != id);
            peer.in_flight = None;
            return Some((peer.addr.clone(), n));
        }
    }

    // 2. Otherwise emit a single fragment of the postcard-encoded observation.
    let mut obs_buf = [0u8; consts::MAX_FRAME * 2];
    let obs_bytes = postcard::to_slice(&obs, &mut obs_buf).ok()?;
    let chunk = mtu.saturating_sub(crate::frag::FRAG_OVERHEAD);
    if chunk == 0 {
        return None;
    }
    #[allow(clippy::manual_div_ceil)]
    let count = (obs_bytes.len() + chunk - 1) / chunk;
    if count == 0 || count > MAX_FRAGMENTS {
        return None;
    }

    let start = (index as usize).saturating_mul(chunk);
    let end = core::cmp::min(start + chunk, obs_bytes.len());
    let data = &obs_bytes[start..end];
    let frag_data = Vec::<u8, { consts::MAX_FRAME }>::from_slice(data).ok()?;
    let frame = Frame::Frag {
        from: me,
        id,
        index,
        count: count as u8,
        data: frag_data,
    };
    let n = frame.encode(out).ok()?;

    if (index as u32).wrapping_add(1) >= count as u32 {
        peer.send_queue.retain(|&x| x != id);
        peer.in_flight = None;
    } else {
        peer.in_flight = Some((id, index.wrapping_add(1)));
    }
    Some((peer.addr.clone(), n))
}

/// Rebuilds a peer's send queue to the ids in `store` they do not already hold
/// (per their summary), kept youngest-first and capped at `SEND_QUEUE_CAP`.
fn rebuild_send_queue<A, S: Store>(peer: &mut Peer<A>, store: &S, now: Tick) {
    let Some(their) = peer.their_summary.as_ref() else {
        return;
    };

    // Top-K youngest-first: a sorted (age ascending) buffer capped at the queue
    // capacity. When full, only a candidate younger than the current oldest
    // (last element) is admitted, dropping that oldest.
    let mut heap: Vec<(ObservationId, u64), { consts::SEND_QUEUE_CAP }> = Vec::new();
    store.for_each_id(&mut |id| {
        if their.might_contain(id) {
            return;
        }
        let obs = match store.get(id, now) {
            Some(obs) => obs,
            None => return,
        };
        insert_youngest(&mut heap, *id, obs.age_ms);
    });

    peer.send_queue.clear();
    for (id, _) in heap {
        peer.send_queue
            .push(id)
            .expect("heap is capped at SEND_QUEUE_CAP");
    }
}

/// Inserts `(id, age)` into the age-ascending top-K buffer, keeping it sorted
/// and capped at `SEND_QUEUE_CAP`.
fn insert_youngest(
    heap: &mut Vec<(ObservationId, u64), { consts::SEND_QUEUE_CAP }>,
    id: ObservationId,
    age: u64,
) {
    if heap.len() < consts::SEND_QUEUE_CAP {
        let pos = insert_pos(heap, age);
        let _ = heap.insert(pos, (id, age));
        return;
    }
    // Full: admit only if younger than the current oldest (last element).
    if let Some((_, oldest)) = heap.last() {
        if age < *oldest {
            heap.pop();
            let pos = insert_pos(heap, age);
            let _ = heap.insert(pos, (id, age));
        }
    }
}

/// Returns the index at which `age` belongs to keep `heap` age-ascending.
fn insert_pos(heap: &Vec<(ObservationId, u64), { consts::SEND_QUEUE_CAP }>, age: u64) -> usize {
    let mut pos = heap.len();
    while pos > 0 && heap[pos - 1].1 > age {
        pos -= 1;
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemStore, Observation, Payload, Topic};
    use std::vec::Vec;

    const ME: NodeId = [42; 16];

    fn obs(
        observer: [u8; 16],
        seq: u64,
        age: u64,
        at: Option<u64>,
        topic: &[u8],
        payload: &[u8],
    ) -> Observation {
        Observation {
            observer,
            seq,
            age_ms: age,
            observed_at: at,
            topic: Topic::from_slice(topic).unwrap(),
            payload: Payload::from_slice(payload).unwrap(),
            to: None,
            lifetime_ms: None,
        }
    }

    /// Encodes a frame into a stack [`FrameBuf`] for feeding [`Event::Frame`].
    fn frame_bytes(f: &Frame) -> heapless::Vec<u8, { consts::MAX_FRAME }> {
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).expect("frame fits in MAX_FRAME");
        heapless::Vec::from_slice(&buf[..n]).expect("encoded frame fits")
    }

    #[test]
    fn contact_triggers_summary_frame() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);

        let mut buf = [0u8; consts::MAX_FRAME];
        let (addr, n) = ex
            .next_frame(&store, consts::MAX_FRAME, 0, &mut buf)
            .expect("summary on contact");
        assert_eq!(addr, 1);
        match Frame::decode(&buf[..n]).unwrap() {
            Frame::Summary { from, .. } => assert_eq!(from, ME),
            other => panic!("expected a Summary frame, got {other:?}"),
        }
    }

    #[test]
    fn summary_from_peer_queues_only_what_they_lack() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let a = obs([2; 16], 1, 100, None, b"t", b"a");
        let b = obs([2; 16], 2, 50, None, b"t", b"b");
        let c = obs([2; 16], 3, 10, None, b"t", b"c");
        let id_a = a.id();
        let id_b = b.id();
        let id_c = c.id();
        store.insert(a, 0).unwrap();
        store.insert(b, 0).unwrap();
        store.insert(c, 0).unwrap();

        // Peer contacts us, then sends a summary containing only id B.
        ex.on_event(Event::Contact(1), &mut store, 0);
        let mut their = Bloom::new(0);
        their.insert(&id_b);
        let bits = heapless::Vec::from_slice(their.bits()).unwrap();
        let sum = Frame::Summary {
            from: [2; 16],
            salt: 0,
            bits,
        };
        ex.on_event(Event::Frame(1, frame_bytes(&sum)), &mut store, 0);

        // First poll still emits our own Summary (peer never saw ours).
        let mut emitted = Vec::new();
        for _ in 0..10 {
            let mut buf = [0u8; consts::MAX_FRAME];
            match ex.next_frame(&store, consts::MAX_FRAME, 0, &mut buf) {
                Some((_, n)) => emitted.push(Frame::decode(&buf[..n]).unwrap()),
                None => break,
            }
        }

        assert_eq!(emitted.len(), 3, "our summary + Obs for A and C only");
        match &emitted[0] {
            Frame::Summary { from, .. } => assert_eq!(*from, ME),
            other => panic!("expected our Summary first, got {other:?}"),
        }
        // Youngest-first among {A, C}: C (age 10) before A (age 100).
        assert_obs_id(&emitted[1], id_c);
        assert_obs_id(&emitted[2], id_a);
        assert!(
            !emitted
                .iter()
                .any(|f| matches!(f, Frame::Obs { obs, .. } if obs.id() == id_b)),
            "B must be omitted"
        );
        assert!(ex
            .next_frame(&store, consts::MAX_FRAME, 0, &mut [0u8; consts::MAX_FRAME])
            .is_none());
    }

    fn assert_obs_id(frame: &Frame, id: ObservationId) {
        match frame {
            Frame::Obs { obs, .. } => assert_eq!(obs.id(), id),
            other => panic!("expected an Obs frame, got {other:?}"),
        }
    }

    #[test]
    fn received_obs_is_stored_and_returned() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);
        let obs = obs([2; 16], 7, 0, None, b"t", b"hi");
        let frame = Frame::Obs {
            from: [2; 16],
            obs: obs.clone(),
        };
        let returned = ex
            .on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0)
            .expect("obs returned");
        assert_eq!(returned, obs);
        assert!(store.contains(&obs.id()));
    }

    #[test]
    fn duplicate_obs_returns_none() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);
        let obs = obs([2; 16], 7, 0, None, b"t", b"hi");
        let frame = Frame::Obs {
            from: [2; 16],
            obs: obs.clone(),
        };
        assert!(ex
            .on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0)
            .is_some());
        let dup = Frame::Obs {
            from: [2; 16],
            obs: obs.clone(),
        };
        assert!(ex
            .on_event(Event::Frame(1, frame_bytes(&dup)), &mut store, 0)
            .is_none());
    }

    #[test]
    fn malformed_frame_is_dropped() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);
        let before = store.len();
        let bogus = Event::Frame(1, heapless::Vec::from_slice(&[0xFFu8; 50]).unwrap());
        assert_eq!(ex.on_event(bogus, &mut store, 0), None);
        assert_eq!(
            store.len(),
            before,
            "store must be unchanged by a malformed frame"
        );
    }

    #[test]
    fn frame_from_unknown_addr_creates_peer() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // No Contact: a Summary from addr 9 must implicitly create the peer.
        let bits = heapless::Vec::from_slice(&[0u8; consts::SUMMARY_BYTES]).unwrap();
        let sum = Frame::Summary {
            from: [9; 16],
            salt: 0,
            bits,
        };
        ex.on_event(Event::Frame(9, frame_bytes(&sum)), &mut store, 0);
        let peers = ex.peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, Some([9; 16]));
    }

    #[test]
    fn lost_removes_peer() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);
        assert_eq!(ex.peers().len(), 1);
        ex.on_event(Event::Lost(1), &mut store, 0);
        assert_eq!(ex.peers().len(), 0);
    }

    #[test]
    fn peer_table_full_ignores_new_contact() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        for i in 1..=consts::MAX_PEERS as u8 {
            ex.on_event(Event::Contact(i), &mut store, 0);
        }
        assert_eq!(ex.peers().len(), consts::MAX_PEERS);
        // Overflow contact is ignored, not a panic.
        ex.on_event(Event::Contact(200), &mut store, 0);
        assert_eq!(ex.peers().len(), consts::MAX_PEERS);
    }

    #[test]
    fn summary_resent_after_interval() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);

        let mut buf = [0u8; consts::MAX_FRAME];
        let (_, n) = ex
            .next_frame(&store, consts::MAX_FRAME, 0, &mut buf)
            .expect("first summary");
        match Frame::decode(&buf[..n]).unwrap() {
            Frame::Summary { salt, .. } => assert_eq!(salt, 0),
            other => panic!("expected a Summary, got {other:?}"),
        }

        // Immediately again: our summary is fresh, nothing else queued -> None.
        assert!(ex
            .next_frame(&store, consts::MAX_FRAME, 0, &mut [0u8; consts::MAX_FRAME])
            .is_none());

        // After the interval: another Summary, now rotated to salt 1.
        let mut buf = [0u8; consts::MAX_FRAME];
        let (_, n) = ex
            .next_frame(
                &store,
                consts::MAX_FRAME,
                consts::SUMMARY_INTERVAL_MS,
                &mut buf,
            )
            .expect("second summary");
        match Frame::decode(&buf[..n]).unwrap() {
            Frame::Summary { salt, .. } => assert_eq!(salt, 1),
            other => panic!("expected a Summary, got {other:?}"),
        }
    }

    #[test]
    fn salt_rotates_each_summary() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);
        let s0 = summary_salt(&mut ex, &store, 0);
        let s1 = summary_salt(&mut ex, &store, consts::SUMMARY_INTERVAL_MS);
        assert_eq!(s0, 0);
        assert_eq!(s1, s0 + 1);
    }

    fn summary_salt(ex: &mut Exchange<u8>, store: &MemStore, now: Tick) -> u32 {
        let mut buf = [0u8; consts::MAX_FRAME];
        let (_, n) = ex
            .next_frame(store, consts::MAX_FRAME, now, &mut buf)
            .expect("summary");
        match Frame::decode(&buf[..n]).unwrap() {
            Frame::Summary { salt, .. } => salt,
            other => panic!("expected a Summary, got {other:?}"),
        }
    }

    #[test]
    fn large_obs_goes_out_as_frags() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let big = obs(ME, 1, 0, None, b"t", &[0u8; 1024]);
        let id = big.id();
        store.insert(big, 0).unwrap();

        // Peer sends an empty summary (holds nothing) so it lacks everything.
        ex.on_event(Event::Contact(1), &mut store, 0);
        let bits = heapless::Vec::from_slice(&[0u8; consts::SUMMARY_BYTES]).unwrap();
        let sum = Frame::Summary {
            from: [1; 16],
            salt: 0,
            bits,
        };
        ex.on_event(Event::Frame(1, frame_bytes(&sum)), &mut store, 0);

        // mtu 200 forces fragmentation; first poll still emits our Summary.
        let mtu = 200usize;
        let mut indices = Vec::new();
        for _ in 0..consts::MAX_FRAME {
            let mut buf = [0u8; consts::MAX_FRAME];
            match ex.next_frame(&store, mtu, 0, &mut buf) {
                Some((_, n)) => match Frame::decode(&buf[..n]).unwrap() {
                    Frame::Summary { .. } => {}
                    Frame::Frag { index, count, .. } => indices.push((index, count)),
                    other => panic!("expected only Frag frames, got {other:?}"),
                },
                None => break,
            }
        }

        assert!(
            !indices.is_empty(),
            "the large observation must be fragmented"
        );
        let count = indices[0].1;
        assert!(
            count >= 2,
            "a 1024-byte payload must span multiple fragments"
        );
        for (i, &(index, c)) in indices.iter().enumerate() {
            assert_eq!(index, i as u8, "fragments must carry increasing indices");
            assert_eq!(c, count, "all fragments share the same count");
        }
        let last = indices.last().copied().unwrap();
        assert_eq!(
            last.0 + 1,
            last.1,
            "fragment indices are contiguous to count"
        );
        assert_eq!(indices.len(), count as usize);
        // After draining, in_flight clears and the queue drops the id.
        assert!(ex.peers()[0].in_flight.is_none());
        assert!(!ex.peers()[0].send_queue.contains(&id));
    }

    #[test]
    fn inbound_frags_are_reassembled_and_stored() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let big = obs([2; 16], 7, 0, None, b"t", &[0u8; 1024]);
        let id = big.id();

        // Fragment the observation exactly as the peer would, then feed each
        // Frag frame through the exchange's inbound path (no prior Contact).
        let mut frames: Vec<heapless::Vec<u8, { consts::MAX_FRAME }>> = Vec::new();
        crate::frag::fragment([2; 16], &big, 200, &mut |b| {
            frames.push(heapless::Vec::from_slice(b).unwrap());
        })
        .expect("fragment large obs");

        for f in frames {
            let _ = ex.on_event(Event::Frame(9, f), &mut store, 0);
        }
        assert!(store.contains(&id), "reassembled obs must be stored");
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn expired_addressed_obs_is_rejected() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);
        // Addressed to us, lifetime 100ms, but age 200ms -> expired.
        let mut o = obs([2; 16], 7, 0, None, b"t", b"hi");
        o.to = Some(ME);
        o.lifetime_ms = Some(100);
        o.age_ms = 200;
        let frame = Frame::Obs { from: [2; 16], obs: o.clone() };
        assert!(
            ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0)
                .is_none(),
            "an expired addressed obs must not be delivered"
        );
        assert_eq!(store.len(), 0, "an expired addressed obs must not be stored");
    }

    #[test]
    fn expired_addressed_frag_is_rejected() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // A large addressed obs that must be fragmented; already expired.
        let mut big = obs([2; 16], 7, 0, None, b"t", &[0u8; 1024]);
        big.to = Some(ME);
        big.lifetime_ms = Some(100);
        big.age_ms = 200;
        let mut frames: Vec<heapless::Vec<u8, { consts::MAX_FRAME }>> = Vec::new();
        crate::frag::fragment([2; 16], &big, 200, &mut |b| {
            frames.push(heapless::Vec::from_slice(b).unwrap());
        })
        .expect("fragment the addressed obs into frags");
        for f in frames {
            let _ = ex.on_event(Event::Frame(9, f), &mut store, 0);
        }
        assert_eq!(
            store.len(),
            0,
            "an expired addressed obs must not survive fragmentation"
        );
    }

    #[test]
    fn addressing_invariant_violation_is_dropped() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);
        // to = Some but lifetime_ms = None: the lifetime==to invariant is
        // violated, so this frame is malformed and must be dropped, not stored.
        let mut o = obs([2; 16], 7, 0, None, b"t", b"hi");
        o.to = Some(ME);
        o.lifetime_ms = None;
        let frame = Frame::Obs { from: [2; 16], obs: o.clone() };
        assert!(
            ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0)
                .is_none()
        );
        assert_eq!(
            store.len(),
            0,
            "an invariant-violating frame must not be stored"
        );
    }

    #[test]
    fn addressed_obs_not_in_summary() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let b = obs([2; 16], 1, 0, None, b"t", b"b");
        let b_id = b.id();
        let mut a = obs([2; 16], 2, 0, None, b"t", b"a");
        a.to = Some([2; 16]);
        let a_id = a.id();
        store.insert(b, 0).unwrap();
        store.insert(a, 0).unwrap();

        // Peer contacts us, so a summary is due on the next poll.
        ex.on_event(Event::Contact(1), &mut store, 0);

        let mut buf = [0u8; consts::MAX_FRAME];
        let (_, n) = ex
            .next_frame(&store, consts::MAX_FRAME, 0, &mut buf)
            .expect("summary on contact");
        match Frame::decode(&buf[..n]).unwrap() {
            Frame::Summary { salt, bits, .. } => {
                let bloom = Bloom::from_parts(salt, &bits).expect("valid summary bloom");
                assert!(bloom.might_contain(&b_id), "broadcast id must be present");
                assert!(
                    !bloom.might_contain(&a_id),
                    "addressed id must be absent from the summary"
                );
            }
            other => panic!("expected a Summary frame, got {other:?}"),
        }
    }
}
