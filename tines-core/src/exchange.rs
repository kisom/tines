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
use crate::{
    Bloom, Event, Frame, NodeId, Observation, ObservationId, Reassembler, SendError, Store, Tick,
    WireError,
};
use heapless::Vec;

/// Worst-case bytes a [`Frame::Summary`] adds around its bloom bits: the
/// `WIRE_VERSION` byte (1), the variant tag (1), `from` (16), `salt` as a
/// varint (up to 5), and the bits length as a varint (up to 2).
const SUMMARY_OVERHEAD: usize = 25;

/// A fragmented observation part-way through transmission to one peer.
///
/// Every fragment of a stream is cut from the same encoding: the age is frozen
/// when the first fragment goes out, and `len`/`count` let a later fragment
/// detect that the stored observation no longer encodes the same way (or the
/// MTU changed). The stream is then abandoned and restarted rather than
/// sending bytes the receiver cannot reassemble.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InFlight {
    /// The observation being fragmented.
    pub id: ObservationId,
    /// Index of the next fragment to send.
    pub next: u8,
    /// Total fragments in the stream.
    pub count: u8,
    /// `age_ms` the stream was encoded with.
    pub age_ms: u64,
    /// Length of the encoded observation the fragments are cut from. (A
    /// fragment cut from the wrong offset would reassemble to corrupt bytes.)
    pub len: usize,
}

/// Why the [`Exchange`] dropped an inbound frame it could not use; reported
/// via [`NodeEvent::Dropped`]. A peer's [`Exchange`] rejects expired addressed
/// observations by policy — those are *not* counted as drops.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DropReason {
    /// The frame's leading `WIRE_VERSION` byte did not match
    /// [`consts::WIRE_VERSION`].
    VersionMismatch {
        /// The version byte read off the wire.
        version: u8,
    },
    /// The frame was not a valid [`Frame`] (truncated, garbage, or empty).
    Malformed,
}

/// Emitted by [`crate::Node::drain_events`] for either a broadcast a peer
/// confirmed holding (a *confirmed delivery*) or an inbound frame the peer
/// dropped and counted.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NodeEvent<A> {
    /// A broadcast observation the peer at `peer` confirmed holding (via an
    /// `Ack` receipt); `id` is the broadcast's content-ID.
    Delivered {
        /// The node id of the peer that acknowledged receiving the observation.
        peer: NodeId,
        /// The content-ID of the broadcast observation that was acknowledged.
        id: ObservationId,
    },
    /// The peer at `addr` sent a frame we dropped and counted under `reason`.
    /// `count` is how many frames were counted for this `(addr, reason)` pair
    /// since the last drain (always `>= 1`); `addr` is `None` when the
    /// drop-counter table is full and the count lives in the single overflow
    /// slot instead.
    Dropped {
        /// The wire address of the peer that sent the dropped frame. `None`
        /// only for the overflow slot, which has no address to name.
        addr: Option<A>,
        /// Why the frame was dropped.
        reason: DropReason,
        /// The number of frames folded into this counter since the last drain.
        count: u32,
    },
}

/// The [`Exchange`]'s counted-drop table: up to [`consts::DROP_TABLE_CAP`]
/// distinct `(addr, DropReason)` pairs, each with a saturating count, plus a
/// single overflow counter. Counters are drained — and reset — through
/// [`Exchange::drain_events`].
#[derive(Clone, Debug)]
pub(crate) struct DropTable<A> {
    /// `(addr, reason, count)`, in insertion order; `count` saturates at `u32`.
    entries: Vec<(A, DropReason, u32), { consts::DROP_TABLE_CAP }>,
    /// A single slot for drops that arrive once the table is full: no address,
    /// just the reason and a combined count. Reported with `addr: None`.
    overflow: Option<(DropReason, u32)>,
}

impl<A: Clone + PartialEq> DropTable<A> {
    /// Creates an empty drop-counter table.
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            overflow: None,
        }
    }

    /// Records one drop for `(addr, reason)`: bumps the matching entry's count
    /// if present, otherwise adds a new entry, or folds into the single overflow
    /// counter once the table is full.
    fn record(&mut self, addr: A, reason: DropReason) {
        for entry in self.entries.iter_mut() {
            if entry.0 == addr && entry.1 == reason {
                entry.2 = entry.2.saturating_add(1);
                return;
            }
        }
        if self.entries.is_full() {
            match self.overflow {
                Some((_, ref mut count)) => *count = count.saturating_add(1),
                None => self.overflow = Some((reason, 1)),
            }
        } else {
            let _ = self.entries.push((addr, reason, 1));
        }
    }

    /// Emits one [`NodeEvent::Dropped`] per non-zero entry (resetting its count,
    /// keeping the key) followed by the overflow counter, if any.
    fn drain(&mut self) -> Vec<NodeEvent<A>, { consts::DROP_TABLE_CAP + 1 }> {
        let mut out: Vec<NodeEvent<A>, { consts::DROP_TABLE_CAP + 1 }> = Vec::new();
        let mut i = 0;
        while let Some(entry) = self.entries.get_mut(i) {
            let (addr, reason, count) = (entry.0.clone(), entry.1, entry.2);
            if count > 0 {
                let _ = out.push(NodeEvent::Dropped {
                    addr: Some(addr),
                    reason,
                    count,
                });
                entry.2 = 0;
            }
            i += 1;
        }
        if let Some((reason, count)) = self.overflow.take() {
            if count > 0 {
                let _ = out.push(NodeEvent::Dropped {
                    addr: None,
                    reason,
                    count,
                });
            }
        }
        out
    }
}

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
    /// An observation currently mid-fragmentation to this peer. It is finished
    /// before any other observation is sent to the peer.
    pub in_flight: Option<InFlight>,
    /// Whether the next new observation for this peer should come from the
    /// broadcast diff rather than custody, so neither starves the other.
    prefer_broadcast: bool,
    /// The last addressed id started for this peer. Custody resumes after it,
    /// so every candidate gets a turn even when the bounce table overflows.
    custody_cursor: Option<ObservationId>,
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
            prefer_broadcast: false,
            custody_cursor: None,
        }
    }
}

/// The bookkeeping [`Exchange::next_frame`] changed for the frame it last
/// produced, kept so [`Exchange::on_send_error`] can put it back.
struct Undo<A> {
    addr: A,
    salt: u32,
    /// `Some(previous)` when the frame was a summary.
    last_summary_sent: Option<Option<Tick>>,
    in_flight: Option<InFlight>,
    prefer_broadcast: bool,
    custody_cursor: Option<ObservationId>,
    /// `(position, id)` removed from the send queue.
    requeue: Option<(usize, ObservationId)>,
    /// Id whose `(id, addr)` bounce record this frame added.
    bounce: Option<ObservationId>,
}

/// A bounded ring of `(observation-id, addr)` pairs awaiting an `Ack`
/// emission. The oldest entry is dropped when full.
#[derive(Clone, Debug)]
struct AckBuffer<A> {
    entries: Vec<(ObservationId, A), { consts::ACK_BUFFER_CAP }>,
}

impl<A: Clone> AckBuffer<A> {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn front(&self) -> Option<&(ObservationId, A)> {
        self.entries.first()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Enqueue `(id, addr)`, dropping the oldest entry if the buffer is full.
    fn enqueue(&mut self, id: ObservationId, addr: A) {
        if self.entries.is_full() {
            self.entries.remove(0);
        }
        let _ = self.entries.push((id, addr));
    }
}

/// The ids removed from a pending buffer by a failed (Busy) frame
/// emission, so [`Exchange::on_send_error`] can restore them for retry.
/// Dropped on `TooLarge`.
#[derive(Debug)]
struct UndoAck<A> {
    addr: A,
    ids: Vec<ObservationId, { consts::ACK_IDS_CAP }>,
    is_bcast: bool,
}

/// The exchange state machine, tracking up to [`consts::MAX_PEERS`] peers.
pub struct Exchange<A> {
    me: NodeId,
    salt: u32,
    peers: Vec<Peer<A>, { consts::MAX_PEERS }>,
    rr: usize,
    /// `(id, addr, direction)` for each addressed message received from, or
    /// fully sent to, `addr` during its current contact. [`Exchange::next_frame`]
    /// uses it so an addressed message is offered to each peer at most once per
    /// contact and never bounced straight back to its source. Records for an
    /// address are dropped when its contact starts or ends. Bounded at
    /// `SEND_QUEUE_CAP`: on overflow the oldest record is evicted, so
    /// suppression only weakens (ID dedup absorbs a repeat) and never fails
    /// closed on a `no_std` target. The `direction` (a `Sent` or `Received`)
    /// lets a bounce only suppress an id the local node actually handed out to,
    /// or received from, `addr` — never one merely observed while forwarding.
    bounce: Vec<(ObservationId, A, BounceKind), { consts::SEND_QUEUE_CAP }>,
    /// `(id, addr)` for each broadcast observation dispatched to `addr` on the
    /// broadcast emit path. Used to validate inbound `Ack` receipts against
    /// broadcasts we actually sent; overflow drops the oldest record.
    bcast_sent: Vec<(ObservationId, A), { consts::BCAST_SENT_CAP }>,
    /// One [`NodeEvent::Delivered`] per broadcast id a peer has confirmed,
    /// pushed drop-oldest when full. Drain through [`Exchange::drain_events`].
    events: Vec<NodeEvent<A>, { consts::EVENT_QUEUE_CAP }>,
    /// Counted drops: distinct `(addr, DropReason)` pairs, each with a
    /// saturating count, plus a single overflow counter; reported through
    /// [`Exchange::drain_events`] and reset on drain.
    drops: DropTable<A>,
    /// Pending addressed acks: ids awaiting an `Ack` for a peer that sent an
    /// addressed observation.
    acks: AckBuffer<A>,
    /// Pending broadcast acks: ids awaiting an `Ack` for a peer that sent a
    /// broadcast observation.
    bcast_acks: AckBuffer<A>,
    /// How to undo the last frame [`Exchange::next_frame`] produced.
    undo: Option<Undo<A>>,
    /// The ids drained from a pending buffer by an in-flight `Ack` that the
    /// transport rejected with `Busy`; [`Exchange::on_send_error`] restores
    /// them. Cleared when a frame is successfully sent or a new frame is
    /// emitted, so a `Busy` retry re-queues the ids rather than re-sending.
    ack_undo: Option<UndoAck<A>>,
}

impl<A: Clone + PartialEq + core::fmt::Debug> Exchange<A> {
    /// Creates an empty exchange for local node `me`. The salt starts at 0.
    pub fn new(me: NodeId) -> Self {
        Exchange {
            me,
            salt: 0,
            peers: Vec::new(),
            rr: 0,
            bounce: Vec::new(),
            bcast_sent: Vec::new(),
            events: Vec::new(),
            drops: DropTable::new(),
            undo: None,
            ack_undo: None,
            acks: AckBuffer::new(),
            bcast_acks: AckBuffer::new(),
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
    /// returning the destination address and byte count. The frame is never
    /// longer than `mtu`.
    ///
    /// Priority: (1) send a Summary to any peer due for one, then (2) emit one
    /// Obs/Frag frame from a peer via round-robin. Within a peer, a fragment
    /// stream in flight is finished first; otherwise custody (addressed) and
    /// broadcast observations take turns.
    ///
    /// The exchange's state assumes the frame is sent. If the transport
    /// refuses it, call [`Exchange::on_send_error`] before the next call.
    pub fn next_frame<S: Store>(
        &mut self,
        store: &S,
        mtu: usize,
        now: Tick,
        out: &mut [u8],
    ) -> Option<(A, usize)> {
        self.undo = None;
        self.ack_undo = None;
        let me = self.me;
        // Priority 1: a summary is due for some peer.
        for peer in self.peers.iter_mut() {
            if !summary_due(peer, now) {
                continue;
            }
            // A link too narrow for even a 1-byte filter cannot carry summaries.
            let Some(bytes) = summary_bytes(mtu) else {
                break;
            };
            let mut bloom = Bloom::new(self.salt);
            store.for_each_broadcast_id(&mut |id| bloom.insert(id));
            let bloom = bloom.fold(bytes)?;
            let bits = Vec::<u8, { consts::SUMMARY_BYTES }>::from_slice(bloom.bits()).ok()?;
            let frame = Frame::Summary {
                from: me,
                salt: self.salt,
                bits,
            };
            let n = frame.encode(out).ok()?;
            if n > mtu {
                break;
            }
            self.undo = Some(Undo {
                addr: peer.addr.clone(),
                salt: self.salt,
                last_summary_sent: Some(peer.last_summary_sent),
                in_flight: peer.in_flight,
                prefer_broadcast: peer.prefer_broadcast,
                custody_cursor: peer.custody_cursor,
                requeue: None,
                bounce: None,
            });
            peer.last_summary_sent = Some(now);
            self.salt = self.salt.wrapping_add(1);
            return Some((peer.addr.clone(), n));
        }

        // Priority 1.5: emit a pending Ack for a peer that has one outstanding,
        // before any further addressed/broadcast traffic.
        if let Some(res) = self.drain_pending_ack(mtu, out) {
            return Some(res);
        }

        // Priority 2: round-robin, emit one frame from a single peer.
        let len = self.peers.len();
        if len == 0 {
            return None;
        }
        for _ in 0..len {
            let idx = self.rr % len;
            self.rr = self.rr.wrapping_add(1);
            if let Some(res) = self.emit_for_peer(idx, store, mtu, now, out) {
                return Some(res);
            }
        }
        None
    }

    /// Reverts the bookkeeping for the frame the last
    /// [`Exchange::next_frame`] produced, after the transport failed to send
    /// it.
    ///
    /// For `Busy` and `Unreachable` the next call produces the same frame
    /// again: a summary stays due, an observation or fragment stays queued, and
    /// an addressed message is not counted as delivered. `TooLarge` is final —
    /// a frame the transport can never carry is dropped rather than retried on
    /// every poll.
    pub fn on_send_error(&mut self, err: &SendError) {
        let pending = self.undo.take();
        if !matches!(err, SendError::TooLarge) {
            // A pending-ack drain is reverted on Busy (but dropped on TooLarge)
            // independently of the undo struct, which is None for an Ack-only
            // frame.
            if let Some(ack) = self.ack_undo.take() {
                self.restore_ack_drain(ack);
            }
        }
        let Some(undo) = pending else {
            return;
        };
        if matches!(err, SendError::TooLarge) {
            return;
        }
        self.salt = undo.salt;
        if let Some(id) = undo.bounce {
            if let Some(pos) = self
                .bounce
                .iter()
                .position(|(b, a, _)| *b == id && *a == undo.addr)
            {
                self.bounce.remove(pos);
            }
        }
        let Some(peer) = self.peers.iter_mut().find(|p| p.addr == undo.addr) else {
            return;
        };
        if let Some(prev) = undo.last_summary_sent {
            peer.last_summary_sent = prev;
        }
        peer.in_flight = undo.in_flight;
        peer.prefer_broadcast = undo.prefer_broadcast;
        peer.custody_cursor = undo.custody_cursor;
        if let Some((pos, id)) = undo.requeue {
            if !peer.send_queue.contains(&id) {
                let pos = pos.min(peer.send_queue.len());
                let _ = peer.send_queue.insert(pos, id);
            }
        }
    }

    /// Drain up to one pending `Ack` from the addressed then broadcast
    /// buffers. Returns `(dest_addr, bytes)` when an `Ack` was emitted into
    /// `out`, else `None`.
    fn drain_pending_ack(&mut self, mtu: usize, out: &mut [u8]) -> Option<(A, usize)> {
        // Addressed acks take priority over broadcast acks.
        if let Some(res) = self.drain_ack_buffer(false, mtu, out) {
            return Some(res);
        }
        self.drain_ack_buffer(true, mtu, out)
    }

    /// Drain one `Ack` for a single destination from the given buffer,
    /// splitting across frames to fit `mtu`. On success the ids that did not
    /// fit are left in the buffer; on a `Busy` rejection they are restored by
    /// [`Exchange::on_send_error`] (see [`Exchange::ack_undo`]).
    fn drain_ack_buffer(
        &mut self,
        is_bcast: bool,
        mtu: usize,
        out: &mut [u8],
    ) -> Option<(A, usize)> {
        // Pull every pending id for the destination out of the buffer.
        let addr;
        let mut send_n = 0usize;
        let mut pulled: heapless::Vec<ObservationId, { consts::ACK_IDS_CAP }> =
            heapless::Vec::new();
        {
            let buffer = if is_bcast {
                &mut self.bcast_acks
            } else {
                &mut self.acks
            };
            addr = buffer.front()?.1.clone();
            let mut i = 0;
            while i < buffer.len() {
                if buffer.entries[i].1 == addr {
                    let (id, _a) = buffer.entries.remove(i);
                    if pulled.push(id).is_err() {
                        // Overflow while pulling out: restore this entry and stop.
                        let _ = buffer.entries.insert(i, (id, addr.clone()));
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            // How many ids fit in `mtu`? Each id dropped shrinks the frame.
            for k in (1..=pulled.len()).rev() {
                if Self::encode_ack(self.me, &pulled[..k], out, mtu).is_some() {
                    send_n = k;
                    break;
                }
            }
            if send_n == 0 {
                // Even a single id overflows `mtu`: drop this addr's oldest
                // pending ack and re-keep the rest for the next poll, so the
                // remainder retries rather than stalling. The offending addr's
                // entries were already pulled into `pulled`, so only they are
                // touched here — never a *different* peer's oldest entry.
                for id in &pulled[1..] {
                    let _ = buffer.entries.push((*id, addr.clone()));
                }
                return None;
            }
        }
        // Emit the fitting ids (they fit by the loop above).
        let n = Self::encode_ack(self.me, &pulled[..send_n], out, mtu).expect("fits per loop");
        // Keep the ids that did not fit for a later poll.
        for id in &pulled[send_n..] {
            let buffer = if is_bcast {
                &mut self.bcast_acks
            } else {
                &mut self.acks
            };
            let _ = buffer.entries.push((*id, addr.clone()));
        }
        // Record the ids we removed and did not re-keep so a `Busy` retry can
        // re-queue them (the retained ids are already back in the buffer).
        let mut sent: heapless::Vec<ObservationId, { consts::ACK_IDS_CAP }> = heapless::Vec::new();
        for id in &pulled[..send_n] {
            let _ = sent.push(*id);
        }
        let return_addr = addr.clone();
        self.ack_undo = Some(UndoAck {
            addr,
            ids: sent,
            is_bcast,
        });
        Some((return_addr, n))
    }

    /// Re-add the ids drained from a pending buffer by a failed (Busy) frame so
    /// they are retried on the next poll.
    fn restore_ack_drain(&mut self, ack: UndoAck<A>) {
        let buffer = if ack.is_bcast {
            &mut self.bcast_acks
        } else {
            &mut self.acks
        };
        for id in ack.ids.iter() {
            let _ = buffer.entries.push((*id, ack.addr.clone()));
        }
    }

    /// Encode an `Ack { from: me, ids }` into `out`, returning the byte count if
    /// it fits within `mtu`. Returns `None` if it overflows `mtu` (or the ids
    /// exceed [`consts::ACK_IDS_CAP`]).
    fn encode_ack(me: NodeId, ids: &[ObservationId], out: &mut [u8], mtu: usize) -> Option<usize> {
        let ids_vec =
            heapless::Vec::<ObservationId, { consts::ACK_IDS_CAP }>::from_slice(ids).ok()?;
        let frame = Frame::Ack {
            from: me,
            ids: ids_vec,
        };
        let n = frame.encode(out).ok()?;
        if n <= mtu {
            Some(n)
        } else {
            None
        }
    }

    /// Emits at most one Obs/Frag frame for the peer at `idx`.
    fn emit_for_peer<S: Store>(
        &mut self,
        idx: usize,
        store: &S,
        mtu: usize,
        now: Tick,
        out: &mut [u8],
    ) -> Option<(A, usize)> {
        // Finish the stream in flight first: starting another observation
        // would abandon it part-way. An observation that has since been
        // trimmed or expired, or that no longer encodes as the stream began,
        // is abandoned (and restarted from fragment 0 if still wanted).
        if let Some(f) = self.peers[idx].in_flight {
            if let Some(obs) = store.get(&f.id, now).filter(|o| !reject(o)) {
                if let Some(res) = self.emit(idx, f.id, &obs, Some(f), mtu, out) {
                    return Some(res);
                }
            }
            self.peers[idx].in_flight = None;
        }

        // Custody and broadcast take turns, so neither starves the other.
        let broadcast_first = self.peers[idx].prefer_broadcast;
        for broadcast in [broadcast_first, !broadcast_first] {
            let res = if broadcast {
                self.emit_broadcast(idx, store, mtu, now, out)
            } else {
                self.forward_addressed(idx, store, mtu, now, out)
            };
            if res.is_some() {
                self.peers[idx].prefer_broadcast = !broadcast;
                return res;
            }
        }
        None
    }

    /// Starts sending the front of the peer's broadcast send queue, dropping
    /// ids that are no longer stored or cannot be sent at this MTU.
    fn emit_broadcast<S: Store>(
        &mut self,
        idx: usize,
        store: &S,
        mtu: usize,
        now: Tick,
        out: &mut [u8],
    ) -> Option<(A, usize)> {
        let addr = self.peers[idx].addr.clone();
        loop {
            let front = *self.peers[idx].send_queue.first()?;
            if let Some(obs) = store.get(&front, now) {
                if let Some(res) = self.emit(idx, front, &obs, None, mtu, out) {
                    self.record_bcast_sent(front, addr.clone());
                    return Some(res);
                }
            }
            self.peers[idx].send_queue.remove(0);
        }
    }

    /// Records that a broadcast for `id` was dispatched to `addr`, evicting the
    /// oldest record when `bcast_sent` is full.
    fn record_bcast_sent(&mut self, id: ObservationId, addr: A) {
        if bcast_sent_contains(&self.bcast_sent, id, &addr) {
            return;
        }
        if self.bcast_sent.is_full() {
            self.bcast_sent.remove(0);
        }
        let _ = self.bcast_sent.push((id, addr));
    }

    /// Addressed store-and-forward for the peer at `idx`: start sending one
    /// addressed observation, preferring *delivery* (`to == peer.id`) over
    /// *epidemic* forwarding (`to == Some(other != me)`).
    ///
    /// Never emits if the peer's id is unknown or equals `me`. Candidates that
    /// were already exchanged with this peer this contact, or that cannot be
    /// sent at this MTU, are skipped; the rest are taken in id order, resuming
    /// after the last one started for this peer. Returns `None` when nothing
    /// qualifies.
    fn forward_addressed<S: Store>(
        &mut self,
        idx: usize,
        store: &S,
        mtu: usize,
        now: Tick,
        out: &mut [u8],
    ) -> Option<(A, usize)> {
        let me = self.me;
        let recipient = self.peers[idx].id?;
        if recipient == me {
            return None;
        }
        let addr = self.peers[idx].addr.clone();
        let cursor = self.peers[idx].custody_cursor;
        for deliver in [true, false] {
            let found = select_addressed(store, now, &self.bounce, &addr, cursor, |id, obs| {
                let wanted = if deliver {
                    obs.dest.as_ref().map(|d| d.to) == Some(recipient)
                } else {
                    obs.dest.is_some() && obs.dest.as_ref().map(|d| d.to) != Some(me)
                };
                // `out` is scratch here; the chosen frame is re-encoded below.
                wanted && encode_frame(me, *id, obs, None, mtu, out).is_some()
            });
            if let Some((id, obs)) = found {
                let res = self.emit(idx, id, &obs, None, mtu, out);
                if res.is_some() {
                    self.peers[idx].custody_cursor = Some(id);
                }
                return res;
            }
        }
        None
    }

    /// Encodes the next frame of `obs` for the peer at `idx` (see
    /// [`encode_frame`]) and records what it changes. Once the last frame of
    /// the observation is out, `id` leaves the peer's send queue and, if
    /// addressed, is recorded as exchanged with this peer.
    fn emit(
        &mut self,
        idx: usize,
        id: ObservationId,
        obs: &Observation,
        resume: Option<InFlight>,
        mtu: usize,
        out: &mut [u8],
    ) -> Option<(A, usize)> {
        let (n, next) = encode_frame(self.me, id, obs, resume, mtu, out)?;
        let peer = &mut self.peers[idx];
        let addr = peer.addr.clone();
        let mut undo = Undo {
            addr: addr.clone(),
            salt: self.salt,
            last_summary_sent: None,
            in_flight: peer.in_flight,
            prefer_broadcast: peer.prefer_broadcast,
            custody_cursor: peer.custody_cursor,
            requeue: None,
            bounce: None,
        };
        peer.in_flight = next;
        if next.is_none() {
            if let Some(pos) = peer.send_queue.iter().position(|&x| x == id) {
                peer.send_queue.remove(pos);
                undo.requeue = Some((pos, id));
            }
            if obs.dest.is_some()
                && record_bounce(&mut self.bounce, id, addr.clone(), BounceKind::Sent)
            {
                undo.bounce = Some(id);
            }
        }
        self.undo = Some(undo);
        Some((addr, n))
    }

    /// Returns the current peers.
    pub fn peers(&self) -> &[Peer<A>] {
        &self.peers
    }

    /// [`Exchange`]s emit [`NodeEvent`]s — confirmed deliveries plus counted
    /// drops — through `Exchange::drain_events`; `tines` surfaces them to its
    /// transport, so the drain method is unused inside `tines-core` itself.
    ///
    /// Returns confirmed deliveries first, oldest first, then one
    /// [`NodeEvent::Dropped`] per non-zero counter (resetting each counter).
    /// Sized to hold the entire delivery queue plus the whole drop table and
    /// its overflow slot, so a full delivery queue never evicts a counted drop.
    #[allow(dead_code)]
    pub(crate) fn drain_events(
        &mut self,
    ) -> Vec<NodeEvent<A>, { consts::EVENT_QUEUE_CAP + consts::DROP_TABLE_CAP + 1 }> {
        let mut drained: Vec<
            NodeEvent<A>,
            { consts::EVENT_QUEUE_CAP + consts::DROP_TABLE_CAP + 1 },
        > = Vec::new();
        // Confirmed deliveries, oldest first.
        while !self.events.is_empty() {
            if drained.is_full() {
                break;
            }
            let ev = self.events.remove(0);
            let _ = drained.push(ev);
        }
        // Then one counted drop per non-zero counter, resetting each.
        for dropped in self.drops.drain() {
            let _ = drained.push(dropped);
        }
        drained
    }

    fn add_peer(&mut self, addr: A) {
        if self.peers.iter().any(|p| p.addr == addr) {
            return;
        }
        self.forget_bounces(&addr);
        if self.peers.len() < consts::MAX_PEERS {
            let _ = self.peers.push(Peer::new(addr));
        }
    }

    fn remove_peer(&mut self, addr: &A) {
        if let Some(idx) = self.peers.iter().position(|p| p.addr == *addr) {
            self.peers.remove(idx);
        }
        self.forget_bounces(addr);
    }

    /// Drops the bounce records for `addr`: suppression lasts one contact.
    fn forget_bounces(&mut self, addr: &A) {
        self.bounce.retain(|(_, baddr, _)| baddr != addr);
    }

    /// Returns the index of the peer at `addr`, adding it as an implicit
    /// contact if unknown and the table is not full.
    fn peer_index(&mut self, addr: &A) -> Option<usize> {
        if let Some(idx) = self.peers.iter().position(|p| &p.addr == addr) {
            return Some(idx);
        }
        if self.peers.len() < consts::MAX_PEERS {
            self.forget_bounces(addr);
            let _ = self.peers.push(Peer::new(addr.clone()));
            return Some(self.peers.len() - 1);
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
            Err(WireError::VersionMismatch { version }) => {
                // A wrong-version frame is a counted drop, not a silent drop.
                self.drops
                    .record(addr, DropReason::VersionMismatch { version });
                return None;
            }
            // Any other decode failure (malformed, truncated, or the encode-only
            // TooLarge which decode never yields) is a counted malformed drop.
            Err(_) => {
                self.drops.record(addr, DropReason::Malformed);
                return None;
            }
        };
        let idx = self.peer_index(&addr)?;
        let peer = &mut self.peers[idx];
        peer.id = Some(frame.sender());
        match frame {
            Frame::Summary { salt, bits, .. } => {
                if let Some(bloom) = Bloom::from_parts(salt, &bits) {
                    peer.their_summary = Some(bloom);
                    rebuild_send_queue(peer, &*store, now);
                }
                None
            }
            Frame::Obs { from: _, obs } => self.accept(obs, addr, store, now),
            Frame::Frag {
                from: _,
                id,
                index,
                count,
                data,
            } => {
                let obs = peer.reassembler.feed(id, index, count, &data, now)?;
                self.accept(obs, addr, store, now)
            }
            Frame::Ack { from, ids } => self.on_ack(addr, from, ids, store, now),
        }
    }

    /// Handles an inbound `Ack` receipt from the peer at `addr`, whose node id
    /// is `from`.
    ///
    /// For each id in the receipt: an id the local node `Sent` to `addr` (per
    /// the bounce table) is released from custody so it is never forwarded
    /// again; an id the node dispatched as a broadcast to `addr` (per
    /// `bcast_sent`) fires a [`NodeEvent::Delivered`]. A receipt for an id the
    /// node never dispatched to `addr` is silently ignored. No path panics on
    /// malformed input — an oversized id list just drops the overflow.
    fn on_ack<S: Store>(
        &mut self,
        addr: A,
        from: NodeId,
        ids: Vec<ObservationId, { consts::ACK_IDS_CAP }>,
        store: &mut S,
        now: Tick,
    ) -> Option<Observation> {
        for id in ids.iter() {
            if was_sent(&self.bounce, *id, &addr) {
                // Addressed: release custody for the local copy on the first
                // exact Ack from ANY peer the node dispatched it to — a relay
                // that merely holds an epidemic-forwarded copy, or the intended
                // recipient alike. The custody chain tracks that a copy left us,
                // so we stop forwarding this id; final delivery to the recipient
                // is best-effort, not guaranteed.
                if store.get(id, now).is_some_and(|obs| obs.dest.is_some()) {
                    store.release(id);
                }
            } else if bcast_sent_contains(&self.bcast_sent, *id, &addr) {
                // Broadcast: the peer confirms it holds our broadcast.
                let _ = self.events.push(NodeEvent::Delivered {
                    peer: from,
                    id: *id,
                });
            }
        }
        None
    }

    /// Stores an observation received from `addr`, returning it if it was new.
    ///
    /// An addressed message is recorded against `addr` whether it was new or
    /// a duplicate: the sender evidently holds it, so it is never offered back
    /// this contact.
    fn accept<S: Store>(
        &mut self,
        obs: Observation,
        addr: A,
        store: &mut S,
        now: Tick,
    ) -> Option<Observation> {
        if reject(&obs) {
            return None;
        }
        let id = obs.id();
        let addressed = obs.dest.is_some();
        let returned = obs.clone();
        let res = store.insert(obs, now);
        // Enqueue a pending ack only for what the store actually kept: a newly
        // stored observation, or a duplicate whose local copy is still held. A
        // store that rejected the observation (full, backend error) holds
        // nothing, so it must not ack — for an addressed offer that would
        // release the sender's custody for a message nobody holds, and for a
        // broadcast it would fire a false confidence event at the sender. The
        // single exception among the kept copies is a duplicate *addressed*
        // offer whose local copy has been released — a released holder must
        // not ack.
        if res.is_ok() {
            let duplicate = !matches!(res, Ok(true));
            let released = store.is_released(&id);
            if !(duplicate && addressed && released) {
                self.enqueue_pending_ack(id, addr.clone(), addressed);
            }
        }
        if addressed && res.is_ok() {
            record_bounce(&mut self.bounce, id, addr, BounceKind::Received);
        }
        match res {
            Ok(true) => Some(returned),
            _ => None,
        }
    }

    /// Enqueue `(id, addr)` as a pending ack into the addressed (`acks`) or
    /// broadcast (`bcast_acks`) buffer, dropping the oldest entry if full.
    fn enqueue_pending_ack(&mut self, id: ObservationId, addr: A, addressed: bool) {
        let buffer = if addressed {
            &mut self.acks
        } else {
            &mut self.bcast_acks
        };
        buffer.enqueue(id, addr);
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

/// The largest bloom size, in bytes, whose Summary frame fits in `mtu`: a
/// power of two (see [`Bloom::fold`]) no larger than `SUMMARY_BYTES`. Narrower
/// links get a smaller filter, which only raises the false-positive rate —
/// a delayed propagation, never an inconsistency, and salt rotation varies
/// which ids are affected on each round.
fn summary_bytes(mtu: usize) -> Option<usize> {
    let room = mtu
        .saturating_sub(SUMMARY_OVERHEAD)
        .min(consts::SUMMARY_BYTES);
    if room == 0 {
        return None;
    }
    Some(1 << (usize::BITS - 1 - room.leading_zeros()))
}

/// Returns `true` when a received [`Observation`] must be dropped before it is
/// stored: it is an addressed message (`dest = Some`) whose age has passed its
/// lifetime.
///
/// An addressed message always carries a lifetime, so the only way to reject
/// is age exceeding that lifetime. Broadcasts (`dest = None`) are never
/// rejected. The `Dest` type now enforces the addressed/broadcast pairing, so
/// there is no separate "malformed addressing" case to drop.
fn reject(obs: &Observation) -> bool {
    obs.dest
        .as_ref()
        .is_some_and(|d| obs.age_ms > d.lifetime_ms)
}

/// Finds the non-expired, well-formed addressed observation matching
/// `predicate` with the smallest id after `after`, wrapping round to the
/// smallest id overall, and skipping any id already exchanged with `addr` this
/// contact.
///
/// `reject` excludes expired/malformed addressed messages (the expiry
/// invariant) and `is_sourced` enforces bounce suppression. `predicate` is
/// only consulted for an id that would beat the current pick. Returns the
/// stored key and age-adjusted observation.
fn select_addressed<S: Store, A: PartialEq>(
    store: &S,
    now: Tick,
    bounce: &[(ObservationId, A, BounceKind)],
    addr: &A,
    after: Option<ObservationId>,
    mut predicate: impl FnMut(&ObservationId, &Observation) -> bool,
) -> Option<(ObservationId, Observation)> {
    let mut ahead: Option<(ObservationId, Observation)> = None;
    let mut wrapped: Option<(ObservationId, Observation)> = None;
    store.for_each_addressed_id(&mut |id| {
        let is_ahead = after.is_none_or(|a| *id > a);
        let beats = |pick: &Option<(ObservationId, Observation)>| {
            pick.as_ref().is_none_or(|(best, _)| id < best)
        };
        let improves = if is_ahead {
            beats(&ahead)
        } else {
            ahead.is_none() && beats(&wrapped)
        };
        if !improves || is_sourced(bounce, *id, addr) {
            return;
        }
        let Some(obs) = store.get(id, now) else {
            return;
        };
        if obs.dest.is_none() || reject(&obs) || store.is_released(id) || !predicate(id, &obs) {
            return;
        }
        if is_ahead {
            ahead = Some((*id, obs));
        } else {
            wrapped = Some((*id, obs));
        }
    });
    ahead.or(wrapped)
}

/// The direction that produced a bounce record: the local node `Sent` the id
/// to `addr`, or `Received` it from `addr`. Used to make bounce suppression
/// direction-aware — a `Sent` record can only release custody, a `Received`
/// record can only suppress re-offering.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BounceKind {
    Sent,
    Received,
}

/// Returns `true` when `(id, addr)` has been recorded in the bounce table —
/// i.e. `id` was received from, or sent to, `addr` this contact under either
/// direction. Offering it to that peer again would only bounce, so it is
/// skipped.
fn is_sourced<A: PartialEq>(
    bounce: &[(ObservationId, A, BounceKind)],
    id: ObservationId,
    addr: &A,
) -> bool {
    bounce
        .iter()
        .any(|(bid, baddr, _kind)| *bid == id && baddr == addr)
}

/// Whether `id` was `Sent` to `addr`. An inbound `Ack` from `addr` may only
/// release custody of an id the local node actually dispatched to it — never
/// an id it only received, or merely observed while forwarding.
fn was_sent<A: PartialEq>(
    bounce: &[(ObservationId, A, BounceKind)],
    id: ObservationId,
    addr: &A,
) -> bool {
    bounce
        .iter()
        .any(|(bid, baddr, kind)| *bid == id && baddr == addr && *kind == BounceKind::Sent)
}

/// Records an `(id, addr, direction)` bounce in the table, evicting the oldest
/// record when full. Returns `true` if a new record was added.
fn record_bounce<A: PartialEq>(
    bounce: &mut Vec<(ObservationId, A, BounceKind), { consts::SEND_QUEUE_CAP }>,
    id: ObservationId,
    addr: A,
    kind: BounceKind,
) -> bool {
    if is_sourced(bounce, id, &addr) {
        return false;
    }
    if bounce.is_full() {
        bounce.remove(0);
    }
    let _ = bounce.push((id, addr, kind));
    true
}

/// Whether `(id, addr)` has been recorded in `bcast_sent` — i.e. the local node
/// actually dispatched that broadcast to that peer. Used to validate inbound
/// `Ack` receipts against broadcasts we truly sent.
fn bcast_sent_contains<A: PartialEq>(
    bcast_sent: &[(ObservationId, A)],
    id: ObservationId,
    addr: &A,
) -> bool {
    bcast_sent
        .iter()
        .any(|(bid, baddr)| *bid == id && baddr == addr)
}

/// Encodes one frame of `obs` (whose id is `id`) into `out`.
///
/// Starting a stream (`resume = None`), the whole observation goes out as one
/// Obs frame if that fits in `mtu`; otherwise fragment 0 of its postcard
/// encoding. Resuming, fragment `resume.next` is cut from the encoding at the
/// frozen `resume.age_ms`, so every fragment of a stream comes from identical
/// bytes even as the observation ages or the clock steps backwards.
///
/// Returns the frame length and the stream state to keep — `None` once the
/// observation's last frame is out. Returns `None` if the observation cannot
/// be sent at this MTU, or no longer encodes to the length and fragment count
/// the stream started with.
fn encode_frame(
    me: NodeId,
    id: ObservationId,
    obs: &Observation,
    resume: Option<InFlight>,
    mtu: usize,
    out: &mut [u8],
) -> Option<(usize, Option<InFlight>)> {
    if resume.is_none() {
        let frame = Frame::Obs {
            from: me,
            obs: obs.clone(),
        };
        if let Ok(n) = frame.encode(out) {
            if n <= mtu {
                return Some((n, None));
            }
        }
    }

    let age_ms = resume.map_or(obs.age_ms, |f| f.age_ms);
    let frozen = Observation {
        age_ms,
        ..obs.clone()
    };
    let mut obs_buf = [0u8; consts::MAX_FRAME * 2];
    let obs_bytes = postcard::to_slice(&frozen, &mut obs_buf).ok()?;
    let (chunk, count) = crate::frag::plan(obs_bytes.len(), mtu)?;
    let index = match resume {
        Some(f) if f.len != obs_bytes.len() || usize::from(f.count) != count => return None,
        Some(f) => f.next,
        None => 0,
    };

    let start = usize::from(index) * chunk;
    let end = core::cmp::min(start + chunk, obs_bytes.len());
    let data = obs_bytes.get(start..end)?;
    let frame = Frame::Frag {
        from: me,
        id,
        index,
        count: count as u8,
        data: Vec::from_slice(data).ok()?,
    };
    let n = frame.encode(out).ok()?;
    if n > mtu {
        return None;
    }

    let next = (usize::from(index) + 1 < count).then_some(InFlight {
        id,
        next: index + 1,
        count: count as u8,
        age_ms,
        len: obs_bytes.len(),
    });
    Some((n, next))
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
    // Broadcasts only (`to == None`): addressed messages are delivered through
    // `forward_addressed` and must never leak into the broadcast diff, mirroring
    // the bloom-summary path in `next_frame` which also iterates broadcast ids.
    store.for_each_broadcast_id(&mut |id| {
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

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use super::*;
    use crate::{
        Dest, MemStore, Observation, Payload, SendError, Store, StoreError, Topic, TrimPolicy,
    };
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
            dest: None,
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

    #[test]
    fn rebuild_send_queue_excludes_addressed() {
        // A store with one broadcast (`dest = None`) and one addressed
        // (`dest = Some([3; 16])`) observation. After a peer contacts us and
        // sends an empty Summary (lacking everything), the broadcast diff must
        // queue only the broadcast id and never leak in the addressed id.
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let b = obs([2; 16], 1, 0, None, b"t", b"b");
        let a_id = {
            let mut a = obs([2; 16], 2, 0, None, b"t", b"a");
            a.dest = Some(Dest {
                to: [3; 16],
                lifetime_ms: consts::SUMMARY_INTERVAL_MS * 10,
            });
            a.id()
        };
        let b_id = b.id();
        store.insert(b, 0).unwrap();
        store
            .insert(
                {
                    let mut a = obs([2; 16], 2, 0, None, b"t", b"a");
                    a.dest = Some(Dest {
                        to: [3; 16],
                        lifetime_ms: consts::SUMMARY_INTERVAL_MS * 10,
                    });
                    a
                },
                0,
            )
            .unwrap();

        // Peer contacts us, then sends an empty Summary (holds nothing).
        ex.on_event(Event::Contact(1), &mut store, 0);
        let bits = heapless::Vec::from_slice(&[0u8; consts::SUMMARY_BYTES]).unwrap();
        let sum = Frame::Summary {
            from: [2; 16],
            salt: 0,
            bits,
        };
        ex.on_event(Event::Frame(1, frame_bytes(&sum)), &mut store, 0);

        let peer = &ex.peers()[0];
        let q: std::vec::Vec<_> = peer.send_queue.iter().copied().collect();
        assert!(q.contains(&b_id), "broadcast id must be queued: {q:?}");
        assert!(!q.contains(&a_id), "addressed id must NOT be queued: {q:?}");
        assert_eq!(q.len(), 1, "only the broadcast id should be queued");
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

    /// Encodes an addressed observation from `observer` to `to` (for ack-path
    /// tests).
    fn observed_addressed(
        observer: NodeId,
        seq: u64,
        to: NodeId,
        topic: &[u8],
        payload: &[u8],
    ) -> Observation {
        let mut o = obs(observer, seq, 0, None, topic, payload);
        o.dest = Some(Dest {
            to,
            lifetime_ms: consts::SUMMARY_INTERVAL_MS * 10,
        });
        o
    }

    /// Drains frames until an `Ack` carrying `id` is found (summary frames are
    /// skipped), reporting whether one was seen.
    fn ack_for(ex: &mut Exchange<u8>, store: &MemStore, id: ObservationId) -> bool {
        let mut buf = [0u8; consts::MAX_FRAME];
        for _ in 0..consts::MAX_FRAME {
            if let Some((_, n)) = ex.next_frame(store, consts::MAX_FRAME, 0, &mut buf) {
                match Frame::decode(&buf[..n]) {
                    Ok(Frame::Ack { ids, .. }) if ids.contains(&id) => return true,
                    _ => {}
                }
            }
        }
        false
    }

    #[test]
    fn released_duplicate_is_not_acked() {
        let a: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        empty_summary(&mut ex, &mut store, 1, a);

        // A held an addressed message to B; B's store already holds it and has
        // released it (e.g. it was delivered and confirmed).
        let x = observed_addressed(a, 1, ME, b"t", b"hi");
        let x_id = x.id();
        store.insert(x.clone(), 0).unwrap();
        store.release(&x_id);
        assert!(store.is_released(&x_id));

        // A re-offers the same addressed message: a duplicate whose local copy
        // is released, so B must not enqueue a pending ack for it.
        let frame = Frame::Obs { from: a, obs: x };
        ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0);

        // The duplicate never enters the buffer and no Ack carries X.
        assert!(
            ex.acks.entries.is_empty(),
            "a released duplicate is not enqueued"
        );
        for _ in 0..consts::MAX_FRAME {
            let mut buf = [0u8; consts::MAX_FRAME];
            if let Some((_, n)) = ex.next_frame(&store, consts::MAX_FRAME, 0, &mut buf) {
                if let Ok(Frame::Ack { ids, .. }) = Frame::decode(&buf[..n]) {
                    assert!(
                        !ids.contains(&x_id),
                        "a released duplicate must not be acked"
                    );
                }
            }
        }
        assert!(
            ex.acks.entries.is_empty(),
            "the released duplicate never left the buffer"
        );
    }

    /// A `Store` that delegates to a [`MemStore`] but rejects `insert` with
    /// `StoreError::Full` once it has held `cap` observations — a stand-in for
    /// a real backend that refuses when full. `MemStore` itself never fills.
    struct FullStore {
        cap: usize,
        inner: MemStore,
    }

    impl FullStore {
        fn new(cap: usize) -> Self {
            Self {
                cap,
                inner: MemStore::new(),
            }
        }
    }

    impl Store for FullStore {
        fn insert(&mut self, obs: Observation, now: Tick) -> Result<bool, StoreError> {
            if self.inner.len() >= self.cap {
                return Err(StoreError::Full);
            }
            self.inner.insert(obs, now)
        }

        fn contains(&self, id: &ObservationId) -> bool {
            self.inner.contains(id)
        }

        fn get(&self, id: &ObservationId, now: Tick) -> Option<Observation> {
            self.inner.get(id, now)
        }

        fn len(&self) -> usize {
            self.inner.len()
        }

        fn for_each_id(&self, f: &mut dyn FnMut(&ObservationId)) {
            self.inner.for_each_id(f)
        }

        fn for_each_broadcast_id(&self, f: &mut dyn FnMut(&ObservationId)) {
            self.inner.for_each_broadcast_id(f)
        }

        fn for_each_addressed_id(&self, f: &mut dyn FnMut(&ObservationId)) {
            self.inner.for_each_addressed_id(f)
        }

        fn release(&mut self, id: &ObservationId) {
            self.inner.release(id)
        }

        fn is_released(&self, id: &ObservationId) -> bool {
            self.inner.is_released(id)
        }

        fn for_each_topic(&self, prefix: &[u8], now: Tick, f: &mut dyn FnMut(&Observation)) {
            self.inner.for_each_topic(prefix, now, f)
        }

        fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize {
            self.inner.trim(policy, now)
        }
    }

    /// A store that rejected a receive (full, or backend error) must not ack,
    /// whether the offer was addressed or a broadcast. Acking would release
    /// the sender's custody for a message nobody holds (addressed) or fire a
    /// false confidence event (broadcast) — custody lost for a message no one
    /// keeps. Regression test for the full-store custody bug.
    #[test]
    fn store_full_receive_is_not_acked() {
        let a: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        // A store that never keeps anything it is asked to insert.
        let mut store = FullStore::new(0);
        // Make peer `a` known and appearing to lack everything.
        let bits = heapless::Vec::from_slice(&[0u8; consts::SUMMARY_BYTES]).unwrap();
        let sum = Frame::Summary {
            from: a,
            salt: 0,
            bits,
        };
        ex.on_event(Event::Frame(1, frame_bytes(&sum)), &mut store, 0);

        // A broadcast from A, received into a store that rejects it.
        let b = obs(a, 1, 0, None, b"t", b"bc");
        let b_id = b.id();
        ex.on_event(
            Event::Frame(1, frame_bytes(&Frame::Obs { from: a, obs: b })),
            &mut store,
            0,
        );

        // An addressed offer from A to ME, received into the same store.
        let x = observed_addressed(a, 2, ME, b"t", b"hi");
        let x_id = x.id();
        ex.on_event(
            Event::Frame(1, frame_bytes(&Frame::Obs { from: a, obs: x })),
            &mut store,
            0,
        );

        // Nothing was kept, so nothing is acked: no pending ack queued and no
        // Ack frame carries either id.
        assert!(
            ex.acks.entries.is_empty(),
            "a store-full addressed receive must not be acked"
        );
        assert!(
            ex.bcast_acks.entries.is_empty(),
            "a store-full broadcast receive must not be acked"
        );
        for _ in 0..consts::MAX_FRAME {
            let mut buf = [0u8; consts::MAX_FRAME];
            if let Some((_, n)) = ex.next_frame(&store, consts::MAX_FRAME, 0, &mut buf) {
                if let Ok(Frame::Ack { ids, .. }) = Frame::decode(&buf[..n]) {
                    assert!(
                        !ids.contains(&b_id) && !ids.contains(&x_id),
                        "a store-full receive must not ack id {b_id:?} or {x_id:?}"
                    );
                }
            }
        }
        assert!(
            ex.acks.entries.is_empty(),
            "the store-full acks never left the buffer"
        );
    }

    #[test]
    fn ack_fits_mtu() {
        let a: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        empty_summary(&mut ex, &mut store, 1, a);

        // Receive 12 broadcasts from A -> 12 pending broadcast acks.
        for seq in 0..12u64 {
            let y = obs(a, seq, 0, None, b"t", b"bc");
            let frame = Frame::Obs { from: a, obs: y };
            ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0);
        }
        assert_eq!(ex.bcast_acks.len(), 12, "each received broadcast is acked");

        // At an mtu too small for all 12, every Ack carries only a few ids,
        // split across frames.
        let mtu = 110usize;
        let mut max_ids = 0usize;
        let mut acks = 0usize;
        for _ in 0..consts::MAX_FRAME {
            let mut buf = [0u8; consts::MAX_FRAME];
            if let Some((_, n)) = ex.next_frame(&store, mtu, 0, &mut buf) {
                match Frame::decode(&buf[..n]) {
                    Ok(Frame::Ack { ids, .. }) => {
                        acks += 1;
                        max_ids = max_ids.max(ids.len());
                    }
                    Ok(Frame::Summary { .. }) => {}
                    _ => {}
                }
            }
        }
        assert!(
            acks >= 2,
            "12 ids must split across multiple Ack frames at mtu {mtu}, got {acks}"
        );
        assert!(
            max_ids <= 5,
            "each Ack stays within mtu (max {max_ids} ids)"
        );
    }

    #[test]
    fn broadcast_flood_does_not_evict_custody_ack() {
        let a: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        empty_summary(&mut ex, &mut store, 1, a);

        // An addressed custody message A -> B, which B received (pending
        // addressed ack).
        let x = observed_addressed(a, 1, ME, b"t", b"hi");
        let x_id = x.id();
        let frame = Frame::Obs { from: a, obs: x };
        ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0);
        assert!(
            !ex.acks.entries.is_empty(),
            "an addressed message has a pending ack"
        );

        // Flood B with far more broadcasts than bcast_acks can hold; overflow
        // drops the oldest.
        for seq in 0..400u64 {
            let y = obs(a, seq, 0, None, b"t", b"bc");
            let frame = Frame::Obs { from: a, obs: y };
            ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0);
        }
        assert!(
            ex.bcast_acks.entries.is_full(),
            "bcast_acks flooded to capacity"
        );

        // Draining must still emit an Ack carrying the addressed custody id X.
        let mut saw_x = false;
        for _ in 0..consts::MAX_FRAME {
            let mut buf = [0u8; consts::MAX_FRAME];
            if let Some((_, n)) = ex.next_frame(&store, consts::MAX_FRAME, 0, &mut buf) {
                if let Ok(Frame::Ack { ids, .. }) = Frame::decode(&buf[..n]) {
                    if ids.contains(&x_id) {
                        saw_x = true;
                    }
                }
            }
        }
        assert!(
            saw_x,
            "an addressed pending ack must survive a broadcast flood"
        );
    }

    #[test]
    fn duplicate_broadcast_is_acked() {
        let a: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        empty_summary(&mut ex, &mut store, 1, a);

        // B receives broadcast Y from A, then the same Y again (a duplicate).
        let y = obs(a, 1, 0, None, b"t", b"bc");
        let y_id = y.id();

        // First reception yields an Ack for Y.
        let frame1 = Frame::Obs {
            from: a,
            obs: y.clone(),
        };
        ex.on_event(Event::Frame(1, frame_bytes(&frame1)), &mut store, 0);
        assert!(ack_for(&mut ex, &store, y_id), "first reception is acked");

        // Duplicate reception yields another Ack for Y (duplicates ARE acked).
        let frame2 = Frame::Obs { from: a, obs: y };
        ex.on_event(Event::Frame(1, frame_bytes(&frame2)), &mut store, 0);
        assert!(
            ack_for(&mut ex, &store, y_id),
            "duplicate reception is acked"
        );
    }

    /// An mtu too small for even one id drops only the offending destination's
    /// oldest pending ack and leaves a *different* peer's pending ack untouched.
    #[test]
    fn drain_ack_buffer_overflow_drops_only_target_peer() {
        let a: NodeId = [2; 16];
        let b: NodeId = [9; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        empty_summary(&mut ex, &mut store, 1, a);
        empty_summary(&mut ex, &mut store, 2, b);

        // One broadcast ack pending for addr 1 (a), one for addr 2 (b).
        let y1 = obs(a, 1, 0, None, b"t", b"bc");
        let y2 = obs(b, 1, 0, None, b"t", b"bc");
        ex.on_event(
            Event::Frame(1, frame_bytes(&Frame::Obs { from: a, obs: y1 })),
            &mut store,
            0,
        );
        ex.on_event(
            Event::Frame(2, frame_bytes(&Frame::Obs { from: b, obs: y2 })),
            &mut store,
            0,
        );
        assert_eq!(ex.bcast_acks.len(), 2, "each broadcast has a pending ack");

        // An mtu too small for even a single id: the first poll drops addr 1's
        // oldest pending ack but must leave addr 2's untouched.
        let mut buf = [0u8; consts::MAX_FRAME];
        assert!(
            ex.next_frame(&store, 4usize, 0, &mut buf).is_none(),
            "no ack fits the tiny mtu"
        );
        assert_eq!(
            ex.bcast_acks.len(),
            1,
            "only the offending destination's ack was dropped"
        );
        let remaining = ex.bcast_acks.entries.first().map(|(_, addr)| *addr);
        assert_eq!(
            remaining,
            Some(2u8),
            "addr 2's pending ack is preserved, not a dropped peer's"
        );
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
        o.dest = Some(Dest {
            to: ME,
            lifetime_ms: 100,
        });
        o.age_ms = 200;
        let frame = Frame::Obs {
            from: [2; 16],
            obs: o.clone(),
        };
        assert!(
            ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0)
                .is_none(),
            "an expired addressed obs must not be delivered"
        );
        assert_eq!(
            store.len(),
            0,
            "an expired addressed obs must not be stored"
        );
    }

    #[test]
    fn expired_addressed_frag_is_rejected() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // A large addressed obs that must be fragmented; already expired.
        let mut big = obs([2; 16], 7, 0, None, b"t", &[0u8; 1024]);
        big.dest = Some(Dest {
            to: ME,
            lifetime_ms: 100,
        });
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
    fn valid_addressed_obs_is_accepted() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        ex.on_event(Event::Contact(1), &mut store, 0);
        // A well-formed, non-expired addressed obs addressed to us IS stored.
        let mut o = obs([2; 16], 7, 0, None, b"t", b"hi");
        o.dest = Some(Dest {
            to: ME,
            lifetime_ms: 1000,
        });
        let id = o.id();
        let frame = Frame::Obs {
            from: [2; 16],
            obs: o.clone(),
        };
        assert!(
            ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0)
                .is_some(),
            "a valid addressed obs must be accepted"
        );
        assert!(store.contains(&id));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn expired_addressed_obs_is_not_forwarded() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // An addressed obs already past its lifetime, inserted directly so it
        // bypasses the receive-side reject (this tests the FORWARD path).
        let mut o = obs([2; 16], 7, 0, None, b"t", b"hi");
        o.dest = Some(Dest {
            to: [3; 16],
            lifetime_ms: 100,
        });
        o.age_ms = 200;
        let id = o.id();
        store.insert(o, 0).unwrap();

        // A peer whose NodeId we learn (not the recipient, so epidemic applies).
        ex.on_event(Event::Contact(1), &mut store, 0);
        let bits = heapless::Vec::from_slice(&[0u8; consts::SUMMARY_BYTES]).unwrap();
        let sum = Frame::Summary {
            from: [2; 16],
            salt: 0,
            bits,
        };
        ex.on_event(Event::Frame(1, frame_bytes(&sum)), &mut store, 0);

        // The expired obs must never be forwarded to any peer.
        assert!(
            drain_addressed_obs(&mut ex, &store, Some([3; 16])).is_none(),
            "an expired addressed obs must not be forwarded"
        );
        // It is still held (expiry is checked at select/forward time, not
        // retroactively removed here).
        assert!(store.contains(&id));
    }

    #[test]
    fn addressed_obs_not_in_summary() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let b = obs([2; 16], 1, 0, None, b"t", b"b");
        let b_id = b.id();
        let mut a = obs([2; 16], 2, 0, None, b"t", b"a");
        a.dest = Some(Dest {
            to: [2; 16],
            lifetime_ms: consts::SUMMARY_INTERVAL_MS * 10,
        });
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

    /// Drains `next_frame` and returns the first `Obs` frame whose destination matches
    /// `want`, along with its destination address — skipping summaries and
    /// non-Obs frames. Breaks on the first match or when `next_frame` yields
    /// `None`.
    fn drain_addressed_obs(
        ex: &mut Exchange<u8>,
        store: &MemStore,
        want: Option<[u8; 16]>,
    ) -> Option<(u8, ObservationId)> {
        let mut result: Option<(u8, ObservationId)> = None;
        for _ in 0..consts::MAX_FRAME {
            let mut buf = [0u8; consts::MAX_FRAME];
            match ex.next_frame(store, consts::MAX_FRAME, 0, &mut buf) {
                Some((addr, n)) => match Frame::decode(&buf[..n]).unwrap() {
                    Frame::Obs { obs, .. } if obs.dest.as_ref().map(|d| d.to) == want => {
                        result = Some((addr, obs.id()));
                        break;
                    }
                    _ => {}
                },
                None => break,
            }
        }
        result
    }

    #[test]
    fn addressed_obs_delivered_to_recipient() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // An addressed obs to [2;16] already held in the store.
        let mut o = obs([2; 16], 5, 0, None, b"t", b"hi");
        o.dest = Some(Dest {
            to: [2; 16],
            lifetime_ms: consts::SUMMARY_INTERVAL_MS * 10,
        });
        let id = o.id();
        store.insert(o, 0).unwrap();

        // Peer at addr 1 contacts us and learns its id from a broadcast frame
        // (so peer.id = Some([2; 16]) without recording a bounce for our obs).
        ex.on_event(Event::Contact(1), &mut store, 0);
        let hello = Frame::Obs {
            from: [2; 16],
            obs: obs([2; 16], 99, 0, None, b"t", b"x"),
        };
        ex.on_event(Event::Frame(1, frame_bytes(&hello)), &mut store, 0);
        assert_eq!(ex.peers()[0].id, Some([2; 16]));

        // Draining must deliver the addressed Obs to its recipient at addr 1.
        let got = drain_addressed_obs(&mut ex, &store, Some([2; 16]));
        let (addr, emitted_id) =
            got.expect("addressed obs must be delivered to its recipient at addr 1");
        assert_eq!(addr, 1, "delivered to the recipient peer");
        assert_eq!(emitted_id, id, "the exact addressed obs was delivered");
    }

    #[test]
    fn addressed_obs_epidemic_forwards_to_non_recipient() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // Addressed to an unknown node [3;16]; the only peer's id is [2;16].
        let mut o = obs([2; 16], 5, 0, None, b"t", b"hi");
        o.dest = Some(Dest {
            to: [3; 16],
            lifetime_ms: consts::SUMMARY_INTERVAL_MS * 10,
        });
        let id = o.id();
        store.insert(o, 0).unwrap();

        ex.on_event(Event::Contact(1), &mut store, 0);
        let hello = Frame::Obs {
            from: [2; 16],
            obs: obs([2; 16], 99, 0, None, b"t", b"x"),
        };
        ex.on_event(Event::Frame(1, frame_bytes(&hello)), &mut store, 0);

        // Draining must epidemic-forward the obs to addr 1 even though the
        // recipient [3;16] is not yet a known peer (never dropped).
        let got = drain_addressed_obs(&mut ex, &store, Some([3; 16]));
        let (addr, emitted_id) =
            got.expect("unknown-recipient addressed obs must be epidemic-forwarded");
        assert_eq!(addr, 1);
        assert_eq!(emitted_id, id);
    }

    #[test]
    fn addressed_obs_does_not_bounce_to_source() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // A single addressed obs, received from addr 1, addressed to [3;16].
        let mut o = obs([2; 16], 7, 0, None, b"t", b"hi");
        o.dest = Some(Dest {
            to: [3; 16],
            lifetime_ms: consts::SUMMARY_INTERVAL_MS * 10,
        });
        let frame = Frame::Obs {
            from: [2; 16],
            obs: o,
        };
        ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0);

        // Draining must never bounce the addressed obs back to its source (addr
        // 1): only a summary should come out, never an Obs to [3;16] at addr 1.
        let mut bounced = false;
        for _ in 0..consts::MAX_FRAME {
            let mut buf = [0u8; consts::MAX_FRAME];
            match ex.next_frame(&store, consts::MAX_FRAME, 0, &mut buf) {
                Some((addr, n)) => match Frame::decode(&buf[..n]).unwrap() {
                    Frame::Obs { obs, .. }
                        if obs.dest.as_ref().map(|d| d.to) == Some([3; 16]) && addr == 1 =>
                    {
                        bounced = true;
                        break;
                    }
                    _ => {}
                },
                None => break,
            }
        }
        assert!(
            !bounced,
            "an addressed obs must not be forwarded back to its source"
        );
    }

    /// A payload whose bytes differ by position, so a fragment cut from the
    /// wrong offset corrupts the reassembled observation.
    fn ramp(len: usize) -> std::vec::Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn addressed(observer: [u8; 16], seq: u64, to: [u8; 16], payload: &[u8]) -> Observation {
        let mut o = obs(observer, seq, 0, None, b"t", payload);
        o.dest = Some(Dest {
            to,
            lifetime_ms: 1_000_000,
        });
        o
    }

    /// Feeds an empty full-size summary from `from` at `addr`, so the peer is
    /// known by id and appears to lack every broadcast observation.
    fn empty_summary(ex: &mut Exchange<u8>, store: &mut MemStore, addr: u8, from: NodeId) {
        let bits = heapless::Vec::from_slice(&[0u8; consts::SUMMARY_BYTES]).unwrap();
        let sum = Frame::Summary {
            from,
            salt: 0,
            bits,
        };
        ex.on_event(Event::Frame(addr, frame_bytes(&sum)), store, 0);
    }

    /// Polls `ex` once at `now`/`mtu` and decodes the frame, if any.
    fn poll_frame(
        ex: &mut Exchange<u8>,
        store: &MemStore,
        mtu: usize,
        now: Tick,
    ) -> Option<(u8, Frame, FrameBufT)> {
        let mut buf = [0u8; consts::MAX_FRAME];
        let (addr, n) = ex.next_frame(store, mtu, now, &mut buf)?;
        assert!(n <= mtu, "emitted a {n}-byte frame over mtu {mtu}");
        let bytes = heapless::Vec::from_slice(&buf[..n]).unwrap();
        Some((addr, Frame::decode(&buf[..n]).unwrap(), bytes))
    }

    type FrameBufT = heapless::Vec<u8, { consts::MAX_FRAME }>;

    #[test]
    fn frag_stream_survives_tick_going_backwards() {
        // Pick a payload whose encoding at age 20 000 (a 3-byte varint) is one
        // byte past a whole number of 136-byte chunks (mtu 200): re-encoding at
        // a 1-byte age would then leave the last fragment's start past the end.
        let chunk = 200 - crate::frag::FRAG_OVERHEAD;
        let big = (900..1024)
            .map(|len| obs([2; 16], 1, 20_000, None, b"t", &ramp(len)))
            .find(|o| {
                let mut buf = [0u8; consts::MAX_FRAME * 2];
                postcard::to_slice(o, &mut buf).unwrap().len() % chunk == 1
            })
            .unwrap();
        let big = Observation { age_ms: 0, ..big };
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let id = big.id();
        store.insert(big, 0).unwrap();
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, [9; 16]);

        let mut rx = Exchange::new([9; 16]);
        let mut rx_store = MemStore::new();
        // Send everything but the last fragment at a large age (3-byte
        // varint), then let the tick jump backwards so the age re-encodes
        // shorter just before the final fragment is cut.
        let mut sent = 0;
        for i in 0..64 {
            let now = if i < 8 { 20_000 } else { 100 };
            let Some((_, frame, bytes)) = poll_frame(&mut ex, &store, 200, now) else {
                break;
            };
            if matches!(frame, Frame::Frag { .. }) {
                sent += 1;
            }
            rx.on_event(Event::Frame(7, bytes), &mut rx_store, now);
        }
        assert!(sent > 1, "the observation must have been fragmented");
        assert!(
            rx_store.contains(&id),
            "receiver must reassemble the observation"
        );
    }

    #[test]
    fn frag_stream_survives_age_crossing_varint_boundary() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let big = obs([2; 16], 1, 0, None, b"t", &ramp(1000));
        let id = big.id();
        store.insert(big, 0).unwrap();
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, [9; 16]);

        let mut rx = Exchange::new([9; 16]);
        let mut rx_store = MemStore::new();
        // Polling every 50 ms carries age_ms past 127 (1 -> 2 varint bytes)
        // part-way through the stream.
        for i in 0..64u64 {
            let now = i * 50;
            let Some((_, _, bytes)) = poll_frame(&mut ex, &store, 200, now) else {
                break;
            };
            rx.on_event(Event::Frame(7, bytes), &mut rx_store, now);
        }
        assert!(
            rx_store.contains(&id),
            "receiver must reassemble the observation"
        );
    }

    #[test]
    fn summary_fits_small_mtu_and_drives_sync() {
        let mtu = 200;
        let mut a = Exchange::new([1; 16]);
        let mut sa = MemStore::new();
        let mut b = Exchange::new([2; 16]);
        let mut sb = MemStore::new();
        let oa = obs([1; 16], 1, 0, None, b"t", b"from-a");
        let ob = obs([2; 16], 1, 0, None, b"t", b"from-b");
        let (ida, idb) = (oa.id(), ob.id());
        sa.insert(oa, 0).unwrap();
        sb.insert(ob, 0).unwrap();
        a.on_event(Event::Contact(2), &mut sa, 0);
        b.on_event(Event::Contact(1), &mut sb, 0);

        for _ in 0..20 {
            if let Some((_, _, bytes)) = poll_frame(&mut a, &sa, mtu, 0) {
                b.on_event(Event::Frame(1, bytes), &mut sb, 0);
            }
            if let Some((_, _, bytes)) = poll_frame(&mut b, &sb, mtu, 0) {
                a.on_event(Event::Frame(2, bytes), &mut sa, 0);
            }
        }
        assert!(
            sa.contains(&idb),
            "a must receive b's observation over a small mtu"
        );
        assert!(
            sb.contains(&ida),
            "b must receive a's observation over a small mtu"
        );
    }

    #[test]
    fn bounce_table_overflow_does_not_starve_broadcast_or_custody() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let mut want = std::collections::BTreeSet::new();
        for seq in 0..300u64 {
            let o = addressed([3; 16], seq, peer, b"x");
            want.insert(o.id());
            store.insert(o, 0).unwrap();
        }
        let bcast = obs([3; 16], 1000, 0, None, b"t", b"broadcast");
        let bid = bcast.id();
        store.insert(bcast, 0).unwrap();
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, peer);

        let mut got = std::collections::BTreeSet::new();
        let mut saw_broadcast = false;
        for _ in 0..700 {
            match poll_frame(&mut ex, &store, consts::MAX_FRAME, 0) {
                Some((_, Frame::Obs { obs, .. }, _)) if obs.dest.is_some() => {
                    got.insert(obs.id());
                }
                Some((_, Frame::Obs { obs, .. }, _)) if obs.id() == bid => saw_broadcast = true,
                _ => {}
            }
        }
        assert!(
            saw_broadcast,
            "custody traffic must not starve the broadcast diff"
        );
        assert_eq!(got, want, "every addressed message must be delivered");
    }

    #[test]
    fn addressed_frag_streams_are_not_clobbered() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let x = addressed([3; 16], 1, peer, &ramp(1000));
        let y = addressed([3; 16], 2, peer, &ramp(999));
        let (idx, idy) = (x.id(), y.id());
        store.insert(x, 0).unwrap();
        store.insert(y, 0).unwrap();
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, peer);

        let mut rx = Exchange::new(peer);
        let mut rx_store = MemStore::new();
        for _ in 0..64 {
            let Some((_, _, bytes)) = poll_frame(&mut ex, &store, 200, 0) else {
                break;
            };
            rx.on_event(Event::Frame(7, bytes), &mut rx_store, 0);
        }
        assert!(
            rx_store.contains(&idx),
            "first addressed stream must complete"
        );
        assert!(
            rx_store.contains(&idy),
            "second addressed stream must complete"
        );
    }

    #[test]
    fn failed_send_is_retried() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let a = addressed([3; 16], 1, peer, b"addressed");
        let b = obs([3; 16], 2, 0, None, b"t", b"broadcast");
        let big = obs([3; 16], 3, 5, None, b"t", &ramp(1000));
        store.insert(a, 0).unwrap();
        store.insert(b, 0).unwrap();
        store.insert(big, 0).unwrap();
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, peer);

        // Every frame that hits a Busy transport must come out again on the
        // next poll, unchanged.
        let mut frames = 0;
        while let Some((_, first, _)) = poll_frame(&mut ex, &store, 200, 0) {
            ex.on_send_error(&SendError::Busy);
            let (_, again, _) = poll_frame(&mut ex, &store, 200, 0).expect("retry after Busy");
            assert_eq!(again, first, "a Busy frame must be re-emitted");
            frames += 1;
            assert!(frames < 64, "exchange never drained");
        }
        assert!(
            frames > 3,
            "summary, addressed, broadcast and fragments were all exercised"
        );
    }

    #[test]
    fn bounce_suppression_resets_on_new_contact() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let o = addressed([3; 16], 1, peer, b"hi");
        let id = o.id();
        store.insert(o, 0).unwrap();
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, peer);
        assert_eq!(
            drain_addressed_obs(&mut ex, &store, Some(peer)),
            Some((1, id))
        );

        // The frame was lost; the peer leaves and comes back.
        ex.on_event(Event::Lost(1), &mut store, 0);
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, peer);
        assert_eq!(
            drain_addressed_obs(&mut ex, &store, Some(peer)),
            Some((1, id)),
            "a new contact must get another delivery attempt"
        );
    }

    #[test]
    fn unemittable_addressed_obs_does_not_block_others() {
        let peer: NodeId = [2; 16];
        // Small enough that a 1000-byte obs needs more than 64 fragments, big
        // enough that a tiny obs fits in one Obs frame.
        let mtu = 80;
        let small = addressed([3; 16], 0, peer, b"s");
        let sid = small.id();
        let mut seq = 1;
        let big = loop {
            let b = addressed([3; 16], seq, peer, &ramp(1000));
            if b.id() < sid {
                break b;
            }
            seq += 1;
        };
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        store.insert(big, 0).unwrap();
        store.insert(small, 0).unwrap();
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, peer);

        let mut delivered = false;
        for _ in 0..8 {
            if let Some((_, Frame::Obs { obs, .. }, _)) = poll_frame(&mut ex, &store, mtu, 0) {
                delivered |= obs.id() == sid;
            }
        }
        assert!(
            delivered,
            "a deliverable message must not wait behind an undeliverable one"
        );
    }

    #[test]
    fn duplicate_addressed_obs_is_not_sent_back() {
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let o = addressed([3; 16], 1, [4; 16], b"hi");
        // Two carriers, at addrs 1 and 2, each hand us the same message.
        for (addr, from) in [(1u8, [5u8; 16]), (2, [6; 16])] {
            let f = Frame::Obs {
                from,
                obs: o.clone(),
            };
            ex.on_event(Event::Frame(addr, frame_bytes(&f)), &mut store, 0);
        }
        for _ in 0..16 {
            if let Some((addr, Frame::Obs { obs, .. }, _)) =
                poll_frame(&mut ex, &store, consts::MAX_FRAME, 0)
            {
                panic!("sent {:?} back to carrier {addr}", obs.id());
            }
        }
    }

    /// A `MemStore` that counts `get` calls.
    struct CountingStore {
        inner: MemStore,
        gets: core::cell::Cell<usize>,
    }

    impl Store for CountingStore {
        fn insert(&mut self, obs: Observation, now: Tick) -> Result<bool, crate::StoreError> {
            self.inner.insert(obs, now)
        }
        fn contains(&self, id: &ObservationId) -> bool {
            self.inner.contains(id)
        }
        fn get(&self, id: &ObservationId, now: Tick) -> Option<Observation> {
            self.gets.set(self.gets.get() + 1);
            self.inner.get(id, now)
        }
        fn len(&self) -> usize {
            self.inner.len()
        }
        fn for_each_id(&self, f: &mut dyn FnMut(&ObservationId)) {
            self.inner.for_each_id(f)
        }
        fn for_each_broadcast_id(&self, f: &mut dyn FnMut(&ObservationId)) {
            self.inner.for_each_broadcast_id(f)
        }
        fn for_each_addressed_id(&self, f: &mut dyn FnMut(&ObservationId)) {
            self.inner.for_each_addressed_id(f)
        }
        fn release(&mut self, id: &ObservationId) {
            self.inner.release(id)
        }
        fn is_released(&self, id: &ObservationId) -> bool {
            self.inner.is_released(id)
        }
        fn for_each_topic(&self, prefix: &[u8], now: Tick, f: &mut dyn FnMut(&Observation)) {
            self.inner.for_each_topic(prefix, now, f)
        }
        fn trim(&mut self, policy: &crate::TrimPolicy, now: Tick) -> usize {
            self.inner.trim(policy, now)
        }
    }

    #[test]
    fn custody_scan_skips_broadcast_observations() {
        let mut ex = Exchange::new(ME);
        let mut store = CountingStore {
            inner: MemStore::new(),
            gets: core::cell::Cell::new(0),
        };
        let mut theirs = Bloom::new(0);
        for seq in 0..200 {
            let o = obs([3; 16], seq, 0, None, b"t", b"x");
            theirs.insert(&o.id());
            store.insert(o, 0).unwrap();
        }
        ex.on_event(Event::Contact(1), &mut store, 0);
        // The peer already holds every broadcast, so there is nothing to send.
        let sum = Frame::Summary {
            from: [2; 16],
            salt: 0,
            bits: heapless::Vec::from_slice(theirs.bits()).unwrap(),
        };
        ex.on_event(Event::Frame(1, frame_bytes(&sum)), &mut store, 0);
        let mut buf = [0u8; consts::MAX_FRAME];
        ex.next_frame(&store, consts::MAX_FRAME, 0, &mut buf); // our summary
        store.gets.set(0);
        for _ in 0..10 {
            assert!(ex
                .next_frame(&store, consts::MAX_FRAME, 0, &mut buf)
                .is_none());
        }
        assert_eq!(
            store.gets.get(),
            0,
            "custody must not read broadcast observations"
        );
    }

    /// Drives `ex` so that `id` (addressed or broadcast) is emitted to `addr`:
    /// contact, an empty summary (so the peer lacks everything), then poll
    /// `next_frame` until an Obs carrying `id` goes out. The emit path records
    /// the Sent bounce (addressed) or the `bcast_sent` entry (broadcast).
    fn deliver_to(ex: &mut Exchange<u8>, store: &mut MemStore, addr: u8, id: ObservationId) {
        ex.on_event(Event::Contact(addr), store, 0);
        empty_summary(ex, store, addr, [2; 16]);
        for _ in 0..consts::MAX_FRAME {
            if matches!(
                poll_frame(ex, store, consts::MAX_FRAME, 0),
                Some((_, Frame::Obs { obs, .. }, _)) if obs.id() == id
            ) {
                break;
            }
        }
    }

    /// An Ack for an addressed observation the peer received releases custody:
    /// the id is marked released and is no longer selected for forwarding.
    #[test]
    fn ack_releases_addressed_custody_after_sent() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let x = addressed([3; 16], 1, peer, b"hi");
        let x_id = x.id();
        store.insert(x, 0).unwrap();
        deliver_to(&mut ex, &mut store, 1, x_id);
        assert!(
            !store.is_released(&x_id),
            "custody is held until the peer confirms delivery"
        );

        let ids = heapless::Vec::from_slice(&[x_id]).unwrap();
        let ack = Frame::Ack { from: peer, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);

        assert!(
            store.is_released(&x_id),
            "an Ack releases custody for a dispatched addressed id"
        );
    }

    /// After an Ack releases custody, a new contact drops the bounce records,
    /// but the released id is still not selected: the is_released mark on the
    /// stored entry keeps it out of forwarding.
    #[test]
    fn ack_released_id_will_reselect_and_reject() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let x = addressed([3; 16], 1, peer, b"hi");
        let x_id = x.id();
        store.insert(x, 0).unwrap();
        deliver_to(&mut ex, &mut store, 1, x_id);

        let ids = heapless::Vec::from_slice(&[x_id]).unwrap();
        let ack = Frame::Ack { from: peer, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);
        assert!(store.is_released(&x_id), "an Ack releases custody of X");

        // A new contact drops the bounce records; the released id must still be
        // refused by the custody scan.
        ex.on_event(Event::Lost(1), &mut store, 0);
        ex.on_event(Event::Contact(1), &mut store, 0);
        empty_summary(&mut ex, &mut store, 1, peer);

        let mut reemitted = false;
        for _ in 0..consts::MAX_FRAME {
            if matches!(
                poll_frame(&mut ex, &store, consts::MAX_FRAME, 0),
                Some((_, Frame::Obs { obs, .. }, _)) if obs.id() == x_id
            ) {
                reemitted = true;
                break;
            }
        }
        assert!(
            !reemitted,
            "a released id must not be selected again after a new contact"
        );
    }

    /// The custody chain releases on the first Ack from ANY peer the node
    /// dispatched the observation to — a relay (a non-recipient that merely
    /// holds an epidemic-forwarded copy), not just the intended recipient. This
    /// is the binding release semantics; release tracks that a copy left us, not
    /// final delivery.
    #[test]
    fn ack_releases_custody_from_a_relay() {
        let relay: NodeId = [2; 16];
        let recipient: NodeId = [3; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // ME holds X, addressed to `recipient`, and dispatched it to the relay.
        let x = observed_addressed([7; 16], 1, recipient, b"t", b"hi");
        let x_id = x.id();
        store.insert(x.clone(), 0).unwrap();
        deliver_to(&mut ex, &mut store, 1, x_id);
        assert!(
            !store.is_released(&x_id),
            "custody is held until an ack from a peer we sent to"
        );

        // A relay (not the recipient) confirms it holds X.
        let ids = heapless::Vec::from_slice(&[x_id]).unwrap();
        let ack = Frame::Ack { from: relay, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);

        assert!(
            store.is_released(&x_id),
            "an Ack from any peer we sent to (a relay here) releases custody"
        );
    }

    /// An Ack for a broadcast we dispatched to the peer fires exactly one
    /// `NodeEvent::Delivered` naming the peer and the broadcast id.
    #[test]
    fn ack_broadcast_fires_confidence_event() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let b = obs([3; 16], 1, 0, None, b"t", b"bc");
        let b_id = b.id();
        store.insert(b, 0).unwrap();
        deliver_to(&mut ex, &mut store, 1, b_id);
        assert!(!store.is_released(&b_id));

        let ids = heapless::Vec::from_slice(&[b_id]).unwrap();
        let ack = Frame::Ack { from: peer, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);

        let events = ex.drain_events();
        assert_eq!(
            events.len(),
            1,
            "one broadcast ack fires one confidence event"
        );
        match events[0] {
            NodeEvent::Delivered {
                peer: ev_peer,
                id: ev_id,
            } => {
                assert_eq!(ev_peer, peer);
                assert_eq!(ev_id, b_id);
            }
            other => panic!("expected a Delivered event, got {other:?}"),
        }
    }

    /// A broadcast Ack does not release custody: there is no local addressed
    /// copy to release, so the broadcast stays held.
    #[test]
    fn ack_broadcast_release_does_not_store_broadcast() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let b = obs([3; 16], 1, 0, None, b"t", b"bc");
        let b_id = b.id();
        store.insert(b, 0).unwrap();
        deliver_to(&mut ex, &mut store, 1, b_id);
        assert!(
            !store.is_released(&b_id),
            "a broadcast is held, not released"
        );

        let ids = heapless::Vec::from_slice(&[b_id]).unwrap();
        let ack = Frame::Ack { from: peer, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);

        assert!(
            !store.is_released(&b_id),
            "an Ack must not release a broadcast we dispatched"
        );
    }

    /// An Ack for an id the local node only received (not dispatched) must NOT
    /// release it: release applies to dispatched ids only.
    #[test]
    fn is_released_not_selected_after_ack() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        // A broadcast received from the peer: the node only received it.
        let x = obs(peer, 1, 0, None, b"t", b"hi");
        let x_id = x.id();
        let frame = Frame::Obs { from: peer, obs: x };
        ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0);
        assert!(store.contains(&x_id));
        assert!(store.get(&x_id, 0).unwrap().dest.is_none());

        let ids = heapless::Vec::from_slice(&[x_id]).unwrap();
        let ack = Frame::Ack { from: peer, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);

        assert!(
            !store.is_released(&x_id),
            "an Ack must not release an id the local node only received"
        );
    }

    /// The custody scan skips a released id but still offers non-released ids:
    /// releasing the broadcast leaves the addressed id offered, and releasing
    /// the addressed id leaves nothing offered.
    #[test]
    fn select_addressed_still_selects_non_released() {
        let mut store = MemStore::new();
        let a = addressed([3; 16], 1, [4; 16], b"a");
        let b = obs([3; 16], 2, 0, None, b"t", b"bc");
        let a_id = a.id();
        let b_id = b.id();
        store.insert(a, 0).unwrap();
        store.insert(b, 0).unwrap();

        // Release the broadcast only: the non-released addressed id is still
        // offered.
        store.release(&b_id);
        let empty: &[(ObservationId, u8, BounceKind)] = &[];
        let found = select_addressed(&store, 0, empty, &1u8, None, |_id, obs| obs.dest.is_some());
        assert_eq!(found.map(|(id, _)| id), Some(a_id));

        // Release the addressed id too: nothing remains selectable.
        store.release(&a_id);
        let found = select_addressed(&store, 0, empty, &1u8, None, |_id, obs| obs.dest.is_some());
        assert!(found.is_none(), "an id released by an Ack is not selected");
    }

    /// After an Ack releases custody, the release mark persists on the stored
    /// entry across a new contact: dropping the exchange's bounce records does
    /// not re-open the id, so it is never offered again.
    #[test]
    fn is_released_not_offered_after_contact_ends() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let x = addressed([3; 16], 1, peer, b"hi");
        let x_id = x.id();
        store.insert(x, 0).unwrap();
        deliver_to(&mut ex, &mut store, 1, x_id);

        let ids = heapless::Vec::from_slice(&[x_id]).unwrap();
        let ack = Frame::Ack { from: peer, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);
        assert!(
            store.is_released(&x_id),
            "an Ack releases custody for a dispatched id"
        );

        // A new contact drops the exchange's bounce records, but the release
        // mark on the stored entry persists.
        ex.on_event(Event::Lost(1), &mut store, 0);
        ex.on_event(Event::Contact(1), &mut store, 0);
        assert!(
            store.is_released(&x_id),
            "the release mark persists across a new contact; the id is not re-offered"
        );
    }

    /// A broadcast Ack from a peer that never held the broadcast fires no
    /// `NodeEvent::Delivered`: the receipt names a broadcast we did not dispatch
    /// to that peer.
    #[test]
    fn ack_broadcast_forged_id_no_confidence() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let b = obs([3; 16], 1, 0, None, b"t", b"bc");
        let b_id = b.id();
        store.insert(b, 0).unwrap();
        // Dispatch the broadcast to a different peer (addr 2), not addr 1.
        deliver_to(&mut ex, &mut store, 2, b_id);

        // Peer 1 (addr 1) claims to hold b_id though it never did.
        let ids = heapless::Vec::from_slice(&[b_id]).unwrap();
        let ack = Frame::Ack { from: peer, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);

        let events = ex.drain_events();
        assert!(
            events.is_empty(),
            "a broadcast Ack for a broadcast not sent to the peer fires no confidence event"
        );
    }

    /// drain_events returns confidence events oldest-first, in the order the
    /// broadcasts were acknowledged.
    #[test]
    fn drain_events_returns_confidence_events_oldest_first() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let a = obs([3; 16], 1, 0, None, b"t", b"bc-a");
        let b = obs([3; 16], 2, 0, None, b"t", b"bc-b");
        let a_id = a.id();
        let b_id = b.id();
        store.insert(a, 0).unwrap();
        store.insert(b, 0).unwrap();
        deliver_to(&mut ex, &mut store, 1, a_id);
        deliver_to(&mut ex, &mut store, 1, b_id);

        let ack_for = |ids: heapless::Vec<ObservationId, { consts::ACK_IDS_CAP }>| Frame::Ack {
            from: peer,
            ids,
        };

        let ids_a = heapless::Vec::from_slice(&[a_id]).unwrap();
        ex.on_event(Event::Frame(1, frame_bytes(&ack_for(ids_a))), &mut store, 0);
        let ids_b = heapless::Vec::from_slice(&[b_id]).unwrap();
        ex.on_event(Event::Frame(1, frame_bytes(&ack_for(ids_b))), &mut store, 0);

        let events = ex.drain_events();
        assert_eq!(
            events.len(),
            2,
            "each broadcast ack fires a confidence event"
        );
        match events[0] {
            NodeEvent::Delivered { id, .. } => {
                assert_eq!(id, a_id, "events come back oldest-first");
            }
            other => panic!("expected a Delivered event, got {other:?}"),
        }
        match events[1] {
            NodeEvent::Delivered { id, .. } => {
                assert_eq!(id, b_id);
            }
            other => panic!("expected a Delivered event, got {other:?}"),
        }
    }

    /// drain_events empties the queue: a second drain yields nothing.
    #[test]
    fn drain_events_empties_queue() {
        let peer: NodeId = [2; 16];
        let mut ex = Exchange::new(ME);
        let mut store = MemStore::new();
        let b = obs([3; 16], 1, 0, None, b"t", b"bc");
        let b_id = b.id();
        store.insert(b, 0).unwrap();
        deliver_to(&mut ex, &mut store, 1, b_id);

        let ids = heapless::Vec::from_slice(&[b_id]).unwrap();
        let ack = Frame::Ack { from: peer, ids };
        ex.on_event(Event::Frame(1, frame_bytes(&ack)), &mut store, 0);

        let first = ex.drain_events();
        assert_eq!(first.len(), 1, "the queued confidence event is drained");
        match first[0] {
            NodeEvent::Delivered { id, .. } => {
                assert_eq!(id, b_id);
            }
            other => panic!("expected a Delivered event, got {other:?}"),
        }

        let second = ex.drain_events();
        assert!(
            second.is_empty(),
            "drain_events empties the queue; a second drain yields nothing"
        );
    }

    /// A frame whose leading byte isn't WIRE_VERSION is a counted
    /// version-mismatch drop, reported with the sender, the version byte, and a
    /// count of one.
    #[test]
    fn version_mismatch_is_counted() {
        let mut store = MemStore::new();
        let mut ex = Exchange::<u8>::new(ME);
        // The first byte is 0, which isn't WIRE_VERSION: a version mismatch.
        let bytes = heapless::Vec::<u8, { consts::MAX_FRAME }>::from_slice(&[0u8; 10]).unwrap();
        assert_eq!(ex.on_event(Event::Frame(1, bytes), &mut store, 0), None);

        let drained = ex.drain_events();
        assert_eq!(drained.len(), 1, "exactly one counted drop");
        match drained.into_iter().next().unwrap() {
            NodeEvent::Dropped {
                addr,
                reason,
                count,
            } => {
                assert_eq!(addr, Some(1u8));
                assert_eq!(reason, DropReason::VersionMismatch { version: 0 });
                assert_eq!(count, 1);
            }
            other => panic!("expected a counted Dropped, got {other:?}"),
        }
    }

    /// A frame that passes the version gate (byte 0 is WIRE_VERSION) but whose
    /// body isn't a valid postcard [`Frame`] is a counted malformed drop.
    #[test]
    fn malformed_is_counted() {
        let mut store = MemStore::new();
        let mut ex = Exchange::<u8>::new(ME);
        let mut bytes = heapless::Vec::<u8, { consts::MAX_FRAME }>::new();
        bytes.push(consts::WIRE_VERSION).unwrap();
        bytes.push(0xFF).unwrap();
        bytes.push(0xFF).unwrap();
        assert_eq!(ex.on_event(Event::Frame(1, bytes), &mut store, 0), None);

        let drained = ex.drain_events();
        assert_eq!(drained.len(), 1, "exactly one counted drop");
        match drained.into_iter().next().unwrap() {
            NodeEvent::Dropped {
                addr,
                reason,
                count,
            } => {
                assert_eq!(addr, Some(1u8));
                assert_eq!(reason, DropReason::Malformed);
                assert_eq!(count, 1);
            }
            other => panic!("expected a counted Dropped, got {other:?}"),
        }
    }

    /// A repeat of the same (addr, reason) folds into one counter whose count
    /// is two: the two frames are one drop, counted once with count 2.
    #[test]
    fn same_drop_increments_count() {
        let mut store = MemStore::new();
        let mut ex = Exchange::<u8>::new(ME);
        let bytes = heapless::Vec::<u8, { consts::MAX_FRAME }>::from_slice(&[0u8; 10]).unwrap();
        assert_eq!(
            ex.on_event(Event::Frame(1, bytes.clone()), &mut store, 0),
            None
        );
        assert_eq!(ex.on_event(Event::Frame(1, bytes), &mut store, 0), None);

        let drained = ex.drain_events();
        assert_eq!(drained.len(), 1, "a repeat is folded into one counter");
        match drained.into_iter().next().unwrap() {
            NodeEvent::Dropped {
                addr,
                reason,
                count,
            } => {
                assert_eq!(addr, Some(1u8));
                assert_eq!(reason, DropReason::VersionMismatch { version: 0 });
                assert_eq!(count, 2, "both frames counted against one peer+reason");
            }
            other => panic!("expected a single counted Dropped with count 2, got {other:?}"),
        }
    }

    /// An addressed observation that arrives already past its lifetime_ms is
    /// rejected by policy; that rejection is NOT a counted drop (unlike a
    /// decode error).
    #[test]
    fn expired_addressed_is_not_a_drop() {
        let mut store = MemStore::new();
        let mut ex = Exchange::<u8>::new(ME);
        // Addressed to us, lifetime 100ms, but age 200ms -> expired.
        let mut o = obs([2; 16], 7, 200, None, b"t", b"hi");
        o.dest = Some(Dest {
            to: ME,
            lifetime_ms: 100,
        });
        let frame = Frame::Obs {
            from: [2; 16],
            obs: o,
        };
        assert_eq!(
            ex.on_event(Event::Frame(1, frame_bytes(&frame)), &mut store, 0),
            None,
            "an expired addressed obs must not be delivered"
        );
        // A policy rejection is not a counted drop.
        assert!(
            ex.drain_events().is_empty(),
            "an expired addressed obs is rejected by policy, not counted as a drop"
        );
    }

    /// A full delivery queue and a counted drop survive together: drops are
    /// appended after the deliveries and none are evicted (the drop-oldest
    /// eviction of the delivery queue is not in play here, and the drop-table
    /// and delivery queue never evict each other).
    #[test]
    fn drops_do_not_evict_delivered() {
        let mut store = MemStore::new();
        let mut ex = Exchange::<u8>::new(ME);
        // Fill the delivery queue to the brim with confirmed broadcasts.
        for _ in 0..consts::EVENT_QUEUE_CAP {
            ex.events
                .push(NodeEvent::Delivered {
                    peer: [7; 16],
                    id: [0u8; 16],
                })
                .unwrap();
        }
        // One counted drop lands alongside the full delivery queue.
        let bytes = heapless::Vec::<u8, { consts::MAX_FRAME }>::from_slice(&[0u8; 10]).unwrap();
        assert_eq!(ex.on_event(Event::Frame(1, bytes), &mut store, 0), None);

        let drained = ex.drain_events();
        let mut delivered = 0usize;
        let mut dropped = 0usize;
        for ev in drained {
            match ev {
                NodeEvent::Delivered { .. } => delivered += 1,
                NodeEvent::Dropped { .. } => dropped += 1,
            }
        }
        assert_eq!(
            delivered,
            consts::EVENT_QUEUE_CAP,
            "every queued delivery survives a full queue"
        );
        assert_eq!(dropped, 1, "the counted drop is reported alongside them");
    }

    /// Once the drop-counter table is full, the next drop folds into a single
    /// overflow counter reported with `addr: None`; the table's 32 entries are
    /// all still reported with a real address.
    #[test]
    fn overflow_counter_has_no_addr() {
        let mut store = MemStore::new();
        let mut ex = Exchange::<u8>::new(ME);
        // Drive DROP_TABLE_CAP + 1 distinct (addr, reason) pairs: the table
        // holds DROP_TABLE_CAP, so the one beyond the cap folds into the single
        // overflow counter and is reported with no address.
        for addr in 0..=(consts::DROP_TABLE_CAP as u8) {
            let bytes = heapless::Vec::<u8, { consts::MAX_FRAME }>::from_slice(&[0u8; 10]).unwrap();
            assert_eq!(
                ex.on_event(Event::Frame(addr, bytes.clone()), &mut store, 0),
                None
            );
        }

        let drained = ex.drain_events();
        let mut reported = 0usize;
        let mut overflow = 0usize;
        for ev in drained {
            match ev {
                NodeEvent::Dropped { addr: Some(..), .. } => reported += 1,
                NodeEvent::Dropped { addr: None, .. } => overflow += 1,
                NodeEvent::Delivered { .. } => {}
            }
        }
        assert_eq!(
            overflow, 1,
            "the drop beyond the table cap must report with no address"
        );
        assert_eq!(
            reported,
            consts::DROP_TABLE_CAP,
            "all capped entries reported"
        );
    }
}
