//! Node facade for tines-core.
//!
//! [`Node`] is the single entry point that ties the [`Store`], the
//! [`Exchange`] state machine, and a [`Transport`] together behind the two
//! operations an application needs: [`Node::observe`] to record an observation,
//! and [`Node::poll`] to drive the exchange.
//!
//! The facade never touches the heap: its only owned buffer, `out`, is a fixed
//! `[u8; MAX_FRAME]`. It also panics on nothing — oversized topics/payloads
//! become [`ObserveError::TooLarge`] rather than truncating, and malformed
//! transport frames are dropped by the [`Exchange`].

use crate::{
    consts, BeliefPolicy, Dest, Exchange, NodeEvent, NodeId, Observation, ObservationId, Payload,
    Store, StoreError, Tick, Topic, Transport, TrimPolicy,
};

/// Errors returned by [`Node::observe`].
#[derive(Debug, PartialEq, Eq)]
pub enum ObserveError {
    /// The topic or payload exceeded `MAX_TOPIC`/`MAX_PAYLOAD`. The
    /// observation is rejected whole and never truncated.
    TooLarge,
    /// The underlying [`Store`] rejected the observation (e.g. it was full).
    Store(StoreError),
}

/// The application-facing facade over the store, exchange, and transport.
///
/// `Node` is generic over any [`Store`] and [`Transport`], so a concrete
/// backend (heap-backed, `no_std`, ...) can be plugged in without changing the
/// facade. The exchange tracks the peer this node meets on `transport`.
pub struct Node<S: Store, T: Transport> {
    me: NodeId,
    next_seq: u64,
    store: S,
    transport: T,
    exchange: Exchange<T::Addr>,
    out: [u8; consts::MAX_FRAME],
}

impl<S: Store, T: Transport> Node<S, T> {
    /// Creates a node for local id `me`, starting observation sequencing at
    /// `first_seq`.
    ///
    /// Persist [`Node::next_seq`] (e.g. `boot_count << 32 | counter`) across
    /// reboots and pass it back as `first_seq`; a repeated seq after reboot
    /// collides in the content id hash and dedupes silently.
    pub fn new(me: NodeId, first_seq: u64, store: S, transport: T) -> Self {
        Node {
            me,
            next_seq: first_seq,
            store,
            transport,
            exchange: Exchange::new(me),
            out: [0u8; consts::MAX_FRAME],
        }
    }

    /// The local node id.
    pub fn id(&self) -> NodeId {
        self.me
    }

    /// The seq the next [`Node::observe`] will use.
    ///
    /// The application must persist this across reboots (see [`Node::new`]).
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Records an observation made "now" (with `age_ms = 0`) under `topic`.
    ///
    /// Returns the observation's content-derived id. An oversized topic or
    /// payload is rejected with [`ObserveError::TooLarge`] rather than
    /// truncated; a store rejection is surfaced as [`ObserveError::Store`].
    /// Duplicate ids are accepted (the id is still returned).
    pub fn observe(
        &mut self,
        topic: &[u8],
        payload: &[u8],
        observed_at: Option<u64>,
        now: Tick,
    ) -> Result<ObservationId, ObserveError> {
        let topic = Topic::from_slice(topic).map_err(|_| ObserveError::TooLarge)?;
        let payload = Payload::from_slice(payload).map_err(|_| ObserveError::TooLarge)?;
        let obs = Observation {
            observer: self.me,
            seq: self.next_seq,
            age_ms: 0,
            observed_at,
            topic,
            payload,
            dest: None,
            sig: None,
        };
        self.next_seq += 1;
        let id = obs.id();
        self.store.insert(obs, now).map_err(ObserveError::Store)?;
        Ok(id)
    }

    /// Records an observation addressed to `to` with a `lifetime_ms`, made
    /// "now" (with `age_ms = 0`) under `topic`.
    ///
    /// The resulting observation has `dest = Some(Dest { to, lifetime_ms })`,
    /// so the exchange will deliver it only
    /// to `to` (see [`Exchange`]). Returns the observation's content-derived
    /// id. An oversized topic or payload is rejected with
    /// [`ObserveError::TooLarge`] rather than truncated; a store rejection is
    /// surfaced as [`ObserveError::Store`]. Duplicate ids are accepted (the id
    /// is still returned).
    pub fn observe_to(
        &mut self,
        to: NodeId,
        lifetime_ms: u64,
        topic: &[u8],
        payload: &[u8],
        observed_at: Option<u64>,
        now: Tick,
    ) -> Result<ObservationId, ObserveError> {
        let topic = Topic::from_slice(topic).map_err(|_| ObserveError::TooLarge)?;
        let payload = Payload::from_slice(payload).map_err(|_| ObserveError::TooLarge)?;
        let obs = Observation {
            observer: self.me,
            seq: self.next_seq,
            age_ms: 0,
            observed_at,
            topic,
            payload,
            dest: Some(Dest { to, lifetime_ms }),
            sig: None,
        };
        self.next_seq += 1;
        let id = obs.id();
        self.store.insert(obs, now).map_err(ObserveError::Store)?;
        Ok(id)
    }

    /// Drains all pending transport events, storing any newly received
    /// observations, then emits at most one outbound frame.
    ///
    /// Returns the number of *new* observations stored this round. If the
    /// transport refuses the frame with `Busy` or `Unreachable`, the exchange
    /// rolls back and offers the same frame on a later poll; a `TooLarge`
    /// frame is dropped (see [`Exchange::on_send_error`]).
    pub fn poll(&mut self, now: Tick) -> usize {
        let mut stored = 0usize;
        while let Some(ev) = self.transport.poll() {
            if self.exchange.on_event(ev, &mut self.store, now).is_some() {
                stored += 1;
            }
        }
        // Hoist the MTU, then borrow `exchange`/`store`, then borrow
        // `transport` for the send — disjoint field borrows, never overlapping.
        let mtu = self.transport.mtu();
        if let Some((addr, n)) = self
            .exchange
            .next_frame(&self.store, mtu, now, &mut self.out)
        {
            if let Err(err) = self.transport.send(&addr, &self.out[..n]) {
                self.exchange.on_send_error(&err);
            }
        }
        stored
    }

    /// Drains the [`NodeEvent`]s the [`Exchange`] has collected: confirmed
    /// deliveries first (oldest-first), then one [`NodeEvent::Dropped`] per
    /// counted drop.
    ///
    /// The returned [`heapless::Vec`] is capped at
    /// [`consts::EVENT_QUEUE_CAP`] confirmed deliveries plus the entire
    /// [`consts::DROP_TABLE_CAP`]-entry drop table and its overflow slot. Index
    /// it directly — a [`NodeEvent::Delivered`] names the peer that held our
    /// broadcast and the broadcast's content-ID; a [`NodeEvent::Dropped`] names
    /// the peer and reason a dropped frame was rejected with.
    pub fn drain_events(
        &mut self,
    ) -> heapless::Vec<NodeEvent<T::Addr>, { consts::EVENT_QUEUE_CAP + consts::DROP_TABLE_CAP + 1 }>
    {
        self.exchange.drain_events()
    }

    /// The current belief for `topic_prefix` under `policy` (an empty prefix
    /// matches all topics).
    ///
    /// Streams through `Store::for_each_topic`, threading an accumulator
    /// through [`BeliefPolicy::step`] — no buffering of the topic's
    /// observations.
    pub fn belief<P: BeliefPolicy>(
        &self,
        topic_prefix: &[u8],
        policy: &P,
        now: Tick,
    ) -> Option<P::Belief> {
        let mut acc: Option<P::Belief> = None;
        self.store.for_each_topic(topic_prefix, now, &mut |obs| {
            // App-facing view: broadcast plus addressed to `me` only.
            // Drop anything addressed to a different node.
            if matches!(obs.dest.as_ref().map(|d| d.to), Some(other) if other != self.me) {
                return;
            }
            let prev = acc.take();
            acc = policy.step(self.me, prev, obs);
        });
        acc
    }

    /// Applies `policy` to the store and returns the number of observations
    /// removed.
    pub fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize {
        self.store.trim(policy, now)
    }

    /// Access the backing store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Mutable access to the backing store.
    ///
    /// Lets a host call backend-specific methods that aren't on the [`Store`]
    /// trait — e.g. `tines_store::LogStore::flush`, the durability boundary a
    /// host reaches on its flush schedule. [`Self::trim`] takes `&mut self`;
    /// this exposes the concrete backend the same way.
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Access the transport (e.g. to `connect`/`disconnect` a peer in tests).
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}
