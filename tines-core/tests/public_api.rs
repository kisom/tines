//! Compile probe for the tines-core 1.0 public surface (spec §3, decisions H
//! and K).
//!
//! Every path and signature here must keep resolving against `tines_core`. A
//! change that removes or reshapes any of these items fails to build here.
//! CI compiles it with and without the default (`alloc`) feature.
//!
//! The probe deliberately names nothing that §3 makes private: `Frame`,
//! `WireError` and the local constants (`MAX_PEERS`, `PEER_IDLE_MS`, ...).
//! Their absence is pinned by `compile_fail` doctests on `wire::Frame` and
//! `consts::MAX_PEERS` inside the crate.
#![allow(unused_imports, dead_code)]

use tines_core::consts::{
    ACK_IDS_CAP, BLOOM_K, MAX_FRAME, MAX_PAYLOAD, MAX_SIG, MAX_TOPIC, SUMMARY_BYTES, WIRE_VERSION,
};
use tines_core::{
    belief::{BeliefPolicy, LatestWins, SelfFirst},
    node::{Node, ObserveError},
    observation::{
        Dest, NodeId, Observation, ObservationId, Payload, SigAlgo, Signature, Tick, Topic,
    },
    store::{Store, StoreError, TrimPolicy, WallClockStore},
    transport::{Event, FrameBuf, SendError, Transport},
    DropReason, NodeEvent,
};
use tines_core::{consts, frame_sender};

// `MemStore` lives behind the `alloc` feature.
#[cfg(feature = "alloc")]
use tines_core::MemStore;

// The crate-root re-exports name the same items as their modules.
use tines_core::{
    BeliefPolicy as _, Dest as _, Event as _, LatestWins as _, Node as _, Observation as _,
    SelfFirst as _, SendError as _, Signature as _, Store as _, StoreError as _, Transport as _,
    TrimPolicy as _, WallClockStore as _,
};

/// `frame_sender` keeps its signature.
const _FRAME_SENDER: fn(&[u8]) -> Option<NodeId> = frame_sender;

/// The `Node` methods §3 lists, with their signatures, for any store and
/// transport.
fn node_methods<S: Store, T: Transport>(mut node: Node<S, T>, now: Tick) {
    let _: NodeId = node.id();
    let _: u64 = node.next_seq();
    let _: Result<ObservationId, ObserveError> = node.observe(b"t", b"p", None, now);
    let _: Result<ObservationId, ObserveError> =
        node.observe_to([0; 16], 1_000, b"t", b"p", None, now);
    let _: usize = node.poll(now);
    for _ev in node.drain_events() {}
    let _: Option<Observation> = node.belief(b"t", &LatestWins, now);
    let _: usize = node.trim(&TrimPolicy::default(), now);
    let _: &S = node.store();
    let _: &mut T = node.transport_mut();
    let _: Result<(), StoreError> = node.flush(now);
    let _ = Node::<S, T>::new;
}

/// `set_wall_clock` and `peer_time_estimate` exist when the store is a
/// `WallClockStore` (decision H).
fn node_wall_clock<S: WallClockStore, T: Transport>(mut node: Node<S, T>, now: Tick) {
    let _: S::FoldReport = node.set_wall_clock(1_700_000_000_000, now);
    let _: Option<u64> = node.peer_time_estimate(now);
}

/// `WallClockStore`'s shape.
fn wall_clock_store<S: WallClockStore>(mut s: S, now: Tick) {
    let _: S::FoldReport = s.set_wall_clock(1_700_000_000_000, now);
    let _: Option<u64> = s.peer_time_estimate(now);
}

/// `NodeEvent` carries the transport address and is matched with a wildcard
/// (it is `#[non_exhaustive]`).
fn node_event<A: Clone>(ev: NodeEvent<A>) {
    match ev {
        NodeEvent::Delivered { peer, id } => {
            let _: (NodeId, ObservationId) = (peer, id);
        }
        NodeEvent::Dropped {
            addr,
            reason,
            count,
        } => {
            let _: (Option<A>, DropReason, u32) = (addr, reason, count);
        }
        _ => {}
    }
}

#[test]
fn wire_constants_are_public() {
    assert_eq!(WIRE_VERSION, 4);
    assert_eq!(
        (MAX_TOPIC, MAX_PAYLOAD, MAX_FRAME, SUMMARY_BYTES),
        (64, 1024, 1500, 512)
    );
    assert_eq!((ACK_IDS_CAP, MAX_SIG, BLOOM_K), (90, 64, 3));
}
