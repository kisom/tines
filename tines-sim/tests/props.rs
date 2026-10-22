//! Property tests for the tines simulator.
//!
//! These are not deterministic scenarios (see `convergence.rs`) but *properties*
//! that must hold over randomly generated scenarios:
//!
//! * [`connected_components_converge`] — every connected component of the
//!   contact graph ends each run fully gossiped.
//! * [`store_is_independent_of_contact_order`] — reordering the contacts of a
//!   fully-connected 3-node graph does not change the resulting store.
//! * [`union_is_monotone`] — a node's id set never shrinks as the simulation
//!   advances (no trimming).
//! * [`garbage_frames_never_panic`] — malformed wire frames are dropped, never
//!   panic the node.
//! * [`addressed_always_held_by_carrier_or_recipient`] — an addressed task
//!   handed off hop-by-hop along a connected contact path is always held by
//!   some node in the path (the recipient, or a carrier that relayed it): the
//!   custody chain releases on the first ack from *any* peer the node sent it
//!   to, so delivery to the final recipient is best-effort, but custody never
//!   vanishes mid-path.
//! * [`bounce_overflow_keeps_a_holder`] — flooding the shared bounce table
//!   past `SEND_QUEUE_CAP` across `MAX_PEERS` peers (drop-oldest) never drops
//!   the last holder of an id, and the custody cursor still gives every
//!   addressed candidate a turn. Pins §6's overflow-safety guarantee.

use proptest::prelude::*;
use tines_core::{FrameBuf, Store};
use tines_sim::{CappedStore, Contact, ContactTrace, Sim, WorldConfig};

/// Mirrors tines-core's peer-table capacity, a crate-private local constant
/// since 1.0 (decision K). `local_caps_match_core` checks the mirror.
const MAX_PEERS: usize = 8;

/// Mirrors tines-core's send-queue / bounce-table capacity, a crate-private
/// local constant since 1.0 (decision K). `local_caps_match_core` checks the
/// mirror.
const SEND_QUEUE_CAP: usize = 256;

/// The mirrored local caps still equal core's. Runs with the `conformance`
/// feature (as `scripts/ci.sh` does), which exposes core's values through its
/// hidden `conformance` module.
#[cfg(feature = "conformance")]
#[test]
fn local_caps_match_core() {
    assert_eq!(MAX_PEERS, tines_core::conformance::MAX_PEERS);
    assert_eq!(SEND_QUEUE_CAP, tines_core::conformance::SEND_QUEUE_CAP);
}

/// A fixed set of short observation topics the tests reuse so the generated
/// observations round-trip cleanly through the core (`MAX_TOPIC` is 64 bytes).
const TOPICS: [&[u8]; 8] = [
    b"temp", b"humid", b"door", b"motion", b"light", b"batt", b"press", b"power",
];

/// A single contact window over `nodes` nodes. `start` is bounded well below the
/// duration floor so every contact in a scenario overlaps in a long common
/// window, keeping every union-find component *temporally connected* (a
/// component that is structurally connected but temporally disconnected can
/// never converge — a correct negative, not a protocol bug — so the scenario is
/// constrained to temporally-connected inputs).
///
/// The unordered pair `a != b` uses the `b_prime` slot shifted past `a`.
fn contact(nodes: usize) -> impl Strategy<Value = Contact> {
    (
        0u64..5_000u64,
        30_000u64..=40_000u64,
        0usize..nodes,
        0usize..nodes.saturating_sub(1),
    )
        .prop_map(|(start, duration, a, b_prime)| {
            let mut b = b_prime;
            if b >= a {
                b += 1;
            }
            Contact {
                start_ms: start,
                end_ms: start + duration,
                a,
                b,
            }
        })
}

/// A single observation: a topic drawn from [`TOPICS`] and a random 1..=64 byte
/// payload.
fn observation() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
    (
        0u32..8u32,
        proptest::collection::vec(any::<u8>(), 1..=64usize),
    )
        .prop_map(|(topic_idx, payload)| (TOPICS[topic_idx as usize].to_vec(), payload))
}

/// A generated scenario: a node count, a contact trace over those nodes, and a
/// flat list of `(node, topic, payload)` observations to apply at `t = 0`.
type Scenario = (usize, ContactTrace, Vec<(usize, Vec<u8>, Vec<u8>)>);

/// The [`scenario`] strategy, built on top of [`contact`] and [`observation`].
///
/// `prop_flat_map` fixes `nodes` first so the same node count drives both the
/// trace and the per-node observation lists.
fn scenario() -> impl Strategy<Value = Scenario> {
    (2..=6usize).prop_flat_map(|nodes| {
        let trace = proptest::collection::vec(contact(nodes), 1..=12usize);
        // Each node publishes 0..=10 observations, flattened to (node, topic,
        // payload) triples so per-node counts vary across scenarios.
        let per_node = proptest::collection::vec(
            proptest::collection::vec(observation(), 0..=10usize),
            nodes..=nodes,
        );
        (trace, per_node).prop_map(move |(contacts, per_node)| {
            let trace = ContactTrace { contacts };
            let mut list = Vec::new();
            for (node, node_obs) in per_node.into_iter().enumerate() {
                for (topic, payload) in node_obs {
                    list.push((node, topic, payload));
                }
            }
            (nodes, trace, list)
        })
    })
}

/// A 3-node observation list, reused by the order-independence test.
fn observations_3nodes() -> impl Strategy<Value = Vec<(usize, Vec<u8>, Vec<u8>)>> {
    proptest::collection::vec(observation(), 3..=3).prop_map(|per_node| {
        per_node
            .into_iter()
            .enumerate()
            .map(|(n, o)| (n, o.0, o.1))
            .collect()
    })
}

/// Maps a `u64` seed to one of the six permutations of three slots.
fn permute3(perm: u64) -> [usize; 3] {
    let bases = [
        [0usize, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    bases[(perm % 6) as usize]
}

/// The forward stride between consecutive edges of [`chain_trace`]: each edge
/// `(i-1, i)` is shifted a full stride past the last, keeping the windows
/// ordered so a task can hop forward along the path.
const PATH_STRIDE: u64 = 4_000;

/// The length of each edge's contact window in [`chain_trace`], leaving a
/// 1_000 ms gap to the next edge for frames to land before that contact closes.
const PATH_WINDOW: u64 = 3_000;

/// A forward path over `nodes` nodes: edges `(0,1),(1,2),...,(n-2,n-1)` with
/// ordered, non-overlapping contact windows. The contact graph is connected and
/// a task observed before the first contact can be relayed hop-by-hop to the
/// max-id node.
fn chain_trace(nodes: usize) -> ContactTrace {
    let mut trace = ContactTrace::default();
    for j in 1..nodes {
        trace.contacts.push(Contact {
            start_ms: j as u64 * PATH_STRIDE,
            end_ms: j as u64 * PATH_STRIDE + PATH_WINDOW,
            a: j - 1,
            b: j,
        });
    }
    trace
}

/// A scenario with a connected path trace and a random tasker node:
/// `(tasker, nodes, trace)`.
fn addressed_scenario() -> impl Strategy<Value = (usize, usize, ContactTrace)> {
    (2..=6usize).prop_flat_map(|nodes| {
        (0usize..nodes).prop_map(move |tasker| (tasker, nodes, chain_trace(nodes)))
    })
}

/// A bounce-table-overflow scenario: a fully-connected `MAX_PEERS`-node contact
/// graph plus a flood of distinct addressed observations past [`SEND_QUEUE_CAP`].
#[derive(Debug)]
struct BounceScenario {
    /// Node count (fixed at [`MAX_PEERS`]).
    nodes: usize,
    /// Fully-connected trace: every pair in contact during one long window.
    trace: ContactTrace,
    /// `(recipient_node_index, payload)` for each addressed observation to
    /// issue from node 0. Payloads are distinct, so every id is distinct.
    addressed: Vec<(usize, Vec<u8>)>,
}

/// The [`bounce_overflow_keeps_a_holder`] scenario: `MAX_PEERS` nodes in a
/// single long fully-connected contact window, so an addressed observation
/// reaches its recipient directly; the flood of more than [`SEND_QUEUE_CAP`]
/// addressed observations overflows the shared drop-oldest bounce table (one
/// `Sent` + one `Received` record per `(id, peer)`) while the custody cursor is
/// exercised across every peer.
fn bounce_scenario() -> impl Strategy<Value = BounceScenario> {
    // Just past capacity up to a bounded margin, so the flood depth — how hard
    // the shared ring evicts — varies across cases. The payload is the global
    // sequence number, so every observation's id is distinct.
    let count = SEND_QUEUE_CAP + 1..=SEND_QUEUE_CAP + 32;
    let window = 4_000u64;
    count.prop_map(move |n| {
        // Every pair in contact during one long window: the graph is connected
        // and stays connected, so each addressed offer reaches its recipient.
        let mut trace = ContactTrace::default();
        for a in 0..MAX_PEERS {
            for b in (a + 1)..MAX_PEERS {
                trace.contacts.push(Contact {
                    start_ms: 0,
                    end_ms: window,
                    a,
                    b,
                });
            }
        }
        // Address node 0's observations round-robin to the other
        // `MAX_PEERS - 1` nodes, so the cursor and every peer's table are
        // exercised. Each id is distinct (unique payload).
        let addressed = (0..n)
            .map(|j| ((j % (MAX_PEERS - 1)) + 1, j.to_le_bytes().to_vec()))
            .collect();
        BounceScenario {
            nodes: MAX_PEERS,
            trace,
            addressed,
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, .. ProptestConfig::default() })]

    /// Every connected component of the contact graph must be fully gossiped by
    /// the end of the trace. Tested at `loss = 0`: the spec's convergence is
    /// best-effort per contact and eventual over contacts, so guaranteed full
    /// convergence under loss within a finite window is NOT a spec property
    /// (a lost summary delays re-diff until the next summary round). Lossy
    /// eventual-convergence is covered deterministically by
    /// `convergence.rs::lossy_link_still_converges_given_time`.
    #[test]
    fn connected_components_converge(scenario in scenario()) {
        let (nodes, trace, obs) = scenario;
        let end = trace.end();
        let cfg = WorldConfig {
            nodes,
            mtu: 512,
            loss: 0.0,
            dup: 0.05,
            seed: 1,
        };
        let mut sim = Sim::new(cfg, trace);
        for (node, topic, payload) in &obs {
            sim.observe(*node, topic, payload);
        }
        sim.run_until(end + 1);
        for component in sim.components() {
            prop_assert!(
                sim.converged(&component),
                "component {:?} over {} nodes did not converge",
                component,
                nodes,
            );
        }
    }

    /// Reordering the contacts of a fully-connected 3-node graph does not
    /// change the per-node store, so long as loss is absent.
    #[test]
    fn store_is_independent_of_contact_order(
        obs in observations_3nodes(),
        perm in any::<u64>(),
    ) {
        // Three time slots, each a 5000 ms window.
        let slots = [(0u64, 5000u64), (6000, 11000), (12000, 17000)];
        // The three node pairs that make contact.
        let pairs = [(0usize, 1usize), (1usize, 2usize), (0usize, 2usize)];

        let build = |order: [usize; 3]| {
            let mut trace = ContactTrace::default();
            for (i, &(start, end)) in slots.iter().enumerate() {
                let (a, b) = pairs[order[i]];
                trace.contacts.push(Contact { start_ms: start, end_ms: end, a, b });
            }
            trace
        };

        let cfg = WorldConfig { nodes: 3, mtu: 512, loss: 0.0, dup: 0.0, seed: 1 };
        let order = permute3(perm);

        let mut a = Sim::new(cfg.clone(), build([0, 1, 2]));
        for (node, topic, payload) in &obs {
            a.observe(*node, topic, payload);
        }
        a.run_until(17000);

        let mut b = Sim::new(cfg.clone(), build(order));
        for (node, topic, payload) in &obs {
            b.observe(*node, topic, payload);
        }
        b.run_until(17000);

        for i in 0..3 {
            prop_assert_eq!(a.id_set(i), b.id_set(i), "node {} id sets differ", i);
        }
    }

    /// A node's id set is monotonically non-decreasing: nothing is ever
    /// trimmed from the store during a normal run.
    #[test]
    fn union_is_monotone(scenario in scenario()) {
        use std::collections::BTreeSet;

        let (nodes, trace, obs) = scenario;
        let end = trace.end();
        let cfg = WorldConfig {
            nodes,
            mtu: 512,
            loss: 0.0,
            dup: 0.0,
            seed: 1,
        };
        let mut sim = Sim::new(cfg, trace);
        for (node, topic, payload) in &obs {
            sim.observe(*node, topic, payload);
        }

        let mut prev: Vec<BTreeSet<_>> = (0..nodes).map(|i| sim.id_set(i)).collect();
        let mut elapsed = 0u64;
        while elapsed < end {
            // Advance one sample period (1000 ms) at the 10 ms step resolution.
            for _ in 0..100 {
                sim.step();
            }
            elapsed += 1000;
            let prev_t = elapsed - 1000;
            let now: Vec<BTreeSet<_>> = (0..nodes).map(|i| sim.id_set(i)).collect();
            for i in 0..nodes {
                prop_assert!(
                    prev[i].is_subset(&now[i]),
                    "node {} lost an observation between t={} and t={}",
                    i,
                    prev_t,
                    elapsed,
                );
            }
            prev = now;
        }
    }

    /// Malformed wire frames are dropped by the exchange and never panic the
    /// node; the store stays empty.
    #[test]
    fn garbage_frames_never_panic(
        bytes in proptest::collection::vec(any::<u8>(), 0..1500),
    ) {
        let cfg = WorldConfig {
            nodes: 1,
            mtu: 512,
            loss: 0.0,
            dup: 0.0,
            seed: 1,
        };
        let mut sim = Sim::new(cfg, ContactTrace::default());
        let mut node = sim.nodes.pop().expect("one node");

        // `bytes` is at most 1499 bytes, which fits the MAX_FRAME buffer.
        let buf = FrameBuf::from_slice(&bytes).expect("payload fits in a MAX_FRAME buffer");
        sim.world.borrow_mut().inject_frame(0, 1, buf);

        let stored = node.poll(0);
        prop_assert_eq!(stored, 0, "a garbage frame must not be stored");
        prop_assert_eq!(node.store().len(), 0, "store must be unchanged by a garbage frame");
    }

    /// An addressed task handed off hop-by-hop along a connected contact path
    /// is always held by some node: the recipient, or a carrier that relayed it.
    ///
    /// The trace is a forward path (see [`chain_trace`]) over `2..=6` nodes, so
    /// it is always connected; a random node issues the task and the max-id
    /// node is the recipient. Addressed messages diverge by design — the
    /// recipient holds a tasking the carriers no longer need — so this asserts
    /// the custody-chain holder invariant instead of epidemic delivery: custody
    /// releases on the first ack from *any* peer the node sent it to (a relay
    /// or the recipient), so the final recipient is best-effort but custody
    /// never vanishes mid-path.
    #[test]
    fn addressed_always_held_by_carrier_or_recipient(
        (tasker, nodes, trace) in addressed_scenario(),
    ) {
        let end = trace.end();
        let cfg = WorldConfig {
            nodes,
            mtu: 512,
            loss: 0.0,
            dup: 0.0,
            seed: 1,
        };
        let mut sim = Sim::new(cfg, trace);
        // The max-id node's id is its index replicated to 16 bytes, matching
        // how `Sim` assigns node ids.
        let recipient = [nodes.saturating_sub(1) as u8; 16];
        let id = sim.observe_to(tasker, recipient, 600_000, b"task", b"investigate");
        sim.run_until(end + 1_000);

        let held = sim.nodes.iter().any(|n| {
            let s = n.store();
            s.contains(&id) && !s.is_released(&id)
        });
        prop_assert!(
            held,
            "addressed task from node {tasker} must still be held by a carrier \
             or the recipient {recipient_idx}; custody never vanishes mid-path",
            recipient_idx = nodes.saturating_sub(1)
        );
    }

    /// Like the holder-invariant test above, but every node is backed by a
    /// [`CappedStore`] whose `insert` returns `StoreError::Full` once the store
    /// has held `8` observations. This drives F1's full-store path: once a
    /// receiver's store is full it must not ack what it could not keep, or
    /// custody for the addressed task is released for a message nobody holds.
    /// Six of each node's own broadcasts are seeded in (all land, since 6 < 8)
    /// and the run's gossiped broadcasts tip each node past capacity, so many
    /// nodes reject inserts and the "at least one holder" invariant must still
    /// hold.
    #[test]
    fn addressed_always_held_by_carrier_or_recipient_with_full_store(
        (tasker, nodes, trace) in addressed_scenario(),
    ) {
        let end = trace.end();
        let cfg = WorldConfig {
            nodes,
            mtu: 512,
            loss: 0.0,
            dup: 0.05,
            seed: 1,
        };
        let mut sim = Sim::<CappedStore>::with_store(cfg, trace);
        // Seed 6 of each node's own broadcasts: 6 < CappedStore's default cap
        // (8), so every seed lands and the run's received broadcasts push each
        // node into the full-store ack path.
        for node in 0..nodes {
            for seq in 0..6u64 {
                let _ = sim.observe(node, b"temp", &seq.to_le_bytes());
            }
        }
        // The max-id node is the recipient.
        let recipient = [nodes.saturating_sub(1) as u8; 16];
        let id = sim.observe_to(tasker, recipient, 600_000, b"task", b"investigate");
        sim.run_until(end + 1_000);

        let held = sim.nodes.iter().any(|n| {
            let s = n.store();
            s.contains(&id) && !s.is_released(&id)
        });
        prop_assert!(
            held,
            "addressed task from node {tasker} must still be held by a carrier \
             or the recipient {recipient_idx}; custody never vanishes mid-path \
             even when a node's store is full",
            recipient_idx = nodes.saturating_sub(1)
        );
    }

    /// The shared bounce table (`SEND_QUEUE_CAP` entries, drop-oldest, shared
    /// across all `MAX_PEERS` peers) overflows when more than its capacity of
    /// distinct observations are exchanged at once. This drives that overflow —
    /// a fully-connected `MAX_PEERS`-node graph flooded with more than
    /// `SEND_QUEUE_CAP` distinct addressed observations from node 0, each
    /// issued to a round-robin recipient so the shared ring evicts across every
    /// peer — and pins the two guarantees the overflow must preserve (§6):
    ///
    /// * (a) **always ≥ 1 holder** — every flooded id is still held by some
    ///   node. A lost *Sent* record leaves a duplicate custodian; a lost
    ///   *Received* record still leaves a holder either way, so the message
    ///   never fully vanishes.
    /// * (b) **custody fairness** — the per-peer cursor still gives every
    ///   addressed candidate a turn, so none is starved by the eviction.
    ///
    /// §6 states the current drop-oldest design already satisfies both, so the
    /// property is expected to pass as-is; a failure here is a finding to
    /// report, not a bug to paper over by loosening either assertion.
    #[test]
    fn bounce_overflow_keeps_a_holder(scenario in bounce_scenario()) {
        let BounceScenario { nodes, trace, addressed } = scenario;
        let end = trace.end();
        let cfg = WorldConfig {
            nodes,
            mtu: 512,
            loss: 0.0,
            dup: 0.0,
            seed: 1,
        };
        let mut sim = Sim::new(cfg, trace);

        // Flood the shared bounce table with more than `SEND_QUEUE_CAP` distinct
        // addressed observations from node 0. Each emit (a `Sent` record) and
        // each accept (a `Received` record) pushes into the shared
        // `SEND_QUEUE_CAP` ring, so fanning these out across `MAX_PEERS` peers
        // evicts past capacity; the payload's global sequence keeps every id
        // distinct.
        let mut ids = Vec::with_capacity(addressed.len());
        for (recipient, payload) in &addressed {
            let to = [*recipient as u8; 16];
            let id = sim.observe_to(0, to, 600_000, b"task", payload);
            ids.push((*recipient, id));
        }
        sim.run_until(end + 1_000);

        // (a) always >= 1 holder for every flooded id: some node still holds it
        // (inserted, and not released — a release only ever happens for an id
        // this node actually dispatched). A lost *Sent*/*Received* bounce
        // record under overflow can only leave a duplicate custodian or a
        // re-send, never a fully-vanished id (§6).
        for (_, id) in &ids {
            let held = sim.nodes.iter().any(|n| {
                let s = n.store();
                s.contains(id) && !s.is_released(id)
            });
            prop_assert!(
                held,
                "flooded id {id:?} has no holder after sending {} distinct \
                 addressed observations across {} peers (SEND_QUEUE_CAP={}); \
                 the bounce table overflow dropped custody for an id",
                ids.len(),
                MAX_PEERS,
                SEND_QUEUE_CAP,
            );
        }

        // (b) the custody cursor still gives every candidate a turn: each
        // recipient ends the run holding its own addressed observation — none
        // starved by the eviction.
        for (recipient, id) in &ids {
            let s = sim.nodes[*recipient].store();
            prop_assert!(
                s.contains(id) && !s.is_released(id),
                "addressed id {id:?} to node {recipient} was never delivered; \
                 the custody cursor starved it after the bounce table overflowed",
            );
        }
    }
}
