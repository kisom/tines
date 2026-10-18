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

use proptest::prelude::*;
use tines_core::{FrameBuf, Store};
use tines_sim::{Contact, ContactTrace, Sim, WorldConfig};

/// A fixed set of short observation topics the tests reuse so the generated
/// observations round-trip cleanly through the core (`MAX_TOPIC` is 64 bytes).
const TOPICS: [&[u8]; 8] = [
    b"temp",
    b"humid",
    b"door",
    b"motion",
    b"light",
    b"batt",
    b"press",
    b"power",
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
        let obs = proptest::collection::vec(observation(), nodes..=nodes);
        (trace, obs).prop_map(move |(contacts, per_node)| {
            let trace = ContactTrace { contacts };
            let mut list = Vec::new();
            for (node, (topic, payload)) in per_node.into_iter().enumerate() {
                list.push((node, topic, payload));
            }
            (nodes, trace, list)
        })
    })
}

/// A 3-node observation list, reused by the order-independence test.
fn observations_3nodes() -> impl Strategy<Value = Vec<(usize, Vec<u8>, Vec<u8>)>> {
    proptest::collection::vec(observation(), 3..=3)
        .prop_map(|per_node| per_node.into_iter().enumerate().map(|(n, o)| (n, o.0, o.1)).collect())
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

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

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
}
