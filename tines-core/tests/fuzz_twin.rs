//! Proptest twin of the `fuzz/` harness (Task 12).
//!
//! The cargo-fuzz crate at `fuzz/` drives this very same module through a
//! scripted [`Node`]/[`MemStore`] transport, decoding arbitrary fuzzed bytes
//! into ordered steps and asserting no-panic / MTU-bounded output / drop-count
//! reconciliation (§7) plus the §10 adversarial-peer properties. The twin
//! reuses that *exact* harness via a `#[path]` include so any bug fixed in the
//! oracle is caught here too — "same harness", two entry points, not two copies.
//!
//! This is the bounded-case-count proptest half of Task 12: it runs under the
//! normal suite (`cargo test -p tines-core`) and needs no nightly toolchain.
//! When `tines-core` is built with the optional `conformance` feature, the
//! harness's own signature round-trip also runs (see its source); otherwise the
//! twin covers store lifecycle through the public `observe()` path instead.
//!
//! The harness drives `MemStore`, which needs `alloc`; the twin is skipped in
//! `--no-default-features` builds (like `pair.rs`).
#![cfg(feature = "alloc")]
#![allow(clippy::needless_range_loop)]

#[path = "../../fuzz/src/lib.rs"]
mod fuzz;

use proptest::prelude::*;
use tines_core::conformance::MAX_PEERS;
use tines_core::consts::{MAX_FRAME, MAX_PAYLOAD, MAX_TOPIC, SUMMARY_BYTES};
use tines_core::Store;

// A single bounded case count keeps the twin fast under `cargo test` while
// still exercising a meaningful surface of the harness.
const CASES: u32 = 32;

fn node_id() -> impl Strategy<Value = [u8; 16]> {
    any::<[u8; 16]>()
}

fn topic() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..=MAX_TOPIC)
}

fn payload() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..MAX_PAYLOAD)
}

// A well-formed broadcast observation spec. `dest` stays broadcast (None): the
// addressed path shares storage/exchange logic and is exercised by the raw
// Frame step below, so broadcasting keeps the twin's invariants unambiguous.
fn obs_spec() -> impl Strategy<Value = fuzz::ObsSpec> {
    (
        any::<u64>(), // addr
        node_id(),    // from
        any::<u64>(), // seq
        any::<u32>(), // age_ms
        topic(),
        payload(),
    )
        .prop_map(|(addr, from, seq, age_ms, topic, payload)| fuzz::ObsSpec {
            addr,
            from,
            seq,
            age_ms: age_ms as u64,
            topic,
            payload,
            dest: None,
        })
}

// A well-formed summary spec with a chosen bits pattern and salt.
fn summary_spec() -> impl Strategy<Value = fuzz::SpecSummary> {
    (
        any::<u64>(),
        node_id(),
        any::<u32>(),
        proptest::collection::vec(any::<u8>(), 0..SUMMARY_BYTES),
    )
        .prop_map(|(addr, from, salt, bits)| fuzz::SpecSummary {
            addr,
            from,
            salt,
            bits,
        })
}

// An Ack spec for 0..=4 ids (sent or unsent).
fn ack_spec() -> impl Strategy<Value = fuzz::AckSpec> {
    (any::<u64>(), proptest::collection::vec(node_id(), 0..=8)).prop_map(|(addr, ids)| {
        fuzz::AckSpec {
            addr,
            from: [0u8; 16],
            ids,
        }
    })
}

// Any valid Step of any kind — drives the Basic mode with real, well-formed
// inputs rather than raw random bytes.
fn any_step() -> impl Strategy<Value = fuzz::Step> {
    prop_oneof![
        // Addressed / broadcast observations (well-formed Obs frames).
        obs_spec().prop_map(fuzz::Step::Obs),
        // Summaries with chosen bits patterns and salts.
        summary_spec().prop_map(fuzz::Step::Summarize),
        // Acks for sent/unsent ids.
        ack_spec().prop_map(fuzz::Step::Ack),
        // Raw Frame: a peer (or our own) byte stream of up to MAX_FRAME bytes,
        // including deliberately malformed content to exercise the drop path.
        (
            any::<u64>(),
            proptest::collection::vec(any::<u8>(), 0..MAX_FRAME),
        )
            .prop_map(|(addr, bytes)| fuzz::Step::Frame(addr, bytes)),
        // Contact / lost + clock advance (may go backwards).
        any::<u64>().prop_map(fuzz::Step::Contact),
        any::<u64>().prop_map(fuzz::Step::Lost),
        any::<i64>().prop_map(fuzz::Step::Advance),
        // Ourselves / ours-to (self observation).
        (0u32..=0).prop_map(|_| fuzz::Step::Ourselves),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: CASES, ..Default::default() })]

    /// §7 core safety: an arbitrary sequence of well-formed steps run through a
    /// randomised-MTU harness must never panic, must stay within the MTU on
    /// every emitted frame, and must not overflow its drop counter. `run`
    /// asserts all three; reaching it clean is the property. A single mixed
    /// sequence may also exceed MAX_PEERS distinct addresses (addr is a full
    /// u64), so this alone exercises the flood path without panic.
    #[test]
    fn basic_run_no_panic_or_overflow(
        steps in proptest::collection::vec(any_step(), 0..=128),
        mtu in 32usize..=4096usize,
    ) {
        if !steps.is_empty() {
            let mut h = fuzz::Harness::new(mtu);
            h.run(&steps);
        }
    }

    /// §10 idempotency: repeating an identical summary must not emit MORE
    /// frames than the first time it was seen.
    #[test]
    fn summary_repeat_does_not_resend(spec in summary_spec(), mtu in 32usize..=4096) {
        let mut h = fuzz::Harness::new(mtu);

        h.step(&fuzz::Step::Summarize(spec.clone()));
        h.drain();
        let first = h.sent_frames().len();

        h.step(&fuzz::Step::Summarize(spec));
        h.drain();
        let second = h.sent_frames().len();

        prop_assert!(second <= first, "repeating a summary must not re-send ({} vs {} frames)", second, first);
    }

    /// §10 flood recovery: feeding MORE than MAX_PEERS distinct-identity
    /// summaries through the transport must never panic and output stays
    /// bounded per received summary.
    #[test]
    fn flood_past_max_peers_does_not_panic(mtu in 32usize..=4096) {
        let n = MAX_PEERS as u64 + 16; // deliberately beyond the cap
        let mut steps = Vec::new();
        for i in 1..=n {
            steps.push(fuzz::Step::Summarize(fuzz::SpecSummary {
                addr: i,
                from: [i as u8; 16],
                salt: (i as u32),
                bits: (0..SUMMARY_BYTES).map(|_| i as u8).collect(),
            }));
        }
        let mut h = fuzz::Harness::new(mtu);
        h.run(&steps);
    }

    /// §10 peer-table recovery after a beyond-MAX_PEERS flood: once the table
    /// has been pushed past its cap, a fresh contact + our own observation must
    /// still be stored (the node keeps working rather than wedging).
    #[test]
    fn peer_table_recovers_after_flood(mtu in 32usize..=4096) {
        let mut h = fuzz::Harness::new(mtu);
        for i in 1..=(MAX_PEERS as u64 + 16) {
            let spec = fuzz::SpecSummary {
                addr: i,
                from: [i as u8; 16],
                salt: (i as u32),
                bits: (0..SUMMARY_BYTES).map(|_| i as u8).collect(),
            };
            h.step(&fuzz::Step::Summarize(spec));
            h.drain();
        }
        // A fresh contact + our own observation after the flood.
        h.step(&fuzz::Step::Contact(1));
        h.step(&fuzz::Step::Ourselves);
        h.drain();
        prop_assert!(!h.store().is_empty(), "node must still store an observation after a beyond-MAX_PEERS flood");
    }

    /// F1: acknowledging an unknown id must not evict a known, stored
    /// observation. Seed a peer broadcast obs (guaranteed stored), then Ack an
    /// id we did not observe and assert the seed is still present.
    #[test]
    fn ack_unknown_does_not_evict_known(obs in obs_spec(), mtu in 32usize..=4096) {
        let mut h = fuzz::Harness::new(mtu);

        // Seed a known broadcast observation from a peer.
        h.step(&fuzz::Step::Obs(obs));
        h.drain();

        let known: Vec<[u8; 16]> = {
            let mut out = Vec::new();
            h.store().for_each_broadcast_id(&mut |id: &[u8; 16]| out.push(*id));
            out
        };
        prop_assert!(!known.is_empty(), "expected a stored broadcast to ack against");

        // An Ack referencing an id we never observed must not remove the seed.
        let unknown = [0xFFu8; 16];
        h.step(&fuzz::Step::Ack(fuzz::AckSpec { addr: 1, from: [0u8; 16], ids: vec![unknown] }));
        h.drain();

        for id in &known {
            prop_assert!(h.store().contains(id), "an Ack of an unknown id must not evict a known stored observation");
        }
    }
}
