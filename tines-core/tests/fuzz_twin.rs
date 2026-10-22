//! Proptest twin of the `fuzz/` harness (spec §7 "Stable twin").
//!
//! The cargo-fuzz crate at `fuzz/` drives a scripted `Node` + `MemStore`
//! through decoded steps and checks the §7 properties after each one. The
//! twin compiles that *exact* harness in through a `#[path]` include, so it is
//! one oracle with two entry points, not two copies:
//!
//! * `fuzz_inputs` feeds random bytes through `fuzz::run_input`, the fuzz
//!   target's own entry point, in both modes;
//! * `structured_*` feed generated steps straight to the harness, so every
//!   case reaches well-formed frames;
//! * the rest pin specific §10 properties as scenarios.
//!
//! It runs on the pinned stable toolchain in the normal gate. It needs the
//! `conformance` feature (the harness builds frames through core's hidden
//! `conformance` helpers) and `alloc` (`MemStore`); Cargo.toml's
//! `required-features` skips it otherwise.
#![allow(clippy::needless_range_loop)]

#[path = "../../fuzz/src/lib.rs"]
mod fuzz;

use fuzz::{AckSpec, FragOrder, FragSpec, Harness, Mode, ObsSpec, SpecSummary, Step, Who};
use proptest::prelude::*;
use tines_core::conformance::{MAX_PEERS, MIN_MTU, PEER_IDLE_MS, SUMMARY_INTERVAL_MS};
use tines_core::consts::{MAX_FRAME, MAX_PAYLOAD, MAX_TOPIC, SUMMARY_BYTES};
use tines_core::Store;

const CASES: u32 = 64;

fn mtu() -> impl Strategy<Value = usize> {
    MIN_MTU..=MAX_FRAME
}

fn who() -> impl Strategy<Value = Who> {
    prop_oneof![
        6 => Just(Who::Peer),
        1 => Just(Who::Me),
        1 => any::<[u8; 16]>().prop_map(Who::Id),
    ]
}

/// Addresses past `MAX_PEERS`, so the peer table overflows.
fn addr() -> impl Strategy<Value = u64> {
    0..(3 * MAX_PEERS as u64)
}

fn sig() -> impl Strategy<Value = Option<fuzz::SigSpec>> {
    proptest::option::of(
        (
            prop_oneof![Just(1u8), any::<u8>()],
            prop_oneof![Just(1u8), any::<u8>()],
            prop_oneof![Just(64usize), 0usize..=64],
        )
            .prop_map(|(version, algo, len)| fuzz::SigSpec {
                version,
                algo,
                bytes: vec![0x5A; len],
            }),
    )
}

fn obs_spec() -> impl Strategy<Value = ObsSpec> {
    (
        (addr(), who(), who(), 0u64..64, any::<u32>()),
        (
            proptest::option::of(any::<u64>()),
            prop_oneof![Just(MAX_TOPIC), 0..=MAX_TOPIC],
            prop_oneof![Just(MAX_PAYLOAD), 0..=MAX_PAYLOAD],
            proptest::option::of((who(), any::<u32>())),
            sig(),
        ),
    )
        .prop_map(
            |((addr, from, observer, seq, age), (observed_at, tl, pl, dest, sig))| ObsSpec {
                addr,
                from,
                observer,
                seq,
                age_ms: u64::from(age),
                observed_at,
                topic: vec![b't'; tl],
                payload: vec![b'p'; pl],
                dest: dest.map(|(to, life)| (to, u64::from(life))),
                sig,
            },
        )
}

fn structured_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        obs_spec().prop_map(Step::Obs),
        (
            obs_spec(),
            1usize..=MAX_FRAME,
            prop_oneof![
                Just(FragOrder::InOrder),
                Just(FragOrder::Reverse),
                proptest::collection::vec(any::<u8>(), 0..8).prop_map(FragOrder::Explicit),
            ],
            proptest::option::of(any::<[u8; 16]>()),
            proptest::option::of(any::<u8>()),
        )
            .prop_map(|(obs, chunk, order, id, count)| Step::Frags(FragSpec {
                obs,
                chunk,
                order,
                id,
                count,
            })),
        (
            addr(),
            who(),
            any::<u32>(),
            0..=SUMMARY_BYTES,
            any::<u8>(),
            1u8..=3
        )
            .prop_map(|(addr, from, salt, len, fill, repeat)| Step::Summarize(
                SpecSummary {
                    addr,
                    from,
                    salt,
                    bits: vec![fill; len],
                    repeat,
                }
            )),
        (
            addr(),
            who(),
            proptest::collection::vec(any::<u16>(), 0..8),
            proptest::collection::vec(any::<[u8; 16]>(), 0..2),
        )
            .prop_map(|(addr, from, sent, raw)| Step::Ack(AckSpec {
                addr,
                from,
                sent,
                raw,
            })),
        addr().prop_map(Step::Contact),
        addr().prop_map(Step::Lost),
        prop_oneof![-5_000i64..=40_000, any::<i64>()].prop_map(Step::Advance),
        any::<u16>().prop_map(|size| Step::Ourselves { size }),
        (who(), any::<u32>(), any::<u16>()).prop_map(|(to, life, size)| Step::OurselvesTo {
            to,
            lifetime_ms: u64::from(life),
            size,
        }),
        (addr(), proptest::collection::vec(any::<u8>(), 0..64))
            .prop_map(|(a, bytes)| Step::Frame(a, bytes)),
    ]
}

/// A broadcast observation from the peer at `addr`, distinct per `seq`.
fn peer_obs(addr: u64, seq: u64) -> ObsSpec {
    ObsSpec {
        addr,
        from: Who::Peer,
        observer: Who::Peer,
        seq,
        age_ms: 0,
        observed_at: None,
        topic: b"t".to_vec(),
        payload: b"p".to_vec(),
        dest: None,
        sig: None,
    }
}

/// A summary from `addr` saying it holds nothing.
fn empty_summary(addr: u64) -> Step {
    Step::Summarize(SpecSummary {
        addr,
        from: Who::Peer,
        salt: 7,
        bits: vec![0; SUMMARY_BYTES],
        repeat: 1,
    })
}

/// Poll the harness `n` times at the current tick.
fn settle(h: &mut Harness, n: usize) {
    for _ in 0..n {
        h.pump();
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: CASES, ..Default::default() })]

    /// The fuzz target's own entry point on random bytes: both modes, a
    /// random MTU, every §7 check after every step.
    #[test]
    fn fuzz_inputs(data in proptest::collection::vec(any::<u8>(), 3..2048)) {
        let _ = fuzz::run_input(&data);
    }

    /// Generated well-formed adversarial steps from more than `MAX_PEERS`
    /// addresses, every §7 check after every step.
    #[test]
    fn structured_adversarial(
        steps in proptest::collection::vec(structured_step(), 1..=64),
        mtu in mtu(),
    ) {
        Harness::new(Mode::Adversarial, mtu).run(&steps);
    }

    /// §10 / F9: a summary replayed within the interval re-sends nothing,
    /// while the next on-time summary re-offers everything the peer still
    /// lacks (lossy convergence is preserved).
    #[test]
    fn summary_repeat_does_not_resend(n in 1usize..=12, mtu in 600usize..=MAX_FRAME) {
        let mut h = Harness::new(Mode::Adversarial, mtu);
        for _ in 0..n {
            h.run_step(&Step::Ourselves { size: 16 });
        }
        prop_assert_eq!(h.store().len(), n);

        // The peer says it holds nothing: every broadcast is offered once.
        h.run_step(&empty_summary(1));
        settle(&mut h, n + 4);
        prop_assert_eq!(h.offered_since_summary(1), n);

        // The same summary again, 1 ms later: ignored, nothing re-sent.
        h.run_step(&Step::Advance(1));
        h.run_step(&empty_summary(1));
        settle(&mut h, n + 4);
        prop_assert_eq!(h.offered_since_summary(1), 0, "a replayed summary re-sent ids");

        // A full interval later the peer still lacks everything, so it gets
        // everything again.
        h.run_step(&Step::Advance(SUMMARY_INTERVAL_MS as i64));
        h.run_step(&empty_summary(1));
        settle(&mut h, n + 4);
        prop_assert_eq!(h.offered_since_summary(1), n);
    }

    /// F1: acknowledging an unknown id must not evict a known, stored
    /// observation.
    #[test]
    fn ack_unknown_does_not_evict_known(obs in obs_spec(), mtu in mtu()) {
        let mut h = Harness::new(Mode::Adversarial, mtu);
        let obs = ObsSpec { dest: None, sig: None, observer: Who::Peer, from: Who::Peer, ..obs };
        let addr = obs.addr;
        h.run_step(&Step::Obs(obs));
        let mut known = Vec::new();
        h.store().for_each_id(&mut |id| known.push(*id));
        prop_assert_eq!(known.len(), 1, "the peer's broadcast must be stored");

        h.run_step(&Step::Ack(AckSpec { addr, from: Who::Peer, sent: vec![], raw: vec![[0xFF; 16]] }));
        for id in &known {
            prop_assert!(h.store().contains(id) && !h.store().is_released(id));
        }
    }
}

/// §10 / F8 peer-table recovery: `MAX_PEERS + 16` addresses each send one
/// frame (filling the table), then everyone goes quiet. After
/// `PEER_IDLE_MS`, a never-seen address must get a slot.
#[test]
#[ignore = "needs S1 (idle sweep): silent peers are never evicted yet"]
fn peer_table_recovers_after_flood() {
    let mut h = Harness::new(Mode::Adversarial, MAX_FRAME);
    for addr in 0..(MAX_PEERS as u64 + 16) {
        h.run_step(&Step::Obs(peer_obs(addr, addr)));
    }
    // Sanity: the flood really filled the table, so the probe tests eviction.
    let before = h.store().len();
    h.run_step(&Step::Obs(peer_obs(1_000, 1_000)));
    assert_eq!(h.store().len(), before, "the table should be full");

    h.check_peer_table_recovers();
}

/// Same flood, but contact-only peers: `Contact` events with no frames must
/// age out too.
#[test]
#[ignore = "needs S1 (idle sweep): contact-only peers are never evicted yet"]
fn peer_table_recovers_after_contact_flood() {
    let mut h = Harness::new(Mode::Adversarial, MAX_FRAME);
    for addr in 0..(MAX_PEERS as u64) {
        h.run_step(&Step::Contact(addr));
    }
    h.check_peer_table_recovers();
}

/// Before the idle window ends the table stays full: the flood isn't
/// trivially recovered by any later frame.
#[test]
fn full_peer_table_refuses_a_newcomer_before_idle() {
    let mut h = Harness::new(Mode::Adversarial, MAX_FRAME);
    for addr in 0..(MAX_PEERS as u64) {
        h.run_step(&Step::Obs(peer_obs(addr, addr)));
    }
    assert_eq!(h.store().len(), MAX_PEERS);
    h.run_step(&Step::Advance(PEER_IDLE_MS as i64 / 2));
    h.run_step(&Step::Obs(peer_obs(500, 500)));
    assert_eq!(h.store().len(), MAX_PEERS);
}
