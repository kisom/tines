//! Backend conformance suite.
//!
//! A `no_std`-clean set of property tests that any [`Store`] backend can run to
//! prove it honors the contract in [`Store`](crate::Store). Each public function
//! exercises exactly one property against a backend produced by a
//! caller-supplied factory, so the same body runs for [`MemStore`], a
//! [`LogStore`](crate::store::LogStore) opened on a fresh file per case and
//! [`CappedStore`] — see the `#[test]` shims in each backend crate.
//!
//! The functions build their own [`Observation`]s, including one carrying a
//! `sig`, which only the crate that owns the `#[non_exhaustive]` type may
//! construct by struct literal — this is why the suite lives *inside*
//! `tines-core` rather than as a `tines-core/tests/` file.
//!
//! The suite is deliberately free of `std`/`alloc` so it can build under
//! `--no-default-features` (with this feature on): every call is `heapless`
//! plus `assert!`.

use crate::observation::Observation;
use crate::{Payload, SigAlgo, Signature, Store, Topic, TrimPolicy};

/// Build a signed broadcast observation for the suite.
///
/// `signature` is a byte string the backend need not understand; the test only
/// checks it round-trips verbatim, so it is any length `<= MAX_SIG`.
fn signed_obs(seq: u64, topic: &[u8], signature: &[u8]) -> Observation {
    let payload = Payload::from_slice(b"pay").expect("payload fits capacity");
    Observation {
        observer: [1u8; 16],
        seq,
        age_ms: 0,
        observed_at: None,
        topic: Topic::from_slice(topic).expect("topic fits capacity"),
        payload,
        dest: None,
        sig: Some(Signature {
            version: 1,
            algo: SigAlgo::Ed25519,
            bytes: heapless::Vec::from_slice(signature).expect("signature fits capacity"),
        }),
    }
}

/// Two observations that carry the *same* id: same content, different age.
/// The signature is ignored by identity, so an aged copy reuses the id.
fn same_id(seq: u64, topic: &[u8]) -> (Observation, Observation) {
    let fresh = signed_obs(seq, topic, &[0u8; 8]);
    let mut aged = signed_obs(seq, topic, &[0u8; 8]);
    aged.age_ms = 500;
    (fresh, aged)
}

/// A freshly inserted observation must be retrievable by id at `now`.
pub fn insert_get<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    let (obs, _) = same_id(1, b"t");
    let id = obs.id();
    assert!(store.insert(obs, 1000).unwrap(), "insert succeeds");
    let got = store.get(&id, 1000).expect("get after insert");
    assert_eq!(got.seq, 1);
    assert_eq!(got.topic.as_slice(), b"t");
    assert_eq!(got.payload.as_slice(), b"pay");
}

/// A second insert of an already-held id is a no-op: the entry, its stored age
/// lineage and its release state are left untouched.
pub fn dedupe<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    assert!(store.is_empty());
    let (fresh, aged) = same_id(1, b"t");
    let id = fresh.id();
    assert_eq!(id, aged.id(), "an aged copy shares the id");
    assert!(store.insert(fresh, 1000).unwrap(), "first insert");
    assert_eq!(store.len(), 1);
    assert!(
        !store.insert(aged, 1500).unwrap(),
        "duplicate is not reinserted"
    );
    assert_eq!(store.len(), 1);
    assert!(store.contains(&id));
}

/// `get` advances age by elapsed ticks; a deduped insert must not reset the
/// stored age lineage.
pub fn age_lineage<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    let (fresh, _) = same_id(1, b"t");
    let id = fresh.id();
    store.insert(fresh, 1000).unwrap();
    let first = store.get(&id, 1500).unwrap();
    assert_eq!(first.age_ms, 500, "age advances to elapsed ticks");
    // A later insert of the same id must not reset stored_at.
    store.insert(same_id(1, b"t").0, 3000).unwrap();
    let second = store.get(&id, 3500).unwrap();
    assert_eq!(
        second.age_ms, 2500,
        "age keeps climbing from the first insert, not the duplicate"
    );
}

/// `get` with `now` before the stored tick saturates to zero age — it must not
/// underflow.
pub fn saturating_ticks<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    let (fresh, _) = same_id(1, b"t");
    let id = fresh.id();
    store.insert(fresh, 2000).unwrap();
    let got = store.get(&id, 1000).unwrap();
    assert_eq!(got.age_ms, 0, "elapsed underflow saturates at zero");
}

/// Releasing marks an id but leaves it stored, contained and gettable; it must
/// not evict the entry.
pub fn release_surviving_get<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    let (fresh, _) = same_id(1, b"t");
    let id = fresh.id();
    store.insert(fresh, 1000).unwrap();
    store.release(&id);
    assert!(store.is_released(&id));
    assert!(store.contains(&id), "release does not evict");
    assert!(
        store.get(&id, 1000).is_some(),
        "released id is still gettable"
    );
    assert_eq!(store.len(), 1);
}

/// A signature carried by an observation round-trips through the store intact,
/// and a different signature over the same content shares the id.
pub fn sig_round_trip<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    let sig_bytes = [7u8; 32];
    let fresh = signed_obs(1, b"sig", &sig_bytes);
    let id = fresh.id();
    store.insert(fresh, 1000).unwrap();
    let got = store.get(&id, 1000).unwrap();
    let sig = got
        .sig
        .expect("the signature round-trips through the store");
    assert_eq!(sig.version, 1);
    assert_eq!(sig.algo, SigAlgo::Ed25519);
    assert_eq!(&sig.bytes[..], &sig_bytes[..]);
    // A different signature over identical content must not change identity.
    let other = signed_obs(1, b"sig", &[9u8; 40]);
    assert_eq!(other.id(), id, "the signature must not affect identity");
}

/// With a count cap, `trim` drops the oldest (highest current age) first and
/// reports how many it removed.
pub fn trim_order<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    for i in 0..5u64 {
        let fresh = signed_obs(i, b"t", &[i as u8]);
        assert!(store.insert(fresh, i * 10).unwrap(), "insert {}", i);
    }
    let policy = TrimPolicy {
        keep_per_topic: None,
        max_age_ms: None,
        max_count: Some(2),
    };
    assert_eq!(
        store.trim(&policy, 50),
        3,
        "trim returns the number removed"
    );
    // The two youngest (seq 3 and 4, lowest current age) survive.
    for dropped in 0..3u64 {
        let fresh = signed_obs(dropped, b"t", &[dropped as u8]);
        assert!(!store.contains(&fresh.id()), "seq {} is trimmed", dropped);
    }
    for kept in 3..5u64 {
        let fresh = signed_obs(kept, b"t", &[kept as u8]);
        assert!(store.contains(&fresh.id()), "seq {} survives", kept);
    }
}

/// A trimmed id is not tombstoned: it can be reinserted and counted again.
pub fn no_tombstones<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    let fresh = signed_obs(1, b"t", &[1u8; 8]);
    let id = fresh.id();
    store.insert(fresh, 0).unwrap();
    let policy = TrimPolicy {
        keep_per_topic: None,
        max_age_ms: Some(1),
        max_count: None,
    };
    assert_eq!(store.trim(&policy, 100), 1);
    assert!(store.is_empty());
    // The trimmed id is not tombstoned: re-insert it (aged, still same id).
    let reinserted = signed_obs(1, b"t", &[1u8; 8]);
    let mut reinserted = reinserted;
    reinserted.age_ms = 5;
    assert_eq!(reinserted.id(), id, "an aged re-insert keeps the id");
    assert!(
        store.insert(reinserted, 0).unwrap(),
        "a trimmed id can be reinserted"
    );
    assert!(store.contains(&id));
    assert_eq!(store.len(), 1);
}

/// The in-process half of the `flush` contract (spec §4): flushing twice is
/// harmless, a flush neither evicts nor changes a reported age, a release
/// survives it, the store stays usable afterwards, and a `now` earlier than
/// the last anchor is clamped rather than moving any age backwards or
/// forwards.
///
/// What a flush makes durable can only be checked across a restart, which
/// this `std`-free suite can't do; a persistent backend tests that itself
/// (`tines-store` does, through the trait and through `Node::flush`).
pub fn idempotent_flush<S: Store>(mut make: impl FnMut() -> S) {
    let mut store = make();
    let fresh = signed_obs(1, b"t", &[1u8; 8]);
    let id = fresh.id();
    store.insert(fresh, 1000).unwrap();
    assert!(store.flush(2000).is_ok(), "the first flush succeeds");
    assert!(
        store.flush(2000).is_ok(),
        "a second flush at the same tick is harmless"
    );
    assert!(store.contains(&id), "flush does not evict");
    assert_eq!(store.len(), 1);
    let got = store.get(&id, 2500).unwrap();
    assert_eq!(got.age_ms, 1500, "age keeps advancing past a flush");

    // An earlier `now` is clamped to the last anchor: no age changes.
    assert!(store.flush(1500).is_ok(), "an earlier now is not an error");
    assert_eq!(store.get(&id, 2500).unwrap().age_ms, 1500);

    // An observation stored after the anchor isn't aged by a flush whose
    // `now` is behind its insert tick.
    let later = signed_obs(2, b"t", &[2u8; 8]);
    let later_id = later.id();
    store.insert(later, 5000).unwrap();
    assert!(store.flush(3000).is_ok());
    assert_eq!(
        store.get(&later_id, 5000).unwrap().age_ms,
        0,
        "a flush never moves an insert tick backwards"
    );

    // A release survives a flush, and the store is still usable after it.
    store.release(&id);
    assert!(store.flush(6000).is_ok());
    assert!(store.is_released(&id), "a flush keeps a release");
    let third = signed_obs(3, b"t", &[3u8; 8]);
    assert!(
        store.insert(third, 6000).unwrap(),
        "the store accepts inserts after a flush"
    );
    assert_eq!(store.len(), 3);
}
