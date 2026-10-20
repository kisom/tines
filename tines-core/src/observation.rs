//! Content-defined `Observation` identity.

use crate::consts;
use blake2::{Blake2s256, Digest};
use heapless::Vec;
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

/// 16-byte node identifier.
pub type NodeId = [u8; 16];
/// 16-byte content-derived observation identifier (first 16 bytes of Blake2s-256).
pub type ObservationId = [u8; 16];
/// Monotonic sequence number / tick.
pub type Tick = u64;
/// Heapless, fixed-capacity topic.
pub type Topic = Vec<u8, { consts::MAX_TOPIC }>;
/// Heapless, fixed-capacity payload.
pub type Payload = Vec<u8, { consts::MAX_PAYLOAD }>;

/// A destination for an addressed [`Observation`]: the single recipient plus
/// the deadline after which the exchange rejects it.
///
/// Its presence is information in itself: `None` is a broadcast, `Some` is
/// addressed to exactly one node and carries a lifetime.
///
/// [`Observation`]: struct@Observation
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dest {
    /// The one node this observation is addressed to.
    pub to: NodeId,
    /// The deadline in ms: the exchange rejects the observation once its
    /// [`Observation::age_ms`] exceeds this value.
    pub lifetime_ms: u64,
}

/// An observation published by a node, with content-derived identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub observer: NodeId,
    pub seq: u64,
    /// The observer's elapsed time for this observation, in ms.
    ///
    /// This is a *lower bound* on the observation's true age: it counts time
    /// only while the observer's clock runs. Time the host spent powered off is
    /// recovered when the store is opened (see [`Self::observed_at`]); the time
    /// in an un-flushed crash window is bounded by the host's flush cadence,
    /// and in-flight time is negligible. Relays never advance it.
    pub age_ms: u64,
    /// The observer's wall-clock time, in ms since the Unix epoch (UTC), when
    /// this observation was published, or `None` if the observer has no
    /// trusted clock. Set once at construction and never changed by a relay.
    pub observed_at: Option<u64>,
    pub topic: Topic,
    pub payload: Payload,
    /// Addressing: `None` is a broadcast, `Some(dest)` is addressed to
    /// `dest.to` and expires per `dest.lifetime_ms`.
    pub dest: Option<Dest>,

    /// The observer's signature over this observation, if any.
    ///
    /// The 1.0 wire reserves this slot on every observation and carries a
    /// signature verbatim (see [`Signature`] / [`SigAlgo`]), but 1.0 does not sign
    /// or verify it. Signing and verification arrive in 1.1.
    pub sig: Option<Signature>,
}

impl Observation {
    /// Content-derived identity: the first 16 bytes of Blake2s-256 over
    /// `observer ‖ seq_le ‖ topic_len_le(u16) ‖ topic ‖ payload_len_le(u16) ‖
    /// payload ‖ to_marker`, where `to_marker` is `0x00` for broadcast
    /// (`dest = None`) and `0x01 ‖ node_id` for an addressed observation
    /// (`dest.map(|d| d.to) = Some(node_id)`).
    ///
    /// The hash ignores `age_ms`, `observed_at` and `lifetime_ms`, so two
    /// observations that differ only in age, time or lifetime are still
    /// treated as the same key.
    pub fn id(&self) -> ObservationId {
        let mut hasher = Blake2s256::new();
        hasher.update(self.observer);
        hasher.update(self.seq.to_le_bytes());
        hasher.update((self.topic.len() as u16).to_le_bytes());
        hasher.update(&self.topic[..]);
        hasher.update((self.payload.len() as u16).to_le_bytes());
        hasher.update(&self.payload[..]);
        // Fold the addressing recipient into the content id so that two
        // observations with otherwise-identical content addressed to
        // different nodes are treated as distinct keys. `dest = None`
        // (broadcast) is encoded as a single `0x00` byte, distinct from a real
        // zero node id (encoded `0x01` + sixteen zero bytes).
        match self.dest.as_ref().map(|d| d.to) {
            None => hasher.update([0x00]),
            Some(node_id) => {
                hasher.update([0x01]);
                hasher.update(node_id);
            }
        }
        let digest = hasher.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&digest[..16]);
        id
    }

    /// Returns a copy with `age_ms` advanced by `elapsed_ms` (saturating).
    pub fn aged(&self, elapsed_ms: u64) -> Observation {
        Observation {
            observer: self.observer,
            seq: self.seq,
            age_ms: self.age_ms.saturating_add(elapsed_ms),
            observed_at: self.observed_at,
            topic: self.topic.clone(),
            payload: self.payload.clone(),
            dest: self.dest.clone(),
            sig: self.sig.clone(),
        }
    }

    /// A broadcast observation (`dest = None`) made "now" (`age_ms = 0`) under
    /// `topic`. An oversized topic or payload panics: callers that must not
    /// panic (e.g. [`crate::node::Node::observe`]) validate sizes first.
    pub fn broadcast(observer: NodeId, seq: u64, topic: &[u8], payload: &[u8]) -> Observation {
        Observation {
            observer,
            seq,
            age_ms: 0,
            observed_at: None,
            topic: Topic::from_slice(topic).expect("topic fits MAX_TOPIC"),
            payload: Payload::from_slice(payload).expect("payload fits MAX_PAYLOAD"),
            dest: None,
            sig: None,
        }
    }

    /// An addressed observation (`dest = Some(Dest { to, lifetime_ms })`) made
    /// "now" (`age_ms = 0`) under `topic`, addressed to `to` and expiring after
    /// `lifetime_ms`. An oversized topic or payload panics: callers that must
    /// not panic (e.g. [`crate::node::Node::observe_to`]) validate sizes first.
    pub fn addressed(
        observer: NodeId,
        seq: u64,
        topic: &[u8],
        payload: &[u8],
        to: NodeId,
        lifetime_ms: u64,
    ) -> Observation {
        Observation {
            observer,
            seq,
            age_ms: 0,
            observed_at: None,
            topic: Topic::from_slice(topic).expect("topic fits MAX_TOPIC"),
            payload: Payload::from_slice(payload).expect("payload fits MAX_PAYLOAD"),
            dest: Some(Dest { to, lifetime_ms }),
            sig: None,
        }
    }

    /// Sets `observed_at` (the observer's wall-clock publish time) and returns
    /// the observation, to chain from [`Self::broadcast`] / [`Self::addressed`].
    pub fn with_observed_at(mut self, at: u64) -> Observation {
        self.observed_at = Some(at);
        self
    }
}

/// A signature scheme identifier, carried in [`Signature::algo`].
///
/// On the wire `algo` is a raw `u8` — the `#[repr(u8)]` discriminant, *not* a
/// postcard enum tag: [`SigAlgo::Ed25519`] is the byte `1`, and an algorithm
/// `tines-core` does not recognize is carried as [`SigAlgo::Unknown`] with its
/// raw id, never rejected. `0` is never valid here (it is `Malformed` on decode).
///
/// Signing and verification arrive in 1.1.
#[non_exhaustive]
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SigAlgo {
    /// Ed25519: a 64-byte Ed25519 signature.
    Ed25519 = 1,
    /// An algorithm id `tines-core` does not recognize, carried verbatim.
    /// Any `u8` other than `0` (the malformed sentinel) and `1` (`Ed25519`) is
    /// preserved so a 1.0 node can forward it untouched to a 1.1 node.
    Unknown(u8),
}

/// An observation's signature: the observer's over the whole observation.
///
/// The slot is reserved on the 1.0 wire but *unsigned* in 1.0: this type only
/// carries a signature verbatim, enforcing the shape rules below on decode.
/// Signing and verification arrive in 1.1.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Signature {
    /// Signature format version. `0` is rejected as `Malformed` on decode;
    /// every other value (including any version `tines-core` does not know) is
    /// carried verbatim.
    pub version: u8,
    /// The algorithm the signature is over. Serialized as a raw `u8` on the
    /// wire (its `#[repr(u8)]` discriminant, so [`SigAlgo::Ed25519`] is `1`);
    /// unknown ids are preserved as [`SigAlgo::Unknown`] rather than rejected.
    #[serde(with = "sig_algo_wire")]
    pub algo: SigAlgo,
    /// The signature bytes. For [`SigAlgo::Ed25519`] these MUST be exactly 64
    /// bytes (a different length is `Malformed`); other algorithms carry any
    /// length. Never rejected for being unparseable — carried as-is.
    pub bytes: heapless::Vec<u8, { consts::MAX_SIG }>,
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawSignature {
            version: u8,
            #[serde(with = "sig_algo_wire")]
            algo: SigAlgo,
            bytes: heapless::Vec<u8, { consts::MAX_SIG }>,
        }
        let raw = RawSignature::deserialize(deserializer)?;
        // A malformed signature (bad version, bad or wrong-length algorithm) is
        // carried by no frame: it is a decode failure, never a panic.
        if raw.version == 0 {
            return Err(serde::de::Error::custom("signature version 0 is malformed"));
        }
        if matches!(raw.algo, SigAlgo::Ed25519) && raw.bytes.len() != 64 {
            return Err(serde::de::Error::custom(
                "Ed25519 signature must be exactly 64 bytes",
            ));
        }
        Ok(Signature {
            version: raw.version,
            algo: raw.algo,
            bytes: raw.bytes,
        })
    }
}

/// Wire encoding of [`SigAlgo`]: a raw `u8` (its `#[repr(u8)]` discriminant),
/// not a postcard enum tag. Used via `#[serde(with = "sig_algo_wire")]` on the
/// [`Signature::algo`] field so an unknown id survives decode as [`SigAlgo::Unknown`].
mod sig_algo_wire {
    use super::SigAlgo;
    use serde::de::Deserializer;
    use serde::ser::Serializer;
    use serde::Deserialize;

    pub fn serialize<S>(algo: &SigAlgo, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match algo {
            // `Ed25519` is the byte `1`; any carried `Unknown(n)` is its raw id.
            SigAlgo::Ed25519 => serializer.serialize_u8(1),
            SigAlgo::Unknown(id) => serializer.serialize_u8(*id),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SigAlgo, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = u8::deserialize(deserializer)?;
        // `0` is the malformed sentinel on the wire; `1` is `Ed25519`; every
        // other id is carried as `Unknown(n)` so it forwards untouched.
        match raw {
            0 => Err(serde::de::Error::custom("signature algo id 0 is malformed")),
            1 => Ok(SigAlgo::Ed25519),
            id => Ok(SigAlgo::Unknown(id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(seq: u64, age: u64, at: Option<u64>, dest: Option<Dest>) -> Observation {
        Observation {
            observer: [1; 16],
            seq,
            age_ms: age,
            observed_at: at,
            topic: Topic::from_slice(b"door/3").unwrap(),
            payload: Payload::from_slice(b"open").unwrap(),
            dest,
            sig: None,
        }
    }

    fn sig(version: u8, algo: SigAlgo, bytes: &[u8]) -> Signature {
        Signature {
            version,
            algo,
            bytes: Vec::from_slice(bytes).unwrap(),
        }
    }

    #[test]
    fn sig_excluded_from_id() {
        // A signature must not affect identity: a bare, a full Ed25519 and an
        // arbitrary unknown-algorithm sig all share the observation's id.
        let base = obs(5, 0, None, None);
        let mut signed = obs(5, 0, None, None);
        signed.sig = Some(sig(1, SigAlgo::Ed25519, &[0u8; 64]));
        assert_eq!(base.id(), signed.id(), "sig must not affect id");
        let mut other = obs(5, 0, None, None);
        other.sig = Some(sig(2, SigAlgo::Unknown(9), &[1u8; 10]));
        assert_eq!(base.id(), other.id());
    }

    #[test]
    fn id_ignores_age_and_observed_at() {
        assert_eq!(obs(1, 0, None, None).id(), obs(1, 999, Some(5), None).id());
    }

    #[test]
    fn id_changes_with_seq() {
        assert_ne!(obs(1, 0, None, None).id(), obs(2, 0, None, None).id());
    }

    #[test]
    fn id_changes_with_observer() {
        let mut b = obs(1, 0, None, None);
        b.observer = [2; 16];
        assert_ne!(obs(1, 0, None, None).id(), b.id());
    }

    #[test]
    fn id_distinguishes_topic_payload_boundary() {
        // topic "ab" payload "c" vs topic "a" payload "bc" must differ
        let mut x = obs(1, 0, None, None);
        x.topic = Topic::from_slice(b"ab").unwrap();
        x.payload = Payload::from_slice(b"c").unwrap();
        let mut y = obs(1, 0, None, None);
        y.topic = Topic::from_slice(b"a").unwrap();
        y.payload = Payload::from_slice(b"bc").unwrap();
        assert_ne!(x.id(), y.id());
    }

    #[test]
    fn aged_saturates() {
        assert_eq!(obs(1, u64::MAX - 1, None, None).aged(10).age_ms, u64::MAX);
    }

    #[test]
    fn to_is_part_of_identity() {
        // same observation, broadcast vs addressed to Some([2;16]) — must differ
        assert_ne!(
            obs(7, 0, None, None).id(),
            obs(
                7,
                0,
                None,
                Some(Dest {
                    to: [2; 16],
                    lifetime_ms: 0
                })
            )
            .id()
        );
    }

    #[test]
    fn lifetime_is_not_part_of_identity() {
        // same payload, both addressed to Some([2;16]), lifetime 100 vs 200 — must be equal
        assert_eq!(
            obs(
                7,
                0,
                None,
                Some(Dest {
                    to: [2; 16],
                    lifetime_ms: 100
                })
            )
            .id(),
            obs(
                7,
                0,
                None,
                Some(Dest {
                    to: [2; 16],
                    lifetime_ms: 200
                })
            )
            .id()
        );
    }

    #[test]
    fn broadcast_marker_distinct_from_zero_id() {
        // broadcast marker (dest = None) must not collide with a real zero node id
        assert_ne!(
            obs(7, 0, None, None).id(),
            obs(
                7,
                0,
                None,
                Some(Dest {
                    to: [0; 16],
                    lifetime_ms: 0
                })
            )
            .id()
        );
    }

    #[test]
    fn aged_preserves_addressing() {
        let aged = obs(
            1,
            0,
            None,
            Some(Dest {
                to: [2; 16],
                lifetime_ms: 500,
            }),
        )
        .aged(10);
        assert_eq!(aged.age_ms, 10);
        assert_eq!(aged.dest.as_ref().map(|d| d.to), Some([2; 16]));
        assert_eq!(aged.dest.as_ref().map(|d| d.lifetime_ms), Some(500));
    }

    #[test]
    fn dest_invariant_by_type() {
        // A broadcast observation never carries addressing...
        let b = Observation::broadcast([1; 16], 1, b"t", b"p");
        assert_eq!(b.dest, None);
        // ...and an addressed observation *always* carries a `dest` (this is
        // the invariant the type now enforces — no more to/xor-lifetime pairs).
        let o = Observation::addressed([1; 16], 1, b"t", b"p", [2; 16], 5000);
        assert_eq!(
            o.dest,
            Some(Dest {
                to: [2; 16],
                lifetime_ms: 5000
            }),
            "addressed must always set dest"
        );
    }

    #[test]
    fn id_reads_dest_to() {
        // id() reads the recipient out of `dest`; the lifetime is still not
        // part of identity.
        let x = Observation::addressed([1; 16], 7, b"t", b"p", [2; 16], 100);
        let y = Observation::addressed([1; 16], 7, b"t", b"p", [2; 16], 200);
        assert_eq!(x.id(), y.id());
    }

    #[test]
    fn constructors_set_age_zero() {
        let b = Observation::broadcast([1; 16], 1, b"t", b"p");
        assert_eq!(b.age_ms, 0);
        assert_eq!(b.observed_at, None);
        assert_eq!(b.dest, None);

        let a = Observation::addressed([1; 16], 1, b"t", b"p", [2; 16], 5000);
        assert_eq!(a.age_ms, 0);
        assert_eq!(a.observed_at, None);
        assert_eq!(
            a.dest,
            Some(Dest {
                to: [2; 16],
                lifetime_ms: 5000
            })
        );
    }

    #[test]
    fn with_observed_at_sets_field() {
        let b = Observation::broadcast([1; 16], 1, b"t", b"p").with_observed_at(1234);
        assert_eq!(b.observed_at, Some(1234));
        assert_eq!(b.dest, None);

        let a =
            Observation::addressed([1; 16], 1, b"t", b"p", [2; 16], 5000).with_observed_at(1234);
        assert_eq!(a.observed_at, Some(1234));
        assert_eq!(
            a.dest,
            Some(Dest {
                to: [2; 16],
                lifetime_ms: 5000
            })
        );
    }

    #[test]
    fn id_known_vector() {
        // pin the hash layout: observer=[1;16], seq=7, topic="t", payload="p", dest=None (broadcast)
        let mut o = obs(7, 0, None, None);
        o.topic = Topic::from_slice(b"t").unwrap();
        o.payload = Payload::from_slice(b"p").unwrap();
        assert_eq!(
            o.id(),
            [
                0x4a, 0x33, 0x09, 0x92, 0xf1, 0x61, 0xbe, 0xc9, 0x19, 0x7b, 0x6e, 0x96, 0x11, 0x6f,
                0xa4, 0xa2
            ]
        );
    }

    #[test]
    fn sig_algo_wire_roundtrip() {
        // Review Focus #3: the raw-u8 wire encoding round-trips a known Ed25519
        // and every unknown id verbatim, rejects the malformed sentinel, and
        // never maps a wire byte of 1 to Unknown(1). Exercises the algo field
        // through Signature, since SigAlgo serializes only via its serde module.
        let make = |algo: SigAlgo| Signature {
            version: 1,
            algo,
            bytes: heapless::Vec::<u8, { crate::consts::MAX_SIG }>::from_slice(&[0u8; 64]).unwrap(),
        };
        for (algo, wire_byte) in [
            (SigAlgo::Ed25519, 1u8),
            (SigAlgo::Unknown(7), 7u8),
            (SigAlgo::Unknown(255), 255u8),
        ] {
            let sig = make(algo);
            let mut buf = [0u8; consts::MAX_SIG + 8];
            let n = postcard::to_slice(&sig, &mut buf).unwrap();
            assert_eq!(n[0], 1, "version serializes as the first byte");
            assert_eq!(n[1], wire_byte, "algo is its raw repr discriminant");
            let decoded: Signature = postcard::from_bytes(&n[..]).unwrap();
            assert_eq!(decoded.algo, algo, "algo must round-trip verbatim");
        }
        // `0` is the malformed sentinel on the wire — it must fail to decode.
        let sig = make(SigAlgo::Unknown(0));
        let mut buf = [0u8; consts::MAX_SIG + 8];
        let n = postcard::to_slice(&sig, &mut buf).unwrap();
        assert_eq!(n[1], 0);
        let bad: Result<Signature, _> = postcard::from_bytes::<Signature>(n);
        assert!(bad.is_err(), "algo id 0 must be malformed");
        // A wire byte of 1 is Ed25519, never Unknown(1).
        let sig = make(SigAlgo::Ed25519);
        let mut buf = [0u8; consts::MAX_SIG + 8];
        let n = postcard::to_slice(&sig, &mut buf).unwrap();
        assert_eq!(n[1], 1);
        assert_eq!(
            postcard::from_bytes::<Signature>(n).unwrap().algo,
            SigAlgo::Ed25519,
            "wire byte 1 must decode to Ed25519"
        );
    }
}
