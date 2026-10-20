//! Content-defined `Observation` identity.

use crate::consts;
use blake2::{Blake2s256, Digest};
use heapless::Vec;
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
        }
    }

    /// Sets `observed_at` (the observer's wall-clock publish time) and returns
    /// the observation, to chain from [`Self::broadcast`] / [`Self::addressed`].
    pub fn with_observed_at(mut self, at: u64) -> Observation {
        self.observed_at = Some(at);
        self
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
        }
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
}
