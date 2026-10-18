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

/// An observation published by a node, with content-derived identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub observer: NodeId,
    pub seq: u64,
    pub age_ms: u64,
    pub observed_at: Option<u64>,
    pub topic: Topic,
    pub payload: Payload,
    /// Addressing recipient; `None` means broadcast.
    pub to: Option<NodeId>,
    /// Optional absolute or relative lifetime in ms; `None` means never expires.
    pub lifetime_ms: Option<u64>,
}

impl Observation {
    /// Content-derived identity: the first 16 bytes of Blake2s-256 over
    /// `observer ‖ seq_le ‖ topic_len_le(u16) ‖ topic ‖ payload_len_le(u16) ‖
    /// payload ‖ to_marker`, where `to_marker` is `0x00` for broadcast
    /// (`to = None`) and `0x01 ‖ node_id` for `to = Some(node_id)`.
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
        // different nodes are treated as distinct keys. `to = None` (broadcast)
        // is encoded as a single `0x00` byte, distinct from a real zero node id
        // (encoded `0x01` + sixteen zero bytes).
        match self.to {
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
            to: self.to,
            lifetime_ms: self.lifetime_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(
        seq: u64,
        age: u64,
        at: Option<u64>,
        to: Option<NodeId>,
        lifetime_ms: Option<u64>,
    ) -> Observation {
        Observation {
            observer: [1; 16],
            seq,
            age_ms: age,
            observed_at: at,
            topic: Topic::from_slice(b"door/3").unwrap(),
            payload: Payload::from_slice(b"open").unwrap(),
            to,
            lifetime_ms,
        }
    }

    #[test]
    fn id_ignores_age_and_observed_at() {
        assert_eq!(
            obs(1, 0, None, None, None).id(),
            obs(1, 999, Some(5), None, None).id()
        );
    }

    #[test]
    fn id_changes_with_seq() {
        assert_ne!(
            obs(1, 0, None, None, None).id(),
            obs(2, 0, None, None, None).id()
        );
    }

    #[test]
    fn id_changes_with_observer() {
        let mut b = obs(1, 0, None, None, None);
        b.observer = [2; 16];
        assert_ne!(obs(1, 0, None, None, None).id(), b.id());
    }

    #[test]
    fn id_distinguishes_topic_payload_boundary() {
        // topic "ab" payload "c" vs topic "a" payload "bc" must differ
        let mut x = obs(1, 0, None, None, None);
        x.topic = Topic::from_slice(b"ab").unwrap();
        x.payload = Payload::from_slice(b"c").unwrap();
        let mut y = obs(1, 0, None, None, None);
        y.topic = Topic::from_slice(b"a").unwrap();
        y.payload = Payload::from_slice(b"bc").unwrap();
        assert_ne!(x.id(), y.id());
    }

    #[test]
    fn aged_saturates() {
        assert_eq!(
            obs(1, u64::MAX - 1, None, None, None).aged(10).age_ms,
            u64::MAX
        );
    }

    #[test]
    fn to_is_part_of_identity() {
        // same observation, to = None vs Some([2;16]) — must differ
        assert_ne!(
            obs(7, 0, None, None, None).id(),
            obs(7, 0, None, Some([2; 16]), None).id()
        );
    }

    #[test]
    fn lifetime_is_not_part_of_identity() {
        // same payload, both to = Some([2;16]), lifetime 100 vs 200 — must be equal
        assert_eq!(
            obs(7, 0, None, Some([2; 16]), Some(100)).id(),
            obs(7, 0, None, Some([2; 16]), Some(200)).id()
        );
    }

    #[test]
    fn broadcast_marker_distinct_from_zero_id() {
        // broadcast marker (to = None) must not collide with a real zero node id
        assert_ne!(
            obs(7, 0, None, None, None).id(),
            obs(7, 0, None, Some([0; 16]), None).id()
        );
    }

    #[test]
    fn aged_preserves_addressing() {
        let aged = obs(1, 0, None, Some([2; 16]), Some(500)).aged(10);
        assert_eq!(aged.age_ms, 10);
        assert_eq!(aged.to, Some([2; 16]));
        assert_eq!(aged.lifetime_ms, Some(500));
    }

    #[test]
    fn id_known_vector() {
        // pin the hash layout: observer=[1;16], seq=7, topic="t", payload="p", to=None
        let mut o = obs(7, 0, None, None, None);
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
