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
}

impl Observation {
    /// Content-derived identity: Blake2s-256 over
    /// `(observer, seq, topic_len, topic, payload_len, payload)`, first 16 bytes.
    ///
    /// The hash ignores `age_ms` and `observed_at`, so two observations that
    /// differ only in age/time are still treated as the same key.
    pub fn id(&self) -> ObservationId {
        let mut hasher = Blake2s256::new();
        hasher.update(self.observer);
        hasher.update(self.seq.to_le_bytes());
        hasher.update((self.topic.len() as u16).to_le_bytes());
        hasher.update(&self.topic[..]);
        hasher.update((self.payload.len() as u16).to_le_bytes());
        hasher.update(&self.payload[..]);
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(seq: u64, age: u64, at: Option<u64>) -> Observation {
        Observation {
            observer: [1; 16],
            seq,
            age_ms: age,
            observed_at: at,
            topic: Topic::from_slice(b"door/3").unwrap(),
            payload: Payload::from_slice(b"open").unwrap(),
        }
    }

    #[test]
    fn id_ignores_age_and_observed_at() {
        assert_eq!(obs(1, 0, None).id(), obs(1, 999, Some(5)).id());
    }

    #[test]
    fn id_changes_with_seq() {
        assert_ne!(obs(1, 0, None).id(), obs(2, 0, None).id());
    }

    #[test]
    fn id_changes_with_observer() {
        let mut b = obs(1, 0, None);
        b.observer = [2; 16];
        assert_ne!(obs(1, 0, None).id(), b.id());
    }

    #[test]
    fn id_distinguishes_topic_payload_boundary() {
        // topic "ab" payload "c" vs topic "a" payload "bc" must differ
        let mut x = obs(1, 0, None);
        x.topic = Topic::from_slice(b"ab").unwrap();
        x.payload = Payload::from_slice(b"c").unwrap();
        let mut y = obs(1, 0, None);
        y.topic = Topic::from_slice(b"a").unwrap();
        y.payload = Payload::from_slice(b"bc").unwrap();
        assert_ne!(x.id(), y.id());
    }

    #[test]
    fn aged_saturates() {
        assert_eq!(obs(1, u64::MAX - 1, None).aged(10).age_ms, u64::MAX);
    }

    #[test]
    fn id_known_vector() {
        // pin the hash layout: observer=[1;16], seq=7, topic="t", payload="p"
        let mut o = obs(7, 0, None);
        o.topic = Topic::from_slice(b"t").unwrap();
        o.payload = Payload::from_slice(b"p").unwrap();
        assert_eq!(
            o.id(),
            [
                0x92, 0x37, 0x37, 0xa1, 0x06, 0x9b, 0xd9, 0x5e, 0x08, 0xc2, 0xe6, 0x39, 0x7d, 0x4a, 0x8f, 0xd0
            ]
        );
    }
}
