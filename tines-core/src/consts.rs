//! Compile-time constants for the tines workspace.
//!
//! Every value here is a compile-time `const` so it can drive generic
//! parameters (e.g. `heapless::Vec` capacities) and be optimized away.
//!
//! They split into two groups (§5): **wire** constants, frozen for wire v1 —
//! changing any is a `WIRE_VERSION` bump because it changes how a v1 peer
//! decodes or interprets a frame — and **local** constants, which never cross
//! the wire and can change in a minor release. Each const is tagged `wire` or
//! `local`. The private `MAX_FRAGMENTS` in `crate::frag` is also a wire
//! constant; see its note there.

// ---------------------------------------------------------------------------
// Wire constants — frozen for wire v1 (any change is a `WIRE_VERSION` bump)
// ---------------------------------------------------------------------------

/// WIRE. The first byte of every frame on the wire; changing it is a
/// `WIRE_VERSION` bump. `crate::wire::Frame::decode` rejects a leading byte
/// that differs (`WireError::VersionMismatch`), and [`crate::frame_sender`]
/// emits this. `4` is the first value in the `4..=0x7F` range that fits a
/// single-byte postcard enum tag and sits above the 0.x tag values (`0..=3`).
pub const WIRE_VERSION: u8 = 4;

/// WIRE. Decoder capacity bound: a peer rejects any topic longer than this.
pub const MAX_TOPIC: usize = 64;

/// WIRE. Decoder capacity bound: a peer rejects any payload longer than this.
pub const MAX_PAYLOAD: usize = 1024;

/// WIRE. Both sides must agree; this signature slot is reserved on every
/// observation (`[crate::observation::Signature]`) and carried verbatim,
/// never signed or verified until 1.1. `64` fits Ed25519.
pub const MAX_SIG: usize = 64;

/// WIRE. Per-key summary capacity a peer expects when decoding a summary.
pub const SUMMARY_BYTES: usize = 512;

/// WIRE. Both sides must agree on the number of hash functions to read a
/// Bloom filter in a summary.
pub const BLOOM_K: usize = 3;

/// WIRE. Decoder capacity bound (`Frag.data` is an inline `Vec<u8, MAX_FRAME>`),
/// so a peer cannot decode a bigger frame; a known 2.0 trigger (R3).
pub const MAX_FRAME: usize = 1500;

/// WIRE. Decoder capacity bound on an `Ack` receipt's content-ID list.
pub const ACK_IDS_CAP: usize = 90;

// ---------------------------------------------------------------------------
// Local constants — never cross the wire (safe to change in a minor release)
// ---------------------------------------------------------------------------

/// LOCAL. Node-local peer table; never sent on the wire.
///
/// Local constants are crate-private (decision K):
///
/// ```compile_fail
/// let _ = tines_core::consts::MAX_PEERS;
/// ```
pub(crate) const MAX_PEERS: usize = 8;

/// LOCAL. Emission cadence; local scheduling only.
pub(crate) const SUMMARY_INTERVAL_MS: u64 = 2000;

/// LOCAL. Inbound reassembly timer; local only.
pub(crate) const REASSEMBLY_TIMEOUT_MS: u64 = 5000;

/// LOCAL. Outbound delivery buffer size; local only.
pub(crate) const SEND_QUEUE_CAP: usize = 256;

/// LOCAL. Lower bound of the fragmentation MTU sweep (`crate::frag`): the
/// BLE ATT default MTU. It is **not** the smallest MTU core can use: that is
/// 85 bytes (see [`crate::Transport::mtu`] for every floor, pinned by
/// `tests::mtu_floors`). Between 23 and 85 core still never emits an
/// oversized frame; it just can't send everything. Only tests and the hidden
/// `conformance` module use it.
#[cfg(any(test, feature = "conformance"))]
pub(crate) const MIN_MTU: usize = 23;

/// LOCAL. In-node `Exchange` `events` queue of `NodeEvent`s; never leaves the
/// node (unlike the wire `ACK_IDS_CAP`).
pub(crate) const EVENT_QUEUE_CAP: usize = 64;

/// LOCAL. `Exchange` counted-drop table of distinct `(addr, DropReason)`
/// pairs. Once full, further counted drops fold into a single overflow counter
/// (reported with no address); see [`crate::exchange::NodeEvent::Dropped`].
pub(crate) const DROP_TABLE_CAP: usize = 32;

/// LOCAL. `Exchange` per-peer `bcast_sent` table of broadcast ids dispatched
/// to a peer.
pub(crate) const BCAST_SENT_CAP: usize = 256;

/// LOCAL. Pending ack-id buffer (`acks` for addressed, `bcast_acks` for
/// broadcast) before the next Ack is emitted; overflow drops the oldest entry.
pub(crate) const ACK_BUFFER_CAP: usize = 256;

/// LOCAL. Idle eviction window: a peer with no inbound frame for longer than
/// this many ticks is removed as if Lost (`crate::exchange`). Never crosses
/// the wire; local only.
pub(crate) const PEER_IDLE_MS: u64 = 30_000;

#[cfg(test)]
mod tests {
    use super::{MAX_FRAME, MAX_PAYLOAD, MAX_SIG, MAX_TOPIC};
    use crate::frag::{fragment, plan, FRAG_OVERHEAD};
    use crate::wire::Frame;
    use crate::{Dest, Observation, Payload, SigAlgo, Signature, Topic};

    /// The MTU floors `Transport::mtu`'s rustdoc states (decision K: it
    /// "states the smallest MTU core can use").
    #[test]
    fn mtu_floors() {
        let mut buf = [0u8; MAX_FRAME];

        // A Summary with a 1-byte filter and a worst-case (5-byte varint)
        // salt is 25 bytes, so summaries need mtu >= 26.
        let summary = Frame::Summary {
            from: [0xFF; 16],
            salt: u32::MAX,
            bits: heapless::Vec::from_slice(&[0xFF]).unwrap(),
        };
        assert_eq!(summary.encode(&mut buf), Ok(25));

        // An Ack naming one id is 35 bytes.
        let ack = Frame::Ack {
            from: [0xFF; 16],
            ids: heapless::Vec::from_slice(&[[0xFF; 16]]).unwrap(),
        };
        assert_eq!(ack.encode(&mut buf), Ok(35));

        // Fragmentation needs room past the fragment header.
        assert_eq!(plan(1, FRAG_OVERHEAD), None);
        assert_eq!(plan(1, FRAG_OVERHEAD + 1), Some((1, 1)));
        assert_eq!(FRAG_OVERHEAD + 1, 66);

        // The largest valid observation (signed, addressed, max topic and
        // payload, worst-case varints) encodes to 1233 bytes and first fits
        // in MAX_FRAGMENTS fragments at mtu 85.
        let worst = Observation {
            observer: [0xFF; 16],
            seq: u64::MAX,
            age_ms: u64::MAX,
            observed_at: Some(u64::MAX),
            topic: Topic::from_slice(&[0xFF; MAX_TOPIC]).unwrap(),
            payload: Payload::from_slice(&[0xFF; MAX_PAYLOAD]).unwrap(),
            dest: Some(Dest {
                to: [0xFF; 16],
                lifetime_ms: u64::MAX,
            }),
            sig: Some(Signature {
                version: u8::MAX,
                algo: SigAlgo::Ed25519,
                bytes: heapless::Vec::from_slice(&[0xFF; MAX_SIG]).unwrap(),
            }),
        };
        let mut obs_buf = [0u8; MAX_FRAME * 2];
        let len = postcard::to_slice(&worst, &mut obs_buf).unwrap().len();
        assert_eq!(len, 1233);
        assert!(fragment([0xFF; 16], &worst, 84, &mut |_| {}).is_err());
        assert!(fragment([0xFF; 16], &worst, 85, &mut |_| {}).is_ok());
    }
}
