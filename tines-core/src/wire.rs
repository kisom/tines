//! Wire framing for tines-core using postcard serialization.
//!
//! Every malformed input from the wire is dropped as `Err(WireError::Malformed)`
//! — `tines-core` is `#![no_std]` and never panics on untrusted bytes.

use crate::consts;
use crate::{NodeId, Observation, ObservationId};
use heapless::Vec;
use serde::{Deserialize, Serialize};

/// A single frame carried on the wire.
///
/// `Frag` intentionally carries a full `MAX_FRAME` (1500 byte) payload inline to
/// avoid per-frame heap allocation in the `no_std` core; this keeps the enum
/// large, so the informative `large_enum_variant` lint is suppressed here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum Frame {
    Summary {
        from: NodeId,
        salt: u32,
        bits: Vec<u8, { consts::SUMMARY_BYTES }>,
    },
    Obs {
        from: NodeId,
        obs: Observation,
    },
    Frag {
        from: NodeId,
        id: ObservationId,
        index: u8,
        count: u8,
        data: Vec<u8, { consts::MAX_FRAME }>,
    },
    Ack {
        from: NodeId,
        ids: Vec<ObservationId, { consts::ACK_IDS_CAP }>,
    },
}

impl Frame {
    /// Returns the originating node of this frame.
    pub fn sender(&self) -> NodeId {
        match self {
            Frame::Summary { from, .. } => *from,
            Frame::Obs { from, .. } => *from,
            Frame::Frag { from, .. } => *from,
            Frame::Ack { from, .. } => *from,
        }
    }

    /// Serializes this frame into `out` via postcard, returning the number of
    /// bytes written. A buffer too small to hold the frame yields
    /// `WireError::TooLarge`.
    pub fn encode(&self, out: &mut [u8]) -> Result<usize, WireError> {
        match postcard::to_slice(self, out) {
            Ok(slice) => Ok(slice.len()),
            Err(_) => Err(WireError::TooLarge),
        }
    }

    /// Deserializes a frame from `bytes`. Any error (including truncation or
    /// garbage) becomes `WireError::Malformed` rather than a panic.
    pub fn decode(bytes: &[u8]) -> Result<Frame, WireError> {
        match postcard::from_bytes::<Frame>(bytes) {
            Ok(frame) => Ok(frame),
            Err(_) => Err(WireError::Malformed),
        }
    }
}

/// Peek the [`NodeId`] a wire frame was sent by, without fully decoding it.
///
/// A [`Frame`] envelope is an externally-tagged postcard enum: one tag byte at
/// index `0`, then its fields in declaration order. `from` is the first field,
/// so it occupies the 16 bytes at `bytes[1..17]`. This reads those 16 bytes and
/// returns `None` for anything shorter than a full envelope (so a truncated or
/// non-frame payload reports no sender rather than panicking).
///
/// The leading byte is skipped on purpose: it is the externally-tagged variant
/// tag today and the reserved version-byte slot in 1.0, so reading `from` from
/// a fixed offset keeps this accessor correct when 1.0 prepends a version byte
/// ahead of the envelope.
///
/// `tines-bt` uses this to enforce that a frame's declared sender matches the
/// NodeId advertised by its transport connection (the from-check), without
/// importing the private `Frame` layout.
pub fn frame_sender(bytes: &[u8]) -> Option<NodeId> {
    // The envelope is one tag byte then the `from` field; `NodeId` is 16 bytes.
    const ENVELOPE_OVERHEAD: usize = 1;
    const NODE_ID_BYTES: usize = core::mem::size_of::<NodeId>();
    if bytes.len() < ENVELOPE_OVERHEAD + NODE_ID_BYTES {
        return None;
    }
    let mut from = [0u8; NODE_ID_BYTES];
    from.copy_from_slice(&bytes[ENVELOPE_OVERHEAD..ENVELOPE_OVERHEAD + NODE_ID_BYTES]);
    Some(from)
}

/// Errors produced while (de)serializing frames on the wire.
#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    /// The output buffer was too small to hold the serialized frame.
    TooLarge,
    /// The input bytes were malformed, truncated, or not a valid `Frame`.
    Malformed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Payload;
    use crate::Topic;

    fn obs_frame() -> Frame {
        Frame::Obs {
            from: [7; 16],
            obs: Observation {
                observer: [7; 16],
                seq: 42,
                age_ms: 123,
                observed_at: Some(999),
                topic: Topic::from_slice(b"door/3").unwrap(),
                payload: Payload::from_slice(b"open").unwrap(),
                to: None,
                lifetime_ms: None,
            },
        }
    }

    fn big_obs_frame() -> Frame {
        Frame::Obs {
            from: [1; 16],
            obs: Observation {
                observer: [1; 16],
                seq: 1,
                age_ms: 0,
                observed_at: None,
                topic: Topic::from_slice(b"t").unwrap(),
                payload: Payload::from_slice(&[0u8; 1024]).unwrap(),
                to: None,
                lifetime_ms: None,
            },
        }
    }

    #[test]
    fn obs_frame_roundtrip() {
        let f = obs_frame();
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        let decoded = Frame::decode(&buf[..n]).unwrap();
        assert_eq!(f, decoded);
    }

    #[test]
    fn summary_frame_roundtrip() {
        let f = Frame::Summary {
            from: [3; 16],
            salt: 0xdeadbeef,
            bits: Vec::from_slice(&[0xAAu8; consts::SUMMARY_BYTES]).unwrap(),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        let decoded = Frame::decode(&buf[..n]).unwrap();
        assert_eq!(f, decoded);
    }

    #[test]
    fn frag_frame_roundtrip() {
        let f = Frame::Frag {
            from: [9; 16],
            id: [4; 16],
            index: 2,
            count: 5,
            data: Vec::from_slice(&[0x5Cu8; 100]).unwrap(),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        let decoded = Frame::decode(&buf[..n]).unwrap();
        assert_eq!(f, decoded);
    }

    #[test]
    fn ack_frame_roundtrip() {
        // Ack carrying 3 content-IDs must roundtrip through encode/decode.
        let ids = Vec::<ObservationId, { consts::ACK_IDS_CAP }>::from_slice(&[
            [1u8; 16], [2u8; 16], [3u8; 16],
        ])
        .unwrap();
        let f = Frame::Ack { from: [5; 16], ids };
        // sender() must return `from` for an Ack.
        assert_eq!(f.sender(), [5; 16]);
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        let decoded = Frame::decode(&buf[..n]).unwrap();
        assert_eq!(f, decoded);
    }

    #[test]
    fn summary_golden_vector() {
        // Pinned wire bytes for the exact Summary frame below. postcard tags are
        // positional, so any future reorder of the existing Frame variants changes
        // what these bytes decode to and fails this assertion.
        const GOLDEN: [u8; 536] = [
            0u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8,
            239u8, 253u8, 182u8, 245u8, 13u8, 128u8, 4u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8,
        ];
        let expected = Frame::Summary {
            from: [3; 16],
            salt: 0xdeadbeef,
            bits: Vec::from_slice(&[0xAAu8; consts::SUMMARY_BYTES]).unwrap(),
        };
        assert_eq!(Frame::decode(&GOLDEN).unwrap(), expected);
    }

    #[test]
    fn frame_sender_peeks_from_field() {
        // The golden Summary encodes `from: [3;16]` starting at byte 1.
        const GOLDEN: [u8; 200] = [
            0u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8,
            239u8, 253u8, 182u8, 245u8, 13u8, 128u8, 4u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8, 170u8,
            170u8, 170u8,
        ];
        assert_eq!(frame_sender(&GOLDEN), Some([3u8; 16]));
    }

    #[test]
    fn frame_sender_reads_from_fixed_offset_not_just_the_start() {
        // The leading byte (the externally-tagged variant tag) is skipped, so a
        // payload whose first byte is not the sender must still yield the `from`
        // recorded at byte 1.
        let bytes = {
            let mut b = [0u8; 32];
            b[0] = 0xFF; // tag/version slot
            b[1..17].copy_from_slice(&[9u8; 16]); // from
            b
        };
        assert_eq!(frame_sender(&bytes), Some([9u8; 16]));
    }

    #[test]
    fn frame_sender_truncated_returns_none() {
        assert_eq!(frame_sender(&[0u8; 0]), None);
        assert_eq!(frame_sender(&[0u8; 16]), None); // tag + from missing
        assert_eq!(frame_sender(&[0u8; 17]), Some([0u8; 16])); // exactly tag + from
    }

    #[test]
    fn decode_garbage_is_error_not_panic() {
        for len in [0usize, 1, 7, 64, 1500] {
            let bytes = std::vec![0xFFu8; len];
            assert_eq!(Frame::decode(&bytes).err(), Some(WireError::Malformed));
        }
    }

    #[test]
    fn decode_truncated_is_error() {
        let f = obs_frame();
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        assert!(Frame::decode(&buf[..n / 2]).is_err());
    }

    #[test]
    fn encode_into_too_small_buffer_is_too_large() {
        let f = big_obs_frame();
        let mut buf = [0u8; 16];
        assert_eq!(f.encode(&mut buf).err(), Some(WireError::TooLarge));
    }
}
