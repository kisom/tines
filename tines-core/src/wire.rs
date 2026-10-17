//! Wire framing for tines-core using postcard serialization.
//!
//! Every malformed input from the wire is dropped as `Err(WireError::Malformed)`
//! — `tines-core` is `#![no_std]` and never panics on untrusted bytes.

use crate::consts;
use crate::{Observation, NodeId, ObservationId};
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
}

impl Frame {
    /// Returns the originating node of this frame.
    pub fn sender(&self) -> NodeId {
        match self {
            Frame::Summary { from, .. } => *from,
            Frame::Obs { from, .. } => *from,
            Frame::Frag { from, .. } => *from,
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
