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
    /// bytes written. The leading [`consts::WIRE_VERSION`] byte is prepended, so
    /// the total is `1 + postcard_len`. A buffer too small to hold the frame
    /// (including a zero-length buffer) yields `WireError::TooLarge`.
    pub fn encode(&self, out: &mut [u8]) -> Result<usize, WireError> {
        // A zero-length `out` must not panic on the `&out[1..]` slice below.
        if out.is_empty() {
            return Err(WireError::TooLarge);
        }
        out[0] = consts::WIRE_VERSION;
        match postcard::to_slice(self, &mut out[1..]) {
            Ok(slice) => Ok(1 + slice.len()),
            Err(_) => Err(WireError::TooLarge),
        }
    }

    /// Deserializes a frame from `bytes`. Rejects an empty frame as
    /// `WireError::Malformed` and a wrong-version leading byte as
    /// `WireError::VersionMismatch`; any other error (truncation, garbage)
    /// becomes `Malformed` rather than a panic.
    pub fn decode(bytes: &[u8]) -> Result<Frame, WireError> {
        if bytes.is_empty() {
            return Err(WireError::Malformed);
        }
        if bytes[0] != consts::WIRE_VERSION {
            return Err(WireError::VersionMismatch { version: bytes[0] });
        }
        match postcard::from_bytes::<Frame>(&bytes[1..]) {
            Ok(frame) => Ok(frame),
            Err(_) => Err(WireError::Malformed),
        }
    }
}

/// Peek the [`NodeId`] a wire frame was sent by, without fully decoding it.
///
/// A [`Frame`] envelope is a `WIRE_VERSION` byte, then an externally-tagged
/// postcard enum: `bytes[0]` is the version, `bytes[1]` the variant tag, and
/// `from` (the first field) occupies the 16 bytes at `bytes[2..18]`. This skips
/// the two leading bytes, reads those 16 bytes, and returns `None` for anything
/// shorter than a full envelope or whose leading byte is not
/// [`consts::WIRE_VERSION`] (so a truncated, wrong-version, or non-frame payload
/// reports no sender rather than panicking).
///
/// `tines-bt` uses this to enforce that a frame's declared sender matches the
/// NodeId advertised by its transport connection (the from-check), without
/// importing the private `Frame` layout.
pub fn frame_sender(bytes: &[u8]) -> Option<NodeId> {
    // Two leading bytes: `WIRE_VERSION` then the postcard variant tag, then the
    // 16-byte `from` field; `NodeId` is 16 bytes.
    const ENVELOPE_OVERHEAD: usize = 2;
    const NODE_ID_BYTES: usize = core::mem::size_of::<NodeId>();
    if bytes.len() < ENVELOPE_OVERHEAD + NODE_ID_BYTES {
        return None;
    }
    if bytes[0] != consts::WIRE_VERSION {
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
    /// The frame's leading `WIRE_VERSION` byte did not match `consts::WIRE_VERSION`.
    VersionMismatch { version: u8 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Payload, SigAlgo, Signature, Topic};

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
                dest: None,
                sig: None,
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
                dest: None,
                sig: None,
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
        // Pinned wire bytes for the exact Summary frame below. A `WIRE_VERSION`
        // byte leads the frame (byte 0); postcard tags are positional, so any
        // future reorder of the existing Frame variants changes what these
        // bytes decode to and fails this assertion.
        const GOLDEN: [u8; 537] = [
            4u8, 0u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8,
            3u8, 239u8, 253u8, 182u8, 245u8, 13u8, 128u8, 4u8, 170u8, 170u8, 170u8, 170u8, 170u8,
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
            170u8, 170u8, 170u8,
        ];
        let expected = Frame::Summary {
            from: [3; 16],
            salt: 0xdeadbeef,
            bits: Vec::from_slice(&[0xAAu8; consts::SUMMARY_BYTES]).unwrap(),
        };
        assert_eq!(Frame::decode(&GOLDEN).unwrap(), expected);
    }

    #[test]
    fn rejects_0x_frame() {
        // The pre-change (0.x) Summary golden bytes — captured before the
        // `WIRE_VERSION` byte was added — decode today as a version mismatch:
        // their leading byte is the postcard variant tag (0), not WIRE_VERSION.
        const ZERO_X: [u8; 536] = [
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
        assert_eq!(
            Frame::decode(&ZERO_X).err(),
            Some(WireError::VersionMismatch { version: 0 })
        );
    }

    #[test]
    fn encode_prepends_version_byte() {
        // A Summary serializes with `WIRE_VERSION` as byte 0 and still
        // round-trips through `Frame::decode` read across the *whole* buffer
        // (version byte included — never `&buf[1..]`, which lacks it).
        let f = Frame::Summary {
            from: [3; 16],
            salt: 0xdeadbeef,
            bits: Vec::from_slice(&[0xAAu8; consts::SUMMARY_BYTES]).unwrap(),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        assert_eq!(buf[0], consts::WIRE_VERSION);
        assert_eq!(Frame::decode(&buf[..n]).unwrap(), f);
    }

    #[test]
    fn decode_wrong_version_is_mismatch() {
        // A frame whose leading byte is `WIRE_VERSION + 1` is a version
        // mismatch rather than `Malformed`.
        let f = obs_frame();
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        buf[0] = consts::WIRE_VERSION + 1;
        assert_eq!(
            Frame::decode(&buf[..n]).err(),
            Some(WireError::VersionMismatch {
                version: consts::WIRE_VERSION + 1
            })
        );
    }

    #[test]
    fn decode_empty_is_malformed() {
        // An empty frame is `Malformed`, never a spurious
        // `VersionMismatch { version: 0 }` and never a panic.
        assert_eq!(Frame::decode(&[]).err(), Some(WireError::Malformed));
    }

    #[test]
    fn frame_sender_skips_version_and_tag() {
        // `[WIRE_VERSION, 0(tag), from..]` yields `Some(from)`: the two leading
        // bytes (version + variant tag) are skipped.
        let from = [9u8; 16];
        let mut b = std::vec![0u8; 2 + 16];
        b[0] = consts::WIRE_VERSION;
        b[1] = 0; // variant tag
        b[2..2 + 16].copy_from_slice(&from);
        assert_eq!(frame_sender(&b), Some(from));
    }

    #[test]
    fn frame_sender_wrong_version_is_none() {
        // `[WIRE_VERSION + 1, 0, from..]` is not our version, so no sender.
        let from = [9u8; 16];
        let mut b = std::vec![0u8; 2 + 16];
        b[0] = consts::WIRE_VERSION + 1;
        b[1] = 0; // variant tag
        b[2..2 + 16].copy_from_slice(&from);
        assert_eq!(frame_sender(&b), None);
    }

    #[test]
    fn frame_sender_peeks_from_field() {
        // Version byte 0, variant tag 1, then `from: [3;16]` at bytes 2..18.
        const GOLDEN: [u8; 201] = [
            4u8, 0u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8, 3u8,
            3u8, 239u8, 253u8, 182u8, 245u8, 13u8, 128u8, 4u8, 170u8, 170u8, 170u8, 170u8, 170u8,
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
            170u8, 170u8, 170u8,
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
            b[0] = consts::WIRE_VERSION; // version slot (skipped by frame_sender)
            b[1] = 0xFF; // variant tag (skipped by frame_sender)
            b[2..18].copy_from_slice(&[9u8; 16]); // from
            b
        };
        assert_eq!(frame_sender(&bytes), Some([9u8; 16]));
    }

    #[test]
    fn frame_sender_truncated_returns_none() {
        assert_eq!(frame_sender(&[0u8; 0]), None);
        assert_eq!(frame_sender(&[0u8; 16]), None); // version + tag + from missing
                                                    // A full envelope (version + tag + `from`) with a matching version
                                                    // yields the peeked sender.
        let mut full = std::vec![0u8; 2 + 16];
        full[0] = consts::WIRE_VERSION;
        assert_eq!(frame_sender(&full), Some([0u8; 16]));
    }

    #[test]
    fn decode_garbage_is_error_not_panic() {
        // A leading byte that is not `WIRE_VERSION` fails on the version gate
        // (`VersionMismatch`) before postcard ever runs...
        for len in [1usize, 7, 64, 1500] {
            let bytes = std::vec![0xFFu8; len];
            assert_eq!(
                Frame::decode(&bytes).err(),
                Some(WireError::VersionMismatch { version: 0xFF })
            );
        }
        // ...while same-length bytes that pass the gate but are not a valid
        // frame decode as `Malformed` rather than panicking.
        let mut garbage = std::vec![consts::WIRE_VERSION];
        garbage.extend(std::vec![0xFFu8; 16]);
        assert_eq!(Frame::decode(&garbage).err(), Some(WireError::Malformed));
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

    fn sig(version: u8, algo: SigAlgo, bytes: &[u8]) -> Signature {
        Signature {
            version,
            algo,
            bytes: Vec::from_slice(bytes).unwrap(),
        }
    }

    fn signed_obs(sig: Signature) -> Observation {
        Observation {
            observer: [7; 16],
            seq: 42,
            age_ms: 123,
            observed_at: Some(999),
            topic: Topic::from_slice(b"door/3").unwrap(),
            payload: Payload::from_slice(b"open").unwrap(),
            dest: None,
            sig: Some(sig),
        }
    }

    #[test]
    fn obs_roundtrip_with_ed25519_signature() {
        let f = Frame::Obs {
            from: [7; 16],
            obs: signed_obs(sig(1, SigAlgo::Ed25519, &[0u8; 64])),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        let decoded = Frame::decode(&buf[..n]).unwrap();
        assert_eq!(f, decoded);
    }

    #[test]
    fn obs_roundtrip_with_unknown_algo() {
        // An unknown algorithm id with short bytes is carried verbatim.
        let f = Frame::Obs {
            from: [7; 16],
            obs: signed_obs(sig(2, SigAlgo::Unknown(7), &[9u8; 10])),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        let decoded = Frame::decode(&buf[..n]).unwrap();
        assert_eq!(f, decoded);
    }

    #[test]
    fn algo_zero_is_malformed() {
        let f = Frame::Obs {
            from: [7; 16],
            obs: signed_obs(sig(1, SigAlgo::Unknown(0), &[0u8; 64])),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        assert_eq!(Frame::decode(&buf[..n]).err(), Some(WireError::Malformed));
    }

    #[test]
    fn version_zero_is_malformed() {
        let f = Frame::Obs {
            from: [7; 16],
            obs: signed_obs(sig(0, SigAlgo::Ed25519, &[0u8; 64])),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        assert_eq!(Frame::decode(&buf[..n]).err(), Some(WireError::Malformed));
    }

    #[test]
    fn ed25519_wrong_length_is_malformed() {
        let f = Frame::Obs {
            from: [7; 16],
            obs: signed_obs(sig(1, SigAlgo::Ed25519, &[0u8; 63])),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        assert_eq!(Frame::decode(&buf[..n]).err(), Some(WireError::Malformed));
    }

    #[test]
    fn raw_one_decodes_to_ed25519() {
        // A wire algo byte of 1 must decode to Ed25519, never Unknown(1).
        let f = Frame::Obs {
            from: [7; 16],
            obs: signed_obs(sig(1, SigAlgo::Unknown(1), &[0u8; 64])),
        };
        let mut buf = [0u8; consts::MAX_FRAME];
        let n = f.encode(&mut buf).unwrap();
        let decoded = Frame::decode(&buf[..n]).unwrap();
        match decoded {
            Frame::Obs { obs, .. } => {
                assert_eq!(obs.sig.as_ref().unwrap().algo, SigAlgo::Ed25519);
            }
            other => panic!("expected Obs, got {other:?}"),
        }
    }

    // Pinned wire vector for a full-size (MAX_PAYLOAD + 64-byte Ed25519) signed
    // Observation, exactly as a 1.0 node would emit it: the golden vector for
    // review focus #5. Decoding it must yield the bare Observation with a
    // SigAlgo::Ed25519 carrying 64 zeroed signature bytes.
    const OBS_GOLDEN_VECTOR_ED25519: [u8; 1139] = [
        0x04, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x06, 0x64, 0x6f, 0x6f, 0x72, 0x2f, 0x33, 0x80,
        0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    #[test]
    fn golden_vector_ed25519_decodes_to_bare_obs() {
        // Review Focus #5: the pinned vector is the golden Obs vector — decoding
        // it must yield the bare Observation with an Ed25519 signature carrying
        // 64 zeroed bytes.
        let frame = Frame::decode(&OBS_GOLDEN_VECTOR_ED25519).unwrap();
        let Frame::Obs { from, obs } = frame else {
            panic!("expected an Obs frame")
        };
        assert_eq!(from, [1; 16]);
        assert_eq!(obs.observer, [1; 16]);
        assert_eq!(obs.seq, 1);
        assert_eq!(obs.age_ms, 0);
        assert_eq!(obs.observed_at, None);
        assert_eq!(&obs.topic[..], b"door/3");
        assert_eq!(obs.payload.len(), consts::MAX_PAYLOAD);
        assert_eq!(obs.dest, None);
        let sig = obs.sig.expect("a signed obs carries a signature");
        assert_eq!(sig.version, 1);
        assert_eq!(sig.algo, SigAlgo::Ed25519);
        assert_eq!(sig.bytes.len(), 64);
        assert!(sig.bytes.iter().all(|b| *b == 0));
    }
}
