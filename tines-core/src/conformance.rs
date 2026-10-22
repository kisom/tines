//! Test-only hooks into crate-private wire types and local constants.
//!
//! **Not public API.** Hidden from the docs, behind the `conformance`
//! feature, and outside the 1.x stability promise: anything here may change
//! or vanish in any release.
//!
//! `Frame` and `WireError` are crate-private (spec §3), and the local
//! capacities and timings are `pub(crate)` (decision K). The fuzz harness in
//! `fuzz/` and its proptest twin (`tines-core/tests/fuzz_twin.rs`) still need
//! to build well-formed frames, read the frames a `Node` sends, and size
//! their scenarios by core's local limits. These functions give them exactly
//! that, as byte-level helpers, without exposing the `Frame` enum.
//!
//! Every function is `no_std`, allocation-free and panic-free on any input.

use crate::consts;
use crate::wire::Frame;
use crate::{NodeId, Observation, ObservationId, SigAlgo, Signature};

/// Core's local peer-table capacity (`consts::MAX_PEERS`).
pub const MAX_PEERS: usize = consts::MAX_PEERS;
/// Core's idle-eviction window (`consts::PEER_IDLE_MS`).
pub const PEER_IDLE_MS: u64 = consts::PEER_IDLE_MS;
/// Core's summary cadence (`consts::SUMMARY_INTERVAL_MS`).
pub const SUMMARY_INTERVAL_MS: u64 = consts::SUMMARY_INTERVAL_MS;
/// Core's per-peer send-queue and bounce-table capacity (`consts::SEND_QUEUE_CAP`).
pub const SEND_QUEUE_CAP: usize = consts::SEND_QUEUE_CAP;
/// Core's `Delivered` event-queue capacity (`consts::EVENT_QUEUE_CAP`).
pub const EVENT_QUEUE_CAP: usize = consts::EVENT_QUEUE_CAP;
/// Core's MTU-sweep lower bound (`consts::MIN_MTU`).
pub const MIN_MTU: usize = consts::MIN_MTU;
/// Bytes a `Frag` frame may add around its data (`frag::FRAG_OVERHEAD`).
pub const FRAG_OVERHEAD: usize = crate::frag::FRAG_OVERHEAD;
/// Most fragments one observation may be split into (`frag::MAX_FRAGMENTS`).
pub const MAX_FRAGMENTS: usize = crate::frag::MAX_FRAGMENTS;

fn encode(frame: &Frame, out: &mut [u8]) -> Option<usize> {
    frame.encode(out).ok()
}

/// Encodes a `Summary` frame into `out`. `None` if `bits` is longer than
/// `SUMMARY_BYTES` or `out` is too small.
pub fn encode_summary(from: NodeId, salt: u32, bits: &[u8], out: &mut [u8]) -> Option<usize> {
    let bits = heapless::Vec::from_slice(bits).ok()?;
    encode(&Frame::Summary { from, salt, bits }, out)
}

/// Encodes an `Obs` frame carrying `obs` into `out`. `None` if `out` is too
/// small.
pub fn encode_obs(from: NodeId, obs: &Observation, out: &mut [u8]) -> Option<usize> {
    let obs = obs.clone();
    encode(&Frame::Obs { from, obs }, out)
}

/// Encodes a `Frag` frame into `out`. `None` if `data` is longer than
/// `MAX_FRAME` or `out` is too small. `index`/`count` are not validated: an
/// adversarial harness sends inconsistent ones on purpose.
pub fn encode_frag(
    from: NodeId,
    id: ObservationId,
    index: u8,
    count: u8,
    data: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let data = heapless::Vec::from_slice(data).ok()?;
    encode(
        &Frame::Frag {
            from,
            id,
            index,
            count,
            data,
        },
        out,
    )
}

/// Encodes an `Ack` frame into `out`. `None` if `ids` is longer than
/// `ACK_IDS_CAP` or `out` is too small.
pub fn encode_ack(from: NodeId, ids: &[ObservationId], out: &mut [u8]) -> Option<usize> {
    let ids = heapless::Vec::from_slice(ids).ok()?;
    encode(&Frame::Ack { from, ids }, out)
}

/// Encodes `obs` alone (no frame envelope) into `out`: the byte stream a
/// sender cuts into `Frag` data. `None` if `out` is too small.
pub fn encode_observation(obs: &Observation, out: &mut [u8]) -> Option<usize> {
    postcard::to_slice(obs, out).ok().map(|s| s.len())
}

/// The sender of `bytes` if they decode as a complete, canonical frame — the
/// same decode the exchange runs — else `None` (the exchange counts such a
/// frame as `VersionMismatch` or `Malformed`).
pub fn decoded_sender(bytes: &[u8]) -> Option<NodeId> {
    Frame::decode(bytes).ok().map(|f| f.sender())
}

/// The observation in `bytes` if they decode as an `Obs` frame.
pub fn obs_of(bytes: &[u8]) -> Option<Observation> {
    match Frame::decode(bytes).ok()? {
        Frame::Obs { obs, .. } => Some(obs),
        _ => None,
    }
}

/// `(id, index, count)` of `bytes` if they decode as a `Frag` frame.
pub fn frag_of(bytes: &[u8]) -> Option<(ObservationId, u8, u8)> {
    match Frame::decode(bytes).ok()? {
        Frame::Frag {
            id, index, count, ..
        } => Some((id, index, count)),
        _ => None,
    }
}

/// Calls `f` on each id of `bytes` if they decode as an `Ack` frame, and
/// returns whether they did.
pub fn ack_of(bytes: &[u8], f: &mut dyn FnMut(&ObservationId)) -> bool {
    match Frame::decode(bytes) {
        Ok(Frame::Ack { ids, .. }) => {
            ids.iter().for_each(f);
            true
        }
        _ => false,
    }
}

/// Whether `bytes` decode as a `Summary` frame.
pub fn is_summary(bytes: &[u8]) -> bool {
    matches!(Frame::decode(bytes), Ok(Frame::Summary { .. }))
}

/// Builds a [`Signature`] (a `#[non_exhaustive]` struct only core can
/// construct). `None` if `bytes` is longer than `MAX_SIG`. The shape rules
/// (version `0`, a non-64-byte Ed25519) are not checked: a harness builds
/// invalid signatures on purpose to see them rejected on decode.
pub fn signature(version: u8, algo: SigAlgo, bytes: &[u8]) -> Option<Signature> {
    Some(Signature {
        version,
        algo,
        bytes: heapless::Vec::from_slice(bytes).ok()?,
    })
}
