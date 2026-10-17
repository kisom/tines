//! Fragmentation and reassembly of `Observation`s over a byte-oriented link.

use crate::consts;
use crate::{Frame, Observation, NodeId, ObservationId, Tick, WireError};
use heapless::Vec;

/// Header allowance per fragment (bytes), pinned.
pub const FRAG_OVERHEAD: usize = 64;

/// Encodes `obs` as one Obs frame if it fits in `mtu`, else as N Frag frames.
/// Returns the number of frames; writes each via `emit(frame_bytes)`.
pub fn fragment(
    from: NodeId,
    obs: &Observation,
    mtu: usize,
    emit: &mut dyn FnMut(&[u8]),
) -> Result<usize, WireError> {
    // 1. Try to send the whole Observation as a single Obs frame.
    let mut buf = [0u8; consts::MAX_FRAME];
    let frame = Frame::Obs {
        from,
        obs: obs.clone(),
    };
    if let Ok(n) = frame.encode(&mut buf) {
        if n <= mtu {
            emit(&buf[..n]);
            return Ok(1);
        }
    }

    // 2. Encode the Observation alone and split it into Frag frames.
    let mut obs_buf = [0u8; consts::MAX_FRAME * 2];
    let obs_bytes = postcard::to_slice(obs, &mut obs_buf).map_err(|_| WireError::TooLarge)?;

    let chunk = mtu.saturating_sub(FRAG_OVERHEAD);
    if chunk == 0 {
        return Err(WireError::TooLarge);
    }
    #[allow(clippy::manual_div_ceil)]
    let count = (obs_bytes.len() + chunk - 1) / chunk;
    if count > 64 {
        return Err(WireError::TooLarge);
    }

    for i in 0..count {
        let start = i * chunk;
        let end = core::cmp::min(start + chunk, obs_bytes.len());
        let data = &obs_bytes[start..end];
        let frag_data = heapless::Vec::from_slice(data).map_err(|_| WireError::TooLarge)?;
        let frame = Frame::Frag {
            from,
            id: obs.id(),
            index: i as u8,
            count: count as u8,
            data: frag_data,
        };
        let mut fb = [0u8; consts::MAX_FRAME];
        let n = frame.encode(&mut fb).map_err(|_| WireError::TooLarge)?;
        emit(&fb[..n]);
    }
    Ok(count)
}

/// A single-slot reassembler for inbound Frag streams.
pub struct Reassembler {
    id: Option<ObservationId>,
    count: u8,
    received: u64,
    started_at: Tick,
    buf: Vec<u8, { consts::MAX_FRAME * 2 }>,
    offs: [u16; 64],
    lens: [u16; 64],
}

impl Reassembler {
    pub fn new() -> Self {
        Reassembler {
            id: None,
            count: 0,
            received: 0,
            started_at: 0,
            buf: heapless::Vec::new(),
            offs: [u16::MAX; 64],
            lens: [0u16; 64],
        }
    }

    pub fn feed(&mut self, id: ObservationId, index: u8, count: u8, data: &[u8], now: Tick) -> Option<Observation> {
        // 1. Drop malformed indices: count must be in 1..=64 and index < count.
        //    `index >= 64` is guarded too so the `1 << index` bitmap never
        //    shifts out of bounds on adversarial wire input.
        if count == 0 || count > 64 || (index as usize) >= 64 || index >= count {
            return None;
        }

        // 2. Reset the slot if it is empty, belongs to a different id/count, or
        //    has exceeded the reassembly timeout.
        let need_reset = self.id.is_none()
            || self.id != Some(id)
            || self.count != count
            || now.saturating_sub(self.started_at) > consts::REASSEMBLY_TIMEOUT_MS;
        if need_reset {
            self.id = Some(id);
            self.count = count;
            self.received = 0;
            self.started_at = now;
            self.buf.clear();
            self.offs = [u16::MAX; 64];
            self.lens = [0u16; 64];
        }

        // 3. Append the fragment data to the flat buffer in arrival order and
        //    record where this index's bytes live.
        let start = self.buf.len();
        if self.buf.len().saturating_add(data.len()) > self.buf.capacity() {
            return None;
        }
        self.buf.extend_from_slice(data).expect("buf capacity check above");
        self.offs[index as usize] = start as u16;
        self.lens[index as usize] = data.len() as u16;
        self.received |= 1u64 << index;

        // 4. Not all fragments have arrived yet.
        let complete: u64 = if count >= 64 { u64::MAX } else { (1u64 << count) - 1 };
        if self.received != complete {
            return None;
        }

        // 5. Reassemble in index order and decode.
        let mut recon = [0u8; consts::MAX_FRAME * 2];
        let mut total = 0usize;
        for i in 0..count {
            let o = self.offs[i as usize] as usize;
            let l = self.lens[i as usize] as usize;
            let frag = &self.buf[o..o + l];
            recon[total..total + l].copy_from_slice(frag);
            total += l;
        }

        match postcard::from_bytes::<Observation>(&recon[..total]) {
            Ok(obs) if obs.id() == id => {
                self.reset_slot();
                Some(obs)
            }
            _ => {
                self.reset_slot();
                None
            }
        }
    }
}

impl Default for Reassembler {
    fn default() -> Self {
        Self::new()
    }
}

impl Reassembler {
    /// Clears a completed/aborted slot so it is ready for a fresh stream.
    fn reset_slot(&mut self) {
        self.id = None;
        self.count = 0;
        self.received = 0;
        self.started_at = 0;
        self.buf.clear();
        self.offs = [u16::MAX; 64];
        self.lens = [0u16; 64];
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Frame, WireError};
    use crate::{Payload, Topic};
    use std::vec::Vec;

    fn obs_with_payload(observer: [u8; 16], payload_len: usize) -> Observation {
        Observation {
            observer,
            seq: 1,
            age_ms: 0,
            observed_at: None,
            topic: Topic::from_slice(b"door/3").unwrap(),
            payload: Payload::from_slice(&std::vec![0u8; payload_len]).unwrap(),
        }
    }

    #[test]
    fn small_obs_is_one_obs_frame() {
        let obs = obs_with_payload([1; 16], 10);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let n = fragment([1; 16], &obs, 1500, &mut |b| frames.push(b.to_vec()))
            .expect("fragment small obs");
        assert_eq!(n, 1);
        assert_eq!(frames.len(), 1);
        match Frame::decode(&frames[0]).unwrap() {
            Frame::Obs { .. } => {}
            _ => panic!("expected a single Obs frame"),
        }
    }

    #[test]
    fn large_obs_is_fragmented_and_reassembles() {
        let obs = obs_with_payload([2; 16], 1024);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let n = fragment([2; 16], &obs, 200, &mut |b| frames.push(b.to_vec()))
            .expect("fragment large obs");
        assert!(n >= 6, "expected >= 6 frames, got {n}");
        for f in &frames {
            match Frame::decode(f).unwrap() {
                Frame::Frag { count, .. } => {
                    assert_eq!(count, n as u8, "all frags must share the same count");
                }
                other => panic!("expected only Frag frames, got {other:?}"),
            }
        }
        let mut r = Reassembler::new();
        let mut last: Option<Observation> = None;
        for f in frames.iter() {
            if let Frame::Frag { id, index, count, data, .. } = Frame::decode(f).unwrap() {
                last = r.feed(id, index, count, &data, 0);
            }
        }
        let reassembled = last.expect("reassembled obs");
        assert_eq!(reassembled, obs, "reassembled observation must equal original");
    }

    #[test]
    fn out_of_order_reassembles() {
        let obs = obs_with_payload([3; 16], 1024);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let _n = fragment([3; 16], &obs, 200, &mut |b| frames.push(b.to_vec()))
            .expect("fragment large obs");
        let mut r = Reassembler::new();
        let mut last: Option<Observation> = None;
        for f in frames.iter().rev() {
            if let Frame::Frag { id, index, count, data, .. } = Frame::decode(f).unwrap() {
                last = r.feed(id, index, count, &data, 0);
            }
        }
        let reassembled = last.expect("reassembled obs");
        assert_eq!(reassembled, obs);
    }

    #[test]
    fn duplicate_fragment_is_harmless() {
        let obs = obs_with_payload([4; 16], 1024);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let n = fragment([4; 16], &obs, 200, &mut |b| frames.push(b.to_vec()))
            .expect("fragment large obs");
        let frags: Vec<(ObservationId, u8, u8, Vec<u8>)> = frames
            .iter()
            .map(|f| {
                if let Frame::Frag { id, index, count, data, .. } = Frame::decode(f).unwrap() {
                    (id, index, count, data.to_vec())
                } else {
                    panic!("expected Frag frames");
                }
            })
            .collect();
        let mut r = Reassembler::new();
        let mut last: Option<Observation> = None;
        let (id0, idx0, cnt0, data0) = &frags[0];
        // feed index 0 twice
        r.feed(*id0, *idx0, *cnt0, data0, 0);
        r.feed(*id0, *idx0, *cnt0, data0, 0);
        for i in 1..n as usize {
            let (id, index, count, data) = &frags[i];
            last = r.feed(*id, *index, *count, data, 0);
        }
        let reassembled = last.expect("reassembled obs");
        assert_eq!(reassembled, obs);
    }

    #[test]
    fn wrong_count_is_dropped() {
        let obs = obs_with_payload([5; 16], 1024);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let n = fragment([5; 16], &obs, 200, &mut |b| frames.push(b.to_vec()))
            .expect("fragment large obs");
        let frags: Vec<(ObservationId, u8, u8, Vec<u8>)> = frames
            .iter()
            .map(|f| {
                if let Frame::Frag { id, index, count, data, .. } = Frame::decode(f).unwrap() {
                    (id, index, count, data.to_vec())
                } else {
                    panic!("expected Frag frames");
                }
            })
            .collect();
        let mut r = Reassembler::new();
        let mut got_some = false;
        for i in 0..n as usize {
            let (id, index, _count, data) = &frags[i];
            // The changing count keeps resetting the slot, so fragments never
            // accumulate and the stream can never complete (no panic).
            let a = r.feed(*id, *index, 2, data, 0);
            let b = r.feed(*id, *index, 3, data, 0);
            if a.is_some() || b.is_some() {
                got_some = true;
            }
        }
        assert!(!got_some, "a wrong-count stream must never complete");
    }

    #[test]
    fn index_ge_count_is_ignored() {
        let mut r = Reassembler::new();
        let id = [6; 16];
        // index 5 with count 3
        assert!(r.feed(id, 5, 3, &[1, 2, 3], 0).is_none());
        // index == count is also invalid
        assert!(r.feed(id, 3, 3, &[1, 2, 3], 0).is_none());
    }

    #[test]
    fn timeout_discards_partial() {
        let obs = obs_with_payload([7; 16], 200);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let n = fragment([7; 16], &obs, 200, &mut |b| frames.push(b.to_vec()))
            .expect("fragment large obs");
        let frags: Vec<(ObservationId, u8, u8, Vec<u8>)> = frames
            .iter()
            .map(|f| {
                if let Frame::Frag { id, index, count, data, .. } = Frame::decode(f).unwrap() {
                    (id, index, count, data.to_vec())
                } else {
                    panic!("expected Frag frames");
                }
            })
            .collect();
        assert_eq!(frags.len(), n as usize);
        let mut r = Reassembler::new();
        let (id0, idx0, cnt0, data0) = &frags[0];
        // feed index 0 at now=0 (incomplete)
        r.feed(*id0, *idx0, *cnt0, data0, 0);
        // feed index 1 at now=timeout+1 -> slot restarts with only index 1, returns None
        let (id1, idx1, cnt1, data1) = &frags[1];
        assert!(r.feed(*id1, *idx1, *cnt1, data1, consts::REASSEMBLY_TIMEOUT_MS + 1).is_none());
        // re-feed index 0 late: proves index 0 from now=0 was discarded, but the
        // restarted slot now holds index 1, so the stream completes to the original
        let (idc, idxc, cntc, datac) = &frags[0];
        let late = r.feed(*idc, *idxc, *cntc, datac, consts::REASSEMBLY_TIMEOUT_MS + 2);
        assert_eq!(late, Some(obs.clone()), "late re-feed must complete the restarted stream");
        // sanity: a fresh feed of the whole set from now=0 also completes
        let mut r2 = Reassembler::new();
        let mut complete: Option<Observation> = None;
        for (id, index, count, data) in &frags {
            complete = r2.feed(*id, *index, *count, data, 0);
        }
        assert_eq!(complete, Some(obs));
    }

    #[test]
    fn reassembled_bytes_must_match_id() {
        let obs = obs_with_payload([8; 16], 1024);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let n = fragment([8; 16], &obs, 200, &mut |b| frames.push(b.to_vec()))
            .expect("fragment large obs");
        let mut frags: Vec<(ObservationId, u8, u8, Vec<u8>)> = frames
            .iter()
            .map(|f| {
                if let Frame::Frag { id, index, count, data, .. } = Frame::decode(f).unwrap() {
                    (id, index, count, data.to_vec())
                } else {
                    panic!("expected Frag frames");
                }
            })
            .collect();
        // corrupt one byte in the payload region of fragment 0
        frags[0].3[0] ^= 0xFF;
        let mut r = Reassembler::new();
        let mut last: Option<Observation> = None;
        for i in 0..n as usize {
            let (id, index, count, data) = &frags[i];
            last = r.feed(*id, *index, *count, data, 0);
        }
        assert!(last.is_none(), "corrupted reassembly must not yield an obs");
    }

    #[test]
    fn fragment_too_large_returns_error() {
        let obs = obs_with_payload([9; 16], consts::MAX_PAYLOAD);
        // mtu smaller than header -> chunk == 0 -> TooLarge
        let got = fragment([9; 16], &obs, FRAG_OVERHEAD, &mut |_| {}).is_err();
        assert!(got, "chunk == 0 must return an error");
    }

    #[test]
    fn decode_garbage_is_not_a_frame() {
        // smoke check that wire decode of the module's dependency behaves
        assert!(Frame::decode(&[0xFFu8, 0xFF, 0xFF]).is_err());
        assert!(WireError::Malformed == WireError::Malformed);
    }
}
