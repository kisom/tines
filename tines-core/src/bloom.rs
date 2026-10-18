//! Salted Bloom filter used as the per-key content summary.
//!
//! `Bloom` is a fixed-size, no-heap bit array with `BLOOM_K` hash-derived
//! bit positions per inserted `ObservationId`. The `salt` value mixes into the
//! hash so that the same id under different salts lands on different bit
//! positions, keeping false positives between independent filters low. The
//! hashing scheme is pinned (see [`positions`]) so that `tines-core` and the
//! simulation agree bit-for-bit across builds.
//!
//! This type is `core`-only: it owns a fixed `[u8; SUMMARY_BYTES]` array and
//! never touches the heap, so it compiles in `no_std` without `alloc`.

use crate::consts;
use crate::observation::ObservationId;

/// A fixed-size salted Bloom filter keyed by `ObservationId`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bloom {
    pub salt: u32,
    bits: [u8; consts::SUMMARY_BYTES],
}

impl Bloom {
    /// Returns a new Bloom filter with all bits cleared and the given `salt`.
    pub fn new(salt: u32) -> Self {
        Bloom {
            salt,
            bits: [0u8; consts::SUMMARY_BYTES],
        }
    }

    /// Sets the `BLOOM_K` bits for `id`.
    pub fn insert(&mut self, id: &ObservationId) {
        for idx in positions(self.salt, id) {
            self.bits[idx / 8] |= 1 << (idx % 8);
        }
    }

    /// Returns `true` if `id` may have been inserted (all of its bits are set).
    pub fn might_contain(&self, id: &ObservationId) -> bool {
        positions(self.salt, id)
            .iter()
            .all(|idx| self.bits[idx / 8] & (1 << (idx % 8)) != 0)
    }

    /// Rebuilds a `Bloom` from its raw parts.
    ///
    /// Returns `None` unless `bits` is exactly `SUMMARY_BYTES` long, so a caller
    /// deserializing from an untrusted source cannot construct a filter from a
    /// truncated or oversized backing array.
    pub fn from_parts(salt: u32, bits: &[u8]) -> Option<Self> {
        let bytes: [u8; consts::SUMMARY_BYTES] = bits.try_into().ok()?;
        Some(Bloom { salt, bits: bytes })
    }

    /// Returns the raw backing bit array as a slice.
    pub fn bits(&self) -> &[u8] {
        &self.bits
    }
}

/// Computes the `BLOOM_K` bit indices for `id` under `salt` using the pinned
/// double-hash scheme shared with the simulation.
///
/// Both 8-byte halves of the 16-byte id are mixed with the salt through
/// independent avalanche (fmix64) functions, so all `BLOOM_K` slots are derived
/// from the full id rather than just the low 12 bits of the prefix. For each
/// `k` in `0..BLOOM_K`:
/// ```text
/// a = u64::from_le_bytes(id[0..8]) ^ (salt as u64).wrapping_mul(0x9E3779B97F4A7C15)
/// b = u64::from_le_bytes(id[8..16]) ^ (salt as u64).wrapping_mul(0xC2B2AE3D27D4EB4F)
/// h1 = fmix64(a)
/// h2 = fmix64(b) | 1
/// capacity = SUMMARY_BYTES * 8
/// bit_index = (h1 + k * h2) % capacity
/// ```
fn positions(salt: u32, id: &ObservationId) -> [usize; consts::BLOOM_K] {
    fn fmix64(mut x: u64) -> u64 {
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51afd7ed558ccd);
        x ^= x >> 33;
        x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
        x ^= x >> 33;
        x
    }
    let mut lo = [0u8; 8];
    let mut hi = [0u8; 8];
    lo.copy_from_slice(&id[0..8]);
    hi.copy_from_slice(&id[8..16]);
    let a = u64::from_le_bytes(lo);
    let b = u64::from_le_bytes(hi);
    let s = salt as u64;
    let h1 = fmix64(a ^ s.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let h2 = fmix64(b ^ s.wrapping_mul(0xC2B2_AE3D_27D4_EB4F)) | 1;
    let capacity = (consts::SUMMARY_BYTES * 8) as u64;
    let mut out = [0usize; consts::BLOOM_K];
    for (k, slot) in out.iter_mut().enumerate() {
        *slot = ((h1.wrapping_add((k as u64).wrapping_mul(h2))) % capacity) as usize;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Small deterministic PRNG (xorshift64) so tests are hermetic and need no
    /// `rand` dependency (tines-core has none).
    struct Rng(u64);

    impl Rng {
        const fn new(seed: u64) -> Self {
            Rng(seed | 1)
        }

        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        /// A distinct ObservationId derived from the PRNG stream.
        fn id(&mut self) -> ObservationId {
            let hi = self.next();
            let lo = self.next();
            let mut id = [0u8; 16];
            id[0..8].copy_from_slice(&hi.to_le_bytes());
            id[8..16].copy_from_slice(&lo.to_le_bytes());
            id
        }
    }

    #[test]
    fn inserted_is_contained() {
        let mut bloom = Bloom::new(7);
        let mut rng = Rng::new(1);
        let ids: [ObservationId; 100] = std::array::from_fn(|_| rng.id());
        for id in ids {
            bloom.insert(&id);
        }
        for id in ids {
            assert!(
                bloom.might_contain(&id),
                "inserted id must be reported present"
            );
        }
    }

    #[test]
    fn empty_contains_nothing() {
        let bloom = Bloom::new(0);
        let mut rng = Rng::new(2);
        let ids: [ObservationId; 100] = std::array::from_fn(|_| rng.id());
        for id in ids {
            assert!(
                !bloom.might_contain(&id),
                "empty filter must report nothing"
            );
        }
    }

    #[test]
    fn false_positive_rate_bounded_at_500_items() {
        let mut bloom = Bloom::new(3);
        let mut rng = Rng::new(4);

        let inserted: [ObservationId; 500] = std::array::from_fn(|_| rng.id());
        for id in inserted {
            bloom.insert(&id);
        }

        // Probe 10_000 *other* ids (continuing the same full-period stream keeps
        // them distinct from the inserted set).
        let mut fp = 0usize;
        for _ in 0..10_000 {
            if bloom.might_contain(&rng.id()) {
                fp += 1;
            }
        }
        let rate = fp as f64 / 10_000.0;
        assert!(rate < 0.05, "false positive rate too high: {rate}");
    }

    #[test]
    fn different_salt_changes_false_positives() {
        let mut rng = Rng::new(5);

        let inserted: [ObservationId; 500] = std::array::from_fn(|_| rng.id());
        let probes: [ObservationId; 10_000] = std::array::from_fn(|_| rng.id());

        let mut a = Bloom::new(1);
        let mut b = Bloom::new(2);
        for id in &inserted {
            a.insert(id);
            b.insert(id);
        }

        let mut under_a = 0usize;
        let mut under_both = 0usize;
        for probe in &probes {
            let fa = a.might_contain(probe);
            let fb = b.might_contain(probe);
            if fa {
                under_a += 1;
                if fb {
                    under_both += 1;
                }
            }
        }

        assert!(under_a > 0, "expected some false positives under salt 1");
        assert!(
            (under_both as f64) < (0.20 * under_a as f64),
            "salted hashes must diverge; both={under_both} under_a={under_a}"
        );
    }

    #[test]
    fn from_parts_rejects_wrong_length() {
        assert!(Bloom::from_parts(1, &[0u8; 10]).is_none());
        assert!(Bloom::from_parts(1, &[]).is_none());

        // And accepts a correctly-sized backing array.
        let raw = [0xABu8; consts::SUMMARY_BYTES];
        let bloom = Bloom::from_parts(9, &raw).expect("correct-length bits must build");
        assert_eq!(bloom.salt, 9);
        assert_eq!(bloom.bits(), &raw[..]);
    }
}
