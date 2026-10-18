//! In-memory observation store backed by a `BTreeMap` keyed by content-derived
//! `ObservationId`.
//!
//! `MemStore` implements [`Store`]. It is unbounded, so it
//! never returns `StoreError::Full`; allocation is therefore required and the
//! module is gated behind the `alloc` feature.

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "alloc")]
use crate::observation::{Observation, ObservationId, Tick};
#[cfg(feature = "alloc")]
use crate::store::{current_age, Store, StoreError, TrimPolicy};
#[cfg(feature = "alloc")]
use alloc::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "alloc")]
use alloc::vec::Vec;

#[cfg(feature = "alloc")]
struct Held {
    obs: Observation,
    stored_at: Tick,
}

/// An unbounded [`Store`] backed by a `BTreeMap` keyed by content-derived
/// [`ObservationId`].
///
/// Because a `BTreeMap` has no capacity limit, `MemStore` never returns
/// `StoreError::Full`. It is the only store type that requires the `alloc`
/// feature; the [`Store`] trait, [`TrimPolicy`], and [`StoreError`] are defined
/// in [`store`](crate::store) and compile without allocation.
#[cfg(feature = "alloc")]
pub struct MemStore {
    map: BTreeMap<ObservationId, Held>,
}

#[cfg(feature = "alloc")]
impl MemStore {
    /// Create an empty store.
    pub fn new() -> Self {
        MemStore {
            map: BTreeMap::new(),
        }
    }
}

#[cfg(feature = "alloc")]
impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "alloc")]
impl MemStore {
    /// Order two stored ids by their current age at `now` (youngest first).
    fn age_cmp(&self, a: &ObservationId, b: &ObservationId, now: Tick) -> core::cmp::Ordering {
        current_age(&self.map[a].obs, self.map[a].stored_at, now).cmp(&current_age(
            &self.map[b].obs,
            self.map[b].stored_at,
            now,
        ))
    }
}

#[cfg(feature = "alloc")]
impl Store for MemStore {
    fn insert(&mut self, obs: Observation, now: Tick) -> Result<bool, StoreError> {
        let id = obs.id();
        if self.map.contains_key(&id) {
            // Dedupe: keep the first-stored entry and its age lineage.
            return Ok(false);
        }
        self.map.insert(
            id,
            Held {
                obs,
                stored_at: now,
            },
        );
        Ok(true)
    }

    fn contains(&self, id: &ObservationId) -> bool {
        self.map.contains_key(id)
    }

    fn get(&self, id: &ObservationId, now: Tick) -> Option<Observation> {
        let held = self.map.get(id)?;
        let elapsed = now.saturating_sub(held.stored_at);
        Some(held.obs.aged(elapsed))
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn for_each_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        for id in self.map.keys() {
            f(id);
        }
    }

    fn for_each_broadcast_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        for (id, held) in self.map.iter() {
            if held.obs.to.is_none() {
                f(id);
            }
        }
    }

    fn for_each_topic(&self, prefix: &[u8], now: Tick, f: &mut dyn FnMut(&Observation)) {
        for held in self.map.values() {
            if held.obs.topic.starts_with(prefix) {
                let elapsed = now.saturating_sub(held.stored_at);
                f(&held.obs.aged(elapsed));
            }
        }
    }

    fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize {
        let mut removed: BTreeSet<ObservationId> = BTreeSet::new();

        // (a) keep_per_topic: per exact topic, drop everything but the `N`
        // youngest (lowest current age).
        if let Some(keep) = policy.keep_per_topic {
            let mut groups: BTreeMap<&[u8], Vec<ObservationId>> = BTreeMap::new();
            for (id, held) in self.map.iter() {
                if !removed.contains(id) {
                    groups
                        .entry(held.obs.topic.as_slice())
                        .or_default()
                        .push(*id);
                }
            }
            for member_ids in groups.values() {
                if member_ids.len() <= keep {
                    continue;
                }
                let mut ids = member_ids.clone();
                ids.sort_by(|&a, &b| self.age_cmp(&a, &b, now));
                for id in ids.into_iter().skip(keep) {
                    removed.insert(id);
                }
            }
        }

        // (b) max_age_ms: drop anything whose current age exceeds the cutoff.
        if let Some(max_age) = policy.max_age_ms {
            for (id, held) in self.map.iter() {
                if !removed.contains(id) && current_age(&held.obs, held.stored_at, now) > max_age {
                    removed.insert(*id);
                }
            }
        }

        // (c) max_count: drop the oldest (highest current age) until the number
        // of remaining (untrimmed) observations is at most `max_count`.
        if let Some(max_count) = policy.max_count {
            let mut remaining: Vec<ObservationId> = self
                .map
                .keys()
                .copied()
                .filter(|id| !removed.contains(id))
                .collect();
            if remaining.len() > max_count {
                remaining.sort_by(|&a, &b| self.age_cmp(&a, &b, now));
                let over = remaining.len() - max_count;
                for id in remaining.into_iter().rev().take(over) {
                    removed.insert(id);
                }
            }
        }

        self.map.retain(|id, _| !removed.contains(id));
        removed.len()
    }
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use super::MemStore;
    use crate::observation::{Observation, Payload, Topic};
    use crate::store::{Store, TrimPolicy};

    fn obs(observer: [u8; 16], seq: u64, age: u64, at: Option<u64>, topic: &[u8]) -> Observation {
        Observation {
            observer,
            seq,
            age_ms: age,
            observed_at: at,
            topic: Topic::from_slice(topic).unwrap(),
            payload: Payload::from_slice(b"").unwrap(),
            to: None,
            lifetime_ms: None,
        }
    }

    #[test]
    fn insert_then_contains_and_len() {
        let mut s = MemStore::new();
        let o = obs([1; 16], 1, 0, None, b"door/1");
        let id = o.id();
        assert!(s.insert(o, 1000).unwrap());
        assert!(s.contains(&id));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn duplicate_id_is_not_reinserted() {
        let mut s = MemStore::new();
        let o1 = obs([1; 16], 1, 100, None, b"door/1");
        let id = o1.id();
        assert!(s.insert(o1, 1000).unwrap());
        // Same (observer, seq, topic, payload), different age -> deduped.
        let o2 = obs([1; 16], 1, 999, None, b"door/1");
        assert_eq!(id, o2.id());
        assert!(!s.insert(o2, 2000).unwrap());
        assert_eq!(s.len(), 1);
        // get() still returns the first-stored age lineage.
        let g = s.get(&id, 1500).unwrap();
        assert_eq!(g.age_ms, 600);
    }

    #[test]
    fn get_adjusts_age_by_elapsed() {
        let mut s = MemStore::new();
        let o = obs([1; 16], 1, 100, None, b"door/1");
        let id = o.id();
        s.insert(o, 1000).unwrap();
        let g = s.get(&id, 1500).unwrap();
        assert_eq!(g.age_ms, 600);
    }

    #[test]
    fn get_with_backwards_tick_saturates() {
        let mut s = MemStore::new();
        let o = obs([1; 16], 1, 100, None, b"door/1");
        let id = o.id();
        s.insert(o, 1000).unwrap();
        let g = s.get(&id, 500).unwrap();
        assert_eq!(g.age_ms, 100);
    }

    #[test]
    fn for_each_topic_prefix_matches() {
        let mut s = MemStore::new();
        s.insert(obs([1; 16], 1, 0, None, b"door/1"), 0).unwrap();
        s.insert(obs([1; 16], 2, 0, None, b"door/2"), 0).unwrap();
        s.insert(obs([1; 16], 3, 0, None, b"wall/1"), 0).unwrap();
        let mut count = 0;
        s.for_each_topic(b"door/", 0, &mut |_o| {
            count += 1;
        });
        assert_eq!(count, 2);
    }

    #[test]
    fn for_each_topic_exact_empty_prefix_is_all() {
        let mut s = MemStore::new();
        s.insert(obs([1; 16], 1, 0, None, b"door/1"), 0).unwrap();
        s.insert(obs([1; 16], 2, 0, None, b"door/2"), 0).unwrap();
        s.insert(obs([1; 16], 3, 0, None, b"wall/1"), 0).unwrap();
        let mut count = 0;
        s.for_each_topic(b"", 0, &mut |_o| {
            count += 1;
        });
        assert_eq!(count, s.len());
    }

    #[test]
    fn trim_keep_per_topic_keeps_youngest() {
        let mut s = MemStore::new();
        for (i, age) in [10u64, 20, 30, 40, 50].iter().enumerate() {
            s.insert(obs([1; 16], i as u64, *age, None, b"t"), 0)
                .unwrap();
        }
        let policy = TrimPolicy {
            keep_per_topic: Some(2),
            max_age_ms: None,
            max_count: None,
        };
        assert_eq!(s.trim(&policy, 0), 3);
        let mut ages = std::collections::BTreeSet::new();
        s.for_each_topic(b"t", 0, &mut |o| {
            ages.insert(o.age_ms);
        });
        assert_eq!(ages, std::collections::BTreeSet::from_iter([10u64, 20]));
    }

    #[test]
    fn trim_max_age_drops_old() {
        let mut s = MemStore::new();
        s.insert(obs([1; 16], 1, 10, None, b"t"), 0).unwrap();
        s.insert(obs([1; 16], 2, 10_000, None, b"t"), 0).unwrap();
        let policy = TrimPolicy {
            keep_per_topic: None,
            max_age_ms: Some(1000),
            max_count: None,
        };
        assert_eq!(s.trim(&policy, 0), 1);
        let mut found = false;
        s.for_each_topic(b"t", 0, &mut |o| {
            assert_eq!(o.age_ms, 10);
            found = true;
        });
        assert!(found);
    }

    #[test]
    fn trim_max_count_drops_oldest() {
        let mut s = MemStore::new();
        for i in 0..10u64 {
            s.insert(obs([1; 16], i, (i + 1) * 10, None, b"t"), 0)
                .unwrap();
        }
        let policy = TrimPolicy {
            keep_per_topic: None,
            max_age_ms: None,
            max_count: Some(4),
        };
        assert_eq!(s.trim(&policy, 0), 6);
        assert_eq!(s.len(), 4);
        let mut ages = std::collections::BTreeSet::new();
        s.for_each_topic(b"t", 0, &mut |o| {
            ages.insert(o.age_ms);
        });
        assert_eq!(
            ages,
            std::collections::BTreeSet::from_iter([10u64, 20, 30, 40])
        );
    }

    #[test]
    fn trimmed_obs_can_be_reinserted() {
        let mut s = MemStore::new();
        let o = obs([1; 16], 1, 10, None, b"t");
        let id = o.id();
        assert!(s.insert(o, 0).unwrap());
        assert_eq!(s.len(), 1);
        let policy = TrimPolicy {
            keep_per_topic: None,
            max_age_ms: Some(1),
            max_count: None,
        };
        assert_eq!(s.trim(&policy, 0), 1);
        assert_eq!(s.len(), 0);
        assert!(!s.contains(&id));
        // Trimmed then re-received is accepted.
        let o2 = obs([1; 16], 1, 20, None, b"t");
        assert_eq!(id, o2.id());
        assert!(s.insert(o2, 0).unwrap());
        assert!(s.contains(&id));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn trim_keep_per_topic_uses_elapsed_age() {
        let mut s = MemStore::new();
        // All stored with age_ms=0, but at different ticks: seq 1 is oldest
        // (stored first), seq 3 youngest (stored last).
        s.insert(obs([1; 16], 1, 0, None, b"t"), 1000).unwrap();
        s.insert(obs([1; 16], 2, 0, None, b"t"), 2000).unwrap();
        s.insert(obs([1; 16], 3, 0, None, b"t"), 3000).unwrap();
        let policy = TrimPolicy {
            keep_per_topic: Some(2),
            max_age_ms: None,
            max_count: None,
        };
        // At now=4000 the current ages are 3000/2000/1000 for seq 1/2/3, so
        // the two youngest (seq 2 and 3) survive and seq 1 (oldest) drops.
        assert_eq!(s.trim(&policy, 4000), 1);
        let mut seqs = std::collections::BTreeSet::new();
        s.for_each_topic(b"t", 4000, &mut |o| {
            seqs.insert(o.seq);
        });
        assert_eq!(seqs, std::collections::BTreeSet::from_iter([2u64, 3]));
    }

    #[test]
    fn for_each_broadcast_id_skips_addressed() {
        let mut s = MemStore::new();
        let broadcast = obs([1; 16], 1, 0, None, b"door/1");
        let broadcast_id = broadcast.id();
        let mut addressed = obs([1; 16], 2, 0, None, b"door/1");
        addressed.to = Some([2; 16]);
        s.insert(broadcast, 0).unwrap();
        s.insert(addressed, 0).unwrap();

        let mut ids = std::collections::BTreeSet::new();
        s.for_each_broadcast_id(&mut |id| {
            ids.insert(*id);
        });
        assert_eq!(ids, std::collections::BTreeSet::from_iter([broadcast_id]));
    }

    #[test]
    fn trim_max_count_drops_oldest_addressed() {
        let mut s = MemStore::new();
        // 10 addressed observations with increasing current age.
        for i in 0..10u64 {
            let mut addressed = obs([1; 16], i, (i + 1) * 10, None, b"t");
            addressed.to = Some([2; 16]);
            s.insert(addressed, 0).unwrap();
        }
        // One broadcast, youngest of all, so the count trim keeps it.
        let broadcast = obs([1; 16], 100, 0, None, b"t");
        let broadcast_id = broadcast.id();
        s.insert(broadcast, 0).unwrap();

        let policy = TrimPolicy {
            keep_per_topic: None,
            max_age_ms: None,
            max_count: Some(4),
        };
        assert_eq!(s.trim(&policy, 0), 7);
        assert_eq!(s.len(), 4);
        // The broadcast survives even though it shares topic/observer with the
        // dropped addressed observations.
        assert!(s.contains(&broadcast_id));
        // The three youngest addressed (seq 0..2, ages 10/20/30) survive along
        // with the broadcast (seq 100); seq 3..9 are dropped.
        let mut seqs = std::collections::BTreeSet::new();
        s.for_each_topic(b"t", 0, &mut |o| {
            seqs.insert(o.seq);
        });
        assert_eq!(
            seqs,
            std::collections::BTreeSet::from_iter([0u64, 1, 2, 100])
        );
    }
}
