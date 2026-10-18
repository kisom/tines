//! Append-only observation log backing [`LogStore`].
//!
//! The log is a single file of self-describing, length-prefixed records:
//!
//! ```text
//! [u32 little-endian length][postcard bytes of the record]
//! ```
//!
//! Record kinds:
//!
//! * `Record::Obs` — a stored [`Observation`] (deduped on `id` at load).
//! * `Record::Trim` — the id of an observation removed by `trim`; the last
//!   event for an id wins, so a re-inserted (new `Obs` record) id survives.
//! * `Record::Meta` — the current `next_seq`, the last `now` seen (the clock
//!   marker), and the full set of released ids. The last `Meta` on disk wins,
//!   so it always reflects the latest flushed state.
//!
//! On load the index is rebuilt by replaying the log, then every age is
//! rebased to load time: the time elapsed since the last persisted clock marker
//! is folded into `age_ms` and `stored_at` is set to the load `now`, so ages
//! continue across a reboot instead of freezing at their pre-shutdown value.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tines_core::{Observation, ObservationId, Store, StoreError, Tick, TrimPolicy};

/// Current (age-adjusted) age of a stored observation at tick `now`.
fn current_age(obs: &Observation, stored_at: Tick, now: Tick) -> u64 {
    obs.age_ms.saturating_add(now.saturating_sub(stored_at))
}

#[derive(Serialize, Deserialize)]
enum Record {
    Obs {
        id: ObservationId,
        obs: Box<Observation>,
    },
    Trim {
        id: ObservationId,
    },
    Meta {
        next_seq: u64,
        last_clock: Tick,
        released: Vec<ObservationId>,
    },
}

#[derive(Clone)]
struct Entry {
    obs: Observation,
    stored_at: Tick,
    released: bool,
}

/// A persistent, append-only [`Store`] backed by a single observation log.
///
/// See the [module documentation](crate) for the on-disk record format and the
/// rebase-on-load behavior.
pub struct LogStore {
    file: File,
    index: BTreeMap<ObservationId, Entry>,
    next_seq: u64,
    now: Tick,
    last_clock: Option<Tick>,
}

impl LogStore {
    /// Open (creating if necessary) the log at `path` and rebuild the index.
    ///
    /// `now` is the current tick and is used both as the base for rebasing ages
    /// and as the store's current clock. `first_seq` is the next sequence
    /// number to hand out; the persisted `next_seq` (if any) is honored when it
    /// is larger, so a resumed run never reuses a sequence.
    pub fn new(path: &Path, now: Tick, first_seq: u64) -> Result<Self, StoreError> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)
            .map_err(|_| StoreError::Backend)?;

        let mut index: BTreeMap<ObservationId, Entry> = BTreeMap::new();
        let mut dead: HashSet<ObservationId> = HashSet::new();
        let mut next_seq = first_seq;
        let mut last_clock: Option<Tick> = None;

        let mut file = file;
        while let Some(rec) = read_record(&mut file)? {
            match rec {
                Record::Obs { id, obs } => {
                    // A preceding Trim for this id is cancelled by the
                    // re-insert, so revive it; otherwise dedupe to the
                    // first (oldest) occurrence.
                    dead.remove(&id);
                    index.entry(id).or_insert(Entry {
                        obs: *obs,
                        stored_at: now,
                        released: false,
                    });
                }
                Record::Trim { id } => {
                    dead.insert(id);
                    index.remove(&id);
                }
                Record::Meta {
                    next_seq: ns,
                    last_clock: lc,
                    released,
                } => {
                    next_seq = ns.max(next_seq);
                    last_clock = Some(lc);
                    for rid in released {
                        if let Some(entry) = index.get_mut(&rid) {
                            entry.released = true;
                        }
                    }
                }
            }
        }

        for id in dead {
            index.remove(&id);
        }

        // Fold the pre-reboot elapsed time into every age and reset the stored
        // base to load time. When there is no clock marker yet (a fresh store)
        // or the clock moved backwards, nothing is folded (saturating).
        let elapsed = now.saturating_sub(last_clock.unwrap_or(now));
        for entry in index.values_mut() {
            entry.obs.age_ms = entry.obs.age_ms.saturating_add(elapsed);
            entry.stored_at = now;
        }

        Ok(LogStore {
            file,
            index,
            next_seq,
            now,
            last_clock,
        })
    }

    /// The next sequence number this store will hand out.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Persist the current index state (released flags, `next_seq`, and the
    /// clock marker) and the observation log to disk.
    pub fn flush(&mut self) -> Result<(), StoreError> {
        let mut released: Vec<ObservationId> = self
            .index
            .iter()
            .filter(|(_, entry)| entry.released)
            .map(|(id, _)| *id)
            .collect();
        released.sort_unstable();

        let record = Record::Meta {
            next_seq: self.next_seq,
            last_clock: self.now,
            released,
        };
        write_record(&mut self.file, &record)?;
        self.file.sync_all().map_err(|_| StoreError::Backend)?;
        self.last_clock = Some(self.now);
        Ok(())
    }

    /// Append a single length-prefixed record to the log.
    fn append(&mut self, record: &Record) -> Result<(), StoreError> {
        write_record(&mut self.file, record)
    }
}

/// Write a record as `[u32 length][postcard bytes]`.
fn write_record(file: &mut File, record: &Record) -> Result<(), StoreError> {
    let bytes = postcard::to_stdvec(record).map_err(|_| StoreError::Backend)?;
    let len = bytes.len() as u32;
    file.write_all(&len.to_le_bytes())
        .map_err(|_| StoreError::Backend)?;
    file.write_all(&bytes).map_err(|_| StoreError::Backend)?;
    Ok(())
}

/// Read one length-prefixed record, returning `Ok(None)` at end of file.
fn read_record(file: &mut File) -> Result<Option<Record>, StoreError> {
    let mut len_buf = [0u8; 4];
    if file.read_exact(&mut len_buf).is_err() {
        // Clean end-of-file: no length prefix left to read.
        return Ok(None);
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf).map_err(|_| StoreError::Backend)?;
    let record = postcard::from_bytes(&buf).map_err(|_| StoreError::Backend)?;
    Ok(Some(record))
}

impl Store for LogStore {
    fn insert(&mut self, obs: Observation, now: Tick) -> Result<bool, StoreError> {
        let id = obs.id();
        if self.index.contains_key(&id) {
            // Dedupe: keep the first-stored entry and its age lineage.
            return Ok(false);
        }
        self.now = now;
        let record = Record::Obs {
            id,
            obs: Box::new(obs.clone()),
        };
        self.append(&record)?;
        self.index.insert(
            id,
            Entry {
                obs,
                stored_at: now,
                released: false,
            },
        );
        self.next_seq += 1;
        Ok(true)
    }

    fn contains(&self, id: &ObservationId) -> bool {
        self.index.contains_key(id)
    }

    fn get(&self, id: &ObservationId, now: Tick) -> Option<Observation> {
        let entry = self.index.get(id)?;
        let elapsed = now.saturating_sub(entry.stored_at);
        Some(entry.obs.aged(elapsed))
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn for_each_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        for id in self.index.keys() {
            f(id);
        }
    }

    fn for_each_broadcast_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        for (id, entry) in self.index.iter() {
            if entry.obs.to.is_none() {
                f(id);
            }
        }
    }

    fn for_each_addressed_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        for (id, entry) in self.index.iter() {
            if entry.obs.to.is_some() && !entry.released {
                f(id);
            }
        }
    }

    fn release(&mut self, id: &ObservationId) {
        if let Some(entry) = self.index.get_mut(id) {
            entry.released = true;
        }
    }

    fn is_released(&self, id: &ObservationId) -> bool {
        self.index
            .get(id)
            .map(|entry| entry.released)
            .unwrap_or(false)
    }

    fn for_each_topic(&self, prefix: &[u8], now: Tick, f: &mut dyn FnMut(&Observation)) {
        for entry in self.index.values() {
            if entry.obs.topic.starts_with(prefix) {
                let elapsed = now.saturating_sub(entry.stored_at);
                f(&entry.obs.aged(elapsed));
            }
        }
    }

    fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize {
        let mut removed: BTreeSet<ObservationId> = BTreeSet::new();

        // (a) keep_per_topic: per exact topic, drop everything but the `N`
        // youngest (lowest current age).
        if let Some(keep) = policy.keep_per_topic {
            let mut groups: BTreeMap<&[u8], Vec<ObservationId>> = BTreeMap::new();
            for (id, entry) in self.index.iter() {
                if !removed.contains(id) {
                    groups
                        .entry(entry.obs.topic.as_slice())
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
            for (id, entry) in self.index.iter() {
                if !removed.contains(id) && current_age(&entry.obs, entry.stored_at, now) > max_age
                {
                    removed.insert(*id);
                }
            }
        }

        // (c) max_count: drop the oldest (highest current age) until the number
        // of remaining (untrimmed) observations is at most `max_count`.
        if let Some(max_count) = policy.max_count {
            let mut remaining: Vec<ObservationId> = self
                .index
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

        for id in &removed {
            // `Store::trim` cannot surface I/O errors, so best-effort: the
            // in-memory index is updated regardless of the append result.
            let _ = self.append(&Record::Trim { id: *id });
            self.index.remove(id);
        }
        removed.len()
    }
}

impl LogStore {
    /// Order two stored ids by their current age at `now` (youngest first).
    fn age_cmp(&self, a: &ObservationId, b: &ObservationId, now: Tick) -> core::cmp::Ordering {
        current_age(&self.index[a].obs, self.index[a].stored_at, now).cmp(&current_age(
            &self.index[b].obs,
            self.index[b].stored_at,
            now,
        ))
    }
}
