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
//! * `Record::Obs` — a stored [`Observation`] together with the tick it was
//!   inserted at (`stored_at`). Deduped on `id` at load.
//! * `Record::Trim` — the id of an observation removed by `trim`; the last
//!   event for an id wins, so a re-inserted (new `Obs` record) id survives.
//! * `Record::Release` — the id of an observation that was released; the last
//!   event for an id wins, so a re-inserted (new `Obs` record) id becomes
//!   un-released again. Appended immediately by [`Store::release`], so a
//!   release survives a kill without a flush.
//! * `Record::Boot` — the boot counter, written by [`LogStore::open`] and
//!   incremented on every open. `first_seq() = boot_count << 32`, which is
//!   core's documented contract; a resumed node therefore never reuses a
//!   sequence number.
//! * `Record::Clock` — a tick marker written by [`LogStore::flush`]. On load
//!   each live observation's age is folded up to the last clock marker so
//!   pre-reboot elapsed time is recovered (see [`LogStore::open`]).
//!
//! On load the index is rebuilt by replaying the log, then every age is
//! rebased to load time: each entry's `age_ms` is folded by the time elapsed
//! since its own `stored_at` (the tick it was inserted, carried on the `Obs`
//! record) up to the last persisted clock marker, and then from the marker to
//! load time. Anchoring the fold to each entry's insert tick rather than to a
//! single persisted clock marker makes the reopen fold exact for idle stores
//! and for observations inserted before the last flush — ages continue across a
//! reboot instead of freezing at (and under-counting from) their pre-shutdown
//! value.
//!
//! Crash recovery & robustness
//!
//! * Torn trailing records. The log is append-only, so a crash mid-append can
//!   leave a partial trailing record — a length prefix whose body ran past
//!   end-of-file — or a bit-roted/corrupt record. [`LogStore::open`] recovers by
//!   truncating the file back to the last *complete* record boundary and
//!   loading what survived: the partial tail is dropped, complete records are
//!   preserved. A corrupt length prefix that claims an absurd body size is
//!   rejected before any allocation (see [`MAX_RECORD_BYTES`]) and dropped the
//!   same way, so a malformed prefix cannot exhaust memory or make the store
//!   unloadable.
//!
//! * Durability contract. `flush()` appends a `Record::Clock` and `sync_all`s,
//!   making the appended observation and release records durable across a power
//!   loss. `insert`, `release`, and `trim` update the in-memory index and (for
//!   `insert`/`trim`) append records, but those records reach disk only on the
//!   next `flush()`. Hosts that need crash durability must `flush()` before
//!   shutdown. `release`, by contrast, appends its `Release` record immediately
//!   (best-effort: an append error is logged), so a single release is durable
//!   across a kill-without-flush without waiting for a flush.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tines_core::{Observation, ObservationId, Store, StoreError, Tick, TrimPolicy};

/// Maximum accepted body size, in bytes, for a single log record.
///
/// A record is `[u32 little-endian length][postcard bytes]`; this caps the
/// postcard body so a corrupt or malicious length prefix (e.g. `0xFFFFFFFF`,
/// which would otherwise try to reserve ~4 GiB) is rejected *before* any
/// allocation. The bound is comfortably larger than the largest legitimate
/// record — a fully populated `Record::Obs` is about 1.3 KiB of postcard plus
/// 4 bytes of framing — so valid inserts are never dropped, while still
/// bounding the allocation against a malformed prefix.
pub const MAX_RECORD_BYTES: usize = 8 * 1024;

/// Current (age-adjusted) age of a stored observation at tick `now`.
fn current_age(obs: &Observation, stored_at: Tick, now: Tick) -> u64 {
    obs.age_ms.saturating_add(now.saturating_sub(stored_at))
}

#[derive(Serialize, Deserialize)]
enum Record {
    Obs {
        id: ObservationId,
        obs: Box<Observation>,
        stored_at: Tick,
    },
    Trim {
        id: ObservationId,
    },
    Release {
        id: ObservationId,
    },
    Boot {
        count: u64,
    },
    Clock {
        tick: Tick,
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
/// boot counter, durable releases, and rebase-on-load behavior.
pub struct LogStore {
    file: File,
    index: BTreeMap<ObservationId, Entry>,
    boot_count: u64,
    now: Tick,
}

impl LogStore {
    /// Open (creating if necessary) the log at `path` and rebuild the index.
    ///
    /// `now` is the current tick: it is the base for rebasing ages and the
    /// store's current clock. On open the log's boot counter is read,
    /// incremented, and durably written (`fsync`) before returning, so
    /// [`Self::first_seq`] strictly increases across reboots and a resumed node
    /// never reuses a sequence number.
    ///
    /// The boot counter is the number of times this log has been opened (a
    /// fresh log starts at boot 1). It is exposed as
    /// `first_seq() = boot_count << 32`, core's documented contract: a boot can
    /// hand out up to 2³² sequence numbers before the high word would roll
    /// over, which is more than any robot needs.
    pub fn open(path: &Path, now: Tick) -> Result<Self, StoreError> {
        // Open for reads (to load) and appends (to write later). Append mode is
        // correct because we load by reading the whole file below, and every
        // mutation appends to the end of the log.
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(path)
            .map_err(|_| StoreError::Backend)?;

        // Read the whole log into memory to rebuild the index. Loading from a
        // slice (rather than seeking the open file) lets us detect a torn or
        // corrupt trailing record by byte offsets and truncate the file to the
        // last complete boundary — see the crash-recovery note in the module docs.
        let data = std::fs::read(path).map_err(|_| StoreError::Backend)?;

        let mut index: BTreeMap<ObservationId, Entry> = BTreeMap::new();
        let mut dead: HashSet<ObservationId> = HashSet::new();
        let mut records_loaded: usize = 0;

        // Byte offset just past the last *complete* record replayed so far. A
        // torn or corrupt trailing record (from a crash mid-append, or a
        // bit-roted/corrupt record) stops the loop and is truncated back to this
        // boundary, so the store loads with whatever complete records survived
        // instead of failing outright. See the crash-recovery note in the module docs.
        let mut pos = 0usize;
        let mut good_end = 0usize;

        // The boot counter read off the last `Record::Boot` on disk, and the
        // tick of the last `Record::Clock` (the load-time age fold's anchor).
        let mut last_boot_count: Option<u64> = None;
        let mut last_clock_tick: Option<Tick> = None;

        loop {
            match parse_record(&data, pos) {
                Ok(Some((new_pos, rec))) => {
                    match rec {
                        Record::Obs { id, obs, stored_at } => {
                            // A preceding Trim for this id is cancelled by the
                            // re-insert, so revive it; otherwise dedupe to the
                            // first (oldest) occurrence. The age is folded after
                            // the loop (once the last clock marker is known), so
                            // it starts from its stored, insert-time age.
                            dead.remove(&id);
                            index.entry(id).or_insert_with(|| Entry {
                                obs: *obs,
                                stored_at,
                                released: false,
                            });
                        }
                        Record::Trim { id } => {
                            dead.insert(id);
                            index.remove(&id);
                        }
                        Record::Release { id } => {
                            // Mark released if the id is still live; a later
                            // re-insert (a fresh `Obs`) makes it un-released.
                            if let Some(entry) = index.get_mut(&id) {
                                entry.released = true;
                            }
                        }
                        Record::Boot { count } => {
                            last_boot_count = Some(count);
                        }
                        Record::Clock { tick } => {
                            last_clock_tick = Some(tick);
                        }
                    }
                    records_loaded += 1;
                    // Advance the last-complete-record boundary now that this
                    // record is fully read.
                    good_end = new_pos;
                    pos = new_pos;
                }
                // Clean end-of-file: nothing left to read.
                Ok(None) => break,
                // Torn or corrupt trailing record. `parse_record` only errors on
                // a bad trailing record (a partial length prefix, a body that
                // runs past end-of-file, a body length over MAX_RECORD_BYTES, or
                // a decode failure). Chop the file back to the last complete
                // boundary and load what survived — only the partial tail of a
                // crashed append is lost, never a complete record.
                Err(_) => break,
            }
        }

        for id in dead {
            index.remove(&id);
        }

        // We stopped before EOF: the trailing bytes are a torn or corrupt record.
        // Truncate the file to the last complete boundary so it has a clean tail.
        let dropped = data.len().saturating_sub(good_end);
        if dropped > 0 {
            file.set_len(good_end as u64)
                .map_err(|_| StoreError::Backend)?;
            eprintln!(
                "tines-store: dropped {} torn/corrupt byte(s) at the tail of {} \
                 while loading ({} complete record(s) loaded); call flush() \
                 after the next mutation to rewrite a clean tail.",
                dropped,
                path.display(),
                records_loaded,
            );
        }

        // Increment the boot counter and durably persist it before returning, so
        // a later open reads a strictly higher value and first_seq() climbs.
        let boot_count = last_boot_count.map(|c| c + 1).unwrap_or(1);
        let record = Record::Boot { count: boot_count };
        write_record(&mut file, &record)?;
        file.sync_all().map_err(|_| StoreError::Backend)?;

        // Fold each live observation's age up to the last clock marker and then
        // to load time. Anchoring to each entry's insert tick keeps the fold
        // exact even for observations inserted before the last flush (see the
        // module docs).
        for entry in index.values_mut() {
            entry.obs.age_ms = match last_clock_tick {
                Some(marker) if marker >= entry.stored_at => entry
                    .obs
                    .age_ms
                    .saturating_add(marker.saturating_sub(entry.stored_at))
                    .saturating_add(now.saturating_sub(marker)),
                _ => entry
                    .obs
                    .age_ms
                    .saturating_add(now.saturating_sub(entry.stored_at)),
            };
            entry.stored_at = now;
        }

        Ok(LogStore {
            file,
            index,
            boot_count,
            now,
        })
    }

    /// The sequence number the next `Node::observe` should use for this boot.
    ///
    /// Equals `boot_count << 32` (see [`LogStore::open`]). It strictly increases
    /// across reboots, so a resumed node's sequence numbers can't collide
    /// (content-id dedupe) with the previous boot's observations.
    pub fn first_seq(&self) -> u64 {
        self.boot_count << 32
    }

    /// Persist the current index state to disk.
    ///
    /// This is the durability boundary: `flush()` advances each entry's age to
    /// the current tick, appends a `Record::Clock` tick marker, and calls
    /// `sync_all`, after which the appended observation and release records are
    /// durable across a power loss. Because `insert` only reaches disk via a
    /// subsequent `flush()`, hosts that need crash durability must call
    /// `flush()` before shutdown (see the durability contract in the module
    /// docs).
    pub fn flush(&mut self) -> Result<(), StoreError> {
        // Advance every entry's age up to this tick in memory, so each
        // `entry.obs.age_ms` is the true current age and `stored_at` marks when
        // that age was computed. Ages are recomputed from each entry's own
        // `stored_at` on the next load (see [`LogStore::open`]), so this keeps
        // the in-memory representation coherent within this run.
        for entry in self.index.values_mut() {
            entry.obs.age_ms = entry
                .obs
                .age_ms
                .saturating_add(self.now.saturating_sub(entry.stored_at));
            entry.stored_at = self.now;
        }

        // Record a clock marker so the load-time age fold has a durable anchor
        // for the time elapsed up to this flush.
        let record = Record::Clock { tick: self.now };
        write_record(&mut self.file, &record)?;
        self.file.sync_all().map_err(|_| StoreError::Backend)?;
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

/// Parse one length-prefixed record from `data` at byte offset `pos`.
///
/// This is a pure slice parser (no file I/O), so a torn or corrupt trailing
/// record is detectable purely by byte offsets, which is exactly what
/// [`LogStore::open`] needs to truncate to the last complete boundary.
///
/// * `Ok(None)` — clean end-of-file: `pos` is exactly at the end of `data`.
/// * `Ok(Some((new_pos, rec)))` — a complete record was parsed; `new_pos` is
///   the byte offset just past it (`pos + 4 + body_len`).
/// * `Err(StoreError::Backend)` — a torn or corrupt *trailing* record: fewer
///   than 4 prefix bytes remain (a partial prefix from a crash mid-append), the
///   declared body length exceeds [`MAX_RECORD_BYTES`] (rejected *before* any
///   allocation, so a corrupt `0xFFFFFFFF` prefix cannot exhaust memory), the
///   body runs past `data.len()`, or the body fails to decode.
///   [`LogStore::open`] treats this as a torn tail and truncates the file.
fn parse_record(data: &[u8], pos: usize) -> Result<Option<(usize, Record)>, StoreError> {
    // Clean EOF: nothing left.
    if pos == data.len() {
        return Ok(None);
    }
    // A partial length prefix (1..3 leftover bytes) from a crash mid-append.
    if pos + 4 > data.len() {
        return Err(StoreError::Backend);
    }
    let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    // Reject an over-length declared body *before* allocating, so a corrupt or
    // malicious prefix (e.g. `0xFFFFFFFF`, ~4 GiB) cannot exhaust memory.
    // See [`MAX_RECORD_BYTES`].
    if len > MAX_RECORD_BYTES {
        return Err(StoreError::Backend);
    }
    let start = pos + 4;
    let end = start + len;
    // A body that runs past end-of-file is a torn trailing record.
    if end > data.len() {
        return Err(StoreError::Backend);
    }
    let rec = postcard::from_bytes(&data[start..end]).map_err(|_| StoreError::Backend)?;
    Ok(Some((end, rec)))
}

impl Store for LogStore {
    /// Insert an observation.
    ///
    /// The record is appended to the log, but is durable across a power loss
    /// only after a call to [`Self::flush`]; hosts that need crash durability
    /// must flush before shutdown (see the durability contract in the module
    /// docs).
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
            stored_at: now,
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

    /// Visit the ids of observations addressed to someone (`to == Some`),
    /// excluding those already released.
    ///
    /// This is stricter than the [`Store`] trait's documented contract
    /// (`addressed == to == Some`): a released addressed observation has
    /// already been sent to its recipient, so it is intentionally skipped and
    /// not offered for exchange again until (and if) it is re-inserted.
    fn for_each_addressed_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        for (id, entry) in self.index.iter() {
            if entry.obs.to.is_some() && !entry.released {
                f(id);
            }
        }
    }

    /// Mark the observation with `id` as released: it has been sent and must
    /// not be offered for exchange again.
    ///
    /// The mark is set in memory immediately and a `Record::Release` is
    /// appended to the log at the same time (best-effort: an append error is
    /// logged, not raised, keeping [`Store::release`] infallible). The release
    /// is therefore durable across a kill-without-flush — unlike a flush-only
    /// release, it does not wait for a later [`Self::flush`].
    fn release(&mut self, id: &ObservationId) {
        if let Some(entry) = self.index.get_mut(id) {
            entry.released = true;
        }
        // Best-effort durable append: `release` is infallible, so a backend
        // failure is logged rather than raised (the worst case is a duplicate
        // custodian after a reboot).
        if let Err(e) = self.append(&Record::Release { id: *id }) {
            eprintln!("tines-store: failed to append release record for {id:?}: {e:?}");
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

    /// Apply [`TrimPolicy`] rules in order (per-topic, age, count) and return
    /// the number of observations removed. Trimmed ids are not tombstoned:
    /// re-inserting them later is accepted.
    ///
    /// Each removed id appends a `Record::Trim` record, but that record is
    /// durable across a power loss only after a [`Self::flush`]; hosts that
    /// need crash durability must flush before shutdown.
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
                ids.sort_unstable();
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
