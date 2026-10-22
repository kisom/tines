//! Append-only observation log backing [`LogStore`].
//!
//! The log is a single file of self-describing, checksummed records with an
//! identifying header:
//!
//! ```text
//! file    = header ‖ record*
//! header  = magic "TINESLOG" ‖ format_version: u16 ‖ reserved: u16
//! record  = sync: u32 (constant) ‖ len: u32 ‖ crc32c: u32 ‖ body[len]
//! ```
//!
//! * `sync` is [`SYNC_MARKER`]: a 4-byte marker so a torn or corrupt record can
//!   be located and skipped by scanning for the next marker.
//! * `crc32c` is a CRC-32C (Castagnoli) over `len ‖ body`. It guards against
//!   accidental corruption (bit rot, a crash mid-append); it is not a keyed MAC
//!   (threat model F5, A5).
//!
//! Record kinds:
//!
//! * `Record::Obs` — a stored [`Observation`] together with the tick it was
//!   inserted at (`stored_at`). The `id` is *not* stored: it is recomputed from
//!   `obs.id()` on load. Deduped on `id` at load.
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
//! * `Record::Clock` — a tick marker written by `flush` (the [`Store::flush`]
//!   impl) and at the end of every snapshot, with a wall time once trusted
//!   time is set. On load each live observation's age is folded up to the
//!   last clock marker so pre-reboot elapsed time is recovered (see
//!   [`LogStore::open`]).
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
//! Snapshots (spec §9, decision I)
//!
//! A `stored_at` is a tick from the boot that wrote it, so an append-only log
//! would mix tick domains across boots, and the fold's results would live only
//! in memory. So the log is rewritten as a snapshot at two points: at the end
//! of [`LogStore::open`] (after the tick fold) and at the end of
//! [`LogStore::set_wall_clock`]. A snapshot is the header, the current `Boot`
//! record, every live observation with its current age and `stored_at = now`,
//! a `Release` record for every released id still held, and a closing clock
//! marker at `now` (with the wall time after `set_wall_clock`, none at open).
//! It holds no trimmed records. Every `stored_at` on disk then belongs to the
//! current boot, and the fold's results (expiries, floors, known-downtime
//! advances) are durable.
//!
//! Crash recovery & robustness
//!
//! * Framing & checksums. Every record carries a [`SYNC_MARKER`] and a CRC-32C
//!   over its body, so any corruption is detectable rather than silently
//!   misparsed.
//! * Torn trailing records. Between snapshots the log is append-only, so a
//!   crash mid-append can leave a partial trailing record — a header whose
//!   body ran past end-of-file — or a bit-roted/corrupt record.
//!   [`LogStore::open`] recovers by scanning for the next well-formed record: a
//!   record whose marker, length, CRC or body-decode is bad is *skipped* (scan
//!   forward for the next `SYNC_MARKER` and verify its CRC before accepting
//!   it), and the bytes leading up to it are dropped. Only bad bytes at the
//!   very end, with no valid record after them, count as a torn tail. The open
//!   snapshot rewrites the log without either.
//! * Snapshots are atomic. A snapshot is written to a temporary file in the
//!   log's directory, fsynced, renamed over the log, and then the directory is
//!   fsynced. A crash before the rename leaves the old log intact (the next
//!   open repeats the same fold and removes the leftover temporary file);
//!   after it, the snapshot is the log.
//! * Torn first header. A file shorter than the header that starts with the
//!   magic was torn while the log was first created; it holds no records, so
//!   it opens as a new log. Any other short file is refused.
//! * Write cap. `write_record` refuses a record over [`MAX_RECORD_BYTES`], so
//!   the writer can no longer produce a record the reader rejects.
//!
//! * Durability contract. `flush(now)` appends a `Record::Clock` and
//!   `sync_all`s, making the appended observation and release records durable
//!   across a power loss. `insert`, `release`, and `trim` update the in-memory
//!   index and append records, but those records are durable across a power
//!   loss only after the next flush (or snapshot). Hosts that need crash
//!   durability must flush before shutdown. `release` appends its `Release`
//!   record immediately (best-effort: an append error is logged), so a release
//!   survives a process kill without waiting for a flush.
//!
//! Isolation & integrity
//!
//! * Exclusive lock. `open` takes an exclusive `File::try_lock` on the log
//!   file; a second opener gets a clear error instead of interleaving on the
//!   same file. A snapshot locks its temporary file before renaming it over
//!   the log, so the file at the log's path stays locked while the store is
//!   alive, and an opener that locked a file a snapshot just replaced notices
//!   and retries on the new one.
//! * Permissions. The log file (and a snapshot's temporary file) is created
//!   mode `0600` and its parent directory (created if missing) mode `0700`.
//! * Format version. The file starts with a `"TINESLOG"` header carrying a
//!   `format_version`; a missing or unknown header is an error that names the
//!   fix (delete the log). The 1.0 work bumps `format_version` for the new
//!   `Observation` encoding.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tines_core::{Observation, ObservationId, Store, StoreError, Tick, TrimPolicy, WallClockStore};

mod crc32c {
    //! A minimal, std-only CRC-32C (Castagnoli).
    //!
    //! Reflected input/output, initial value `0xFFFFFFFF`, final XOR
    //! `0xFFFFFFFF`, polynomial `0x82F6_3B78`. The canonical check value is
    //! `CRC32C("123456789") == 0xE306_9283`; the test module pins it.
    const POLY: u32 = 0x82F6_3B78;

    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0usize;
        while i < 256 {
            let mut crc = i as u32;
            let mut j = 8;
            while j > 0 {
                crc = (crc >> 1) ^ if crc & 1 != 0 { POLY } else { 0 };
                j -= 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    };

    /// Compute the CRC-32C (Castagnoli) of `bytes`.
    pub fn crc32c(bytes: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFF_FFFF;
        for &b in bytes {
            let idx = ((crc ^ b as u32) & 0xFF) as usize;
            crc = (crc >> 8) ^ TABLE[idx];
        }
        crc ^ 0xFFFF_FFFF
    }

    #[cfg(test)]
    mod tests {
        use super::crc32c;

        #[test]
        fn check_value() {
            // Canonical CRC32C check value.
            assert_eq!(crc32c(b"123456789"), 0xE306_9283);
            assert_eq!(crc32c(b""), 0x0000_0000);
            // Castagnoli CRC-32C (not the IEEE CRC-32 which is 0xCBF4_3926).
            assert_eq!(
                crc32c(b"The quick brown fox jumps over the lazy dog"),
                0x2262_0404
            );
        }
    }
}

/// Magic header: the first 8 bytes of every v1 log.
const MAGIC: [u8; 8] = *b"TINESLOG";
/// The `format_version` field of the header. Bump for an incompatible encoding.
const FORMAT_VERSION: u16 = 2;
/// 4-byte record sync marker (`b"TING"`), lets a torn/corrupt record be
/// located and skipped by scanning forward. Made `pub` so hosts/tests can emit
/// (and corrupt) framing directly.
pub const SYNC_MARKER: u32 = 0x5449_4E47;
/// `magic(8) ‖ format_version(2) ‖ reserved(2)`.
const HEADER_LEN: usize = 12;

/// Maximum accepted body size, in bytes, for a single log record.
///
/// A record is `[sync(4) ‖ len(4) ‖ crc32c(4) ‖ body]`; this caps the postcard
/// body so a corrupt or malicious length prefix (e.g. `0xFFFFFFFF`, which would
/// otherwise try to reserve ~4 GiB) is rejected *before* any allocation, both
/// at write time (via [`write_record_for_test`]) and at read time. The bound is
/// comfortably larger than the largest legitimate record — a fully populated
/// `Record::Obs` is about 1.3 KiB of postcard plus 12 bytes of framing — so
/// valid inserts are never dropped.
pub const MAX_RECORD_BYTES: usize = 8 * 1024;

/// Build and append the on-disk bytes for a record from raw `body` bytes:
/// `sync ‖ len ‖ crc32c ‖ body`, where `crc32c` covers `len ‖ body`.
///
/// This is the framing a host would use to write a record directly. It refuses
/// a `body` over [`MAX_RECORD_BYTES`] (the write cap, F5), so a writer can no
/// longer produce a record the reader would reject.
pub fn write_record_for_test(file: &mut dyn std::io::Write, body: &[u8]) -> Result<(), StoreError> {
    let len = body.len();
    // Write cap: never emit a record the reader would reject.
    if len > MAX_RECORD_BYTES {
        return Err(StoreError::Backend);
    }
    let sync_le = SYNC_MARKER.to_le_bytes();
    let len_le = (len as u32).to_le_bytes();
    // CRC-32C covers `len ‖ body`.
    let mut crc_in = Vec::with_capacity(4 + len);
    crc_in.extend_from_slice(&len_le);
    crc_in.extend_from_slice(body);
    let crc = crc32c::crc32c(&crc_in);
    let crc_le = crc.to_le_bytes();
    file.write_all(&sync_le).map_err(|_| StoreError::Backend)?;
    file.write_all(&len_le).map_err(|_| StoreError::Backend)?;
    file.write_all(&crc_le).map_err(|_| StoreError::Backend)?;
    file.write_all(body).map_err(|_| StoreError::Backend)?;
    Ok(())
}

/// Current (age-adjusted) age of a stored observation at tick `now`.
fn current_age(obs: &Observation, stored_at: Tick, now: Tick) -> u64 {
    obs.age_ms.saturating_add(now.saturating_sub(stored_at))
}

#[derive(Serialize, Deserialize)]
enum Record {
    Obs {
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
        /// The wall-clock value stamped at write time: derived as
        /// `wall_at_set + (tick - tick_at_set)`. `Some` only for markers written
        /// after a trusted wall clock is established (see
        /// [`LogStore::set_wall_clock`]); `None` for any marker written before
        /// that point, or while the wall clock is still unknown. A `None` here is
        /// what makes the reopen fold treat that marker's downtime as unknown.
        wall: Option<u64>,
        /// The caller's `now` at the point this marker was written (the tick
        /// value passed to `flush`).
        tick: Tick,
    },
}

#[derive(Clone)]
struct Entry {
    obs: Observation,
    stored_at: Tick,
    released: bool,
    /// Set at load if this observation was held while the wall clock was
    /// unknown. While set, the observation is withheld from broadcast delivery
    /// (see [`LogStore::for_each_broadcast_id`] and
    /// [`LogStore::for_each_addressed_id`]) but still contained so dedupe
    /// works. Cleared by [`LogStore::set_wall_clock`]; not persisted.
    quarantined: bool,
}

/// A persistent, append-only [`Store`] backed by a single observation log.
///
/// See the [module documentation](crate) for the on-disk record format and the
/// boot counter, durable releases, crash recovery, exclusive lock, and
/// permissions.
pub struct LogStore {
    /// Where the log lives; the snapshot's temporary file sits beside it.
    path: PathBuf,
    /// The live log, opened for appends and holding the exclusive lock.
    file: File,
    index: BTreeMap<ObservationId, Entry>,
    boot_count: u64,
    /// Number of corrupt records skipped while rebuilding the index on the last
    /// [`Self::open`] (0 if the log was clean). Exposed for hosts to report.
    skipped_records: usize,
    /// The previous boot's last clock marker, `(tick, wall)`, as read from the
    /// log at [`Self::open`]. The open snapshot replaces it on disk with a
    /// marker that has no wall time, so it is kept here for the downtime fold
    /// in [`Self::set_wall_clock`].
    last_marker: Option<(Tick, Option<u64>)>,
    /// Caller ticks of downtime already folded during [`Self::open`]: `now`
    /// at open minus the previous marker's tick, saturating.
    counted_ticks: u64,
    /// The caller's `now` at [`Self::open`]. The fold subtracts the ticks
    /// elapsed since then exactly once (S2 of the 1.0 review).
    open_tick: Tick,
    /// The time anchor: the latest tick the store has been given (open,
    /// insert, flush or fold). An earlier `now` passed to `flush` or
    /// `set_wall_clock` is clamped up to it, so `stored_at` and the clock
    /// markers never move backwards.
    anchor: Tick,
    /// The `unknown_downtime_age_ms` broadcast floor configured on open.
    unknown_downtime_age_ms: u64,
    /// Wall-clock value captured by [`Self::set_wall_clock`]; `Some` once the
    /// wall clock has been trusted.
    wall_at_set: Option<u64>,
    /// The caller's `now` when [`Self::set_wall_clock`] captured the wall clock.
    tick_at_set: Option<u64>,
    /// The [`FoldReport`] produced by the first [`Self::set_wall_clock`], cached
    /// so a second call is a no-op and the report stays readable.
    fold_report: Option<FoldReport>,
}

/// The outcome of [`LogStore::set_wall_clock`]: how much wall-clock downtime was
/// folded into held ages, and how many held observations expired or were floored
/// by it.
///
/// Printed once on stderr by the host at the moment of the fold, then read back
/// (see [`LogStore::fold_report`]) for any record. A `None`
/// [`Self::downtime_ms`] signals unknown downtime; the host is expected to
/// surface that (e.g. log a warning) rather than the fold hiding it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FoldReport {
    /// The wall-clock milliseconds of downtime folded in, `Some` only when the
    /// downtime was known; `None` for unknown downtime.
    pub downtime_ms: Option<u64>,
    /// Number of held observations addressed to others that were aged past their
    /// lifetime (and will not be delivered/forwarded while held).
    pub expired_addressed: usize,
    /// Number of held broadcast observations whose age was floored to
    /// `unknown_downtime_age_ms`.
    pub floored_broadcasts: usize,
}

/// A partially-built [`LogStore`] carrying the `unknown_downtime_age_ms`
/// broadcast floor. Obtain one from [`LogStore::builder`] and finish with
/// [`LogStoreBuilder::open`].
#[derive(Clone)]
pub struct LogStoreBuilder {
    path: PathBuf,
    unknown_downtime_age_ms: u64,
}

impl LogStoreBuilder {
    /// Set the `unknown_downtime_age_ms` broadcast floor (the floor applied to
    /// held broadcasts under the fold's unknown-downtime branch). The floor
    /// should stay below the host's trim `max_age_ms`; the host config carries
    /// this value (see `Config::unknown_downtime_age_ms`).
    pub fn set_unknown_downtime_age_ms(self, unknown_downtime_age_ms: u64) -> Self {
        Self {
            unknown_downtime_age_ms,
            ..self
        }
    }

    /// Open the store, creating it if necessary, with the configured broadcast
    /// floor. See [`LogStore::open`].
    pub fn open(self, now: Tick) -> Result<LogStore, StoreError> {
        LogStore::open_with(self.path.as_ref(), now, self.unknown_downtime_age_ms)
    }
}

impl LogStore {
    /// The `unknown_downtime_age_ms` a host gets when it opens the store via
    /// [`LogStore::open`] instead of configuring it with the builder.
    ///
    /// A host that *can* tell the downtime is up should prefer
    /// [`LogStore::set_wall_clock`] with a known wall clock so ages advance
    /// exactly; this floor is a conservative stand-in when the downtime is
    /// unknown. A day is a reasonable minimum age for a broadcast whose true
    /// age we cannot establish: old data is better delivered than silently
    /// trusted.
    const DEFAULT_UNKNOWN_DOWNTIME_MS: u64 = 24 * 60 * 60 * 1000;

    /// Open (creating if necessary) the log at `path` and rebuild the index,
    /// holding every observation already on disk in quarantine.
    ///
    /// This is the host's entry point and deliberately takes *no* wall time: it
    /// cannot know whether the machine was down since the last marker, so it
    /// quarantines every loaded observation until the host calls
    /// [`LogStore::set_wall_clock`]. Use [`LogStore::builder`] to configure the
    /// `unknown_downtime_age_ms` broadcast floor (otherwise
    /// the default `DEFAULT_UNKNOWN_DOWNTIME_MS` floor applies).
    ///
    /// See [`LogStore::open_with`] for the load semantics; the difference here is
    /// only that the broadcast-floor option defaults rather than being explicit.
    pub fn open(path: &Path, now: Tick) -> Result<Self, StoreError> {
        Self::builder(path).open(now)
    }

    /// Start building a [`LogStore`] with the `unknown_downtime_age_ms`
    /// broadcast floor configured. See [`LogStore::open`] and
    /// [`LogStore::set_wall_clock`].
    pub fn builder(path: &Path) -> LogStoreBuilder {
        LogStoreBuilder {
            path: path.to_path_buf(),
            unknown_downtime_age_ms: LogStore::DEFAULT_UNKNOWN_DOWNTIME_MS,
        }
    }
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
    ///
    /// If the log has a corrupt middle record it is skipped and a torn tail is
    /// dropped; the number of corrupt records skipped is reported once on
    /// stderr and is available via [`Self::skipped_records`].
    ///
    /// Before returning, `open` rewrites the log as a snapshot of what it
    /// loaded (spec §9, decision I; see the module docs), with every age
    /// folded to `now` and a closing clock marker at `now` that carries no
    /// wall time. That makes the new boot counter durable, keeps every
    /// `stored_at` on disk in this boot's ticks, and compacts the log. If the
    /// snapshot can't be written, `open` fails and the old log is untouched.
    ///
    /// An exclusive lock is taken on the file, so a second `open` while this
    /// one is alive fails with [`StoreError::Backend`].
    pub fn open_with(
        path: &Path,
        now: Tick,
        unknown_downtime_age_ms: u64,
    ) -> Result<Self, StoreError> {
        // Ensure the parent directory exists (mode 0700 if we create it).
        ensure_parent_dir(path)?;

        // Open the log and take its exclusive lock (see `open_locked`).
        let file = open_locked(path)?;

        // We hold the lock, so nobody else is mid-snapshot: a temporary file
        // left beside the log is from a snapshot interrupted before its
        // rename. The log itself is intact, so the leftover is discarded.
        let tmp_path = snapshot_tmp_path(path);
        if tmp_path.exists() {
            let _ = std::fs::remove_file(&tmp_path);
            eprintln!(
                "tines-store: removed {} left by an interrupted snapshot; \
                 the log itself is intact.",
                tmp_path.display()
            );
        }

        let data = std::fs::read(path).map_err(|_| StoreError::Backend)?;

        // Header: an empty log (or one whose header was torn while it was
        // first created) is new; otherwise validate it. A missing or unknown
        // header is an error that names the fix (delete the log).
        let data: &[u8] = if is_torn_new_log(&data) {
            &[]
        } else {
            validate_header(&data, path)?;
            &data
        };

        let mut index: BTreeMap<ObservationId, Entry> = BTreeMap::new();
        let mut dead: HashSet<ObservationId> = HashSet::new();
        let mut records_loaded: usize = 0;

        // Byte offset just past the last *complete* record replayed so far.
        let mut pos = HEADER_LEN;
        let mut good_end = HEADER_LEN;
        // Corrupt records skipped (marker/len/CRC/decode bad) while scanning
        // forward for the next valid record.
        let mut skipped = 0usize;

        // The boot counter read off the last `Record::Boot` on disk, and the
        // last `Record::Clock` (load-time age fold anchor, and the anchor for
        // the downtime fold). `last_marker` is `(tick, wall)`; a `None` wall
        // means no trusted wall clock exists yet, so the reopen fold treats any
        // gap since this marker as unknown-downtime.
        let mut last_boot_count: Option<u64> = None;
        let mut last_marker: Option<(Tick, Option<u64>)> = None;

        loop {
            // Clean end-of-file: nothing left to read.
            if pos == data.len() {
                break;
            }
            // `parse_valid_record` returns the first well-formed record starting
            // at or after `pos`. If it starts after `pos`, the bytes in between
            // were a corrupt/torn record we skipped. If there is no well-formed
            // record left, the remainder is a torn tail.
            match parse_valid_record(data, pos) {
                Some((start, end, rec)) => {
                    if start > pos {
                        skipped += 1;
                    }
                    match rec {
                        Record::Obs { obs, stored_at } => {
                            // Ids are recomputed from the body, not stored. A
                            // preceding Trim for this id is cancelled by the
                            // re-insert, so revive it; otherwise dedupe to the
                            // first (oldest) occurrence. The age is folded after
                            // the loop (once the last clock marker is known), so
                            // it starts from its stored, insert-time age.
                            let id = obs.id();
                            dead.remove(&id);
                            index.entry(id).or_insert_with(|| Entry {
                                obs: *obs,
                                stored_at,
                                released: false,
                                quarantined: true,
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
                        Record::Clock { tick, wall } => {
                            last_marker = Some((tick, wall));
                        }
                    }
                    records_loaded += 1;
                    // Advance the last-complete-record boundary now that this
                    // record is fully read.
                    good_end = end;
                    pos = end;
                }
                // No valid record from `pos` onward: torn tail.
                None => break,
            }
        }

        for id in dead {
            index.remove(&id);
        }

        // A torn tail (bad bytes at EOF with no valid record after) is dropped:
        // the open snapshot below rewrites the log from the records loaded.
        let dropped = data.len().saturating_sub(good_end);
        if dropped > 0 {
            eprintln!(
                "tines-store: dropped {} torn byte(s) at the tail of {} \
                 while loading ({} complete record(s) loaded).",
                dropped,
                path.display(),
                records_loaded,
            );
        }

        if skipped > 0 {
            eprintln!(
                "tines-store: skipped {} corrupt record(s) in {} while loading \
                 ({} complete record(s) loaded); the open snapshot rewrites \
                 the log without them.",
                skipped,
                path.display(),
                records_loaded,
            );
        }

        // Increment the boot counter. The open snapshot below makes it durable
        // before returning, so a later open reads a strictly higher value and
        // first_seq() climbs. Saturating: a corrupt-but-valid huge count must
        // not panic (first_seq then repeats, which core dedupes safely).
        let boot_count = last_boot_count.map_or(1, |c| c.saturating_add(1));

        // Fold each live observation's age up to the last clock marker and then
        // to load time. Anchoring to each entry's insert tick keeps the fold
        // exact even for observations inserted before the last flush (see the
        // module docs). At reopen the wall clock is still unknown, so every
        // loaded observation is quarantined (set at construction) and the fold
        // is a plain advance to load time.
        for entry in index.values_mut() {
            entry.obs.age_ms = match last_marker {
                Some((marker, _)) if marker >= entry.stored_at => entry
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

        let mut store = LogStore {
            path: path.to_path_buf(),
            file,
            index,
            boot_count,
            skipped_records: skipped,
            last_marker,
            counted_ticks: last_marker.map_or(0, |(tick, _)| now.saturating_sub(tick)),
            open_tick: now,
            anchor: now,
            unknown_downtime_age_ms,
            wall_at_set: None,
            tick_at_set: None,
            fold_report: None,
        };

        // Decision I: rewrite the log as a snapshot, so every `stored_at` on
        // disk belongs to this boot and the new boot count is durable. The
        // store can't know the wall time at open, so the closing marker has
        // none: a restart before trusted time takes the unknown path.
        store.write_snapshot(now, None)?;
        Ok(store)
    }

    /// The sequence number the next `Node::observe` should use for this boot.
    ///
    /// Equals `boot_count << 32` (see [`LogStore::open`]). It strictly increases
    /// across reboots, so a resumed node's sequence numbers can't collide
    /// (content-id dedupe) with the previous boot's observations.
    pub fn first_seq(&self) -> u64 {
        self.boot_count << 32
    }

    /// Number of corrupt records skipped while rebuilding the index on the most
    /// recent [`Self::open`] (0 if the log was clean).
    ///
    /// A corrupt record is one whose sync marker, length, CRC or body decode
    /// was bad and was skipped to reach the next well-formed record. This is the
    /// counter a host reports once on open.
    pub fn skipped_records(&self) -> usize {
        self.skipped_records
    }

    /// Clamp `now` to the time anchor and advance the anchor to it.
    fn advance_anchor(&mut self, now: Tick) -> Tick {
        self.anchor = self.anchor.max(now);
        self.anchor
    }

    /// Advance every entry's age to `now` in memory and set its `stored_at` to
    /// `now`. `now` must not be behind any entry's `stored_at` (callers pass
    /// the anchor), so no `stored_at` moves backwards.
    fn rebase_to(&mut self, now: Tick) {
        for entry in self.index.values_mut() {
            entry.obs.age_ms = current_age(&entry.obs, entry.stored_at, now);
            entry.stored_at = entry.stored_at.max(now);
        }
    }

    /// The wall time a clock marker written at tick `now` carries: derived as
    /// `wall_at_set + (now - tick_at_set)` once trusted time is set, else
    /// `None`. Saturating, so a peer's absurd wall time can't overflow it.
    fn marker_wall(&self, now: Tick) -> Option<u64> {
        match (self.wall_at_set, self.tick_at_set) {
            (Some(wall_at_set), Some(tick_at_set)) => {
                Some(wall_at_set.saturating_add(now.saturating_sub(tick_at_set)))
            }
            _ => None,
        }
    }

    /// Rewrite the log as a snapshot of the current state at tick `now`
    /// (decision I): header, the current `Boot` record, every live observation
    /// with its current age and `stored_at = now`, a `Release` record for every
    /// released id still held, and a closing clock marker at `now` carrying
    /// `wall`. Trimmed records are not carried over.
    ///
    /// The snapshot goes to a temporary file beside the log (created mode
    /// `0600` and locked before it becomes the log), is fsynced, renamed over
    /// the log, and then the directory is fsynced. A crash before the rename
    /// leaves the old log intact; after it, the snapshot is the log. Because
    /// the temporary file is locked before the rename, the file at the log's
    /// path is locked by this store at every instant (see `open_locked`).
    ///
    /// `now` must already be clamped to the anchor. The in-memory entries are
    /// rebased to `now` as well, so memory and disk agree.
    fn write_snapshot(&mut self, now: Tick, wall: Option<u64>) -> Result<(), StoreError> {
        self.rebase_to(now);

        let mut buf = Vec::new();
        buf.extend_from_slice(&header_bytes());
        buf.extend(encode_record(&Record::Boot {
            count: self.boot_count,
        })?);
        for entry in self.index.values() {
            buf.extend(encode_record(&Record::Obs {
                obs: Box::new(entry.obs.clone()),
                stored_at: now,
            })?);
        }
        for (id, entry) in self.index.iter() {
            if entry.released {
                buf.extend(encode_record(&Record::Release { id: *id })?);
            }
        }
        buf.extend(encode_record(&Record::Clock { tick: now, wall })?);

        let tmp_path = snapshot_tmp_path(&self.path);
        // We hold the log's lock, so any file here is a leftover.
        let _ = std::fs::remove_file(&tmp_path);
        let tmp = match write_locked_tmp(&tmp_path, &buf) {
            Ok(tmp) => tmp,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp_path);
                return Err(e);
            }
        };
        if std::fs::rename(&tmp_path, &self.path).is_err() {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(StoreError::Backend);
        }
        // The snapshot is the log now: append to it from here on. Dropping the
        // old handle releases the lock on the old, now unlinked, file.
        self.file = tmp;
        sync_dir(&self.path)
    }

    /// Append a single record to the log.
    fn append(&mut self, record: &Record) -> Result<(), StoreError> {
        write_record(&mut self.file, record)
    }

    /// Fold the downtime (if any) out of every quarantined observation's age
    /// and lift the quarantine, then start wall-clock time for clock markers.
    ///
    /// Until this runs the store cannot tell whether the machine was down since
    /// the last clock marker (see [`LogStore::open`]), so every loaded
    /// observation is quarantined. This is the moment the host reports a
    /// *trusted* wall clock.
    ///
    /// **Known downtime**: the previous boot's last marker (as read at open)
    /// carries a wall time and `wall_now` is not behind it. Every quarantined
    /// observation gains the wall time elapsed since that marker
    /// (`wall_now - marker_wall`), minus the ticks already counted for it:
    /// those the open fold added past the marker tick, and this boot's ticks
    /// since open (`now - open_tick`), counted once whether or not a flush
    /// ran in between. Saturating to zero.
    ///
    /// **Unknown downtime**: no marker, a marker without a wall time, or a
    /// marker whose wall time is ahead of `wall_now`. Quarantined addressed
    /// observations are aged past their lifetime (never delivered/forwarded);
    /// quarantined broadcasts are floored to `max(age, unknown_downtime_age_ms)`.
    ///
    /// The fold then rewrites the log as a snapshot (decision I) whose closing
    /// marker carries `wall_now`, so its results are durable: an addressed
    /// observation it expired stays released after a later reboot, and a
    /// floored broadcast keeps its floor. If the snapshot fails, the fold
    /// still holds in memory, but clock markers carry no wall time until the
    /// next open, so that open takes the unknown path again.
    ///
    /// A `now` earlier than the store's time anchor is clamped to it.
    ///
    /// This runs exactly once; a second call returns the first report unchanged
    /// (see the `set_wall_clock_runs_once` test).
    ///
    /// Returns a [`FoldReport`]. The host prints it to stderr once and keeps it
    /// for records. A `downtime_ms` of `None` means the host could not
    /// establish the downtime and should warn rather than hide it.
    pub fn set_wall_clock(&mut self, wall_now: u64, now: Tick) -> FoldReport {
        // Runs once. The second call is a no-op and returns the first report.
        if let Some(report) = self.fold_report {
            return report;
        }

        let now = self.advance_anchor(now);
        let mut expired_addressed = 0usize;
        let mut floored_broadcasts = 0usize;
        let mut known_downtime_ms: Option<u64> = None;

        match self.last_marker {
            // There is a marker with a usable wall time. Downtime is known.
            Some((_marker_tick, Some(marker_wall))) if marker_wall <= wall_now => {
                let downtime_ms = wall_now.saturating_sub(marker_wall);
                // The wall time since the marker, minus the ticks the open fold
                // already counted (open tick past the marker tick), minus this
                // boot's ticks since open. Every quarantined entry's age already
                // includes the latter, whether or not a flush has rebased it in
                // between, so it is subtracted once for all (S2).
                let added = downtime_ms
                    .saturating_sub(self.counted_ticks)
                    .saturating_sub(now.saturating_sub(self.open_tick));
                for entry in self.index.values_mut() {
                    if !entry.quarantined {
                        continue;
                    }
                    entry.obs.age_ms = entry.obs.age_ms.saturating_add(added);
                    entry.quarantined = false;
                }
                known_downtime_ms = Some(downtime_ms);
            }
            _ => {
                // Unknown downtime: aged addressed observations expire; broadcasts
                // are floored to the unknown-downtime age floor.
                for entry in self.index.values_mut() {
                    if !entry.quarantined {
                        continue;
                    }
                    if entry.obs.dest.is_some() {
                        // Past its lifetime: never delivered/forwarded. Under
                        // unknown downtime the age of a held addressed
                        // observation cannot be verified against its recipients,
                        // so every held addressed observation is aged past its
                        // lifetime and released (withheld from delivery), exactly
                        // as if it had been delivered and acknowledged.
                        let lifetime = entry.obs.dest.as_ref().map(|d| d.lifetime_ms).unwrap_or(0);
                        // Saturating: at `lifetime_ms = u64::MAX` no age is
                        // past it, and `released` alone withholds it.
                        if entry.obs.age_ms <= lifetime {
                            entry.obs.age_ms = lifetime.saturating_add(1);
                            expired_addressed += 1;
                        }
                        entry.released = true;
                    } else {
                        if entry.obs.age_ms < self.unknown_downtime_age_ms {
                            entry.obs.age_ms = self.unknown_downtime_age_ms;
                            floored_broadcasts += 1;
                        }
                    }
                    entry.quarantined = false;
                }
            }
        }

        // Start wall-clock time for markers: future flushes write a derived
        // `wall_at_set + (now - tick_at_set)` and the marker records it. This
        // runs once, the first time set_wall_clock is called, *regardless* of
        // whether the downtime turned out to be known or unknown — the host has
        // a wall clock (that is why it called set_wall_clock), so future
        // markers carry it and a later reopen can establish a *known* downtime
        // (a marker only carries a wall once a wall has been set).
        self.wall_at_set = Some(wall_now);
        self.tick_at_set = Some(now);

        // Decision I: snapshot the folded state, so the fold is durable (an
        // expired message stays released, a floored broadcast keeps its floor)
        // and the closing marker carries the wall time.
        if self.write_snapshot(now, Some(wall_now)).is_err() {
            // The fold holds in memory, but not on disk. If later markers
            // carried a wall time, the next open would see a *known* downtime
            // and the unfolded records on disk would come back: an expired
            // message resurrected. So markers stay without a wall time for
            // this run, and the next open takes the unknown path again.
            self.wall_at_set = None;
            self.tick_at_set = None;
            eprintln!(
                "tines-store: failed to snapshot {} after the wall-clock fold; \
                 clock markers carry no wall time until the next open.",
                self.path.display()
            );
        }

        let report = FoldReport {
            downtime_ms: known_downtime_ms,
            expired_addressed,
            floored_broadcasts,
        };
        self.fold_report = Some(report);
        let downtime = match report.downtime_ms {
            Some(ms) => format!("known, {ms} ms"),
            None => "unknown — broadcasts floored to the unknown-downtime age floor".into(),
        };
        eprintln!(
            "tines-store: wall-clock downtime folded ({downtime}): {} addressed \
             observation(s) expired past lifetime, {} broadcast(s) floored; \
             quarantine lifted on held observations.",
            report.expired_addressed, report.floored_broadcasts,
        );
        report
    }

    /// Estimate the current wall time from peers' observations: the latest
    /// estimate among the observations that aren't quarantined and carry an
    /// `observed_at` (decision J).
    ///
    /// Each candidate's estimate is
    ///
    /// ```text
    /// observed_at + age_ms + (now - stored_at)
    /// ```
    ///
    /// i.e. the observer's wall time when it made the observation, plus its
    /// age when it arrived, plus the ticks elapsed since. Every term saturates.
    /// Because `age_ms` is a lower bound, an honest estimate is never later
    /// than the true time, so the latest estimate is the closest one.
    ///
    /// Only observations inserted since this open count: anything held from
    /// before it is quarantined, and its age isn't trusted yet.
    ///
    /// **Peer time is unauthenticated** until signatures land in 1.1: a peer
    /// can claim any `observed_at`, including one absurdly far ahead (this
    /// returns `u64::MAX` rather than overflowing). The host bounds the
    /// estimate and collects estimates for a short window before adopting
    /// the latest one with [`Self::set_wall_clock`] (spec §9).
    ///
    /// Returns `None` when no candidate carries an `observed_at` yet.
    pub fn peer_time_estimate(&self, now: Tick) -> Option<u64> {
        self.index
            .values()
            .filter(|entry| !entry.quarantined)
            .filter_map(|entry| {
                entry.obs.observed_at.map(|observed_at| {
                    observed_at
                        .saturating_add(entry.obs.age_ms)
                        .saturating_add(now.saturating_sub(entry.stored_at))
                })
            })
            .max()
    }

    /// The [`FoldReport`] from the (single) `set_wall_clock`, before it runs or
    /// otherwise (mirrors [`LogStore::fold_report`]).
    pub fn fold_report(&self) -> Option<FoldReport> {
        self.fold_report
    }
}

impl WallClockStore for LogStore {
    type FoldReport = FoldReport;

    fn set_wall_clock(&mut self, wall_now: u64, now: Tick) -> FoldReport {
        LogStore::set_wall_clock(self, wall_now, now)
    }

    fn peer_time_estimate(&self, now: Tick) -> Option<u64> {
        LogStore::peer_time_estimate(self, now)
    }
}

/// The log header: `magic ‖ format_version ‖ reserved`.
fn header_bytes() -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[0..8].copy_from_slice(&MAGIC);
    out[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    // out[10..12]: reserved for future use, zero.
    out
}

/// Whether `data` is a log with no records yet: empty, or a header torn by a
/// crash while the log was first created.
///
/// A torn header is shorter than [`HEADER_LEN`] and starts with (a prefix of)
/// the magic. Records only ever follow a complete header, so such a file holds
/// no data, and treating it as new loses nothing. Any other short file may not
/// be ours, so it is left to [`validate_header`] to refuse.
fn is_torn_new_log(data: &[u8]) -> bool {
    if data.len() >= HEADER_LEN {
        return false;
    }
    let n = data.len().min(MAGIC.len());
    data[..n] == MAGIC[..n]
}

/// The temporary file a snapshot is written to before it is renamed over the
/// log at `path`: same directory, so the rename is atomic.
fn snapshot_tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".snapshot-tmp");
    path.with_file_name(name)
}

/// Open the log at `path` (creating it, mode `0600`) and take its exclusive
/// lock.
///
/// The lock lives on the log file itself. A snapshot replaces that file by
/// rename, so the lock must follow it: the snapshot locks its temporary file
/// *before* renaming it over the log (see [`LogStore::write_snapshot`]), so
/// whatever file is at `path` is locked while the store is alive. A second
/// opener can still race a snapshot: it opens the old file, and the store
/// releases that file's lock right after the rename. So after locking, the
/// opener checks that its file is still the one at `path`. If not, it retries
/// on the new file, whose lock the live store holds.
fn open_locked(path: &Path) -> Result<File, StoreError> {
    const ATTEMPTS: usize = 8;
    for _ in 0..ATTEMPTS {
        let mut opts = OpenOptions::new();
        opts.read(true).create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts.open(path).map_err(|_| StoreError::Backend)?;
        // Enforce 0600 whether or not the file pre-existed.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|_| StoreError::Backend)?;
        }
        // Exclusive lock: a second opener must not interleave on the same file.
        file.try_lock().map_err(|_| {
            eprintln!(
                "tines-store: refusing to open {}: another process already \
                 holds the store lock (an earlier instance may still be \
                 running); delete nothing, just wait for it to release.",
                path.display()
            );
            StoreError::Backend
        })?;
        if is_file_at(&file, path) {
            return Ok(file);
        }
    }
    eprintln!(
        "tines-store: refusing to open {}: the log kept being replaced while \
         taking its lock; another process may be using it.",
        path.display()
    );
    Err(StoreError::Backend)
}

/// Whether `file` is the file currently at `path`, not one a rename replaced.
#[cfg(unix)]
fn is_file_at(file: &File, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (file.metadata(), std::fs::metadata(path)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

/// Without inode numbers there is nothing to compare; trust the open.
#[cfg(not(unix))]
fn is_file_at(_file: &File, _path: &Path) -> bool {
    true
}

/// Create `tmp_path` (mode `0600`, which must not exist yet), lock it, write
/// `bytes` and fsync it. Returns the locked handle, ready to append to once it
/// is renamed over the log.
fn write_locked_tmp(tmp_path: &Path, bytes: &[u8]) -> Result<File, StoreError> {
    let mut opts = OpenOptions::new();
    opts.read(true).create_new(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(tmp_path).map_err(|_| StoreError::Backend)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| StoreError::Backend)?;
    }
    file.try_lock().map_err(|_| StoreError::Backend)?;
    file.write_all(bytes).map_err(|_| StoreError::Backend)?;
    file.sync_all().map_err(|_| StoreError::Backend)?;
    Ok(file)
}

/// Reject a log whose header is missing, truncated, or an unknown version,
/// naming the fix (delete the log) in the message.
fn reject_bad_header(path: &Path, reason: &str) -> Result<(), StoreError> {
    eprintln!(
        "tines-store: log header invalid ({reason}): {} is not a v{} \
         'TINESLOG' log — delete it and start fresh (fleet-fixes F5).",
        path.display(),
        FORMAT_VERSION,
    );
    Err(StoreError::Backend)
}

/// Validate an existing header: `magic ‖ format_version ‖ reserved`.
fn validate_header(data: &[u8], path: &Path) -> Result<(), StoreError> {
    if data.len() < HEADER_LEN {
        return reject_bad_header(path, "truncated header");
    }
    if data[0..8] != MAGIC {
        return reject_bad_header(path, "unknown magic");
    }
    let version = u16::from_le_bytes([data[8], data[9]]);
    if version != FORMAT_VERSION {
        return reject_bad_header(
            path,
            &format!(
                "on-disk format_version {version} != {FORMAT_VERSION}: the \
                 Observation encoding changed in 1.0, so this log can no longer \
                 be decoded"
            ),
        );
    }
    Ok(())
}

/// Build the on-disk bytes for a record:
/// `sync ‖ len ‖ crc32c ‖ body`, where `crc32c` covers `len ‖ body`.
///
/// Returns [`StoreError::Backend`] if the body exceeds [`MAX_RECORD_BYTES`], so
/// the writer can never emit a record the reader would reject.
fn encode_record(record: &Record) -> Result<Vec<u8>, StoreError> {
    let body = postcard::to_stdvec(record).map_err(|_| StoreError::Backend)?;
    let len = body.len();
    // Write cap: never emit a record the reader would reject.
    if len > MAX_RECORD_BYTES {
        return Err(StoreError::Backend);
    }
    let sync_le = SYNC_MARKER.to_le_bytes();
    let len_le = (len as u32).to_le_bytes();
    // CRC-32C covers `len ‖ body`.
    let mut crc_in = Vec::with_capacity(4 + len);
    crc_in.extend_from_slice(&len_le);
    crc_in.extend_from_slice(&body);
    let crc = crc32c::crc32c(&crc_in);
    let crc_le = crc.to_le_bytes();
    let mut out = Vec::with_capacity(12 + len);
    out.extend_from_slice(&sync_le);
    out.extend_from_slice(&len_le);
    out.extend_from_slice(&crc_le);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Write a record as `sync ‖ len ‖ crc32c ‖ body`.
fn write_record(file: &mut File, record: &Record) -> Result<(), StoreError> {
    let bytes = encode_record(record)?;
    file.write_all(&bytes).map_err(|_| StoreError::Backend)?;
    Ok(())
}

/// Return the first well-formed record beginning at an offset >= `start`:
/// `(start, end, record)`, where `end` is the byte offset just past the record.
///
/// Scans forward byte-by-byte for a [`SYNC_MARKER`] whose length, CRC and
/// body-decode all verify, so a corrupt or torn record (bad marker, bad length,
/// bad CRC, or a body that fails to decode) is skipped rather than fatal.
/// Returns `None` if no such record exists in `data[start..]` — i.e. the
/// remainder is garbage or a torn tail.
fn parse_valid_record(data: &[u8], start: usize) -> Option<(usize, usize, Record)> {
    let marker = SYNC_MARKER.to_le_bytes();
    let mut pos = start;
    // A record needs at least its 12-byte header to even consider.
    while pos + 12 <= data.len() {
        if data[pos..pos + 4] != marker {
            pos += 1;
            continue;
        }
        let len = u32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]])
            as usize;
        // Reject an over-length declared body, *before* using it, so a corrupt
        // `0xFFFFFFFF` prefix cannot drive a bad slice/allocation.
        if len > MAX_RECORD_BYTES {
            pos += 1;
            continue;
        }
        let body_start = pos + 12;
        let body_end = body_start + len;
        // A body that runs past end-of-file is torn.
        if body_end > data.len() {
            pos += 1;
            continue;
        }
        let stored_crc =
            u32::from_le_bytes([data[pos + 8], data[pos + 9], data[pos + 10], data[pos + 11]]);
        // Verify the CRC over `len ‖ body` before trusting the record. The
        // CRC lives *between* len and body on disk, so concatenate the two
        // slices here to reproduce what the writer checksummed.
        let mut crc_in = Vec::with_capacity(4 + len);
        crc_in.extend_from_slice(&data[pos + 4..pos + 8]);
        crc_in.extend_from_slice(&data[pos + 12..body_end]);
        if crc32c::crc32c(&crc_in) != stored_crc {
            pos += 1;
            continue;
        }
        // CRC verified: decode the body. A decode failure means the body is
        // not a real record here, so keep scanning.
        match postcard::from_bytes::<Record>(&data[body_start..body_end]) {
            Ok(rec) => return Some((pos, body_end, rec)),
            Err(_) => {
                pos += 1;
            }
        }
    }
    None
}

/// Create the parent directory of `path` if it is missing (mode 0700 on unix).
/// Existing directories are left untouched. Walks up the path creating every
/// missing ancestor (so a multi-level parent path is created top-down).
fn ensure_parent_dir(path: &Path) -> Result<(), StoreError> {
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        // Collect the missing ancestors, parent-most first, then create them so
        // every parent exists before its child. `DirBuilder::create_all` is not
        // available on this toolchain, so build the chain manually.
        let mut missing: Vec<PathBuf> = Vec::new();
        let mut cur: Option<&Path> = Some(parent);
        while let Some(dir) = cur {
            if !dir.exists() {
                missing.push(dir.to_path_buf());
            }
            cur = dir.parent();
        }
        for dir in missing.iter().rev() {
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            // Ignore AlreadyExists: a racing create may have made it first.
            let _ = builder.create(dir);
        }
    }
    Ok(())
}

/// fsync the parent directory so a truncation/rewrite of a file in it is durable.
#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<(), StoreError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    let dir = File::open(parent).map_err(|_| StoreError::Backend)?;
    dir.sync_all().map_err(|_| StoreError::Backend)?;
    Ok(())
}

/// fsync a directory so a truncation/rewrite of a file in it is durable.
#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<(), StoreError> {
    Ok(())
}

impl Store for LogStore {
    /// Insert an observation.
    ///
    /// The record is appended to the log, but is durable across a power loss
    /// only after a call to [`Store::flush`]; hosts that need crash durability
    /// must flush before shutdown (see the durability contract in the module
    /// docs).
    fn insert(&mut self, obs: Observation, now: Tick) -> Result<bool, StoreError> {
        let id = obs.id();
        if self.index.contains_key(&id) {
            // Dedupe: keep the first-stored entry and its age lineage.
            return Ok(false);
        }
        // The id is recomputed from `obs` on load, so it is not stored here.
        let record = Record::Obs {
            obs: Box::new(obs.clone()),
            stored_at: now,
        };
        self.append(&record)?;
        self.advance_anchor(now);
        self.index.insert(
            id,
            Entry {
                obs,
                stored_at: now,
                released: false,
                // A fresh insert is live, not held. Only observations loaded
                // from disk while the wall clock was unknown start quarantined.
                quarantined: false,
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
            // A quarantined broadcast (held while the wall clock was unknown) is
            // withheld until [`LogStore::set_wall_clock`] lifts the quarantine.
            if entry.obs.dest.is_none() && !entry.quarantined {
                f(id);
            }
        }
    }

    /// Visit the ids of observations addressed to someone (`dest` is `Some`),
    /// excluding those already released.
    ///
    /// This is stricter than the [`Store`] trait's documented contract
    /// (`addressed` == `dest` is `Some`): a released addressed observation has
    /// already been sent to its recipient, so it is intentionally skipped and
    /// not offered for exchange again until (and if) it is re-inserted. A
    /// quarantined addressed observation is likewise withheld until the wall
    /// clock is trusted.
    fn for_each_addressed_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        for (id, entry) in self.index.iter() {
            if entry.obs.dest.is_some() && !entry.released && !entry.quarantined {
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
    /// release, it does not wait for a later [`Store::flush`].
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
        match self.index.get(id) {
            Some(entry) => {
                // A held (quarantined) observation is treated as released for
                // exchange purposes: it must not be delivered, forwarded or
                // acked again until the wall clock is trusted, exactly like a
                // released one (this is what stops a held addressed observation
                // from being re-offered and acked by a relay/peer).
                let addressed = entry.obs.dest.is_some();
                entry.released || (addressed && entry.quarantined)
            }
            None => false,
        }
    }

    fn for_each_topic(&self, prefix: &[u8], now: Tick, f: &mut dyn FnMut(&Observation)) {
        for entry in self.index.values() {
            // A quarantined observation is withheld until the wall clock is
            // trusted (see [`LogStore::set_wall_clock`]).
            if entry.obs.topic.starts_with(prefix) && !entry.quarantined {
                let elapsed = now.saturating_sub(entry.stored_at);
                f(&entry.obs.aged(elapsed));
            }
        }
    }

    /// Apply [`TrimPolicy`] rules in order (per-topic, age, count) and return
    /// the number of observations removed. Trimmed ids are not tombstoned:
    /// re-inserting them later is accepted.
    ///
    /// **Quarantined observations are exempt from every rule** (spec §9,
    /// decision L): `keep_per_topic`, `max_age_ms` and `max_count` all rank by
    /// age, and a quarantined observation's age isn't trusted until
    /// [`LogStore::set_wall_clock`] folds the downtime in. They are neither
    /// trimmed nor counted, so a store in quarantine can briefly hold more
    /// than `max_count`, until the fold.
    ///
    /// Each removed id appends a `Record::Trim` record, but that record is
    /// durable across a power loss only after a [`Store::flush`]; hosts that
    /// need crash durability must flush before shutdown.
    fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize {
        let mut removed: BTreeSet<ObservationId> = BTreeSet::new();

        // (a) keep_per_topic: per exact topic, drop everything but the `N`
        // youngest (lowest current age).
        if let Some(keep) = policy.keep_per_topic {
            let mut groups: BTreeMap<&[u8], Vec<ObservationId>> = BTreeMap::new();
            for (id, entry) in self.index.iter() {
                if !removed.contains(id) && !entry.quarantined {
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
                if !removed.contains(id)
                    && !entry.quarantined
                    && current_age(&entry.obs, entry.stored_at, now) > max_age
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
                .iter()
                .filter(|(id, entry)| !removed.contains(*id) && !entry.quarantined)
                .map(|(id, _)| *id)
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

    /// The durability point (spec §4).
    ///
    /// Clamps `now` to the time anchor (an earlier `now` than any tick the
    /// store has been given counts as that tick, so nothing moves backwards),
    /// rebases every entry's age to it in memory, appends a clock marker at
    /// `now` and fsyncs the log. Everything inserted, released or trimmed so
    /// far is then durable, and the next [`LogStore::open`] folds ages from
    /// that marker. After [`LogStore::set_wall_clock`] the marker carries the
    /// derived wall time `wall_at_set + (now - tick_at_set)`; before it, none.
    ///
    /// Flushing twice is harmless: it appends a second marker. A failure
    /// returns [`StoreError::Backend`] and leaves the store usable; the
    /// in-memory state is unchanged apart from the rebase, which doesn't
    /// change the age `get` reports at `now` or later.
    fn flush(&mut self, now: Tick) -> Result<(), StoreError> {
        let now = self.advance_anchor(now);
        self.rebase_to(now);
        // The marker records the caller's `now`, not the last insert tick
        // (that stale-marker bug was F18).
        let record = Record::Clock {
            tick: now,
            wall: self.marker_wall(now),
        };
        write_record(&mut self.file, &record)?;
        self.file.sync_all().map_err(|_| StoreError::Backend)?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    fn unique_path(tag: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key = n as u128 * 1_000_000 + nanos;
        let mut p = std::env::temp_dir();
        p.push(format!(
            "tines-store-unit-{}-{}-{}.log",
            tag,
            std::process::id(),
            key
        ));
        p
    }

    #[test]
    fn header_is_written_and_validated() {
        let path = unique_path("hdr");
        {
            let _s = LogStore::open(&path, 0).expect("open fresh");
        }
        let data = std::fs::read(&path).unwrap();
        assert_eq!(&data[0..8], &MAGIC);
        assert_eq!(u16::from_le_bytes([data[8], data[9]]), FORMAT_VERSION);
    }

    #[test]
    fn refuses_oversized_record_at_write() {
        // write_record must refuse a body over MAX_RECORD_BYTES with a Backend
        // error, so the writer can never emit a record the reader rejects.
        let path = unique_path("oversize");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&header_bytes()).unwrap();

        // A body exactly at the cap is accepted; one byte over is refused. The
        // framing entry point exercises the same cap guard the private
        // write_record uses (a real Record's postcard never reaches the cap).
        let mut body = vec![0u8; MAX_RECORD_BYTES];
        assert!(write_record_for_test(&mut f, &body).is_ok());
        body.push(0);
        assert!(write_record_for_test(&mut f, &body).is_err());
    }

    #[test]
    fn corrupt_middle_record_is_skipped() {
        // A corrupt record in the middle is skipped; the records around it
        // survive, and nothing is truncated (clean EOF after the tail).
        let path = unique_path("corrupt");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let a = obs([1; 16], 1, 0, Some(10));
            let b = obs([2; 16], 2, 0, Some(20));
            let c = obs([3; 16], 3, 0, Some(30));
            assert!(s.insert(a, 1000).unwrap());
            assert!(s.insert(b, 1001).unwrap());
            assert!(s.insert(c, 1002).unwrap());
            s.flush(1002).unwrap();
        }
        // Flip a byte in the body of the second data record (Obs(b)): the
        // open snapshot wrote Boot and Clock first, then Obs(a), Obs(b).
        corrupt_body(&path, 3);
        {
            let s = LogStore::open(&path, 3000).unwrap();
            assert!(
                s.contains(&obs([1; 16], 1, 0, Some(10)).id()),
                "record before corrupt survives"
            );
            assert!(
                !s.contains(&obs([2; 16], 2, 0, Some(20)).id()),
                "the corrupt record is dropped"
            );
            assert!(
                s.contains(&obs([3; 16], 3, 0, Some(30)).id()),
                "record after corrupt survives"
            );
            assert_eq!(s.skipped_records(), 1);
        }
    }

    #[test]
    fn torn_tail_is_truncated_not_skipped() {
        // A torn trailing record (header whose body ran past EOF) is truncated;
        // the records before it survive and nothing after it can exist.
        let path = unique_path("torn");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let a = obs([1; 16], 1, 0, Some(10));
            assert!(s.insert(a, 1000).unwrap());
            s.flush(1000).unwrap();
        }
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&SYNC_MARKER.to_le_bytes()).unwrap();
            f.write_all(&64u32.to_le_bytes()).unwrap();
            f.write_all(&[0u8; 20]).unwrap();
        }
        {
            let s = LogStore::open(&path, 2000).unwrap();
            assert!(s.contains(&obs([1; 16], 1, 0, Some(10)).id()));
            assert_eq!(
                s.skipped_records(),
                0,
                "a torn tail is truncated, not skipped"
            );
        }
    }

    #[test]
    fn unknown_header_is_rejected() {
        let path = unique_path("badhdr");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"NOTLOG!!").unwrap(); // not our magic
        }
        assert!(
            LogStore::open(&path, 0).is_err(),
            "an unknown header must be rejected"
        );
    }

    #[test]
    fn interrupted_snapshot_is_discarded() {
        // A crash before the snapshot's rename leaves a temporary file beside
        // an intact log. The next open ignores it (even a well-formed one) and
        // removes it.
        let path = unique_path("tmp_left");
        let a = obs([1; 16], 1, 0, None);
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            s.insert(a.clone(), 0).unwrap();
            s.flush(0).unwrap();
        }
        // A complete, valid log holding something else, never renamed.
        let tmp = snapshot_tmp_path(&path);
        {
            let other = unique_path("tmp_other");
            let mut s = LogStore::open(&other, 0).unwrap();
            s.insert(obs([2; 16], 2, 0, None), 0).unwrap();
            s.flush(0).unwrap();
            drop(s);
            std::fs::rename(&other, &tmp).unwrap();
        }
        let s = LogStore::open(&path, 0).unwrap();
        assert!(s.contains(&a.id()), "the log is what was renamed last");
        assert!(!s.contains(&obs([2; 16], 2, 0, None).id()));
        assert_eq!(s.len(), 1);
        assert!(!tmp.exists(), "the leftover is removed");
    }

    #[test]
    fn open_rewrites_the_log_as_a_snapshot() {
        // Trimmed records and the previous boots' records don't survive the
        // open snapshot: the log holds just the current state.
        let path = unique_path("compact");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            for seq in 0..20 {
                s.insert(obs([1; 16], seq, 0, None), 0).unwrap();
            }
            let mut policy = TrimPolicy::default();
            policy.max_count = Some(1);
            assert_eq!(s.trim(&policy, 0), 19);
            s.flush(0).unwrap();
        }
        let before = std::fs::metadata(&path).unwrap().len();
        drop(LogStore::open(&path, 0).unwrap());
        let after = std::fs::metadata(&path).unwrap().len();
        assert!(after < before, "{after} < {before}");
        let data = std::fs::read(&path).unwrap();
        let mut kinds = Vec::new();
        let mut pos = HEADER_LEN;
        while let Some((_, end, rec)) = parse_valid_record(&data, pos) {
            kinds.push(match rec {
                Record::Obs { .. } => "obs",
                Record::Trim { .. } => "trim",
                Record::Release { .. } => "release",
                Record::Boot { .. } => "boot",
                Record::Clock { wall, .. } => {
                    assert_eq!(wall, None, "the open snapshot's marker has no wall");
                    "clock"
                }
            });
            pos = end;
        }
        assert_eq!(kinds, ["boot", "obs", "clock"]);
    }

    #[cfg(unix)]
    #[test]
    fn a_lock_on_a_replaced_file_is_detected() {
        // The race `open_locked` guards against: an opener's file is renamed
        // over between its open and its lock.
        let path = unique_path("replaced");
        std::fs::write(&path, b"old").unwrap();
        let stale = File::open(&path).unwrap();
        assert!(is_file_at(&stale, &path));
        let other = unique_path("replacement");
        std::fs::write(&other, b"new").unwrap();
        std::fs::rename(&other, &path).unwrap();
        assert!(!is_file_at(&stale, &path));
    }

    fn obs(observer: [u8; 16], seq: u64, age: u64, at: Option<u64>) -> Observation {
        // `Observation` is `#[non_exhaustive]` outside `tines-core` (Task 5);
        // build it via the public constructor instead of a struct literal.
        let base = Observation::broadcast(observer, seq, b"door/3", b"open").aged(age);
        if let Some(at) = at {
            base.with_observed_at(at)
        } else {
            base
        }
    }

    /// Walk the records after the header and flip a byte in the body of the
    /// `n`-th record (0 = the Boot record). Corrupts the body, not the header.
    fn corrupt_body(path: &PathBuf, n: usize) {
        let mut data = std::fs::read(path).unwrap();
        let mut pos = HEADER_LEN;
        let mut i = 0;
        while i < n {
            assert!(pos + 12 <= data.len(), "record {} out of range", i);
            let len =
                u32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]])
                    as usize;
            pos += 12 + len;
            i += 1;
        }
        data[pos + 12] ^= 0xFF; // flip a body byte (body starts after 12-byte header)
        std::fs::write(path, data).unwrap();
    }
}

/// Conformance-suite shim for [`LogStore`].
///
/// Each suite case opens a brand-new temp log, so a case never inherits
/// records (or an age-fold state) from a previous one. Run with:
/// `cargo test -p tines-store --features conformance`.
#[cfg(all(test, feature = "conformance"))]
mod conformance_tests {
    use crate::log::LogStore;
    use std::path::PathBuf;
    use tines_core::store_conformance;

    fn unique_path() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key = n as u128 * 1_000_000 + nanos;
        let mut p = std::env::temp_dir();
        p.push(format!(
            "tines-log-conformance-{}-{}",
            std::process::id(),
            key
        ));
        p
    }

    fn make() -> impl FnMut() -> LogStore {
        // A fresh file (and thus a fresh age-fold state) on every call.
        move || LogStore::open(&unique_path(), 0).expect("open fresh log for conformance")
    }

    #[test]
    fn log_conforms() {
        store_conformance::insert_get(make());
        store_conformance::dedupe(make());
        store_conformance::age_lineage(make());
        store_conformance::saturating_ticks(make());
        store_conformance::release_surviving_get(make());
        store_conformance::sig_round_trip(make());
        store_conformance::trim_order(make());
        store_conformance::no_tombstones(make());
        store_conformance::idempotent_flush(make());
    }
}
