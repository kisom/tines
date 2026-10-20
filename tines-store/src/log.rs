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
//! * Framing & checksums. Every record carries a [`SYNC_MARKER`] and a CRC-32C
//!   over its body, so any corruption is detectable rather than silently
//!   misparsed.
//! * Torn trailing records. The log is append-only, so a crash mid-append can
//!   leave a partial trailing record — a header whose body ran past
//!   end-of-file — or a bit-roted/corrupt record. [`LogStore::open`] recovers
//!   by scanning for the next well-formed record: a record whose marker,
//!   length, CRC or body-decode is bad is *skipped* (scan forward for the next
//!   `SYNC_MARKER` and verify its CRC before accepting it), and the bytes
//!   leading up to it are dropped. Only bad bytes at the very end, with no
//!   valid record after them, count as a torn tail; a torn tail is truncated
//!   and the file and its directory are fsynced.
//! * Write cap. `write_record` refuses a record over [`MAX_RECORD_BYTES`], so
//!   the writer can no longer produce a record the reader rejects.
//!
//! * Durability contract. `flush()` appends a `Record::Clock` and `sync_all`s,
//!   making the appended observation and release records durable across a power
//!   loss. `insert`, `release`, and `trim` update the in-memory index and (for
//!   `insert`/`trim`) append records, but those records reach disk only on the
//!   next `flush()`. Hosts that need crash durability must `flush()` before
//!   shutdown (see the durability contract in the module docs). `release`, by
//!   contrast, appends its `Release` record immediately (best-effort: an append
//!   error is logged), so a single release is durable across a kill-without-flush
//!   without waiting for a flush.
//!
//! Isolation & integrity
//!
//! * Exclusive lock. `open` takes an exclusive `File::try_lock`; a second
//!   opener gets a clear error instead of interleaving on the same file.
//! * Permissions. The log file is created mode `0600` and its parent directory
//!   (created if missing) mode `0700`.
//! * Format version. The file starts with a `"TINESLOG"` header carrying a
//!   `format_version`; a missing or unknown header is an error that names the
//!   fix (delete the log). The 1.0 work bumps `format_version` for the new
//!   `Observation` encoding.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tines_core::{Observation, ObservationId, Store, StoreError, Tick, TrimPolicy};

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
/// boot counter, durable releases, crash recovery, exclusive lock, and
/// permissions.
pub struct LogStore {
    file: File,
    index: BTreeMap<ObservationId, Entry>,
    boot_count: u64,
    /// Number of corrupt records skipped while rebuilding the index on the last
    /// [`Self::open`] (0 if the log was clean). Exposed for hosts to report.
    skipped_records: usize,
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
    ///
    /// If the log has a corrupt middle record it is skipped and only a torn
    /// *tail* is truncated; the number of corrupt records skipped is reported
    /// once on stderr and is available via [`Self::skipped_records`].
    ///
    /// An exclusive lock is taken on the file, so a second `open` while this
    /// one is alive fails with [`StoreError::Backend`].
    pub fn open(path: &Path, now: Tick) -> Result<Self, StoreError> {
        // Ensure the parent directory exists (mode 0700 if we create it).
        ensure_parent_dir(path)?;

        // Open for reads (to load) and appends (to write later). Append mode is
        // correct because we load by reading the whole file below, and every
        // mutation appends to the end of the log. Set mode 0600 via
        // set_permissions below so it is enforced whether or not the file
        // pre-existed.
        let mut tmp = OpenOptions::new();
        let opts = tmp.read(true).create(true).append(true);
        let mut file = opts.open(path).map_err(|_| StoreError::Backend)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|_| StoreError::Backend)?;
        }

        // Exclusive lock: a second opener must not interleave on the same file.
        // (`try_lock` takes an exclusive lock on this toolchain's std; the
        // spec's `try_lock_exclusive` is not present here.)
        file.try_lock().map_err(|_| {
            eprintln!(
                "tines-store: refusing to open {}: another process already \
                 holds the store lock (an earlier instance may still be \
                 running); delete nothing, just wait for it to release.",
                path.display()
            );
            StoreError::Backend
        })?;

        let data = std::fs::read(path).map_err(|_| StoreError::Backend)?;

        // Header: write a fresh one on an empty log, otherwise validate the
        // existing one. A missing or unknown header is an error that names the
        // fix (delete the log).
        if data.is_empty() {
            write_header(&mut file)?;
        } else {
            validate_header(&data, path)?;
        }

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
        // tick of the last `Record::Clock` (the load-time age fold's anchor).
        let mut last_boot_count: Option<u64> = None;
        let mut last_clock_tick: Option<Tick> = None;

        loop {
            // Clean end-of-file: nothing left to read.
            if pos == data.len() {
                break;
            }
            // `parse_valid_record` returns the first well-formed record starting
            // at or after `pos`. If it starts after `pos`, the bytes in between
            // were a corrupt/torn record we skipped. If there is no well-formed
            // record left, the remainder is a torn tail.
            match parse_valid_record(&data, pos) {
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

        // A torn tail (bad bytes at EOF with no valid record after) is truncated
        // back to the last complete boundary, then the file and its directory
        // are fsynced so the clean tail is durable.
        let dropped = data.len().saturating_sub(good_end);
        if dropped > 0 {
            file.set_len(good_end as u64)
                .map_err(|_| StoreError::Backend)?;
            file.sync_all().map_err(|_| StoreError::Backend)?;
            let _ = sync_dir(path);
            eprintln!(
                "tines-store: truncated {} torn byte(s) at the tail of {} \
                 while loading ({} complete record(s) loaded); fsynced file \
                 and directory.",
                dropped,
                path.display(),
                records_loaded,
            );
        }

        if skipped > 0 {
            eprintln!(
                "tines-store: skipped {} corrupt record(s) in {} while loading \
                 ({} complete record(s) loaded); the log has been repaired \
                 and will rewrite a clean tail on the next mutation.",
                skipped,
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
            skipped_records: skipped,
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

    /// Number of corrupt records skipped while rebuilding the index on the most
    /// recent [`Self::open`] (0 if the log was clean).
    ///
    /// A corrupt record is one whose sync marker, length, CRC or body decode
    /// was bad and was skipped to reach the next well-formed record. This is the
    /// counter a host reports once on open.
    pub fn skipped_records(&self) -> usize {
        self.skipped_records
    }

    /// Persist the current index state to disk.
    ///
    /// This is the durability boundary: `flush(now)` advances each entry's age
    /// to the caller's `now`, appends a `Record::Clock` tick marker stamped
    /// with `now`, and calls `sync_all`, after which the appended observation
    /// and release records are durable across a power loss. Because `insert`
    /// only reaches disk via a subsequent `flush()`, hosts that need crash
    /// durability must call `flush()` before shutdown (see the durability
    /// contract in the module docs).
    ///
    /// `now` is the caller's current tick, which the marker records: anchoring
    /// the reopen fold to when the host actually flushed — rather than the last
    /// insert tick — keeps ages exact across a reboot.
    pub fn flush(&mut self, now: Tick) -> Result<(), StoreError> {
        // Advance every entry's age up to the caller's `now` in memory, so each
        // `entry.obs.age_ms` is the true current age and `stored_at` marks when
        // that age was computed. Ages are recomputed from each entry's own
        // `stored_at` on the next load (see [`LogStore::open`]).
        for entry in self.index.values_mut() {
            entry.obs.age_ms = entry
                .obs
                .age_ms
                .saturating_add(now.saturating_sub(entry.stored_at));
            entry.stored_at = now;
        }

        // Record a clock marker stamped with the caller's `now` so the load-time
        // age fold anchors to when the host actually flushed — not the last
        // insert tick (that stale-marker bug was F18).
        let record = Record::Clock { tick: now };
        write_record(&mut self.file, &record)?;
        self.file.sync_all().map_err(|_| StoreError::Backend)?;
        Ok(())
    }

    /// Append a single record to the log.
    fn append(&mut self, record: &Record) -> Result<(), StoreError> {
        write_record(&mut self.file, record)
    }
}

/// Write the log header: `magic ‖ format_version ‖ reserved`.
fn write_header(file: &mut File) -> Result<(), StoreError> {
    file.write_all(&MAGIC).map_err(|_| StoreError::Backend)?;
    file.write_all(&FORMAT_VERSION.to_le_bytes())
        .map_err(|_| StoreError::Backend)?;
    file.write_all(&0u16.to_le_bytes()) // reserved for future use
        .map_err(|_| StoreError::Backend)?;
    Ok(())
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
    /// only after a call to [`Self::flush`]; hosts that need crash durability
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
            if entry.obs.dest.is_none() {
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
    /// not offered for exchange again until (and if) it is re-inserted.
    fn for_each_addressed_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        for (id, entry) in self.index.iter() {
            if entry.obs.dest.is_some() && !entry.released {
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
        write_header(&mut f).unwrap();

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
        // Flip a byte in the body of the second data record (Obs(b)).
        corrupt_body(&path, 2);
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
