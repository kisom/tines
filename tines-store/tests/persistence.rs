//! Persistence tests for the append-only [`LogStore`] backend.
//!
//! Each test runs against its own unique file under the temp directory so the
//! parallel test runner cannot collide on disk state.

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use tines_core::{Observation, Store};
use tines_store::LogStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

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

fn store_path(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let key = n as u128 * 1_000_000 + nanos;
    let mut p = std::env::temp_dir();
    p.push(format!(
        "tines-store-{}-{}-{}.log",
        tag,
        std::process::id(),
        key
    ));
    p
}

/// A log whose header declares an older `format_version` must be refused on
/// open. The 1.0 work changed the Observation encoding (the `dest` field
/// replaces the old `to`/`lifetime_ms` pair), so a v1 log's records can no
/// longer be decoded: `open` fails and names the fix (delete the log).
#[test]
fn old_format_is_refused() {
    let path = store_path("oldfmt");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        // magic ‖ format_version (v1, little-endian) ‖ reserved: a genuine v1
        // header that this build (v2) no longer accepts.
        f.write_all(b"TINESLOG").unwrap();
        f.write_all(&1u16.to_le_bytes()).unwrap();
        f.write_all(&0u16.to_le_bytes()).unwrap();
        f.flush().unwrap();
    }
    assert!(
        LogStore::open(&path, 0).is_err(),
        "open must refuse a log whose format_version is older than this build"
    );
}

#[test]
fn reopen_preserves_store() {
    let path = store_path("reopen");

    {
        let mut s = LogStore::open(&path, 0).expect("open fresh store");
        let a = obs([1; 16], 1, 0, Some(10));
        let b = obs([2; 16], 2, 0, Some(20));
        let ib = b.id();

        assert!(s.insert(a, 1000).unwrap());
        assert!(s.insert(b, 1001).unwrap());
        assert!(!s.is_released(&ib));
        s.release(&ib);
        assert!(s.is_released(&ib));
        s.flush(1001).expect("flush before drop");
    }

    {
        let s = LogStore::open(&path, 2000).expect("reopen store");
        let a = obs([1; 16], 1, 0, Some(10));
        let b = obs([2; 16], 2, 0, Some(20));
        assert!(s.contains(&a.id()));
        assert!(s.contains(&b.id()));
        assert!(!s.is_released(&a.id()));
        assert!(s.is_released(&b.id()));
    }
}

#[test]
fn ages_rebase_on_load() {
    let path = store_path("ages");

    {
        let mut s = LogStore::open(&path, 1000).unwrap();
        let o = obs([1; 16], 1, 500, Some(10));
        assert!(s.insert(o, 1000).unwrap());
        s.flush(1000).unwrap();
    }

    {
        let s = LogStore::open(&path, 5000).unwrap();
        let o = obs([1; 16], 1, 500, Some(10));
        let id = o.id();
        // 500ms original age plus the 4000ms that elapsed while the store was
        // closed (5000 - 1000). A store that forgot to fold the downtime in
        // would report only the frozen 500ms.
        let g = s.get(&id, 5000).unwrap();
        assert_eq!(g.age_ms, 4500);
    }
}

#[test]
fn ages_rebase_after_nonidle_flush() {
    // An observation inserted *before* the last flush must lose no time when
    // ages are rebased on load: the reopen fold has to be exact, not folded
    // against the flush clock as if the entry had been inserted then.
    let path = store_path("ages_nonidle");

    {
        let mut s = LogStore::open(&path, 1000).unwrap();
        let a = obs([1; 16], 1, 100, Some(10));
        assert!(s.insert(a, 1000).unwrap());
        // Advance the store clock via a second, distinct insert so the flush
        // below runs at a tick later than `a`'s insert — the store is
        // non-idle.
        let b = obs([2; 16], 2, 0, Some(20));
        assert!(s.insert(b, 3000).unwrap());
        s.flush(3000).unwrap();
    }

    {
        // Reopen at 9000. `a`'s true age is 100 (insert-time) + (9000 - 1000)
        // = 8100. A flush that fails to catch `age_ms` up folds the reopen
        // against the flush clock (3000) and under-counts by the 2000ms
        // between insert and flush, reporting only 6100.
        let s = LogStore::open(&path, 9000).unwrap();
        let a = obs([1; 16], 1, 100, Some(10));
        let g = s.get(&a.id(), 9000).unwrap();
        assert_eq!(g.age_ms, 8100);
    }
}

#[test]
fn ages_include_time_to_clock_marker() {
    // A flush records a Clock marker; on load the age fold has to recover the
    // elapsed time from each observation's insert tick up to that marker as
    // well as from the marker to now, so an observation idle across the flush
    // keeps its true age rather than freezing at the flush tick.
    let path = store_path("clock_age");

    {
        let mut s = LogStore::open(&path, 1000).unwrap();
        let o = obs([1; 16], 1, 500, Some(10));
        assert!(s.insert(o, 1000).unwrap());
        // A second insert advances the store clock to 3000; the flush below
        // records a Clock marker at that tick even though `o` was inserted
        // earlier (at 1000) and sits idle until the store is dropped.
        let d = obs([2; 16], 2, 0, Some(20));
        assert!(s.insert(d, 3000).unwrap());
        s.flush(3000).unwrap();
    }

    {
        // Reopen at 9000. `o`'s true age is 500 (insert-time) + (9000 - 1000)
        // = 8500; the marker at 3000 must fold both the pre-flush 2000ms and
        // the post-flush 6000ms.
        let s = LogStore::open(&path, 9000).unwrap();
        let o = obs([1; 16], 1, 500, Some(10));
        let g = s.get(&o.id(), 9000).unwrap();
        assert_eq!(g.age_ms, 8500);
    }
}

#[test]
fn boot_counter_first_seq_increases_and_no_collision() {
    // A boot counter makes `first_seq` strictly increase across reboots, so a
    // resumed node's sequence numbers land in a disjoint range and cannot
    // collide (content-id dedupe) with the previous boot's observations.
    let path = store_path("boot");

    let first1;
    let first2;

    // Boot 1: first_seq is 1 << 32.
    {
        let mut s = LogStore::open(&path, 0).expect("open boot 1");
        first1 = s.first_seq();
        assert_eq!(first1, 1u64 << 32);
        // A node seeded here observes first1, first1 + 1, ...
        let a = obs([1; 16], first1, 0, Some(10));
        let a2 = obs([1; 16], first1 + 1, 0, Some(10));
        assert!(s.insert(a, 10).unwrap());
        assert!(s.insert(a2, 11).unwrap());
        drop(s);
    }

    // Boot 2: first_seq strictly increased. A fresh observation at the new
    // first_seq is a *new* id (Ok(true)), not deduped against boot 1's ids
    // that would happen if the seq range were reused.
    {
        let mut s = LogStore::open(&path, 100).expect("open boot 2");
        first2 = s.first_seq();
        assert!(first2 > first1);
        let c = obs([1; 16], first2, 0, Some(20));
        assert!(
            s.insert(c, 101).unwrap(),
            "first_seq must advance so boot-2 ids don't collide with boot-1 ids"
        );
        // Boot 1's observations are still present (content-addressed).
        assert!(s.contains(&obs([1; 16], first1, 0, Some(10)).id()));
        drop(s);
    }

    // Boot 3: still strictly increasing.
    {
        let s = LogStore::open(&path, 200).expect("open boot 3");
        assert!(s.first_seq() > first2);
    }
}

#[test]
fn release_survives_kill_without_flush() {
    // release appends a durable Release record immediately, so a release that
    // is never followed by a flush (a kill without graceful shutdown) still
    // survives a reopen.
    let path = store_path("release");

    {
        let mut s = LogStore::open(&path, 0).expect("open release store");
        let a = obs([1; 16], 1, 0, Some(10));
        let b = obs([2; 16], 2, 0, Some(20));
        assert!(s.insert(a, 100).unwrap());
        assert!(s.insert(b.clone(), 101).unwrap());
        let ib = b.id();
        s.release(&ib);
        assert!(s.is_released(&ib));
        // Drop WITHOUT flush: the release record must already be on disk
        // (appended immediately), not waiting for a flush to persist it.
        drop(s);
    }

    {
        let s = LogStore::open(&path, 200).expect("reopen after kill w/o flush");
        let b = obs([2; 16], 2, 0, Some(20));
        assert!(s.contains(&b.id()));
        assert!(
            s.is_released(&b.id()),
            "a release must survive a kill without a flush"
        );
    }
}

#[test]
fn reopen_recovers_from_torn_trailing_record() {
    // A crash mid-append can leave a partial trailing record — a length prefix
    // whose body never reached disk. Reopen must truncate the torn tail and
    // load the complete records, not fail the whole store.
    let path = store_path("torn");

    {
        let mut s = LogStore::open(&path, 0).expect("open fresh store");
        let a = obs([1; 16], 1, 0, Some(10));
        let b = obs([2; 16], 2, 0, Some(20));
        assert!(s.insert(a, 1000).unwrap());
        assert!(s.insert(b, 1001).unwrap());
        s.flush(1001).expect("flush before drop");
    }

    // Simulate a torn append: write a length prefix claiming 64 body bytes but
    // only write 20 of them. The body runs past end-of-file.
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open log for torn write");
        f.write_all(&64u32.to_le_bytes())
            .expect("write torn prefix");
        f.write_all(&[0u8; 20]).expect("write partial body");
        f.flush().expect("flush torn tail");
    }

    // Reopen: the torn tail is truncated, the two complete records survive.
    // The boot counter has advanced one boot (this is the store's second open).
    {
        let s = LogStore::open(&path, 3000).expect("reopen after torn tail");
        assert!(s.contains(&obs([1; 16], 1, 0, Some(10)).id()));
        assert!(s.contains(&obs([2; 16], 2, 0, Some(20)).id()));
        assert_eq!(s.first_seq(), 2u64 << 32);
    }
}

#[test]
fn reopen_recovers_from_oversized_record_length() {
    // A corrupt length prefix (e.g. u32::MAX) must neither trigger a giant
    // allocation nor make the store unloadable; it should be dropped like a
    // torn record, leaving the earlier complete records intact.
    let path = store_path("oversize");

    {
        let mut s = LogStore::open(&path, 0).expect("open fresh store");
        let a = obs([1; 16], 1, 0, Some(10));
        assert!(s.insert(a, 1000).unwrap());
        s.flush(1000).expect("flush before drop");
    }

    {
        // Declare a body of u32::MAX bytes with no actual body. parse_record
        // must reject it before allocating any buffer.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open log for oversized write");
        f.write_all(&u32::MAX.to_le_bytes())
            .expect("write oversized length prefix");
        f.flush().expect("flush oversized tail");
    }

    {
        let s = LogStore::open(&path, 3000).expect("reopen after oversized prefix");
        assert!(s.contains(&obs([1; 16], 1, 0, Some(10)).id()));
    }
}

// The five F5 integrity/recovery tests. They run on unix only: they exercise
// the exclusive lock and file permissions, which are unix APIs.
#[cfg(unix)]
mod fleet_fixes_f5 {
    use super::*;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use tines_core::{Observation, Store};
    use tines_store::LogStore;

    fn a_obs(observer: [u8; 16], seq: u64) -> Observation {
        // `Observation` is `#[non_exhaustive]` outside `tines-core` (Task 5);
        // build it via the public constructor instead of a struct literal.
        Observation::broadcast(observer, seq, b"door/3", b"open").with_observed_at(10)
    }

    /// Flip a byte in the body of the `n`-th record after the header (0 = Boot),
    /// mirroring on-disk corruption of a complete record.
    fn corrupt_record(path: &Path, n: usize) {
        let mut data = std::fs::read(path).expect("read log");
        let mut pos = 12usize; // past the header
        let mut i = 0;
        while i < n {
            assert!(pos + 12 <= data.len(), "record {i} out of range");
            let len =
                u32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]])
                    as usize;
            pos += 12 + len;
            i += 1;
        }
        assert!(pos + 12 <= data.len(), "record {n} out of range");
        data[pos + 12] ^= 0xFF; // a body byte (after the 12-byte record header)
        std::fs::write(path, data).expect("write corrupted log");
    }

    #[test]
    fn corrupt_middle_record_later_records_survive() {
        // A corrupt record in the middle costs that record only: the records
        // before and after it survive, and nothing is truncated.
        let path = store_path("f5_corrupt");
        {
            let mut s = LogStore::open(&path, 0).expect("open fresh");
            assert!(s.insert(a_obs([1; 16], 1), 1000).unwrap());
            assert!(s.insert(a_obs([2; 16], 2), 1001).unwrap());
            assert!(s.insert(a_obs([3; 16], 3), 1002).unwrap());
            s.flush(1002).expect("flush");
        }
        // Corrupt the middle observation (record index 2: Boot, obs-1, obs-2).
        corrupt_record(&path, 2);
        {
            let s = LogStore::open(&path, 3000).expect("reopen");
            assert!(
                s.contains(&a_obs([1; 16], 1).id()),
                "earlier record survives"
            );
            assert!(
                !s.contains(&a_obs([2; 16], 2).id()),
                "the corrupt record is dropped"
            );
            assert!(s.contains(&a_obs([3; 16], 3).id()), "later record survives");
            assert_eq!(s.skipped_records(), 1);
        }
    }

    #[test]
    fn torn_tail_is_truncated_on_reopen() {
        // A trailing record whose body ran past EOF is truncated; the complete
        // records before it survive, and it is truncated (not skipped).
        let path = store_path("f5_torn");
        {
            let mut s = LogStore::open(&path, 0).expect("open fresh");
            assert!(s.insert(a_obs([1; 16], 1), 1000).unwrap());
            assert!(s.insert(a_obs([2; 16], 2), 1001).unwrap());
            s.flush(1001).expect("flush");
        }
        // Append a record header (sync + len = 64) with only 20 of the body bytes.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .expect("open for torn write");
            f.write_all(&tines_store::log::SYNC_MARKER.to_le_bytes())
                .expect("torn sync");
            f.write_all(&64u32.to_le_bytes()).expect("torn len");
            f.write_all(&[0u8; 20]).expect("torn partial body");
        }
        {
            let s = LogStore::open(&path, 4000).expect("reopen");
            assert!(s.contains(&a_obs([1; 16], 1).id()));
            assert!(s.contains(&a_obs([2; 16], 2).id()));
            assert_eq!(
                s.skipped_records(),
                0,
                "a torn tail is truncated, never counted as a skipped record"
            );
        }
    }

    #[test]
    fn second_open_fails_while_locked() {
        // `open` takes an exclusive lock; a second opener fails while the first
        // still holds it.
        let path = store_path("f5_lock");
        {
            let mut first = LogStore::open(&path, 0).expect("open first");
            assert!(first.insert(a_obs([1; 16], 1), 1000).unwrap());
            assert!(
                LogStore::open(&path, 1).is_err(),
                "a second open must fail while the first holds the lock"
            );
            assert!(
                LogStore::open(&path, 2).is_err(),
                "a third open must also fail"
            );
            // Keep the lock held across a read to prove the first still works.
            assert!(first.contains(&a_obs([1; 16], 1).id()));
            drop(first);
        }
        // After the first opener drops, the lock is released and reopen works.
        let _ = LogStore::open(&path, 3).expect("open after lock released");
    }

    #[test]
    fn log_file_is_mode_0600() {
        // The log file is created mode 0600.
        let path = store_path("f5_perms");
        {
            let _s = LogStore::open(&path, 0).expect("open");
        }
        let mode = std::fs::metadata(&path).unwrap().mode() & 0o777;
        assert_eq!(mode, 0o600, "log file must be mode 0600, got {mode:o}");
    }

    #[test]
    fn oversized_record_is_refused_at_write() {
        // The framing refuses to emit a record whose body exceeds
        // MAX_RECORD_BYTES, so a writer can no longer produce a record the
        // reader would reject. The store only writes bounded records, so this
        // is exercised against the public framing entry point.
        use tines_store::log::write_record_for_test;

        let path = store_path("f5_oversize");
        std::fs::File::create(&path).unwrap();
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();

        // Exactly at the cap is accepted.
        let ok_body = vec![0u8; tines_store::log::MAX_RECORD_BYTES];
        assert!(write_record_for_test(&mut f, &ok_body).is_ok());

        // One byte over the cap is refused.
        let too_big = vec![0u8; tines_store::log::MAX_RECORD_BYTES + 1];
        assert!(
            write_record_for_test(&mut f, &too_big).is_err(),
            "a body over MAX_RECORD_BYTES must be refused at write"
        );
    }
}

// The quarantine + downtime-fold tests (Task 10). They reopen logs that were
// written while "down" and inspect the held/quarantined state before and after
// the host trusts a wall clock. The marker is established through the public
// API (set_wall_clock + flush) so nothing here reaches into crate internals.
mod quarantine_and_downtime {
    use super::*;
    use tines_core::Store;
    use tines_store::LogStore;

    fn addressed(observer: [u8; 16], seq: u64, to: [u8; 16], lifetime: u64) -> Observation {
        Observation::addressed(observer, seq, b"task", b"go", to, lifetime)
    }

    /// Held (quarantined) observations are withheld from every delivery pass
    /// until the host trusts a wall clock.
    #[test]
    fn held_is_quarantined_until_wall_clock() {
        let path = store_path("held_quarantined");
        // Seed a broadcast and an addressed observation, then trust a wall
        // clock so the marker below carries a wall (a *known* downtime on
        // reopen) rather than a bare one.
        {
            let mut s = LogStore::open(&path, 0).expect("open fresh");
            let b = obs([1; 16], 1, 0, Some(0));
            let a = addressed([2; 16], 1, [3; 16], 60_000).with_observed_at(0);
            s.insert(b, 0).unwrap();
            s.insert(a, 0).unwrap();
            s.set_wall_clock(500, 0);
            s.flush(0).unwrap();
        }

        // Reopen after a downtime. Until set_wall_clock the copies are held.
        {
            let mut s = LogStore::open(&path, 1000).expect("reopen");
            let b_id = obs([1; 16], 1, 0, Some(0)).id();
            let a_id = addressed([2; 16], 1, [3; 16], 60_000).id();

            assert!(s.contains(&b_id), "broadcast still present while held");
            assert!(s.contains(&a_id), "addressed still present while held");
            assert!(
                s.is_released(&a_id),
                "a held addressed observation is treated as released"
            );

            let mut broadcasts = 0usize;
            s.for_each_broadcast_id(&mut |_| broadcasts += 1);
            assert_eq!(broadcasts, 0, "held broadcasts are not delivered");
            let mut addressed = 0usize;
            s.for_each_addressed_id(&mut |_| addressed += 1);
            assert_eq!(
                addressed, 0,
                "held addressed observations are not delivered"
            );
            let mut topics = 0usize;
            s.for_each_topic(b"", 1000, &mut |_obs| {
                topics += 1;
            });
            assert_eq!(topics, 0, "held observations are not offered for exchange");

            // Trust the wall clock: the downtime folds and the quarantine lifts.
            let report = s.set_wall_clock(1000, 1000);
            assert_eq!(
                report.downtime_ms,
                Some(500),
                "known downtime since the marker"
            );

            let mut broadcasts = 0usize;
            s.for_each_broadcast_id(&mut |_| broadcasts += 1);
            assert_eq!(
                broadcasts, 1,
                "the broadcast is delivered again after trust"
            );
            let mut addressed = 0usize;
            s.for_each_addressed_id(&mut |_| addressed += 1);
            assert_eq!(addressed, 1, "the addressed observation is delivered again");
            let mut topics = 0usize;
            s.for_each_topic(b"", 1000, &mut |_obs| {
                topics += 1;
            });
            assert!(topics >= 1, "topics are visited after trust");
        }
    }

    /// An observation inserted into a freshly opened store is never held.
    #[test]
    fn new_inserts_not_quarantined() {
        let path = store_path("fresh_insert");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let b = obs([1; 16], 1, 0, Some(0));
            let a = addressed([2; 16], 1, [3; 16], 60_000).with_observed_at(0);
            let a_id = a.id();
            assert!(s.insert(b.clone(), 0).unwrap());
            assert!(s.insert(a.clone(), 0).unwrap());

            // Delivered at once: nothing was held across a downtime.
            let mut broadcasts = 0usize;
            s.for_each_broadcast_id(&mut |_| broadcasts += 1);
            assert_eq!(broadcasts, 1);
            let mut addressed = 0usize;
            s.for_each_addressed_id(&mut |_| addressed += 1);
            assert_eq!(addressed, 1);

            // The fold cannot touch a held observation that was never held.
            let before_b = s.get(&b.id(), 0).unwrap().age_ms;
            let before_a = s.get(&a_id, 0).unwrap().age_ms;
            s.set_wall_clock(2000, 0);
            assert_eq!(
                s.get(&b.id(), 0).unwrap().age_ms,
                before_b,
                "the fold leaves freshly-inserted broadcasts alone"
            );
            assert_eq!(
                s.get(&a_id, 0).unwrap().age_ms,
                before_a,
                "the fold leaves freshly-inserted addressed observations alone"
            );
        }
    }

    /// With a known downtime, set_wall_clock folds the wall-clock elapsed into
    /// every held age.
    #[test]
    fn downtime_known_advances_ages() {
        let path = store_path("known_downtime");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let b = obs([1; 16], 1, 0, Some(0));
            s.insert(b, 0).unwrap();
            // Trust a wall clock at 1000 so the marker below carries a wall.
            s.set_wall_clock(1000, 0);
            s.flush(0).unwrap();
        }
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let before = s.get(&obs([1; 16], 1, 0, Some(0)).id(), 0).unwrap().age_ms;
            assert_eq!(before, 0);

            // The caller clock is frozen at 0 while the wall climbs 1000 -> 2000.
            let report = s.set_wall_clock(2000, 0);
            assert_eq!(report.downtime_ms, Some(1000));
            let after = s.get(&obs([1; 16], 1, 0, Some(0)).id(), 0).unwrap().age_ms;
            // Held age advanced by the wall downtime (2000 - 1000 = 1000),
            // minus the ticks already counted during reopen (0).
            assert_eq!(after, 1000);
        }
    }

    /// Without a marker, the downtime is unknown and held addressed observations
    /// expire (and are counted as such).
    #[test]
    fn downtime_unknown_expires_addressed() {
        let path = store_path("unknown_expires");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let a = addressed([2; 16], 1, [3; 16], 10_000).with_observed_at(0);
            assert!(s.insert(a, 0).unwrap());
            // Drop without flushing: no Clock marker on disk.
            drop(s);
        }
        {
            let mut s = LogStore::open(&path, 2000).unwrap();
            let mut delivered = 0usize;
            s.for_each_addressed_id(&mut |_| delivered += 1);
            assert_eq!(
                delivered, 0,
                "a held addressed observation is never delivered"
            );

            let report = s.set_wall_clock(2000, 2000);
            assert_eq!(
                report.downtime_ms, None,
                "no marker on disk -> unknown downtime"
            );
            assert_eq!(
                report.expired_addressed, 1,
                "the held addressed observation is aged past its lifetime"
            );
            let mut delivered = 0usize;
            s.for_each_addressed_id(&mut |_| delivered += 1);
            assert_eq!(
                delivered, 0,
                "an expired held observation is never delivered"
            );
        }
    }

    /// A marker without a wall time is treated as unknown downtime, so held
    /// addressed observations expire.
    #[test]
    fn marker_without_wall_is_unknown() {
        let path = store_path("marker_no_wall");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let a = addressed([2; 16], 1, [3; 16], 10_000).with_observed_at(0);
            assert!(s.insert(a, 0).unwrap());
            // Flush BEFORE set_wall_clock: the marker carries wall:None.
            s.flush(0).unwrap();
        }
        {
            let mut s = LogStore::open(&path, 2000).unwrap();
            let report = s.set_wall_clock(2000, 2000);
            assert_eq!(
                report.downtime_ms, None,
                "a marker without a wall is unknown"
            );
            assert_eq!(report.expired_addressed, 1);
            let mut delivered = 0usize;
            s.for_each_addressed_id(&mut |_| delivered += 1);
            assert_eq!(delivered, 0);
        }
    }

    /// Without a known downtime, a held broadcast is floored to the configured
    /// unknown-downtime age floor.
    #[test]
    fn downtime_unknown_floors_broadcasts() {
        let path = store_path("floor_broadcasts");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let b = obs([1; 16], 1, 0, Some(0));
            assert!(s.insert(b, 0).unwrap());
            s.flush(0).unwrap();
        }
        {
            // Configure the floor explicitly via open_with (the builder's
            // setter is unexposed; open_with reaches the same field).
            let mut s = LogStore::open_with(&path, 2000, 30_000).unwrap();
            let report = s.set_wall_clock(2000, 2000);
            assert_eq!(report.downtime_ms, None);
            assert_eq!(report.floored_broadcasts, 1, "a held broadcast is floored");
            let g = s.get(&obs([1; 16], 1, 0, Some(0)).id(), 2000).unwrap();
            assert_eq!(
                g.age_ms, 30_000,
                "the held broadcast's age is raised to unknown_downtime_age_ms"
            );
        }
    }

    /// A marker whose wall time is ahead of now means the clock went backwards;
    /// the downtime is unknown.
    #[test]
    fn clock_went_backwards_is_unknown() {
        let path = store_path("clock_backwards");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let a = addressed([2; 16], 1, [3; 16], 10_000).with_observed_at(0);
            assert!(s.insert(a, 0).unwrap());
            // Trust the wall at 5000 before the marker is written.
            s.set_wall_clock(5000, 0);
            s.flush(0).unwrap();
        }
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let report = s.set_wall_clock(1000, 0);
            assert_eq!(
                report.downtime_ms, None,
                "a wall behind the marker (clock went backwards) is unknown"
            );
            assert_eq!(report.expired_addressed, 1);
            let mut delivered = 0usize;
            s.for_each_addressed_id(&mut |_| delivered += 1);
            assert_eq!(delivered, 0);
        }
    }

    /// The wall a marker carries is derived: flush() after set_wall_clock(W, T)
    /// writes wall W + (flush_now - T); a flush before set_wall_clock writes
    /// wall:None.
    #[test]
    fn marker_wall_time_is_derived() {
        // (a) A flush before set_wall_clock writes a marker with wall:None.
        {
            let path = store_path("marker_derived_none");
            {
                let mut s = LogStore::open(&path, 0).unwrap();
                let b = obs([1; 16], 1, 0, Some(0));
                s.insert(b, 0).unwrap();
                s.flush(0).unwrap(); // no wall clock yet
            }
            {
                let mut s = LogStore::open(&path, 0).unwrap();
                let report = s.set_wall_clock(15_000, 0);
                assert_eq!(
                    report.downtime_ms, None,
                    "a flush before set_wall_clock writes wall:None"
                );
            }
        }
        // (b) set_wall_clock(W, T) then flush(T + 5000) writes W + 5000.
        {
            let path = store_path("marker_derived_wall");
            {
                let mut s = LogStore::open(&path, 0).unwrap();
                let b = obs([1; 16], 1, 0, Some(0));
                s.insert(b, 0).unwrap();
                s.set_wall_clock(5000, 1000); // wall_at_set = 5000
                s.flush(6000).unwrap(); // marker wall = 5000 + (6000 - 1000)
            }
            {
                let mut s = LogStore::open(&path, 0).unwrap();
                // Known downtime = wall_now(15000) - marker_wall(10000) = 5000,
                // which only holds if the marker carried the derived wall.
                let report = s.set_wall_clock(15_000, 0);
                assert_eq!(report.downtime_ms, Some(5000));
            }
        }
    }

    /// set_wall_clock runs exactly once: a second call is a no-op.
    #[test]
    fn set_wall_clock_runs_once() {
        let path = store_path("set_once");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let b = obs([1; 16], 1, 0, Some(0));
            s.insert(b, 0).unwrap();
            s.flush(0).unwrap();
        }
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let r1 = s.set_wall_clock(2000, 0);
            let before = s.get(&obs([1; 16], 1, 0, Some(0)).id(), 0).unwrap().age_ms;
            let r2 = s.set_wall_clock(2000, 0);
            let after = s.get(&obs([1; 16], 1, 0, Some(0)).id(), 0).unwrap().age_ms;
            assert_eq!(r2, r1, "a second set_wall_clock returns the first report");
            assert_eq!(before, after, "a second set_wall_clock changes nothing");
            assert_eq!(r2.downtime_ms, None, "the fold still ran once");
        }
    }

    /// The FoldReport is readable after the fold (like skipped_records).
    #[test]
    fn fold_reported_once() {
        let path = store_path("fold_report");
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            let b = obs([1; 16], 1, 0, Some(0));
            let a = addressed([2; 16], 1, [3; 16], 1_000).with_observed_at(0);
            assert!(s.insert(b, 0).unwrap());
            assert!(s.insert(a, 0).unwrap());
            s.flush(0).unwrap();
        }
        {
            let mut s = LogStore::open(&path, 0).unwrap();
            assert!(s.fold_report().is_none(), "no fold has happened yet");

            let r1 = s.set_wall_clock(2000, 0);
            let report = s
                .fold_report()
                .expect("the FoldReport is readable after the fold");
            assert_eq!(r1, report);
            assert_eq!(
                report.downtime_ms, None,
                "no marker on disk -> unknown downtime"
            );
            assert_eq!(report.floored_broadcasts, 1);

            // It ran once: the report is unchanged by a second call.
            let r2 = s.set_wall_clock(2000, 0);
            assert_eq!(r2, r1, "a second call returns the report unchanged");
        }
    }
}

// An exchange-level test over LogStore + a mock transport. The store is
// LogStore (tines-sim's harness uses MemStore), so the exchange is driven
// directly through Node<LogStore, Mock>.
mod quarantined_duplicate_exchange {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::rc::Rc;
    use tines_core::{consts, Event, Node, NodeId, SendError, Store, Transport};
    use tines_store::LogStore;

    fn addressed(observer: [u8; 16], seq: u64, to: [u8; 16], lifetime: u64) -> Observation {
        Observation::addressed(observer, seq, b"task", b"go", to, lifetime)
    }

    /// A point-to-point link: a send on one side is delivered verbatim as a
    /// frame on the other. `sent` counts every frame delivered over it.
    struct Link {
        rx_a: RefCell<VecDeque<Event<NodeId>>>,
        rx_b: RefCell<VecDeque<Event<NodeId>>>,
        sent: Cell<usize>,
    }

    struct Mock {
        own: NodeId,
        link: Rc<RefCell<Link>>,
    }

    impl Transport for Mock {
        type Addr = NodeId;

        fn poll(&mut self) -> Option<Event<NodeId>> {
            match self.own[0] {
                1 => self.link.borrow_mut().rx_a.borrow_mut().pop_front(),
                2 => self.link.borrow_mut().rx_b.borrow_mut().pop_front(),
                _ => None,
            }
        }

        fn send(&mut self, to: &NodeId, frame: &[u8]) -> Result<(), SendError> {
            let to = *to;
            let buf = tines_core::FrameBuf::from_slice(frame).expect("frame fits MAX_FRAME");
            let link = self.link.borrow_mut();
            match (self.own[0], to[0]) {
                (1, 2) => link.rx_b.borrow_mut().push_back(Event::Frame(to, buf)),
                (2, 1) => link.rx_a.borrow_mut().push_back(Event::Frame(to, buf)),
                _ => return Err(SendError::Unreachable),
            }
            link.sent.set(link.sent.get() + 1);
            Ok(())
        }

        fn mtu(&self) -> usize {
            consts::MAX_FRAME
        }
    }

    #[test]
    fn quarantined_addressed_duplicate_not_acked() {
        use std::cell::Cell;
        use std::collections::VecDeque;
        use std::rc::Rc;

        let a_id = [1u8; 16];
        let b_id = [2u8; 16];
        let id = |s: u64| addressed(a_id, s, b_id, 60_000).id();

        let a_path = store_path("quar_a");
        let b_path = store_path("quar_b");

        // A holds the addressed observation, inserted into a store that is used
        // directly by its node (never reopened), so it is never held.
        let mut a_store = LogStore::open(&a_path, 0).unwrap();
        assert!(a_store.insert(addressed(a_id, 0, b_id, 60_000), 0).unwrap());

        // B holds the *same* observation, but quarantined across a downtime: a
        // wall-bearing marker (set via the public API) + a reopen after the wall.
        {
            let mut b = LogStore::open(&b_path, 0).unwrap();
            assert!(b.insert(addressed(a_id, 0, b_id, 60_000), 0).unwrap());
            b.set_wall_clock(1000, 0);
            b.flush(0).unwrap();
        }
        let b_store = LogStore::open(&b_path, 2000).unwrap();
        assert!(
            b_store.is_released(&id(0)),
            "B's copy is quarantined before the exchange"
        );

        let link = Rc::new(RefCell::new(Link {
            rx_a: RefCell::new(VecDeque::new()),
            rx_b: RefCell::new(VecDeque::new()),
            sent: Cell::new(0),
        }));
        let mut a = Node::new(
            a_id,
            0,
            a_store,
            Mock {
                own: a_id,
                link: link.clone(),
            },
        );
        let mut b = Node::new(
            b_id,
            0,
            b_store,
            Mock {
                own: b_id,
                link: link.clone(),
            },
        );

        // Establish the link: A hears about B, B hears about A.
        link.borrow_mut()
            .rx_a
            .borrow_mut()
            .push_back(Event::Contact(b_id));
        link.borrow_mut()
            .rx_b
            .borrow_mut()
            .push_back(Event::Contact(a_id));

        // Drive the exchange until no more frames are produced.
        loop {
            let before = link.borrow().sent.get();
            a.poll(2000);
            b.poll(2000);
            if link.borrow().sent.get() == before {
                break;
            }
        }

        // B held a duplicate of a quarantined addressed observation, so it must
        // NOT ack. A never releasing custody proves no Ack came back: a released
        // duplicate would have acked and released A.
        assert!(
            !a.store().is_released(&id(0)),
            "a quarantined duplicate must not ack — A's custody stays held"
        );
        // Non-vacuous: the exchange ran and A offered the held observation.
        assert!(
            link.borrow().sent.get() >= 1,
            "A must have offered the held observation to B"
        );
    }
}
