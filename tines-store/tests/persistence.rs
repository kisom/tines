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
        s.flush().expect("flush before drop");
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
        s.flush().unwrap();
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
        s.flush().unwrap();
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
        s.flush().unwrap();
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
        s.flush().expect("flush before drop");
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
        s.flush().expect("flush before drop");
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
            s.flush().expect("flush");
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
            s.flush().expect("flush");
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
