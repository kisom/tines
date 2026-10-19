//! Persistence tests for the append-only [`LogStore`] backend.
//!
//! Each test runs against its own unique file under the temp directory so the
//! parallel test runner cannot collide on disk state.

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use tines_core::{Observation, Payload, Store, Topic};
use tines_store::LogStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn obs(observer: [u8; 16], seq: u64, age: u64, at: Option<u64>) -> Observation {
    Observation {
        observer,
        seq,
        age_ms: age,
        observed_at: at,
        topic: Topic::from_slice(b"door/3").unwrap(),
        payload: Payload::from_slice(b"open").unwrap(),
        to: None,
        lifetime_ms: None,
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

#[test]
fn reopen_preserves_store() {
    let path = store_path("reopen");

    {
        let mut s = LogStore::new(&path, 0, 1).expect("open fresh store");
        let a = obs([1; 16], 1, 0, Some(10));
        let b = obs([2; 16], 2, 0, Some(20));
        let ib = b.id();

        assert!(s.insert(a, 1000).unwrap());
        assert!(s.insert(b, 1001).unwrap());
        assert!(!s.is_released(&ib));
        s.release(&ib);
        assert!(s.is_released(&ib));
        assert_eq!(s.next_seq(), 3);
        s.flush().expect("flush before drop");
    }

    {
        let s = LogStore::new(&path, 2000, 3).expect("reopen store");
        let a = obs([1; 16], 1, 0, Some(10));
        let b = obs([2; 16], 2, 0, Some(20));
        assert!(s.contains(&a.id()));
        assert!(s.contains(&b.id()));
        assert!(!s.is_released(&a.id()));
        assert!(s.is_released(&b.id()));
        assert_eq!(s.next_seq(), 3);
    }
}

#[test]
fn ages_rebase_on_load() {
    let path = store_path("ages");

    {
        let mut s = LogStore::new(&path, 1000, 1).unwrap();
        let o = obs([1; 16], 1, 500, Some(10));
        assert!(s.insert(o, 1000).unwrap());
        s.flush().unwrap();
    }

    {
        let s = LogStore::new(&path, 5000, 1).unwrap();
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
        let mut s = LogStore::new(&path, 1000, 1).unwrap();
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
        let s = LogStore::new(&path, 9000, 1).unwrap();
        let a = obs([1; 16], 1, 100, Some(10));
        let g = s.get(&a.id(), 9000).unwrap();
        assert_eq!(g.age_ms, 8100);
    }
}

#[test]
fn next_seq_persists() {
    let path = store_path("seq");

    {
        let mut s = LogStore::new(&path, 0, 1).unwrap();
        for i in 0..3u64 {
            let o = obs([1; 16], 1 + i, 0, Some(10));
            assert!(s.insert(o, 1 + i).unwrap());
        }
        assert_eq!(s.next_seq(), 4);
        s.flush().unwrap();
    }

    {
        let s = LogStore::new(&path, 100, 1).unwrap();
        assert_eq!(s.next_seq(), 4);
    }
}

#[test]
fn reopen_recovers_from_torn_trailing_record() {
    // A crash mid-append can leave a partial trailing record — a length prefix
    // whose body never reached disk. Reopen must truncate the torn tail and
    // load the complete records, not fail the whole store.
    let path = store_path("torn");

    {
        let mut s = LogStore::new(&path, 0, 1).expect("open fresh store");
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
    {
        let s = LogStore::new(&path, 3000, 1).expect("reopen after torn tail");
        assert!(s.contains(&obs([1; 16], 1, 0, Some(10)).id()));
        assert!(s.contains(&obs([2; 16], 2, 0, Some(20)).id()));
        assert_eq!(s.next_seq(), 3);
    }
}

#[test]
fn reopen_recovers_from_oversized_record_length() {
    // A corrupt length prefix (e.g. u32::MAX) must neither trigger a giant
    // allocation nor make the store unloadable; it should be dropped like a
    // torn record, leaving the earlier complete records intact.
    let path = store_path("oversize");

    {
        let mut s = LogStore::new(&path, 0, 1).expect("open fresh store");
        let a = obs([1; 16], 1, 0, Some(10));
        assert!(s.insert(a, 1000).unwrap());
        s.flush().expect("flush before drop");
    }

    {
        // Declare a body of u32::MAX bytes with no actual body. read_record
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
        let s = LogStore::new(&path, 3000, 1).expect("reopen after oversized prefix");
        assert!(s.contains(&obs([1; 16], 1, 0, Some(10)).id()));
    }
}
