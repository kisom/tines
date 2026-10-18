//! Persistence tests for the append-only [`LogStore`] backend.
//!
//! Each test runs against its own unique file under the temp directory so the
//! parallel test runner cannot collide on disk state.

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
