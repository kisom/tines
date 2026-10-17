# tines core + simulator — Implementation Plan

> **For the implementing agent:** Work task-by-task, in order. Each task
> is TDD: write the failing test, run it and see it fail, implement,
> run it and see it pass, commit. Do not skip ahead; later tasks consume
> the exact names and signatures earlier tasks produce. Steps use
> checkbox (`- [ ]`) syntax — tick them as you go. Do not edit the spec.

**Goal:** A `no_std` Rust core that stores timestamped-by-age observations,
exchanges them opportunistically with peers over any datagram transport,
and derives beliefs from them — plus a `std` simulator that proves
convergence and order-independence under lossy, intermittent contact.

**Architecture:** Three layers in one core crate: `Store` (append-only
observation log, trait + in-memory impl), exchange state machine driven by
a `Transport` trait (summaries are salted Bloom filters, diffs are sent
one frame per `poll`), and `BeliefPolicy` (fold observations into a
belief at read time). A `Node` facade owns one store, one transport, and
the exchange state. The simulator implements `Transport` with a shared
world that models contact windows, loss and duplication, and runs
property tests over many nodes.

**Tech Stack:** Rust 2021, stable toolchain ≥ 1.80. Core deps: `heapless`
(bounded containers), `serde` + `postcard` (no_std wire encoding),
`blake2` (observation IDs). Sim deps: `proptest`, `rand`.

**Spec:** `docs/superpowers/specs/2026-10-16-tines-design.md` — read it
first. The plan argues from it; where the plan is silent, the spec rules.

## Global Constraints

- `tines-core` is `#![no_std]`. `alloc` is a cargo feature (default on).
  With `--no-default-features` the crate must build for
  `thumbv7em-none-eabihf`. Only `MemStore` may require `alloc`.
- No async, no threads, no I/O, no panics on untrusted input in core.
  Every frame from the wire is untrusted: malformed bytes are dropped,
  never unwrapped.
- Core never maps transport `Addr` to `NodeId`; every frame carries the
  sender's `NodeId`.
- Identity hash covers `(observer, seq, topic, payload)` only. `age_ms`
  and `observed_at` are excluded.
- Convergence is best-effort per contact, eventual over contacts. No
  end-of-contact handshake.
- Compile-time constants, in `tines-core/src/consts.rs`, with these exact
  defaults:
  `MAX_TOPIC = 64`, `MAX_PAYLOAD = 1024`, `MAX_PEERS = 8`,
  `SUMMARY_BYTES = 512`, `BLOOM_K = 3`, `SUMMARY_INTERVAL_MS = 2000`,
  `REASSEMBLY_TIMEOUT_MS = 5000`, `SEND_QUEUE_CAP = 256`,
  `MAX_FRAME = 1500`.
- `NodeId = [u8; 16]`, `ObservationId = [u8; 16]`, `Tick = u64` (local
  monotonic milliseconds, supplied by the application on every call).
- Commit after every task with the message given. Run `cargo test
  --workspace` and `cargo clippy --workspace -- -D warnings` before each
  commit.

## Review Focus

Input classes the spec implies but no single task's tests would otherwise
exercise. Each has a test pinned to its owning task below.

1. **Reused `seq` after reboot** — an observer that repeats `(seq, topic,
   payload)` produces a duplicate ID; the store must dedupe silently and
   `insert` must return `Ok(false)`. (Task 3)
2. **Oversized `topic` / `payload` at `observe()`** — must return
   `Err(ObserveError::TooLarge)`, never truncate or panic. (Task 10)
3. **Garbage frames** — random bytes, truncated frames, and frames with a
   wrong `count` in a fragment must be dropped with no panic and no store
   change. (Tasks 2, 6, 7, 13)
4. **Frame from an address with no prior `Contact`** — must be treated as
   an implicit contact, not dropped; radios lose beacons. (Task 7)
5. **Tick going backwards** (`now < stored_at`) — age must saturate at its
   stored value, never underflow or wrap. (Task 3)

---

## File Structure

```
Cargo.toml                         workspace: members = ["tines-core", "tines-sim"]
rust-toolchain.toml                channel = "stable"
.cargo/config.toml                 (nothing target-specific; present for CI alias)
scripts/check-no-std.sh            cargo build -p tines-core --no-default-features --target thumbv7em-none-eabihf
tines-core/
  Cargo.toml
  src/lib.rs                       pub mod re-exports
  src/consts.rs                    the constants above
  src/observation.rs               NodeId, ObservationId, Topic, Payload, Observation, id()
  src/wire.rs                      Frame enum, encode/decode (postcard)
  src/store.rs                     Store trait, TrimPolicy, StoreError
  src/mem_store.rs                 MemStore (alloc)
  src/bloom.rs                     Bloom summary
  src/transport.rs                 Transport trait, Event, SendError
  src/exchange.rs                  per-peer state, Exchange state machine
  src/frag.rs                      fragment / reassemble
  src/belief.rs                    BeliefPolicy, LatestWins, SelfFirst
  src/node.rs                      Node facade
  tests/pair.rs                    two Nodes over an in-process PairTransport
tines-sim/
  Cargo.toml
  src/lib.rs
  src/world.rs                     SimWorld: contact model, loss, duplication, mtu
  src/transport.rs                 SimTransport (one per node, shares SimWorld)
  src/trace.rs                     ContactTrace, Contact { start_ms, end_ms, a, b }
  src/sim.rs                       Sim harness: nodes, step(), run()
  tests/convergence.rs             deterministic scenarios
  tests/props.rs                   proptest: convergence, order-independence, loss
```

---

### Task 1: Workspace, core skeleton, Observation and its identity

**Files:**
- Create: `Cargo.toml`, `rust-toolchain.toml`, `scripts/check-no-std.sh`
- Create: `tines-core/Cargo.toml`, `tines-core/src/lib.rs`,
  `tines-core/src/consts.rs`, `tines-core/src/observation.rs`

**Interfaces:**
- Produces:
  ```rust
  pub type NodeId = [u8; 16];
  pub type ObservationId = [u8; 16];
  pub type Tick = u64;
  pub type Topic = heapless::Vec<u8, { consts::MAX_TOPIC }>;
  pub type Payload = heapless::Vec<u8, { consts::MAX_PAYLOAD }>;

  #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
  pub struct Observation {
      pub observer: NodeId,
      pub seq: u64,
      pub age_ms: u64,
      pub observed_at: Option<u64>,
      pub topic: Topic,
      pub payload: Payload,
  }
  impl Observation {
      pub fn id(&self) -> ObservationId;          // Blake2s-256 over (observer, seq, topic_len, topic, payload_len, payload); first 16 bytes
      pub fn aged(&self, elapsed_ms: u64) -> Observation;  // copy with age_ms saturating_add(elapsed_ms)
  }
  ```

- [ ] **Step 1: Create the workspace**

`Cargo.toml`: `[workspace] members = ["tines-core", "tines-sim"]`,
`resolver = "2"`. `rust-toolchain.toml`: `[toolchain] channel = "stable"`.
`tines-core/Cargo.toml`: `edition = "2021"`, features
`default = ["alloc"]`, `alloc = []`; deps `heapless = { version = "0.8",
features = ["serde"] }`, `serde = { version = "1", default-features =
false, features = ["derive"] }`, `postcard = { version = "1",
default-features = false }`, `blake2 = { version = "0.10",
default-features = false }`. `tines-sim/Cargo.toml` can be a stub
(`[package]` only, empty `src/lib.rs`) so the workspace resolves.

`scripts/check-no-std.sh`:
```sh
#!/bin/sh
set -e
rustup target add thumbv7em-none-eabihf
cargo build -p tines-core --no-default-features --target thumbv7em-none-eabihf
```

- [ ] **Step 2: Write the failing tests** in `tines-core/src/observation.rs` (`#[cfg(test)] mod tests`, with `extern crate std;` at crate root under `cfg(test)`)

```rust
fn obs(seq: u64, age: u64, at: Option<u64>) -> Observation {
    Observation { observer: [1; 16], seq, age_ms: age, observed_at: at,
                  topic: Topic::from_slice(b"door/3").unwrap(),
                  payload: Payload::from_slice(b"open").unwrap() }
}
#[test] fn id_ignores_age_and_observed_at() {
    assert_eq!(obs(1, 0, None).id(), obs(1, 999, Some(5)).id());
}
#[test] fn id_changes_with_seq() { assert_ne!(obs(1,0,None).id(), obs(2,0,None).id()); }
#[test] fn id_changes_with_observer() {
    let mut b = obs(1,0,None); b.observer = [2;16];
    assert_ne!(obs(1,0,None).id(), b.id());
}
#[test] fn id_distinguishes_topic_payload_boundary() {
    // topic "ab" payload "c" vs topic "a" payload "bc" must differ
    let mut x = obs(1,0,None); x.topic = Topic::from_slice(b"ab").unwrap(); x.payload = Payload::from_slice(b"c").unwrap();
    let mut y = obs(1,0,None); y.topic = Topic::from_slice(b"a").unwrap();  y.payload = Payload::from_slice(b"bc").unwrap();
    assert_ne!(x.id(), y.id());
}
#[test] fn aged_saturates() { assert_eq!(obs(1, u64::MAX - 1, None).aged(10).age_ms, u64::MAX); }
#[test] fn id_known_vector() {
    // pin the hash layout: observer=[1;16], seq=7, topic="t", payload="p"
    let mut o = obs(7,0,None); o.topic = Topic::from_slice(b"t").unwrap(); o.payload = Payload::from_slice(b"p").unwrap();
    let id = o.id();
    // Record the value produced by the first correct implementation here and keep it fixed thereafter.
    assert_eq!(id.len(), 16);
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p tines-core`
Expected: compile error — `Observation` not defined.

- [ ] **Step 4: Implement `observation.rs` and `consts.rs`**

Hash input layout, in order: `observer` (16 bytes), `seq` as 8-byte
little-endian, `topic.len()` as 2-byte LE, topic bytes, `payload.len()` as
2-byte LE, payload bytes. Blake2s-256; take bytes `[0..16]`. After the
first green run, paste the actual 16-byte value into `id_known_vector`
as a byte-array literal and re-run.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p tines-core` — Expected: all pass.
Run: `sh scripts/check-no-std.sh` — Expected: builds.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(core): workspace, Observation, content-derived id"
```

---

### Task 2: Wire frames

**Files:**
- Create: `tines-core/src/wire.rs`

**Interfaces:**
- Consumes: `Observation`, `NodeId`, `ObservationId` (Task 1)
- Produces:
  ```rust
  #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub enum Frame {
      Summary { from: NodeId, salt: u32, bits: heapless::Vec<u8, { consts::SUMMARY_BYTES }> },
      Obs     { from: NodeId, obs: Observation },
      Frag    { from: NodeId, id: ObservationId, index: u8, count: u8, data: heapless::Vec<u8, { consts::MAX_FRAME }> },
  }
  impl Frame {
      pub fn sender(&self) -> NodeId;
      pub fn encode(&self, out: &mut [u8]) -> Result<usize, WireError>;   // postcard::to_slice
      pub fn decode(bytes: &[u8]) -> Result<Frame, WireError>;            // postcard::from_bytes; any error -> WireError::Malformed
  }
  #[derive(Debug, PartialEq, Eq)] pub enum WireError { TooLarge, Malformed }
  ```

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn obs_frame_roundtrip() { /* encode into [0u8; MAX_FRAME], decode, assert_eq */ }
#[test] fn summary_frame_roundtrip() { /* bits = [0xAA; SUMMARY_BYTES] */ }
#[test] fn frag_frame_roundtrip() { /* index 2, count 5, data 100 bytes */ }
#[test] fn decode_garbage_is_error_not_panic() {
    for len in [0usize, 1, 7, 64, 1500] {
        let bytes = vec![0xFFu8; len];
        assert_eq!(Frame::decode(&bytes).err(), Some(WireError::Malformed));
    }
}
#[test] fn decode_truncated_is_error() {
    let f = /* an Obs frame */; let mut buf = [0u8; MAX_FRAME]; let n = f.encode(&mut buf).unwrap();
    assert!(Frame::decode(&buf[..n/2]).is_err());
}
#[test] fn encode_into_too_small_buffer_is_too_large() {
    let f = /* an Obs frame with 1024-byte payload */; let mut buf = [0u8; 16];
    assert_eq!(f.encode(&mut buf).err(), Some(WireError::TooLarge));
}
```

- [ ] **Step 2: Run tests to verify they fail** — `cargo test -p tines-core wire` — compile error.
- [ ] **Step 3: Implement `wire.rs`** — derive serde on `Observation` and `Frame`; map every `postcard::Error` from decode to `Malformed` and from encode to `TooLarge`.
- [ ] **Step 4: Run tests to verify they pass** — `cargo test -p tines-core wire`.
- [ ] **Step 5: Commit** — `git commit -am "feat(core): wire frames with postcard"`

---

### Task 3: Store trait, TrimPolicy, MemStore

**Files:**
- Create: `tines-core/src/store.rs`, `tines-core/src/mem_store.rs`

**Interfaces:**
- Consumes: `Observation`, `ObservationId`, `Tick` (Task 1)
- Produces:
  ```rust
  #[derive(Clone, Debug, Default)]
  pub struct TrimPolicy {
      pub keep_per_topic: Option<usize>,   // keep the N youngest (lowest age) per exact topic
      pub max_age_ms: Option<u64>,         // drop anything older
      pub max_count: Option<usize>,        // drop oldest until len <= max_count
  }
  #[derive(Debug, PartialEq, Eq)] pub enum StoreError { Full, Backend }
  pub trait Store {
      /// Ok(true) if newly inserted, Ok(false) if already present (by id).
      fn insert(&mut self, obs: Observation, now: Tick) -> Result<bool, StoreError>;
      fn contains(&self, id: &ObservationId) -> bool;
      /// Returns the observation with age_ms advanced by (now - stored_at), saturating at 0 elapsed if now < stored_at.
      fn get(&self, id: &ObservationId, now: Tick) -> Option<Observation>;
      fn len(&self) -> usize;
      fn for_each_id(&self, f: &mut dyn FnMut(&ObservationId));
      /// Every observation whose topic starts with `prefix`, age-adjusted to `now`, in no particular order.
      fn for_each_topic(&self, prefix: &[u8], now: Tick, f: &mut dyn FnMut(&Observation));
      /// Returns number removed.
      fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize;
  }
  #[cfg(feature = "alloc")]
  pub struct MemStore { /* BTreeMap<ObservationId, Held>, Held { obs: Observation, stored_at: Tick } */ }
  #[cfg(feature = "alloc")] impl MemStore { pub fn new() -> Self; }
  ```

- [ ] **Step 1: Write the failing tests** in `mem_store.rs`

```rust
#[test] fn insert_then_contains_and_len() { /* insert -> Ok(true); contains; len == 1 */ }
#[test] fn duplicate_id_is_not_reinserted() {
    // Review Focus 1: same (observer, seq, topic, payload), different age -> Ok(false), len stays 1,
    // and get() still returns the first-stored age lineage.
}
#[test] fn get_adjusts_age_by_elapsed() {
    // insert age_ms=100 at now=1000; get at now=1500 -> age_ms == 600
}
#[test] fn get_with_backwards_tick_saturates() {
    // Review Focus 5: insert age 100 at now=1000; get at now=500 -> age_ms == 100
}
#[test] fn for_each_topic_prefix_matches() { /* "door/1","door/2","wall/1" with prefix b"door/" -> 2 */ }
#[test] fn for_each_topic_exact_empty_prefix_is_all() { /* prefix b"" -> len() */ }
#[test] fn trim_keep_per_topic_keeps_youngest() {
    // 5 obs on "t" with ages 10,20,30,40,50; keep_per_topic=2 -> remaining ages {10,20}; returns 3
}
#[test] fn trim_max_age_drops_old() { /* ages 10 and 10_000, max_age_ms 1000 -> 1 left */ }
#[test] fn trim_max_count_drops_oldest() { /* 10 obs, max_count 4 -> 4 youngest remain */ }
#[test] fn trimmed_obs_can_be_reinserted() { /* spec: trimmed then re-received is accepted -> Ok(true) */ }
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p tines-core store`
- [ ] **Step 3: Implement `store.rs` and `mem_store.rs`** — age adjustment is `obs.aged(now.saturating_sub(stored_at))`. Trim applies rules in the order listed in `TrimPolicy` (per-topic, then age, then count).
- [ ] **Step 4: Run to verify pass** — `cargo test -p tines-core store`; also `sh scripts/check-no-std.sh` (MemStore must be gated out).
- [ ] **Step 5: Commit** — `git commit -am "feat(core): Store trait, TrimPolicy, MemStore"`

---

### Task 4: Salted Bloom summary

**Files:**
- Create: `tines-core/src/bloom.rs`

**Interfaces:**
- Consumes: `ObservationId`
- Produces:
  ```rust
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct Bloom { pub salt: u32, bits: [u8; consts::SUMMARY_BYTES] }
  impl Bloom {
      pub fn new(salt: u32) -> Self;
      pub fn insert(&mut self, id: &ObservationId);
      pub fn might_contain(&self, id: &ObservationId) -> bool;
      pub fn from_parts(salt: u32, bits: &[u8]) -> Option<Self>;   // None unless bits.len() == SUMMARY_BYTES
      pub fn bits(&self) -> &[u8];
  }
  ```
  Hash positions: for `k in 0..BLOOM_K`, `h = u64::from_le_bytes(id[0..8]) ^ (salt as u64).wrapping_mul(0x9E3779B97F4A7C15).rotate_left(k*17 as u32)`, bit index `h % (SUMMARY_BYTES*8)`. (Exact formula is pinned so sim and core agree; any deterministic mixing that uses `salt` and `k` is acceptable if you change it in one place.)

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn inserted_is_contained() { /* 100 random ids: all might_contain == true */ }
#[test] fn empty_contains_nothing() { /* 100 random ids on new(0) -> all false */ }
#[test] fn false_positive_rate_bounded_at_500_items() {
    // insert 500 ids; probe 10_000 others; FP rate < 0.05
}
#[test] fn different_salt_changes_false_positives() {
    // insert the same 500 ids under salt 1 and salt 2; of the probes that are FP under salt 1,
    // fewer than 20% are also FP under salt 2.
}
#[test] fn from_parts_rejects_wrong_length() { assert!(Bloom::from_parts(1, &[0u8; 10]).is_none()); }
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p tines-core bloom`
- [ ] **Step 3: Implement `bloom.rs`**
- [ ] **Step 4: Run to verify pass**
- [ ] **Step 5: Commit** — `git commit -am "feat(core): salted Bloom summary"`

---

### Task 5: Transport trait

**Files:**
- Create: `tines-core/src/transport.rs`

**Interfaces:**
- Produces:
  ```rust
  pub type FrameBuf = heapless::Vec<u8, { consts::MAX_FRAME }>;
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub enum Event<A> { Contact(A), Lost(A), Frame(A, FrameBuf) }
  #[derive(Debug, PartialEq, Eq)] pub enum SendError { Busy, Unreachable, TooLarge }
  pub trait Transport {
      type Addr: Clone + PartialEq + core::fmt::Debug;
      fn poll(&mut self) -> Option<Event<Self::Addr>>;
      fn send(&mut self, to: &Self::Addr, frame: &[u8]) -> Result<(), SendError>;
      fn mtu(&self) -> usize;   // must be <= MAX_FRAME
  }
  ```

- [ ] **Step 1: Write a compile-only test**: a `NullTransport` in the test module implementing the trait with `Addr = u8`, `poll` → `None`, `mtu` → 1500; assert it compiles and `poll()` is `None`.
- [ ] **Step 2: Implement `transport.rs`** — trait and types only.
- [ ] **Step 3: Run** — `cargo test -p tines-core transport`
- [ ] **Step 4: Commit** — `git commit -am "feat(core): Transport trait"`

---

### Task 6: Fragmentation and reassembly

**Files:**
- Create: `tines-core/src/frag.rs`

**Interfaces:**
- Consumes: `Frame`, `Observation`, `ObservationId`, `NodeId`, `Tick`
- Produces:
  ```rust
  /// Encodes `obs` as one Obs frame if it fits in `mtu`, else as N Frag frames.
  /// Returns the number of frames; writes each via `emit(frame_bytes)`.
  pub fn fragment(from: NodeId, obs: &Observation, mtu: usize, emit: &mut dyn FnMut(&[u8])) -> Result<usize, WireError>;

  pub struct Reassembler { /* single slot: id, count, received bitmap (u64 => max 64 frags), buffer heapless::Vec<u8, { MAX_FRAME * 2 }>, started_at: Tick */ }
  impl Reassembler {
      pub fn new() -> Self;
      /// Feed a Frag frame. Returns Some(obs) when complete and the reassembled bytes decode as an Observation whose id() == id; otherwise None.
      /// A Frag with a different id than the slot's resets the slot. A slot older than REASSEMBLY_TIMEOUT_MS is discarded on the next feed.
      pub fn feed(&mut self, id: ObservationId, index: u8, count: u8, data: &[u8], now: Tick) -> Option<Observation>;
  }
  ```
  Fragment payload = postcard encoding of the `Observation` alone, split into `count` chunks of at most `mtu - FRAG_OVERHEAD` bytes, where `FRAG_OVERHEAD = 64` (header allowance, pinned).

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn small_obs_is_one_obs_frame() { /* payload 10 bytes, mtu 1500 -> 1 frame, decodes to Frame::Obs */ }
#[test] fn large_obs_is_fragmented_and_reassembles() {
    // payload 1024 bytes, mtu 200 -> >= 6 frames, all Frame::Frag; feeding all in order yields Some(obs) with equal fields
}
#[test] fn out_of_order_reassembles() { /* feed in reverse order */ }
#[test] fn duplicate_fragment_is_harmless() { /* feed index 0 twice then the rest -> Some */ }
#[test] fn wrong_count_is_dropped() {
    // Review Focus 3: feed with count=2 then a frag with count=3 for the same id -> slot resets; never Some; no panic
}
#[test] fn index_ge_count_is_ignored() { /* index 5, count 3 -> None, no panic */ }
#[test] fn timeout_discards_partial() {
    // feed index 0 at now=0; feed index 1 of 2 at now=REASSEMBLY_TIMEOUT_MS+1 -> None (slot restarted with only index 1)
}
#[test] fn reassembled_bytes_must_match_id() {
    // corrupt one data byte -> after all frags, None
}
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p tines-core frag`
- [ ] **Step 3: Implement `frag.rs`**
- [ ] **Step 4: Run to verify pass**
- [ ] **Step 5: Commit** — `git commit -am "feat(core): fragmentation and reassembly"`

---

### Task 7: Exchange state machine

**Files:**
- Create: `tines-core/src/exchange.rs`

**Interfaces:**
- Consumes: `Store` (Task 3), `Bloom` (Task 4), `Transport`/`Event` (Task 5), `fragment`/`Reassembler` (Task 6), `Frame` (Task 2)
- Produces:
  ```rust
  pub struct Peer<A> {
      pub addr: A,
      pub id: Option<NodeId>,                              // learned from first frame
      pub their_summary: Option<Bloom>,
      pub send_queue: heapless::Vec<ObservationId, { consts::SEND_QUEUE_CAP }>,  // front = next to send; youngest first
      pub last_summary_sent: Option<Tick>,
      pub reassembler: Reassembler,
      pub in_flight: Option<(ObservationId, u8 /* next frag index */)>,
  }
  pub struct Exchange<A> { me: NodeId, salt: u32, peers: heapless::Vec<Peer<A>, { consts::MAX_PEERS }>, rr: usize }
  impl<A: Clone + PartialEq + core::fmt::Debug> Exchange<A> {
      pub fn new(me: NodeId) -> Self;
      /// Handle one transport event. Returns Some(obs) if a new observation was stored (caller may react).
      pub fn on_event<S: Store>(&mut self, ev: Event<A>, store: &mut S, now: Tick) -> Option<Observation>;
      /// Produce at most one outbound frame. Returns Some((addr, bytes_len)) with the frame written into `out`.
      pub fn next_frame<S: Store>(&mut self, store: &S, mtu: usize, now: Tick, out: &mut [u8]) -> Option<(A, usize)>;
      pub fn peers(&self) -> &[Peer<A>];
  }
  ```
  Behaviour (pinned):
  - `Contact(addr)`: add peer if absent and table not full (if full: ignore, no panic). New peer has `last_summary_sent = None` so a summary goes out on the next `next_frame`.
  - `Lost(addr)`: remove peer.
  - `Frame(addr, bytes)`: decode; on `Malformed` drop silently. If `addr` is unknown, add it as if `Contact` had arrived (Review Focus 4). Set `peer.id = Some(frame.sender())`.
    - `Summary`: `peer.their_summary = Bloom::from_parts(...)`; rebuild `send_queue` = ids in store not `might_contain` in their bloom, sorted by `age_ms` ascending (youngest first), truncated to `SEND_QUEUE_CAP`.
    - `Obs`: `store.insert(obs, now)`; return `Some(obs)` if `Ok(true)`.
    - `Frag`: `peer.reassembler.feed(...)`; on `Some(obs)` insert as above.
  - `next_frame`: in priority order: (1) any peer with `last_summary_sent == None` or `now - last_summary_sent >= SUMMARY_INTERVAL_MS` → send `Summary` built from all store ids under `self.salt`, then `self.salt = self.salt.wrapping_add(1)` after **every** summary sent so false positives rotate; (2) round-robin over peers (`rr`) for one with `in_flight` or non-empty `send_queue` → emit the next Obs/Frag frame for the front id (skip ids no longer in store). Return `None` if nothing to do.

- [ ] **Step 1: Write the failing tests** (unit, using `MemStore` and a hand-built `Event` sequence; no transport)

```rust
#[test] fn contact_triggers_summary_frame() { /* Contact(1); next_frame -> Some((1, n)); decode -> Frame::Summary{from: me, ..} */ }
#[test] fn summary_from_peer_queues_only_what_they_lack() {
    // store has ids A,B,C; peer summary contains B -> next frames (after our own summary) are Obs A and Obs C, youngest first; then None
}
#[test] fn received_obs_is_stored_and_returned() { /* on_event(Frame(Obs)) -> Some(obs); store.contains */ }
#[test] fn duplicate_obs_returns_none() {}
#[test] fn malformed_frame_is_dropped() { /* Review Focus 3: Frame(addr, [0xFF; 50]) -> None; store unchanged; no panic */ }
#[test] fn frame_from_unknown_addr_creates_peer() { /* Review Focus 4: no Contact; Frame(Summary) from addr 9 -> peers().len()==1 */ }
#[test] fn lost_removes_peer() {}
#[test] fn peer_table_full_ignores_new_contact() { /* MAX_PEERS+1 contacts -> peers().len()==MAX_PEERS */ }
#[test] fn summary_resent_after_interval() { /* after first summary, next_frame at now+SUMMARY_INTERVAL_MS -> another Summary with salt+1 */ }
#[test] fn salt_rotates_each_summary() {}
#[test] fn large_obs_goes_out_as_frags() { /* mtu 200, 1024-byte payload -> successive next_frame calls yield Frag frames with increasing index */ }
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p tines-core exchange`
- [ ] **Step 3: Implement `exchange.rs`**
- [ ] **Step 4: Run to verify pass**
- [ ] **Step 5: Commit** — `git commit -am "feat(core): exchange state machine"`

---

### Task 8: Belief policies

**Files:**
- Create: `tines-core/src/belief.rs`

**Interfaces:**
- Produces:
  ```rust
  pub trait BeliefPolicy {
      type Belief;
      /// Streaming fold: called once per matching observation (already age-adjusted to `now`),
      /// in no particular order. `acc` is None before the first call. Return the new accumulator.
      fn step(&self, me: NodeId, acc: Option<Self::Belief>, obs: &Observation) -> Option<Self::Belief>;
  }
  pub struct LatestWins;   // Belief = Observation with the smallest age_ms; ties broken by (observer, seq) ascending
  pub struct SelfFirst;    // Belief = youngest of my own observations if any; else LatestWins
  ```
  Streaming (not iterator-based) so `Node::belief` never buffers observations — a 1 KiB payload × N buffer is unacceptable on the MCU target.

- [ ] **Step 1: Write the failing tests** (drive `step` in a loop over a slice; test both forward and reversed slice order)

```rust
#[test] fn latest_wins_picks_smallest_age() {}
#[test] fn latest_wins_none_on_empty() {}
#[test] fn latest_wins_tie_is_deterministic() { /* two obs same age -> same winner regardless of iteration order */ }
#[test] fn self_first_prefers_own_even_if_older() { /* mine age 500, theirs age 10 -> mine */ }
#[test] fn self_first_falls_back_to_latest() {}
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p tines-core belief`
- [ ] **Step 3: Implement `belief.rs`**
- [ ] **Step 4: Run to verify pass**
- [ ] **Step 5: Commit** — `git commit -am "feat(core): BeliefPolicy, LatestWins, SelfFirst"`

---

### Task 9: PairTransport test helper

**Files:**
- Create: `tines-core/tests/common/mod.rs`

**Interfaces:**
- Produces (test-only, `std`):
  ```rust
  pub struct PairTransport { /* shared Rc<RefCell<VecDeque<FrameBuf>>> inbox for each side; addr of other side */ }
  impl PairTransport { pub fn pair(mtu: usize) -> (PairTransport, PairTransport); pub fn connect(&mut self); pub fn disconnect(&mut self); }
  impl Transport for PairTransport { type Addr = u8; /* 0 and 1 */ ... }
  ```
  `connect()` enqueues `Event::Contact(other)` on both sides; `disconnect()` enqueues `Lost`. `send` pushes to the other side's inbox as `Event::Frame`.

- [ ] **Step 1: Write a test** in `tests/pair.rs`: `pair()` then `a.connect()`; `a.poll()` → `Some(Contact(1))`; `a.send(&1, b"hi")`; `b.poll()` → `Some(Frame(0, "hi"))`.
- [ ] **Step 2: Implement.**
- [ ] **Step 3: Run** — `cargo test -p tines-core --test pair`
- [ ] **Step 4: Commit** — `git commit -am "test(core): PairTransport helper"`

---

### Task 10: Node facade

**Files:**
- Create: `tines-core/src/node.rs`
- Modify: `tines-core/src/lib.rs` (re-export `Node`, `ObserveError`, and all public types)
- Test: `tines-core/tests/pair.rs`

**Interfaces:**
- Consumes: everything above
- Produces:
  ```rust
  #[derive(Debug, PartialEq, Eq)] pub enum ObserveError { TooLarge, Store(StoreError) }
  pub struct Node<S: Store, T: Transport> { me: NodeId, next_seq: u64, store: S, transport: T, exchange: Exchange<T::Addr>, out: [u8; consts::MAX_FRAME] }
  impl<S: Store, T: Transport> Node<S, T> {
      pub fn new(me: NodeId, first_seq: u64, store: S, transport: T) -> Self;
      pub fn id(&self) -> NodeId;
      /// The seq the next observe() will use. The application must persist this across reboots.
      pub fn next_seq(&self) -> u64;
      /// Record an observation made now. age_ms = 0. Returns its id.
      pub fn observe(&mut self, topic: &[u8], payload: &[u8], observed_at: Option<u64>, now: Tick) -> Result<ObservationId, ObserveError>;
      /// Drain all pending transport events, then emit at most one frame. Returns the number of new observations stored.
      pub fn poll(&mut self, now: Tick) -> usize;
      pub fn belief<P: BeliefPolicy>(&self, topic_prefix: &[u8], policy: &P, now: Tick) -> Option<P::Belief>;
      pub fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize;
      pub fn store(&self) -> &S;
      pub fn transport_mut(&mut self) -> &mut T;
  }
  ```
  `belief` runs `store.for_each_topic(prefix, now, ...)` and threads the
  accumulator through `policy.step` — no buffering.

- [ ] **Step 1: Write the failing tests** in `tests/pair.rs`

```rust
#[test] fn observe_assigns_increasing_seq() { /* two observes -> next_seq() == first_seq + 2 */ }
#[test] fn observe_rejects_oversized_topic_and_payload() {
    // Review Focus 2: topic of MAX_TOPIC+1 bytes -> Err(TooLarge); payload MAX_PAYLOAD+1 -> Err(TooLarge); store len unchanged
}
#[test] fn two_nodes_converge_over_pair_transport() {
    // a observes 3 topics, b observes 2; connect; loop { a.poll(t); b.poll(t); t += 10 } for 1000 iterations
    // -> both stores len 5 and identical id sets
}
#[test] fn belief_latest_wins_across_nodes() {
    // a observes "door" at t=0, b observes "door" at t=500; converge; both nodes' LatestWins belief is b's observation
}
#[test] fn cut_off_mid_exchange_leaves_both_valid_and_resumes() {
    // a observes 200 x 1 KiB payloads; connect; poll 20 iterations; disconnect; reconnect; poll until converged
    // -> b.store().len() == 200
}
#[test] fn third_party_observation_propagates_within_contact() {
    // a<->b connected; after convergence, inject a new obs into a via observe(); within SUMMARY_INTERVAL_MS*2 of polling, b has it
}
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p tines-core --test pair`
- [ ] **Step 3: Implement `node.rs`; wire up `lib.rs` re-exports.** `poll` loop: `while let Some(ev) = transport.poll() { exchange.on_event(ev, &mut store, now) }` then `if let Some((addr, n)) = exchange.next_frame(&store, transport.mtu(), now, &mut out) { let _ = transport.send(&addr, &out[..n]); }`. A `SendError` is ignored (the obs stays queued only if the error was `Busy`; otherwise it is dropped and will be re-queued by the next summary round).
- [ ] **Step 4: Run to verify pass** — `cargo test -p tines-core`; `sh scripts/check-no-std.sh`; `cargo clippy -p tines-core -- -D warnings`.
- [ ] **Step 5: Commit** — `git commit -am "feat(core): Node facade"`

---

### Task 11: Simulator world and transport

**Files:**
- Create: `tines-sim/Cargo.toml` (deps: `tines-core`, `rand = "0.8"`, `proptest = "1"` under `[dev-dependencies]`), `tines-sim/src/lib.rs`, `src/world.rs`, `src/transport.rs`, `src/trace.rs`

**Interfaces:**
- Produces:
  ```rust
  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct Contact { pub start_ms: u64, pub end_ms: u64, pub a: usize, pub b: usize }
  #[derive(Clone, Debug, Default)]
  pub struct ContactTrace { pub contacts: Vec<Contact> }
  impl ContactTrace { pub fn in_contact(&self, a: usize, b: usize, t: u64) -> bool; pub fn end(&self) -> u64; }

  #[derive(Clone, Debug)]
  pub struct WorldConfig { pub nodes: usize, pub mtu: usize, pub loss: f64, pub dup: f64, pub seed: u64 }
  pub struct SimWorld { /* config, trace, per-node inbox VecDeque<Event<usize>>, prev contact matrix, rng */ }
  impl SimWorld {
      pub fn new(cfg: WorldConfig, trace: ContactTrace) -> Rc<RefCell<SimWorld>>;
      /// Advance to time t: for each pair whose contact state changed since last advance, enqueue Contact/Lost on both.
      pub fn advance(&mut self, t: u64);
      pub fn frames_sent(&self) -> u64; pub fn frames_dropped(&self) -> u64;
  }
  pub struct SimTransport { world: Rc<RefCell<SimWorld>>, me: usize }
  impl Transport for SimTransport { type Addr = usize; /* send: if not in contact -> Err(Unreachable); roll loss -> drop; roll dup -> enqueue twice; frame.len() > mtu -> Err(TooLarge) */ }
  ```

- [ ] **Step 1: Write the failing tests** in `src/world.rs` tests

```rust
#[test] fn advance_emits_contact_and_lost_at_window_edges() { /* contact (100..200, 0, 1): advance(50) nothing; advance(100) both get Contact; advance(200) both get Lost */ }
#[test] fn send_outside_contact_is_unreachable() {}
#[test] fn send_over_mtu_is_too_large() {}
#[test] fn loss_1_drops_everything() { /* loss 1.0: 100 sends -> peer inbox has 0 frames */ }
#[test] fn dup_1_doubles_everything() { /* dup 1.0, loss 0: 10 sends -> 20 frames */ }
#[test] fn same_seed_same_drops() { /* two worlds, same seed, same send sequence -> same frames_dropped */ }
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p tines-sim`
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run to verify pass**
- [ ] **Step 5: Commit** — `git commit -am "feat(sim): world, contact trace, SimTransport"`

---

### Task 12: Sim harness and deterministic convergence scenarios

**Files:**
- Create: `tines-sim/src/sim.rs`, `tines-sim/tests/convergence.rs`

**Interfaces:**
- Produces:
  ```rust
  pub type SimNode = Node<MemStore, SimTransport>;
  pub struct Sim { pub world: Rc<RefCell<SimWorld>>, pub nodes: Vec<SimNode>, pub t: u64, pub step_ms: u64 }
  impl Sim {
      pub fn new(cfg: WorldConfig, trace: ContactTrace) -> Sim;     // node i has NodeId [i as u8; 16], first_seq 0
      pub fn step(&mut self);                                        // t += step_ms; world.advance(t); every node.poll(t)
      pub fn run_until(&mut self, t_end: u64);
      pub fn observe(&mut self, node: usize, topic: &[u8], payload: &[u8]) -> ObservationId;  // at current t
      pub fn id_set(&self, node: usize) -> BTreeSet<ObservationId>;
      pub fn converged(&self, nodes: &[usize]) -> bool;              // all id_sets equal
      pub fn components(&self) -> Vec<Vec<usize>>;                   // transitive connectivity over the whole trace
  }
  ```
  `step_ms` default 10.

- [ ] **Step 1: Write the failing tests** in `tests/convergence.rs`

```rust
#[test] fn two_nodes_one_contact_converge() { /* trace (0..5000, 0, 1); each observes 10; run_until(5000); converged(&[0,1]) */ }
#[test] fn relay_through_middle_node() {
    // 3 nodes; contacts (0..3000, 0, 1) then (4000..7000, 1, 2); node 0 observes 5 at t=0; run_until(7000)
    // -> node 2 has node 0's 5 observations although 0 and 2 never met
}
#[test] fn disconnected_component_does_not_converge() { /* 4 nodes, contacts only 0-1 and 2-3; components() == [[0,1],[2,3]]; converged(&[0,1,2,3]) == false */ }
#[test] fn lossy_link_still_converges_given_time() { /* loss 0.3, dup 0.1, two nodes, 50 obs each, contact 0..60_000 -> converged */ }
#[test] fn age_tracks_true_elapsed_across_hops() {
    // node 0 observes "x" at t=1000; relay to node 2 via node 1 as above; at t=7000 node 2's LatestWins belief for "x"
    // has age_ms within ±(2 * step_ms) of 6000
}
#[test] fn large_payloads_fragment_over_small_mtu() { /* mtu 256, payloads 1024 bytes, 20 obs -> converged */ }
```

- [ ] **Step 2: Run to verify fail** — `cargo test -p tines-sim --test convergence`
- [ ] **Step 3: Implement `sim.rs`**
- [ ] **Step 4: Run to verify pass**
- [ ] **Step 5: Commit** — `git commit -am "feat(sim): harness and convergence scenarios"`

---

### Task 13: Property tests — convergence and order independence

**Files:**
- Create: `tines-sim/tests/props.rs`

**Interfaces:**
- Consumes: `Sim`, `ContactTrace`, `WorldConfig`

- [ ] **Step 1: Write the property tests**

Strategy `arb_trace(nodes: 2..=6)`: 1..=12 contacts, each `start` in
`0..50_000`, duration `2_000..=10_000`, `a != b`. Strategy
`arb_observations`: for each node 0..=10 observations with topics from a
set of 8 and payloads 1..=64 bytes, all made at `t = 0`.

```rust
proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    #[test] fn connected_components_converge(trace in arb_trace(), obs in arb_observations(), loss in 0.0f64..0.3) {
        // Sim with cfg { loss, dup: 0.05, mtu: 512, seed: 1 }; apply obs; run_until(trace.end() + 1); 
        // then: for every component in sim.components(): prop_assert!(sim.converged(&component))
        // NOTE: a contact shorter than SUMMARY_INTERVAL_MS*3 may legitimately fail; the strategy's minimum duration (2_000) is chosen so that
        // with step_ms=10 at most ~1% of cases fail from that alone. If this flakes, raise the minimum duration before touching the protocol.
    }

    #[test] fn store_is_independent_of_contact_order(obs in arb_observations(), perm in any::<u64>()) {
        // 3 nodes. Trace A: (0..5000, 0,1), (6000..11000, 1,2), (12000..17000, 0,2).
        // Trace B: the same three contacts in an order chosen by `perm % 6`, re-timed to the same slots.
        // Run both with loss 0; prop_assert_eq!(id_set_A(i), id_set_B(i)) for every node i.
    }

    #[test] fn union_is_monotone(trace in arb_trace(), obs in arb_observations()) {
        // Sample every node's id_set every 1000 ms; prop_assert!(later.is_superset(&earlier)) (no trimming in this test).
    }

    #[test] fn garbage_frames_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..1500)) {
        // Review Focus 3: one Node with MemStore + SimTransport; push Event::Frame(1, bytes) directly into its inbox; poll(); no panic; store len 0
    }
}
```

- [ ] **Step 2: Run** — `cargo test -p tines-sim --test props`
Expected: all pass. If `connected_components_converge` fails, print the minimal case (`proptest` shrinks it) and decide whether it is a short-contact artefact (raise the strategy floor) or a protocol bug (fix in `exchange.rs`, add a deterministic regression test to Task 12's file).
- [ ] **Step 3: Commit** — `git commit -am "test(sim): property tests for convergence, order independence, monotonicity"`

---

### Task 14: CI script and README

**Files:**
- Create: `scripts/ci.sh`, `README.md`

- [ ] **Step 1: `scripts/ci.sh`**:
```sh
#!/bin/sh
set -e
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
sh scripts/check-no-std.sh
```
- [ ] **Step 2: `README.md`**: one paragraph from the spec's Intent, the three-layer diagram, a 15-line usage example constructing a `Node<MemStore, T>` and calling `observe` / `poll` / `belief`, and a pointer to the spec.
- [ ] **Step 3: Run** `sh scripts/ci.sh` — Expected: exit 0.
- [ ] **Step 4: Commit** — `git commit -am "chore: ci script and README"`

---

## Self-review notes

- Spec coverage: Observation/identity (T1), wire (T2), Store/trim (T3),
  summary (T4), Transport (T5), fragmentation (T6), exchange protocol
  incl. periodic re-summary and salt rotation (T7), memory layer (T8),
  Node facade incl. seq persistence contract (T10), simulator with
  contact model, loss, dup, assertions (T11–13), `no_std` CI (T1, T14).
  Not in this plan by design: `tines-udp`, `tines-sqlite`, `tines-cli`,
  `tines-serial` — second plan.
- Review Focus 1–5 each pinned: T3, T10, T2+T6+T7+T13, T7, T3.
- Names used across tasks: `Observation::id/aged`, `Frame::{Summary,Obs,Frag,sender,encode,decode}`,
  `Store::{insert,contains,get,len,for_each_id,for_each_topic,trim}`,
  `Bloom::{new,insert,might_contain,from_parts,bits}`,
  `Exchange::{new,on_event,next_frame,peers}`, `Reassembler::{new,feed}`,
  `fragment`, `Node::{new,id,next_seq,observe,poll,belief,trim,store,transport_mut}`,
  `Sim::{new,step,run_until,observe,id_set,converged,components}` — checked consistent.
