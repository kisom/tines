# Fleet Deployment v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land tines on real robots: handoff receipts in the core (custody chain for addressed data, confidence events for world state), a BLE transport, a persistent store backend, a ROS2 node, and a standalone service.

**Architecture:** tines-core (no_std) gains the receipt protocol — an `Ack` frame, `Store::release`/`is_released`, bounce direction bits, pending-ack buffers, and a confidence-event queue. Four host crates sit beside the existing tines-sim: `tines-bt` (BLE transport), `tines-store` (persistent append-only log), `tines-ros2` (self-contained ROS2 node), `tines` (standalone daemon). The custody chain supersedes the addressed-delivery spec's epidemic forwarding.

**Tech Stack:** Rust, no_std core (postcard wire codec, heapless buffers), bluer (BlueZ D-Bus) for BLE, rclrs for ROS2, std host crates.

**Spec:** `docs/superpowers/specs/2026-10-18-tines-fleet-deployment-design.md` (approved) — the plan argues from the spec; executors read both.

## Global Constraints

- `tines-core` is `#![no_std]`; `alloc` is a cargo feature (default on); `--no-default-features` must build for `thumbv7em-none-eabihf`. Only `MemStore` may require `alloc`.
- No async, threads, I/O, or panics on untrusted input in core. Malformed frames drop as `Err`, never unwrap.
- Compile-time constants (exact): `MAX_TOPIC=64`, `MAX_PAYLOAD=1024`, `MAX_PEERS=8`, `SUMMARY_BYTES=512`, `BLOOM_K=3`, `SUMMARY_INTERVAL_MS=2000`, `REASSEMBLY_TIMEOUT_MS=5000`, `SEND_QUEUE_CAP=256`, `MAX_FRAME=1500`. New constants in this plan: `ACK_IDS_CAP=90`, `ACK_BUFFER_CAP=256`, `BCAST_SENT_CAP=256`, `EVENT_QUEUE_CAP=64`.
- Every emitted frame fits the transport's `mtu()`, including summaries and acks.
- `NodeId = [u8; 16]`, `ObservationId = [u8; 16]`, `Tick = u64`.
- Addressed invariants: `to.is_some() == lifetime_ms.is_some()`; addressed never in bloom/broadcast diff; expired addressed never stored.
- Conventional commits (`feat(core): …`, `feat(bt): …`). Gate: `sh scripts/ci.sh` = fmt → clippy `-D warnings` → doc → test → no_std build.
- `Store::release`/`is_released` are a breaking change: `tines-core` ships as **0.3.0** with a CHANGELOG entry.
- Specs are design history: do not edit existing specs; the fleet spec supersedes the addressed-delivery spec's epidemic forwarding.

## Review Focus

1. **Forged/replayed ack** — an Ack for an id the node never *sent* to that address must be rejected (bounce direction + broadcast-send record), so a stranger cannot release custody. → Task 3.
2. **Released duplicate** — a duplicate addressed offer whose local copy is released must NOT be acked, or the custodian releases and nobody carries it. → Task 4.
3. **Ack fits the link** — an Ack must be split across frames to fit `mtu()`, and an overflowing ack buffer must drop oldest without ever causing a false release (sender keeps carrying). → Task 4.
4. **Reboot** — after tines-store reload, released flags survive, ages rebase (no saturation), and `next_seq` is monotonic (no content-id collision). → Task 7.
5. **Malformed Ack** — a truncated/garbage `Ack` frame drops as `Err(Malformed)` with no panic and no custody change. → Task 2.
6. **World-state ack flood** — a large broadcast sync must not evict addressed custody acks (separate buffers), or custody release stalls. → Task 4.

---

### Task 1: Store release contract

**Files:**
- Modify: `tines-core/src/store.rs`
- Modify: `tines-core/src/mem_store.rs`
- Test: `tines-core/src/mem_store.rs` (inline `#[cfg(test)]`)
- Modify: `tines-core/Cargo.toml` (bump version to `0.3.0`)
- Create: `CHANGELOG.md` (workspace root; "0.3.0 — `Store` gains `release`/`is_released` (breaking)")

**Interfaces:**
- Produces (required trait methods, no default): `fn release(&mut self, id: &ObservationId)` and `fn is_released(&self, id: &ObservationId) -> bool` on `Store`.
- `MemStore` entry gains a `released: bool` field.

- [ ] **Step 1: Write the failing test** — `release_marks_and_is_released_reports`: insert an observation, assert `!is_released`, call `release(&id)`, assert `is_released`; `release_unknown_is_noop`: `release(&[0u8;16])` does not panic and `is_released` stays false.
- [ ] **Step 2: Run test to verify it fails** — `cargo test -p tines-core release_` — Expected: FAIL (method not found).
- [ ] **Step 3: Implement `release`/`is_released`** in `store.rs` (trait, no default) and `mem_store.rs` (set/read the `released` flag; unknown id is a no-op; `MemStore::for_each_addressed_id` skips released ids — an optimization, the exchange predicate is the load-bearing check).
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS; full `cargo test -p tines-core` still green.
- [ ] **Step 5: Bump version and CHANGELOG** — `tines-core/Cargo.toml` `version = "0.3.0"`; create `CHANGELOG.md` with the 0.3.0 entry.
- [ ] **Step 6: Commit** — `git add tines-core/src/store.rs tines-core/src/mem_store.rs tines-core/Cargo.toml CHANGELOG.md && git commit -m "feat(core): Store::release and is_released (0.3.0)"`

### Task 2: Ack wire frame

**Files:**
- Modify: `tines-core/src/wire.rs`
- Modify: `tines-core/src/consts.rs` (add `ACK_IDS_CAP = 90`)
- Test: `tines-core/src/wire.rs` (inline)

**Interfaces:**
- Produces: `Frame::Ack { from: NodeId, ids: heapless::Vec<ObservationId, { consts::ACK_IDS_CAP }> }` — **added as the last variant**; existing variants must not be reordered (postcard tags are positional).
- `Frame::sender()` must handle `Ack` (returns `from`).

- [ ] **Step 1: Write the failing test** — `ack_frame_roundtrip`: an `Ack` with 3 ids roundtrips through `encode`/`decode`; `decode_garbage_is_error_not_panic` still holds (its length loop already exercises all variants). For variant-order safety, `summary_golden_vector`: encode a known `Summary` frame, pin the exact bytes as a literal, and assert `Frame::decode` of those bytes yields the same `Summary` — so reordering the existing variants fails this test.
- [ ] **Step 2: Run test to verify it fails** — `cargo test -p tines-core ack_` — Expected: FAIL (variant missing).
- [ ] **Step 3: Implement `Ack`** — add the variant last, update `sender()`.
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS; `cargo test -p tines-core` green.
- [ ] **Step 5: Commit** — `git commit -m "feat(core): Ack wire frame (last Frame variant)"`

### Task 3: Ack receive path — validation, custody release, confidence events

**Files:**
- Modify: `tines-core/src/exchange.rs`
- Modify: `tines-core/src/consts.rs` (add `EVENT_QUEUE_CAP = 64`, `BCAST_SENT_CAP = 256`)
- Modify: `tines-core/src/lib.rs` (export `ConfidenceEvent`)
- Test: `tines-core/src/exchange.rs` (inline)

**Interfaces:**
- Produces: `pub struct ConfidenceEvent { pub peer: NodeId, pub id: ObservationId }` (peer = who received the obs).
- Bounce records become `(ObservationId, A, BounceKind)` where `enum BounceKind { Sent, Received }` (private). `record_bounce` takes a kind; `is_sourced` matches either kind; a new `was_sent(bounce, id, addr)` matches `Sent` only.
- Exchange gains `bcast_sent: heapless::Vec<(ObservationId, A), { consts::BCAST_SENT_CAP }>` — broadcast sends are recorded into it on the emit path — plus `events: heapless::Vec<ConfidenceEvent, { consts::EVENT_QUEUE_CAP }>` (push drop-oldest) and `pub fn drain_events(&mut self) -> impl Iterator<Item = ConfidenceEvent> + '_`.

- [ ] **Step 1: Write the failing test** — `ack_validates_only_sent_ids`: A sends obs X to peer at addr 1; a forged `Ack { from: [2;16], ids: [X] }` from addr 2 (which never received X) is ignored — `store.is_released(&X)` stays false. `ack_releases_custody`: after A sends X to addr 1, an `Ack` from addr 1 listing X releases it (`is_released(&X)` true, `for_each_addressed_id`/forwarding skips it). `ack_fires_confidence_event`: a broadcast obs Y sent to addr 1, an `Ack` from addr 1 listing Y, `drain_events()` yields one `ConfidenceEvent { peer: [2;16], id: Y }`.
- [ ] **Step 2: Run test to verify it fails** — `cargo test -p tines-core ack_` — Expected: FAIL.
- [ ] **Step 3: Implement** — handle `Frame::Ack` in `on_event`: for each id, if `was_sent(bounce, id, addr)` and the local copy is addressed → `store.release(&id)`; else if the id is in `bcast_sent` → push a `ConfidenceEvent`. Update `record_bounce` call sites (receive path = `Received`, send path = `Sent`) and record broadcast sends into `bcast_sent`. Add the load-bearing `!store.is_released(id)` check to the custody scan predicate (`select_addressed`), mirroring the existing `to` re-check. Reject malformed acks as `Err` (no panic).
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(core): ack validation, custody release, confidence events"`

### Task 4: Ack send path — buffers, eligibility, emission

**Files:**
- Modify: `tines-core/src/exchange.rs`
- Modify: `tines-core/src/consts.rs` (add `ACK_BUFFER_CAP = 256`)
- Test: `tines-core/src/exchange.rs` (inline)

**Interfaces:**
- Consumes: `bcast_sent` from Task 3.
- Exchange gains: `acks: heapless::Vec<(ObservationId, A), { consts::ACK_BUFFER_CAP }>` (addressed) and `bcast_acks: heapless::Vec<(ObservationId, A), { consts::ACK_BUFFER_CAP }>` (broadcast). Overflow drops the oldest entry.
- `accept()` enqueues `(id, addr)` into the right buffer (addressed → `acks`, broadcast → `bcast_acks`), except: a duplicate addressed offer whose local copy `is_released` is not enqueued.
- `next_frame` emits a pending Ack after a due summary and before further addressed/broadcast traffic: drain up to `ACK_IDS_CAP` ids for one addr into a `Frame::Ack`, splitting across frames.

- [ ] **Step 1: Write the failing test** — `released_duplicate_is_not_acked`: B releases X, then receives X again from A; B emits no `Ack` for X (drain frames, assert none). `ack_fits_mtu`: with `mtu` small enough for ~5 ids, the emitted `Ack` frames each carry at most that many ids. `broadcast_flood_does_not_evict_custody_ack`: fill `bcast_acks` past capacity, assert an addressed pending ack is still emitted. `duplicate_broadcast_is_acked`: B receives the same broadcast Y twice, both times an Ack for Y is emitted.
- [ ] **Step 2: Run test to verify it fails** — Expected: FAIL.
- [ ] **Step 3: Implement** — the buffers, `accept` eligibility (released-duplicate skip), and Ack emission with mtu-aware splitting.
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(core): ack send path with custody/broadcast buffers"`

### Task 5: Node event surface

**Files:**
- Modify: `tines-core/src/node.rs`
- Test: `tines-core/tests/pair.rs`

**Interfaces:**
- Consumes: `Exchange::drain_events` (Task 3).
- Produces: `pub fn drain_events(&mut self) -> impl Iterator<Item = ConfidenceEvent> + '_` on `Node`, draining the exchange's queue.

- [ ] **Step 1: Write the failing test** — `confidence_event_surfaces_on_node`: two nodes on a `PairTransport`; A `observe`s a broadcast, both poll until B stores it; assert `a_node.drain_events()` yields a `ConfidenceEvent` with `peer == b.id()` and the obs id (note: B receives from A, so A's node surfaces the event). If order is ambiguous, assert the set of events contains it.
- [ ] **Step 2: Run test to verify it fails** — Expected: FAIL (`drain_events` missing).
- [ ] **Step 3: Implement** — delegate to `self.exchange.drain_events()`.
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(core): Node::drain_events for confidence events"`

### Task 6: Simulator coverage

**Files:**
- Modify: `tines-sim/src/sim.rs` (expose `observe_to` already exists; add event access if needed)
- Test: `tines-sim/tests/props.rs`, `tines-sim/tests/relay.rs`

**Interfaces:**
- Consumes: `Node::drain_events`, `Store::release`/`is_released`.

- [ ] **Step 1: Write the failing tests** — `custody_chain_delivers_without_carrier_failure`: on a connected A→B→C path, A `observe_to(C)`; after enough rounds, C holds the message and every carrier that released is not re-forwarding it; `no_carrier_failure_always_held`: property — for any connected topology and any addressed message, while unexpired and no node dropped it, at least one node (carrier or recipient) holds it; `released_duplicate_not_acked` at the sim level: A and B both hold X, B is custodian, A released; reconnect A↔B, B must not end up released-with-nobody-holding.
- [ ] **Step 2: Run test to verify it fails** — `cargo test -p tines-sim` — Expected: FAIL (behavior not yet wired through the sim).
- [ ] **Step 3: Implement** — wire event/custody observation into the sim as needed; add any `Sim` accessor the tests require (keep the public API minimal).
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS; full `cargo test --workspace --exclude tines-ros2` green.
- [ ] **Step 5: Commit** — `git commit -m "test(sim): custody chain and receipt properties"`

### Task 7: tines-store (persistent backend)

**Files:**
- Create: `tines-store/Cargo.toml`, `tines-store/src/lib.rs`, `tines-store/src/log.rs`
- Modify: `Cargo.toml` (workspace root: add member `tines-store`)
- Test: `tines-store/tests/persistence.rs`

**Interfaces:**
- Consumes: `Store`, `StoreError`, `TrimPolicy`, `Observation`, `Tick`, `ObservationId` from `tines-core`.
- Produces: `pub struct LogStore { … }` implementing `Store`, plus `pub fn new(path: &Path, now: Tick, first_seq: u64) -> Result<Self, StoreError>`; `pub fn next_seq(&self) -> u64`; `pub fn flush(&mut self) -> Result<(), StoreError>`. Rebase on load: fold pre-reboot elapsed (measured against the last persisted clock marker) into `age_ms`, set `stored_at = now`; persist `released` flags and `next_seq` in a metadata record; append-only observation log with trim records.

- [ ] **Step 1: Write the failing tests** — `reopen_preserves_store`: insert obs + release one, drop, reopen with a fresh `now`/`first_seq`, assert `contains`, `is_released`, and `next_seq` continuation; `ages_rebase_on_load`: insert at `now=1000`, reopen at `now=5000`, assert `get` age continues from the folded pre-reboot age (not frozen at `age_ms`, not saturating); `next_seq_persists`: after `observe`-style inserts, `next_seq` after reopen equals the persisted value.
- [ ] **Step 2: Run test to verify it fails** — `cargo test -p tines-store` — Expected: FAIL (crate missing).
- [ ] **Step 3: Implement** — the append-only log, trim records, metadata (released flags, next_seq, last clock marker), and `Store` impl delegating to an in-memory index rebuilt at load.
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(store): persistent append-only LogStore"`

### Task 8: tines-bt (BLE transport)

**Files:**
- Create: `tines-bt/Cargo.toml`, `tines-bt/src/lib.rs`, `tines-bt/src/adv.rs`, `tines-bt/src/transport.rs`
- Modify: `Cargo.toml` (workspace root: add member `tines-bt`)
- Test: `tines-bt/tests/adv.rs`

**Interfaces:**
- Consumes: `Transport`, `Event`, `SendError`, `NodeId` from `tines-core`; `bluer` (D-Bus BlueZ).
- Produces: `pub struct BtTransport { … }` implementing `Transport<Addr = NodeId>`; `pub fn advertise(node: NodeId) -> [u8; 31]` (16-bit service UUID + flags + NodeId in service data); the transport reports real MTU via `mtu()`, drops frames whose `from` != the connection's advertised `NodeId`, and the smaller-NodeId node acts as BLE central.
- Radio I/O is behind a small internal trait so unit tests exercise `adv` and the mismatch-drop logic without hardware.

- [ ] **Step 1: Write the failing tests** — `advertising_payload_fits_31_bytes` and `advertising_payload_roundtrips_node_id`; `from_mismatch_is_dropped`: a mocked inbound frame whose `from` != the advertised id yields no `Event::Frame`.
- [ ] **Step 2: Run test to verify it fails** — `cargo test -p tines-bt` — Expected: FAIL.
- [ ] **Step 3: Implement** — advertising payload assembly, `Addr = NodeId`, mismatch-drop, central/peripheral role rule, `mtu()` (L2CAP CoC when available, else ATT MTU via GATT fallback).
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(bt): BLE transport (bluer)"`

### Task 9: tines-ros2 (ROS2 node)

**Files:**
- Create: `tines-ros2/Cargo.toml`, `tines-ros2/src/lib.rs`, `tines-ros2/src/topics.rs`, `tines-ros2/src/node.rs`
- Modify: `Cargo.toml` (workspace root: add member `tines-ros2`); `scripts/ci.sh` (exclude `tines-ros2` from the default gate); Create: `scripts/check-ros2.sh`
- Test: `tines-ros2/tests/topics.rs`

**Interfaces:**
- Consumes: `Node`, `LogStore`, `BtTransport`, `ConfidenceEvent`; `rclrs`.
- Produces: `pub fn topic_name(tines_topic: &[u8]) -> String` — hex-escapes into `/tines/<hex>`; `pub struct TinesNode { … }` — a self-contained ROS2 node owning a `Node<LogStore, BtTransport>`, publishing each tines topic as a ROS2 topic and `/tines/receipts` for confidence events, subscribing for new observations. Lazy topic creation bounded by the store's live topics and a configurable cap.

- [ ] **Step 1: Write the failing tests** — `topic_name_escapes` (arbitrary bytes → valid ROS2 name under `/tines/`), `topic_name_roundtrip` (deterministic, injective for MAX_TOPIC inputs).
- [ ] **Step 2: Run test to verify it fails** — `cargo test -p tines-ros2` — Expected: FAIL.
- [ ] **Step 3: Implement** — topic escaping, then the node (rclrs): bridge topics ↔ observations, publish receipts, subscribe app observations.
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS (unit tests only; live-graph tests are `#[ignore]`d).
- [ ] **Step 5: Wire CI** — `scripts/ci.sh` changes `cargo test --workspace` and related commands to `--exclude tines-ros2`; `scripts/check-ros2.sh` builds/tests `tines-ros2` when ROS2 is present (documented, not run in CI).
- [ ] **Step 6: Commit** — `git commit -m "feat(ros2): self-contained ROS2 node"`

### Task 10: tines (standalone service)

**Files:**
- Create: `tines/Cargo.toml`, `tines/src/main.rs`, `tines/src/ipc.rs`
- Modify: `Cargo.toml` (workspace root: add member `tines`)
- Test: `tines/tests/ipc.rs`

**Interfaces:**
- Consumes: `Node`, `LogStore`, `BtTransport`; a Unix-domain-socket IPC loop.
- Produces: `tines` binary: a standalone service owning `Node<LogStore, BtTransport>` with a Unix socket (small request/response protocol: `observe`, `belief`, `events`, `status`). One instance per device.

- [ ] **Step 1: Write the failing tests** — `ipc_observe_and_belief_roundtrip`: a client sends an `observe` request over the socket, then a `belief` request returns the stored observation; `ipc_events_drains`: after a peer ack, an `events` request surfaces confidence events.
- [ ] **Step 2: Run test to verify it fails** — `cargo test -p tines` — Expected: FAIL.
- [ ] **Step 3: Implement** — the IPC protocol and the `main` wiring (Node + LogStore + BtTransport + poll loop + socket serve).
- [ ] **Step 4: Run test to verify it passes** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat: standalone tines service with Unix-socket IPC"`
