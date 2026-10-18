# Addressed Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add addressed store-and-forward to tines — an observation can name a recipient (`to`), and any node carries it (multi-hop, epidemic, lifetime-bounded) until it reaches that recipient or expires.

**Architecture:** Extend `Observation` with `to: Option<NodeId>` and `lifetime_ms: Option<u64>`; the content ID now includes `to`. Broadcast (`to = None`) keeps the existing bloom-summary gossip unchanged. Addressed (`to = Some(C)`) messages live in the same store, are excluded from blooms, and are forwarded on contact — delivered to the recipient, epidemic-forwarded to anyone else — and dropped once expired or over the store's `max_count`.

**Tech Stack:** Rust (`#![no_std]` core + `std` simulator), postcard (wire), Blake2s-256 (identity), heapless, proptest.

**Spec:** `docs/superpowers/specs/2026-10-18-tines-addressed-delivery.md`

## Global Constraints

- `tines-core` is `#![no_std]`; `alloc` is feature-gated (default on); only `MemStore` may require `alloc`. `--no-default-features` must build for `thumbv7em-none-eabihf`.
- No async, threads, I/O, or panics on untrusted wire input.
- Identity = `first16(Blake2s-256(observer ‖ seq_le ‖ topic_len_le(u16) ‖ topic ‖ payload_len_le(u16) ‖ payload ‖ to_marker))`, where `to_marker` is `0x00` for `None`, `0x01 ‖ node_id` for `Some`. `lifetime_ms` is excluded from identity.
- Invariant: `lifetime_ms.is_some() == to.is_some()`.
- Addressed messages (`to = Some(..)`) never appear in a bloom summary.
- Expired addressed messages are never stored (expiry checked before insertion).
- An addressed message is expired when `current_age > lifetime_ms` (age accumulates per hop).
- Constants unchanged: `MAX_TOPIC=64`, `MAX_PAYLOAD=1024`, `MAX_PEERS=8`, `SUMMARY_BYTES=512`, `BLOOM_K=3`, `SUMMARY_INTERVAL_MS=2000`, `REASSEMBLY_TIMEOUT_MS=5000`, `SEND_QUEUE_CAP=256`, `MAX_FRAME=1500`.

## Review Focus

1. A wire frame whose `to`/`lifetime_ms` violate the invariant (`to = Some` but `lifetime_ms = None`, or the reverse) is dropped, never stored, never panics. (Task 4)
2. An expired addressed message is rejected on both the `Obs` and the reassembled-`Frag` receive paths. (Task 4)
3. A message addressed to a peer whose `NodeId` is not yet known is still held and epidemic-forwarded, never dropped on sight. (Task 5)
4. A message never bounces back to the peer it was received from (loop prevention). (Task 5)
5. `max_count` trim removes the oldest addressed messages too, not only broadcast. (Task 2)

---

### Task 1: Observation gains `to`/`lifetime_ms`; identity includes `to`

**Files:**
- Modify: `tines-core/src/observation.rs` (fields, `id()`, `aged()`)
- Modify (add `to: None, lifetime_ms: None` to every literal): `tines-core/src/node.rs` (`observe`), and test helpers in `observation.rs`, `mem_store.rs`, `belief.rs`, `exchange.rs`, `frag.rs`
- Test: `tines-core/src/observation.rs`

**Interfaces:**
- Produces: `Observation { observer: NodeId, seq: u64, age_ms: u64, observed_at: Option<u64>, topic: Topic, payload: Payload, to: Option<NodeId>, lifetime_ms: Option<u64> }`.
- `id()`: append `to_marker` to the hash — one byte `0x00` if `to.is_none()`, else `0x01` followed by the 16 `NodeId` bytes.
- `aged(elapsed_ms)`: clones `to` and `lifetime_ms` unchanged, advances `age_ms` (saturating) as before.

- [ ] **Step 1: Write the failing tests**

Add to `observation.rs`'s `tests` module (extend the `obs` helper with `to`/`lifetime_ms`, both defaulting to `None`):

- `to_is_part_of_identity` — same `obs(7, 0, None)` with `to = None` vs `to = Some([2; 16])`: `assert_ne!` the ids.
- `lifetime_is_not_part_of_identity` — same payload, both `to = Some([2; 16])`, `lifetime_ms = 100` vs `200`: `assert_eq!` the ids.
- `broadcast_marker_distinct_from_zero_id` — `to = None` vs `to = Some([0; 16])`: `assert_ne!` the ids.
- `aged_preserves_addressing` — `obs` with `to = Some([2; 16])`, `lifetime_ms = Some(500)`; `.aged(10)` has `age_ms` +10 and unchanged `to`/`lifetime_ms`.
- Update `id_known_vector` (broadcast, `to = None`) to expect exactly:
  `[0x4a, 0x33, 0x09, 0x92, 0xf1, 0x61, 0xbe, 0xc9, 0x19, 0x7b, 0x6e, 0x96, 0x11, 0x6f, 0xa4, 0xa2]`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-core observation` — expected: compile errors (`to`, `lifetime_ms` unknown on `Observation`) plus the changed known-vector.

- [ ] **Step 3: Implement** the two fields on `Observation`, the `to_marker` in `id()`, `aged()`'s field clones, and add `to: None, lifetime_ms: None` to every `Observation { .. }` literal across the crate and its test helpers.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --workspace` — expected: all pass (the known-vector now matches the new hash).

- [ ] **Step 5: Commit**

```bash
git add tines-core/src tines-core/tests
git commit -m "feat(core): Observation gains to/lifetime_ms; identity includes to"
```

---

### Task 2: `Store::for_each_broadcast_id`

**Files:**
- Modify: `tines-core/src/store.rs` (trait method), `tines-core/src/mem_store.rs` (impl)
- Test: `tines-core/src/mem_store.rs`

**Interfaces:**
- Produces: `fn for_each_broadcast_id(&self, f: &mut dyn FnMut(&ObservationId))` on `Store` — visits the ids of observations with `to == None`, in no particular order.

- [ ] **Step 1: Write the failing test**

`for_each_broadcast_id_skips_addressed` — insert one broadcast obs (`to = None`) and one addressed obs (`to = Some([2; 16])`); collect `for_each_broadcast_id` ids; `assert_eq!` the set to only the broadcast id. (Review Focus 5 lives here too: `trim_max_count_drops_oldest_addressed` — insert 10 addressed obs and one broadcast, trim `max_count = Some(4)`, assert the broadcast obs survives and only 4 remain.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-core mem_store` — expected: `no method named for_each_broadcast_id`.

- [ ] **Step 3: Implement** the trait method and the `MemStore` impl (iterate the `BTreeMap` values, skip `held.obs.to.is_some()`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines-core mem_store` — expected: pass.

- [ ] **Step 5: Commit**

```bash
git add tines-core/src/store.rs tines-core/src/mem_store.rs
git commit -m "feat(core): Store::for_each_broadcast_id"
```

---

### Task 3: Summaries carry only broadcast observations

**Files:**
- Modify: `tines-core/src/exchange.rs`
- Test: `tines-core/src/exchange.rs`

**Interfaces:**
- Consumes: `Store::for_each_broadcast_id` (Task 2).

- [ ] **Step 1: Write the failing test**

`addressed_obs_not_in_summary` — a store holding one broadcast obs `b` and one addressed obs `a` (`to = Some([2; 16])`); `Contact(1)`, then `next_frame` returns a `Summary`; decode it and assert `Bloom::from_parts(salt, &bits)` has `might_contain(b.id()) == true` and `might_contain(a.id()) == false`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-core exchange::tests::addressed_obs_not_in_summary` — expected: the addressed id is still in the bloom (assert fails).

- [ ] **Step 3: Implement** — in `next_frame`'s summary block, replace `store.for_each_id(..)` with `store.for_each_broadcast_id(..)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines-core exchange` — expected: pass.

- [ ] **Step 5: Commit**

```bash
git add tines-core/src/exchange.rs
git commit -m "feat(core): summaries carry only broadcast observations"
```

---

### Task 4: Reject expired or malformed addressed messages

**Files:**
- Modify: `tines-core/src/exchange.rs` (`on_frame`, both `Obs` and `Frag` arms)
- Test: `tines-core/src/exchange.rs`

**Interfaces:**
- Consumes: `Observation` with `to`/`lifetime_ms`.
- Produces: a receive-side guard — an addressed observation is inserted only if it is not expired and its `to`/`lifetime_ms` satisfy the invariant.

- [ ] **Step 1: Write the failing tests**

- `expired_addressed_obs_is_rejected` — an `Obs` frame with `to = Some(ME)`, `lifetime_ms = Some(100)`, `age_ms = 200`: `on_event` returns `None` and `store.len() == 0`.
- `expired_addressed_frag_is_rejected` — a fragmented observation with the same expired addressing (build via `crate::frag::fragment` after setting `age_ms = 200`, `lifetime_ms = Some(100)`); feed the frags; `store.len() == 0`. (Review Focus 2)
- `addressing_invariant_violation_is_dropped` — `to = Some(ME)`, `lifetime_ms = None`: `on_event` returns `None`, `store.len() == 0`, no panic. (Review Focus 1)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-core exchange` — expected: the expired/malformed messages are still stored (asserts fail).

- [ ] **Step 3: Implement** — a private helper, e.g. `fn reject(obs: &Observation) -> bool { obs.to.is_some() && (obs.lifetime_ms.is_none() || obs.age_ms > obs.lifetime_ms.unwrap()) }` (the invariant check folds `lifetime_ms = None` into "reject"), called in both receive arms before `store.insert`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines-core exchange` — expected: pass.

- [ ] **Step 5: Commit**

```bash
git add tines-core/src/exchange.rs
git commit -m "feat(core): reject expired or malformed addressed messages"
```

---

### Task 5: Addressed store-and-forward in the exchange

**Files:**
- Modify: `tines-core/src/exchange.rs` (forwarding + bounce state)
- Test: `tines-core/src/exchange.rs`

**Interfaces:**
- Consumes: `Observation` with `to`/`lifetime_ms`, `Peer::id`.
- Produces: addressed forwarding in `next_frame` — when a peer's `id` is known, emit (one frame per poll, round-robin over peers and over the store's non-expired addressed observations): delivery when `obs.to == Some(peer.id)`, epidemic otherwise; skip a message's recorded source peer (bounce suppression); addressed forwarding must not starve the broadcast diff.

- [ ] **Step 1: Write the failing tests**

- `addressed_obs_delivered_to_recipient` — store has `to = Some([2; 16])`; `Contact(1)`, then a `Frame` from addr 1 with `from = [2; 16]` (so `peer.id = Some([2; 16])`); drain `next_frame` (skip the summary) and assert an `Obs` with `obs.to == Some([2; 16])` is emitted to addr 1.
- `addressed_obs_epidemic_forwards_to_non_recipient` — same setup but `to = Some([3; 16])`, peer id `[2; 16]`; assert an `Obs` with `to = Some([3; 16])` is emitted to addr 1 (Review Focus 3: the unknown recipient never drops the message).
- `addressed_obs_does_not_bounce_to_source` — store is empty; `on_event(Frame(addr 1, Obs { from: [2; 16], obs: to = Some([3; 16]) }))`; then `next_frame` emits no `Obs` addressed to `[3; 16]` back to addr 1 (only a summary). (Review Focus 4)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-core exchange` — expected: no addressed frames are emitted (asserts fail).

- [ ] **Step 3: Implement** — in `next_frame`, after the summary priority, add addressed forwarding: for the round-robin peer, enumerate the store's non-expired addressed observations and emit one (delivery before epidemic). Record the source addr of each inbound addressed message (e.g. a bounded `heapless::Vec<(ObservationId, A), SEND_QUEUE_CAP>` on `Exchange`) and skip re-forwarding an id to its recorded source. Interleave with the existing broadcast `emit_one` so broadcast work still runs.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines-core exchange` — expected: pass.

- [ ] **Step 5: Commit**

```bash
git add tines-core/src/exchange.rs
git commit -m "feat(core): addressed store-and-forward in the exchange"
```

---

### Task 6: `Node::observe_to` and the app-facing view

**Files:**
- Modify: `tines-core/src/node.rs`
- Test: `tines-core/tests/pair.rs`

**Interfaces:**
- Produces: `pub fn observe_to(&mut self, to: NodeId, lifetime_ms: u64, topic: &[u8], payload: &[u8], observed_at: Option<u64>, now: Tick) -> Result<ObservationId, ObserveError>` — creates an observation with `to = Some(to)`, `lifetime_ms = Some(lifetime_ms)`, `age_ms = 0`, incremented `next_seq`.
- `Node::observe` remains broadcast-only (`to = None`, `lifetime_ms = None`).
- `Node::belief` skips `to = Some(other != me)` observations (app view = broadcast + to-me).

- [ ] **Step 1: Write the failing tests** (in `pair.rs`)

- `observe_to_creates_addressed_message` — `node.observe_to([2; 16], 5000, b"task", b"go", None, 0)` returns an id; the stored observation has `to == Some([2; 16])` and `lifetime_ms == Some(5000)`.
- `observe_to_rejects_oversized_topic` — 65-byte topic → `Err(ObserveError::TooLarge)`.
- `belief_excludes_addressed_to_others` — store a broadcast obs and an addressed-to-other obs under the same topic prefix; `belief(prefix, &LatestWins, now)` returns the broadcast obs's payload (not the addressed one).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-core --test pair` — expected: `no method named observe_to`.

- [ ] **Step 3: Implement** `observe_to`, and add the `to = Some(other != me)` guard to `Node::belief`'s closure.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines-core --test pair` — expected: pass.

- [ ] **Step 5: Commit**

```bash
git add tines-core/src/node.rs tines-core/tests/pair.rs
git commit -m "feat(core): Node::observe_to and belief excludes addressed-to-others"
```

---

### Task 7: Simulator relay scenario, property tests, convergence scoping

**Files:**
- Modify: `tines-sim/src/sim.rs` (add `Sim::observe_to`), `tines-sim/tests/props.rs` (scope + new properties)
- Create: `tines-sim/tests/relay.rs`

**Interfaces:**
- Consumes: `Node::observe_to` (Task 6), `SimWorld`/`SimTransport`.
- Produces: `pub fn observe_to(&mut self, node: usize, to: NodeId, lifetime_ms: u64, topic: &[u8], payload: &[u8]) -> ObservationId` on `Sim`.

- [ ] **Step 1: Write the failing tests**

`relay.rs` — `relayed_task_reaches_recipient`: three nodes, contacts `(0,1)` then `(1,2)`; node 0 `observe_to([2; 16], 60_000, b"task", b"investigate")`; `run_until(trace.end())`; assert `sim.nodes[2].store().contains(&id)`. Second test: `carrier_drops_expired` — a 10 ms lifetime with the same relay; after running well past expiry, assert node 2's store does not contain the id.

`props.rs` — add `addressed_reaches_recipient_along_connected_path` (a 2..=6-node connected trace; one node tasks the max-id node; assert that node's store eventually contains the id). Do **not** assert `converged` for addressed scenarios (addressed messages diverge by design); the existing `connected_components_converge` and `store_is_independent_of_contact_order` keep using broadcast-only `observe` and are unchanged.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-sim` — expected: `no method named observe_to` on `Sim`.

- [ ] **Step 3: Implement** `Sim::observe_to`, and the new tests' setup.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --workspace` — expected: all pass (including the pre-existing 85+ tests).

- [ ] **Step 5: Commit**

```bash
git add tines-sim
git commit -m "test(sim): addressed relay scenario and property tests"
```

---

## Final Gate

After Task 7, run `sh scripts/ci.sh` (fmt, clippy `-D warnings`, doc, test, no_std) — expected: exit 0.
