# Fleet fixes (security assessment F1–F5, F17) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the six M3 findings so the fleet milestone does what its spec promises. No new features.

**Spec:** `docs/superpowers/specs/2026-10-19-tines-fleet-fixes.md` (draft v1). Findings assessed in `THREAT_MODEL.md` at `3b1a644`. Host hardening (F10–F12) and 1.0 core hardening (F6/F8/F9/F13) are separate specs, out of scope here.

**Architecture:** F1 is a core behaviour change (ack eligibility). F17 touches both hosts (tines, tines-ros2). F4+F5 rewrite the tines-store log format (v1: header, CRC32C, sync markers, boot counter, durable releases, clock marker, exclusive lock, 0600 perms). F2 gives the tines service a poll loop. F3 rewrites tines-bt (SeqPacket framing, role assignment, NodeId binding, Contact/Lost, multi-peer) and adds core `frame_sender`.

## Global Constraints

- tines-core invariants unchanged: `#![no_std]`, no I/O, no panic on untrusted input, `Addr` ↔ `NodeId` mapping stays in the transport.
- F1 must keep the fleet sim properties green, plus add the full-store custody property.
- No robots run tines yet → no migration; logs are wiped (1.0 spec D5).
- TDD per AGENTS.md: each finding gets a failing regression test first.
- Gate: `sh scripts/ci.sh` (fmt → clippy `-D warnings` → doc → test → no_std). tines-ros2 stays excluded from the default gate.

## Review Focus

1. **F1**: a store-full (`Err`) receive must NOT be acked — addressed or broadcast — or custody is released for a message nobody kept. → Task 1.
2. **F4**: `first_seq` must be monotonic across reboots (boot counter), and released flags must survive a kill-without-flush. → Task 3.
3. **F5**: a corrupt *middle* record costs that record, not the rest of the log; only a torn *tail* truncates. → Task 3.
4. **F3**: inbound connections must bind to an advertised NodeId, and frame boundaries must survive the BLE stream. → Task 5.

---

### Task 1: F1 — ack only what was kept (tines-core)

**Files:** `tines-core/src/exchange.rs`; test in `tines-core/src/exchange.rs`; sim property in `tines-sim/tests/props.rs` + a capped-store wrapper in `tines-sim`.

**Interfaces:** `Exchange::accept` enqueues a pending ack only when `insert` returns `Ok` (new or non-released duplicate). `Err(_)` (full/backend) → no ack, addressed or broadcast. No bounce record on `Err` (already true).

- [ ] **Step 1:** failing test — a store returning `Full`: no `Ack` emitted; sender's copy stays unreleased.
- [ ] **Step 2:** run, see it fail.
- [ ] **Step 3:** implement (gate the ack enqueue on `res.is_ok()`; fix the `accept` comment "even an un-stored id").
- [ ] **Step 4:** green.
- [ ] **Step 5:** add the sim property (≥ 1 holder with a small-capacity store returning `Full`).
- [ ] **Step 6:** commit — `fix(core): ack only observations the store kept`.

### Task 2: F17 — one monotonic millisecond clock (tines, tines-ros2)

**Files:** `tines/src/ipc.rs` (+ main), `tines-ros2/src/node.rs` (doc).

**Interfaces:** `HostClock` (an `Instant` captured at process start; `now() -> Tick` in ms). Replaces `now_ms()` (which returns wall-clock *seconds*). `LogStore::open` takes `now` from this clock (0 at startup, correct by construction). Wall-clock only for `observed_at`.

- [ ] **Step 1:** failing test — the service's tick advances in ms; an addressed obs with `lifetime_ms = 1000` expires in ~1 s, not ~1000 s.
- [ ] **Step 2:** run, see it fail.
- [ ] **Step 3:** implement `HostClock`, swap `now_ms()` → `now()`, document the clock for `TinesNode::poll`.
- [ ] **Step 4:** green.
- [ ] **Step 5:** commit — `fix(hosts): one monotonic millisecond clock`.

### Task 3: F4 + F5 — tines-store log format v1

**Files:** `tines-store/src/log.rs`, `tines-store/src/lib.rs`, `tines-store/tests/persistence.rs`.

**Interfaces:**
- **F4:** persisted boot counter (`open` increments + `fsync`s); `first_seq() -> u64 = boot_count << 32`; remove the insert-counting `next_seq` and `Meta.next_seq`; `release` appends `Release { id }` immediately (infallible; append error logged); `flush` appends `Clock { tick }` (age fold on load, saturating); both hosts seed `Node::new` from `store.first_seq()`.
- **F5:** log v1 — header `"TINESLOG" ‖ format_version u16 ‖ reserved u16`; record `sync u32 ‖ len u32 ‖ crc32c u32 ‖ body`; CRC over `len ‖ body`; write cap `MAX_RECORD_BYTES`; recovery skips bad records (scan for next `sync`, verify CRC) and truncates only a torn tail; ids recomputed from the decoded observation (drop stored `id`); exclusive `File::try_lock`; file mode `0600`, parent dir `0700`; header error names the fix.

- [ ] **Step 1:** failing tests — reopen twice: `first_seq` strictly increases, no id collision; release + kill-without-flush + reopen survives; ages fold the clock marker; corrupt middle record: later records survive; torn tail truncated; oversized record refused at write; second `open` fails while locked; perms `0600`.
- [ ] **Step 2:** run, see them fail.
- [ ] **Step 3:** implement F4 then F5 (one format change), TDD each.
- [ ] **Step 4:** green (`cargo test -p tines-store` + full gate).
- [ ] **Step 5:** commit — `fix(store): log format v1 with durable releases and crash recovery`.

### Task 4: F2 — the service runs the exchange (tines)

**Files:** `tines/src/main.rs`, `tines/src/ipc.rs` (→ poll thread), `tines/src/lib.rs`.

**Interfaces:** `Node` behind a `Mutex`, shared by the IPC thread and a new exchange thread. Exchange thread: `poll(now)` until 0 or a per-iteration cap; sleep `poll_interval_ms` (default 20); on schedule `trim(policy, now)` + `store.flush()`. Lock poisoning → process exit non-zero. The loop is generic over `Transport`.

- [ ] **Step 1:** failing test — two service instances over an in-memory transport: an observation through one's IPC is a belief through the other's.
- [ ] **Step 2:** run, see it fail.
- [ ] **Step 3:** implement the poll thread + trim/flush schedule + poison-exit.
- [ ] **Step 4:** green.
- [ ] **Step 5:** commit — `fix(service): run the exchange on a poll loop`.

### Task 5: F3 — tines-bt framing, roles, binding, contact events

**Files:** `tines-bt/src/transport.rs`, `tines-bt/src/adv.rs`, `tines-bt/src/lib.rs`; `tines-core/src/wire.rs` (add `pub fn frame_sender(bytes) -> Option<NodeId>`).

**Interfaces:**
- `l2cap::SeqPacket` (frame = one SDU); MTU clamped to `MAX_FRAME`; fallback `u16` length prefix if SeqPacket fails on target BlueZ (record which).
- Roles: everyone advertises + scans; smaller NodeId dials; larger accepts on a `SeqPacketListener` bound to the tines PSM `0x008D`; listener runs the whole session.
- Binding: scanner decodes advertised NodeId (checking UUID `0xFE7E`), records `bt address → NodeId`; allowlist `peers` (empty = any tines advertiser); outbound bound to advertised id; inbound bound via `peer_addr()` in the scan map (unknown → close); use the discovered address type (not hard-coded `LePublic`).
- `Addr` = bound NodeId; `read_loop` stamps frames with it; `poll` drops frames whose decoded `from` ≠ bound `Addr` (peek via `frame_sender`).
- `Contact`/`Lost` on connection up/end; scan+listen continue after a loss.
- Up to `MAX_PEERS` concurrent connections, one per NodeId (replaces the single `peer` field).
- Radio/adapter errors surfaced (`RadioEvent` or returned error), not swallowed.

- [ ] **Step 1:** failing tests — frame boundaries preserved back-to-back; mismatched `from` dropped; `Contact`/`Lost` emitted; unknown inbound rejected; role decided by NodeId order.
- [ ] **Step 2:** run, see them fail.
- [ ] **Step 3:** implement (mock-`Radio` testable; SeqPacket path manually two-device, written up in `tines-bt/README.md`).
- [ ] **Step 4:** green.
- [ ] **Step 5:** commit — `fix(bt): framing, roles, NodeId binding, contact events`.

## Done means

- `sh scripts/ci.sh` green, every regression test added first and seen failing.
- Fleet sim properties pass, plus the full-store custody property.
- Manual two-robot BLE check documented in `tines-bt/README.md`.
- `THREAT_MODEL.md` marks F1–F5 and F17 fixed with the fixing commit.
