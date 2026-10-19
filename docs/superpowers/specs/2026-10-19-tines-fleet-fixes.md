# tines fleet deployment v1 — fixes from the security assessment

Date: 2026-10-19
Status: draft v1. Not yet reviewed.
Fixes: findings F1–F5 and F17 in `THREAT_MODEL.md` (assessed at `3b1a644`)
Amends: `2026-10-18-tines-fleet-deployment-design.md` (milestone M3)

## Intent

The fleet milestone's code has landed, but the security assessment found
that several of its promises don't hold even with no attacker present:

- custody is lost when a store is full;
- the standalone service never exchanges anything;
- BLE can't accept connections and doesn't keep frames intact;
- restarts repeat sequence numbers and forget released messages;
- one corrupt log record deletes the rest of the log;
- the service's clock runs in the wrong units.

This spec fixes those six findings so M3 does what its spec says. It adds
no features.

## Why now, and why not in 1.0

These are M3 bugs. M3 can't ship with them, so they can't wait for 1.0.
All but F1 live in host crates (`tines-store`, `tines-bt`, `tines`,
`tines-ros2`), which stay 0.x and aren't covered by the 1.0 promise (1.0
spec, D4). The core-side hardening that does belong with 1.0 (F6, F8, F9,
F13) is in the 1.0 spec, §10. Host hardening (F10–F12) has its own spec,
`2026-10-19-tines-host-hardening.md`.

## Goals

1. Custody is never released for a message the receiver didn't keep (F1).
2. The standalone `tines` service exchanges observations over its
   transport (F2).
3. `tines-bt` has the following properties (F3):
   - it accepts as well as initiates connections;
   - it preserves frame boundaries;
   - it binds each connection to the advertised NodeId;
   - it reports `Contact`/`Lost`.
4. A robot's `next_seq` never repeats across restarts, and released flags
   survive restarts (F4).
5. Log recovery loses only a torn tail. A corrupt record elsewhere costs
   that record, not the rest of the log (F5).
6. Every host uses one monotonic millisecond clock for `Tick` (F17).

## Non-goals

- **Authentication or encryption of any kind.** The NodeId binding in
  goal 3 is only as trustworthy as an unauthenticated advertisement. It
  stops accidents and makes `from` checks meaningful; it doesn't stop an
  attacker (1.1, 1.0 spec §2b).
- Growth bounds, trimming defaults, bounded channels, IPC permissions, ROS2
  isolation: host hardening spec.
- Peer eviction, summary rate limits, identity pinning in core: 1.0 spec
  §10.
- Multi-peer configuration of the service beyond what F3 needs (see §3).

## Constraints

- tines-core invariants are unchanged (no_std, no I/O, no panics on
  untrusted input, `Addr` ↔ NodeId mapping stays in the transport).
- **The F1 fix is a core behaviour change** and must keep the fleet sim
  properties green, plus add the one they missed.
- No robots run tines yet, so log formats may change without migration:
  existing logs are wiped (1.0 spec, D5).
- TDD per AGENTS.md: each finding gets a failing regression test first.

## The how

### 1. F1 — ack only what was kept (tines-core)

`Exchange::accept` (exchange.rs ~795-820) currently enqueues an ack for
every receive except a released addressed duplicate, including when
`insert` fails.

New rule, matching the fleet spec's own eligibility rule ("B never acks a
… store-full receive"):

| `insert` result | Addressed | Broadcast |
|---|---|---|
| `Ok(true)` (new) | ack | ack |
| `Ok(false)`, local copy not released | ack | ack |
| `Ok(false)`, local copy released | **no ack** (existing rule) | n/a |
| `Err(_)` (full, backend error) | **no ack** | **no ack** |

- Broadcasts follow the same rule. Acking an unstored broadcast would fire
  a false confidence event at the sender.
- No bounce record on `Err` (already true).
- **Tests:**
  - A unit test with a store that returns `Full`: no Ack frame is
    emitted, and the sender's copy stays unreleased.
  - A sim property: the "≥ 1 holder" invariant holds when some nodes use a
    store with a small capacity that returns `Full`. Add a capped wrapper
    store to tines-sim for this; `MemStore` never fills.
- Fix the `accept` comment ("even an un-stored id").

### 2. F2 — the service runs the exchange (tines)

The service gets a poll loop. Today `serve()` only answers IPC.

- **Structure:**
  - `Node` lives behind a `Mutex`, shared by the IPC thread and a new
    exchange thread.
  - The exchange thread loops:
    1. `poll(now)` until it returns 0 or a per-iteration cap is reached;
    2. sleep for `poll_interval_ms` (config, default 20 ms);
    3. on schedule, `trim(policy, now)` and `store.flush()`. The trim
       policy and its defaults are in the host hardening spec; this spec
       only requires that the hook exists and is called.
- **Lock poisoning:** a panic in either thread exits the process with a
  non-zero code. Systemd restarts it (host hardening spec). Continuing with
  a node in an unknown state isn't allowed.
- **Generic over `Transport`:** the service's core loop is generic over
  `Transport`, so tests can run it on an in-memory link.
- **Test:** two service instances over an in-memory transport. An
  observation made through one's IPC becomes a belief readable through the
  other's IPC.

### 3. F3 — tines-bt: framing, roles, binding, contact events

1. **Preserve frame boundaries.** Switch from `l2cap::Stream` to
   `l2cap::SeqPacket` (`SeqPacket::connect` and `SeqPacketListener`, both
   in bluer 0.17). One L2CAP SDU is one frame.
   - The negotiated MTU is still reported through `mtu()`, clamped to
     `MAX_FRAME`.
   - If SeqPacket proves unusable on the Jetson or Pi BlueZ versions, the
     fallback is an explicit `u16` length prefix on a stream. The builder
     records which was chosen and why.
2. **Roles.** Every node both advertises and scans.
   - When a node discovers an eligible peer (see binding), the one with the
     numerically smaller NodeId dials, per the fleet spec rule
     (`is_central`).
   - The larger one only accepts, on a `SeqPacketListener` bound to the
     tines PSM (`0x008D`).
   - So every node runs a listener for the whole session, not only after it
     loses a race.
3. **Binding.** The scanner decodes the advertised NodeId from service data
   (UUID `0xFE7E`, checking the UUID, which `decode_node_id` doesn't do
   today) and records `bt address → NodeId` for each eligible advertiser.
   - **Eligible** means either a NodeId in the configured allowlist
     (`peers`, a list; empty means any tines advertiser), or any tines
     advertiser if no allowlist is set.
   - **Outbound connection:** bound to the NodeId that was advertised by the
     address it dials.
   - **Inbound connection:** bound via `peer_addr()` looked up in the scan
     map. An inbound connection from an address with no recent eligible
     advertisement is closed immediately.
   - **Use the discovered address type.** Today it's hard-coded to
     `LePublic` (transport.rs:307).
4. **Addr and the `from` check.**
   - `Addr` is the bound NodeId of the connection a frame arrived on.
     `read_loop` stamps frames with that bound NodeId, not the configured
     `peer`.
   - `BtTransport::poll` drops any frame whose decoded `from` differs from
     that bound `Addr`. The check now means something; it remains
     unauthenticated.
   - The transport doesn't decode frames; it peeks at the `from` field.
     Because core's `Frame` layout is private after 1.0, core exposes a
     `pub fn frame_sender(bytes) -> Option<NodeId>` for this. That's new
     public API, called out per AGENTS.md, and the only one this spec adds.
     It's added to the 1.0 public surface (1.0 spec §3).
5. **Contact and Lost.**
   - A connection that is up emits `Event::Contact(addr)`.
   - Its end (EOF, error, adapter loss) emits `Event::Lost(addr)`. Today
     neither is emitted, so core's per-contact state never resets.
   - After a loss, scanning and listening continue, so the node reconnects.
6. **Several peers.** The radio supports up to `MAX_PEERS` concurrent
   connections, one per bound NodeId; a second connection to the same
   NodeId replaces the first. The single `peer` config field becomes the
   optional `peers` allowlist (see 3). This is the minimum needed for a
   fleet of more than two robots to work at all.
7. **Errors aren't swallowed.** Failures of the radio thread, adapter
   power-on and advertising are surfaced, either through `RadioEvent` or as
   a returned error the host logs. The radio thread's exit is reported.
   Today all of these are discarded or only logged.

**Tests** (the `Radio` trait already makes the transport testable without
hardware):
- frame boundaries are preserved for back-to-back frames;
- a frame with a mismatched `from` is dropped;
- `Contact`/`Lost` are emitted;
- an inbound connection from an unknown address is rejected;
- the role is decided by NodeId order.

The SeqPacket and BlueZ path gets a manual two-device check, written up in
`tines-bt/README.md`.

### 4. F4 — seq continuity and durable releases (tines-store, both hosts)

1. **Boot counter.** `LogStore` keeps a persisted boot counter.
   - `LogStore::open` increments it and durably writes it (`fsync`) before
     returning.
   - It then exposes `first_seq() -> u64 = boot_count << 32`, which is
     core's documented contract.
   - Both hosts seed `Node::new` from `store.first_seq()`, never from
     config. The `first_seq` config field is removed.
   - A boot can make up to 2³² observations, which is enough for any
     robot.
2. **Remove the insert-counting `next_seq`.** `LogStore::next_seq`
   (log.rs:352) counts received observations too, so it isn't the node's
   seq. It's removed along with the `Meta` record's `next_seq` field.
3. **Releases are written when they happen.**
   - `Store::release` appends a `Release { id }` record immediately, rather
     than relying on a later `flush` to write the whole released set in
     `Meta`. `Meta`'s `released` list goes away, which also removes the
     oversized-`Meta` self-truncation in F5.
   - `release` stays infallible (1.0 spec §4): an append error is logged,
     and the worst case is a duplicate custodian after a reboot.
4. **Flush on a schedule.** Both hosts call `store.flush()` at least every
   `flush_interval_ms` (config, default 1000 ms) and on clean shutdown.
   `flush` is `fsync`.
5. **Clock marker for ages across reboot.** The fleet spec promised that
   pre-reboot elapsed time is folded into ages, but there's no clock-marker
   record today.
   - `flush` appends a `Clock { tick }` record. On load,
     `age += last_clock_tick − stored_at` for each live observation,
     saturating, before rebasing.
   - Downtime stays invisible (no RTC), as the fleet spec accepted.
   - The marker is only meaningful within a boot, so the hosts must use
     the F17 clock.
6. **Tests:**
   - Reopen twice: `first_seq` strictly increases, and observations made
     after each reopen don't collide.
   - Release, kill without flush, reopen: the release survives.
   - Ages after reopen include the time since `stored_at` up to the last
     clock marker.

### 5. F5 — log integrity and recovery (tines-store)

New log format, version 1. Old logs are refused, and logs are wiped
(D5).

```
file    = header record*
header  = magic "TINESLOG" ‖ format_version: u16 ‖ reserved: u16
record  = sync: u32 (constant) ‖ len: u32 ‖ crc32c: u32 ‖ body[len]
```

- **CRC32C** covers `len ‖ body`. A per-record checksum protects against
  accidental corruption only; a keyed MAC is out of scope (threat model
  F5, A5).
- **Write cap.** `write_record` refuses a record over `MAX_RECORD_BYTES`
  (8 KiB) with `StoreError::Backend`, so the writer can no longer produce
  a record the reader rejects.
- **Recovery rule:**
  - A record whose CRC, length or decode is bad is **skipped**: scan
    forward for the next `sync` value, and check that candidate's CRC
    before accepting it.
  - Only bad bytes at the end, with no valid record after them, count as a
    torn tail.
  - A torn tail is truncated, then the file and its directory are fsynced.
  - Skipped corrupt records are counted and reported once on open (stderr,
    and a counter the host can read).
- **Ids are recomputed** from the decoded observation on load. The stored
  `id` field is dropped from the format.
- **Exclusive lock.** `open` takes an exclusive lock (`File::try_lock`,
  stable since Rust 1.89; the toolchain is 1.99). A second opener gets a
  clear error instead of interleaving.
- **Permissions.** Create the file with mode `0600`, and create its parent
  directory (mode `0700`) if it's missing.
- **Header errors.** A missing or unknown header is an error that names
  the fix (delete the log), per 1.0 spec §9. The 1.0 work then bumps
  `format_version` for the new `Observation` encoding, and doesn't
  introduce the header itself.
- **Tests:**
  - corrupt middle record: the records after it survive;
  - torn tail: truncated;
  - an oversized record is refused at write;
  - a second `open` fails while the first holds the lock;
  - permissions are `0600`.

### 6. F17 — one monotonic millisecond clock (tines, tines-ros2)

- Each host has one `HostClock`: an `Instant` captured at process start.
  `now() -> Tick` is the milliseconds elapsed since then.
  - It replaces `now_ms()` in `tines/src/ipc.rs`, which returns
    wall-clock *seconds*.
  - It's the clock the ROS2 host documents for callers of
    `TinesNode::poll(now)`.
- `LogStore::open` takes `now` from the same clock. That's `0` at
  startup, which is now correct by construction: the clock starts at 0.
- **Wall-clock time is used only for `observed_at`** (1.0 spec §2,
  milliseconds since the Unix epoch), and only when the host is configured
  to trust its clock.
- **Test:** the service's tick advances in milliseconds, and an addressed
  observation with `lifetime_ms = 1000` expires after about 1 s, not
  about 1000 s.

## Acceptance

- `sh scripts/ci.sh` is green, with every regression test above added
  first and seen failing.
- The fleet sim properties still pass, plus the new full-store custody
  property.
- A manual two-robot BLE check is done: both directions connect, frames
  round-trip, the connection is lost and re-established, and an
  observation propagates.
- `THREAT_MODEL.md` marks F1–F5 and F17 fixed, with the fixing commit.
- The fleet spec's claims that were false (F3 binding, released flags
  across reboot, ages across reboot) are now true, so no errata are
  needed.

## Left to the builder

- SeqPacket vs length-prefixed stream, if SeqPacket misbehaves on the
  target BlueZ (record the outcome).
- The `sync` constant value, and the CRC crate (`crc32c` or equivalent,
  `std` only).
- The poll loop's per-iteration cap and lock granularity.
- The scan-map retention time for "recent eligible advertisement".

## Sequencing

1. F1 (core, small).
2. F17 (both hosts; the other fixes depend on the clock).
3. F4 and F5 together (one log format change).
4. F2.
5. F3 (largest; needs `frame_sender` from core).
