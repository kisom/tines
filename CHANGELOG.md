# Changelog

All notable changes to the tines crates. Versions follow
[Semantic Versioning](https://semver.org/); before 1.0, a minor bump may break
the API.

## 1.0.0 — 2026-10-21

tines-core 1.0: the wire format is frozen (wire v1), the public API is
narrowed and documented, and the core hardening from the 1.0 spec is in.
This release ships the fleet-deployment milestone (handoff receipts) and the
remaining fleet fixes (F1–F5, F6, F8, F9, F13, F17; see `THREAT_MODEL.md`).

### Breaking (from 0.3.0)

- **Wire v1** (tines-core). Every frame now leads with a `WIRE_VERSION` byte
  (4). 0.x-shaped frames are rejected as `WireError::VersionMismatch`.
- **`Dest` replaces `to`/`lifetime_ms`** (tines-core). Addressed delivery now
  takes `dest: Option<Dest>` where `Dest { to, lifetime_ms }`; the lifetime is
  excluded from observation identity.
- **Signature slot** (tines-core). `Observation` carries a reserved
  `sig: Option<Signature>` (algorithm + up to 64 bytes). 1.0 enforces the
  wire shape but does not sign or verify (1.1).
- **Public API narrowing** (tines-core). Nine items moved `pub(crate)`,
  eleven types are `#[non_exhaustive]` (build via constructors, not struct
  literals). `Frame` and `WireError` are now *public*: the wire format is the
  1.x compatibility contract.
- **`Store` trait settled** (tines-core). `flush(&mut self, now)` and
  `is_empty` joined the trait; `Node::store_mut` is gone. A new opt-in
  `WallClockStore` trait carries the trusted-time operations
  (`set_wall_clock`, `peer_time_estimate`) that only `LogStore` implements.
- **`NodeEvent` + counted drops** (tines-core). Exchange outcomes surface as
  `NodeEvent` (`Delivered`, `Dropped { reason, count }`) through
  `Node::drain_events`; `DropReason` is `VersionMismatch`, `Malformed`, or
  `IdentityChanged`.
- **`Node::poll` is the only pump** (tines-core). The separate custody
  re-offer pass folded into `poll`.
- **Log format v2** (tines-store). `FORMAT_VERSION` bumped for the new
  `Observation` encoding; opening an older log is a hard error that names
  the fix (delete the log).
- **Quarantine + wall-clock fold** (tines-store). `LogStore::open(path, now)`
  takes no wall time; observations held across a power-off are quarantined
  until the host feeds trusted time via `set_wall_clock`.
- **Trusted wall time in the hosts** (tines). A `trusted_clock = "system"`
  config flag or a peer-derived estimate (smallest `observed_at + age_ms`)
  anchors the wall clock once; observations carry `observed_at: None` until
  then.
- **MSRV** (tines-core). `rust-version = "1.82"`; CI builds and tests
  tines-core alone on it.

### Added

- **Handoff receipts** (tines-core). The new `Ack` frame (the last `Frame`
  variant) carries up to `ACK_IDS_CAP` (90) ids. Acks are sent as the
  exchange runs and are accepted only from a peer the id was sent to during
  the current contact.
  - An ack for an addressed observation moves custody: the sender releases
    its copy (`Store::release`) and never offers it again.
  - An ack for a broadcast is confidence only. It surfaces as a
    `NodeEvent<A>::Delivered { peer, id }` from the new `Node::drain_events`.
  - Broadcast confirmations and counted drops both come through
    `Node::drain_events`; a confirmed broadcast arrives as
    `NodeEvent::Delivered`, and a frame the peer dropped arrives as
    `NodeEvent::Dropped { reason, count }` (§3).
- **Counted drop reporting** (tines-core). A frame the peer could not use is no
  dropped silently: the exchange counts it under a `DropReason` (`VersionMismatch`
  or `Malformed`) keyed by the peer's transport address and reports the count
  through `Node::drain_events` as `NodeEvent::Dropped`. Counters are held in a
  bounded table (`consts::DROP_TABLE_CAP`) with a single overflow slot for the
  case where the table is full, so an adversarial peer can't spin forever — it
  just reports `addr: None`. Expired addressed observations dropped by policy are
  *not* counted.
- New constants: `ACK_IDS_CAP`, `EVENT_QUEUE_CAP`, `BCAST_SENT_CAP`,
  `ACK_BUFFER_CAP`.
- `wire::frame_sender(bytes) -> Option<NodeId>`: a frame's claimed sender,
  for transports that bind a connection to an identity.
- `Node::store_mut`, so a host can call backend methods such as
  `LogStore::flush`.
- **New host crates:**
  - `tines-store`: `LogStore`, a persistent append-only backend.
  - `tines-bt`: a BLE transport over BlueZ.
  - `tines`: a standalone service with Unix-socket JSON IPC.
  - `tines-ros2`: a ROS2 node; rclrs is behind the `ros2` feature.

### Changed

- **Addressed delivery is a custody chain.** It used to be epidemic
  forwarding. A message has one custodian at a time, which hands it on and
  releases it when the next holder acks. A lost ack can leave two
  custodians for a while; a message is never dropped by the protocol.
- Bounce records carry a direction, so only records of an actual send
  validate an ack.
- **`LogStore` log format v1** (incompatible; there's no migration, so delete
  old logs):
  - The file starts with a `"TINESLOG"` header and a format version.
  - Each record has a sync marker, a length and a CRC-32C.
  - Ids are recomputed on load, not stored.
- `LogStore` persists a boot counter, and `first_seq()` returns
  `boot_count << 32`. Both hosts seed `Node::new` from it, and the
  `first_seq` config field is gone.
- `LogStore::release` writes a `Release` record immediately. `flush` writes
  a clock marker, so ages carry across a restart.
- `LogStore::open` takes an exclusive lock and creates the file `0600`.
- tines-bt rewrite:
  - L2CAP SeqPacket channels, one frame per packet.
  - Every node advertises and scans; the smaller `NodeId` dials and the
    larger accepts.
  - Connections are bound to the advertised `NodeId`, and frames whose
    claimed sender differs are dropped.
  - Connections emit `Contact` and `Lost`.

### Fixed

- A receiver whose store returned `Err` (for example `Full`) still acked, so
  the sender released custody of a message nobody kept. Now nothing is
  acked unless it was stored, addressed or broadcast (F1).
- The standalone service never polled its `Node`, so it never exchanged
  anything. It now runs the exchange on a poll loop (F2).
- tines-bt had no listener, lost frame boundaries on a stream socket, and
  stamped every frame with the configured peer (F3).
- Sequence numbers repeated after every restart, and released flags were
  never persisted (F4).
- One corrupt record in the middle of the log truncated everything after it.
  Recovery now skips bad records and truncates only a torn tail, with an
  fsync of the file and its directory (F5).
- The service's tick was wall-clock *seconds*. Both hosts now use one
  monotonic millisecond clock (F17).

### Known issues

- The service talks to exactly one configured `peer`. The multi-peer
  allowlist the fleet-fixes spec describes isn't wired up (F3).
- Neither host flushes on a timer, and tines-ros2 never flushes or trims
  (F18).

## 0.3.0 — 2026-10-18

### Breaking

- `Store` now requires `release(&mut self, id)` and
  `is_released(&self, id) -> bool`. Implementations of `Store` — including test
  doubles and future backends — must provide them.

### Added

- `Store::release` marks an observation as sent, so it is no longer offered for
  exchange. `MemStore` tracks a per-entry `released` flag, and its
  `for_each_addressed_id` skips released ids as an optimization (the exchange
  predicate remains the load-bearing check). Releasing an id that is not stored
  is a no-op.

## 0.2.1 — 2026-10-18

### Changed

- Package metadata only: `repository`, `documentation`, `readme`,
  `keywords` and `categories`, shared through `[workspace.package]`. The
  README now appears on crates.io.
- Each crate now ships the Apache-2.0 `LICENSE` text.

## 0.2.0 — 2026-10-18

Hardening from a full-codebase review. Wire-compatible with 0.1.0 except
where noted.

### Added

- `Exchange::on_send_error`: rolls back the last `next_frame` after a failed
  send. `Node::poll` calls it, so a `Busy` or `Unreachable` frame is retried
  instead of being lost.
- `Store::for_each_addressed_id`, with a default that visits every id.
  `MemStore` overrides it, so custody no longer reads every broadcast
  observation on each poll.
- `Bloom::fold`, and `Bloom::from_parts` now accepts any power-of-two length up
  to `SUMMARY_BYTES`. Summaries are folded to fit the transport's MTU; a
  0.1.0 node rejects a folded summary, so mixed-version links only sync when
  the MTU allows a full 512-byte filter.
- `InFlight`, describing a fragment stream in progress.

### Changed

- **Breaking:** `Peer::in_flight` is now `Option<InFlight>` (was
  `Option<(ObservationId, u8)>`).
- Custody (addressed) and broadcast sending alternate per peer, and addressed
  candidates are taken in rotation.
- Bounce suppression lasts one contact, as the addressed-delivery spec says.

### Fixed

- A fragment stream could panic, or send corrupt fragments, when the
  observation's age changed encoded length mid-stream or the tick went
  backwards.
- Summaries larger than the MTU stopped all broadcast sync on narrow links.
- A full bounce table silently stopped recording, so one addressed message
  was re-sent on every poll and starved broadcast.
- An addressed send interrupted another peer's fragment stream, which was
  then never finished.
- An addressed message that couldn't fit the MTU blocked every other
  addressed message for that peer.
- A duplicate addressed message was forwarded back to the carrier that sent
  it.

### CI

- Tests, clippy and docs also run without the `alloc` feature; doc warnings
  fail the build; `--locked` everywhere; toolchain pinned to 1.99.0; actions
  pinned to commit SHAs.

## 0.1.0

Initial release: `tines-core` and `tines-sim`.
