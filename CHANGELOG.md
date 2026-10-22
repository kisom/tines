# Changelog

All notable changes to the tines crates. Versions follow
[Semantic Versioning](https://semver.org/). `tines-core` is 1.x and keeps the
compatibility promise in README's "Stability" section. The host crates
(`tines`, `tines-store`, `tines-bt`, `tines-ros2`, `tines-sim`) stay 0.x,
where a minor bump may break the API.

## tines-core 1.0.0 — 2026-10-22

Implemented on `feat/tines-core-1-0`. The wire
format is frozen as wire v1, the public API is narrowed and documented
(`#![deny(missing_docs)]`), and the core hardening from the 1.0 spec
(`docs/superpowers/specs/2026-10-18-tines-1-0-design.md` §10) is in. It also
carries the fleet-deployment milestone (handoff receipts) and the fleet fixes,
which were never released on their own. Findings F1, F6, F8, F9 and F13 are
fixed in core; see `THREAT_MODEL.md`.

### Breaking (from 0.3.0)

- **Wire v1.**
  - Every frame now starts with a `WIRE_VERSION` byte (4). A 0.x-shaped frame
    is dropped and counted as `DropReason::VersionMismatch`.
  - The new `Ack` frame is the last `Frame` variant (see Added).
  - Frames must be canonical. Trailing bytes after a complete frame, or after
    a reassembled observation, are rejected and counted as `Malformed`.
- **`Dest` replaces `to`/`lifetime_ms`.**
  - An addressed observation carries `dest: Option<Dest>`, where
    `Dest { to, lifetime_ms }`.
  - The identity hash covers `dest.to`. It still excludes `lifetime_ms`,
    `age_ms` and `observed_at`.
- **Signature slot.**
  - `Observation` carries `sig: Option<Signature>` (version, `SigAlgo`, and up
    to `MAX_SIG` = 64 bytes).
  - 1.0 checks the wire shape and carries signatures verbatim. It neither
    signs nor verifies; that's 1.1. `sig` is outside the identity hash.
- **Nine items are now `pub(crate)`:**
  - `Exchange` (and with it `Exchange::on_send_error`; drive the exchange
    through `Node`);
  - `Peer`;
  - `InFlight`;
  - `Bloom` (and its public `salt` field);
  - `Reassembler`;
  - `fragment`;
  - `Frame`;
  - `WireError`;
  - `FRAG_OVERHEAD`.
- **The `bloom`, `exchange`, `frag` and `wire` modules are private**, so paths
  through them are gone:
  - `tines_core::wire::frame_sender` is now `tines_core::frame_sender`;
  - `DropReason` and `NodeEvent` are reached at the crate root.
- **Local constants are `pub(crate)`** (spec decision K). They are defaults
  that a later 1.x release will make configurable:
  - `MAX_PEERS`, `SUMMARY_INTERVAL_MS`, `REASSEMBLY_TIMEOUT_MS`,
    `SEND_QUEUE_CAP`, `EVENT_QUEUE_CAP`, `BCAST_SENT_CAP` and `ACK_BUFFER_CAP`
    are no longer in `tines_core::consts`;
  - the new local constants, `PEER_IDLE_MS`, `DROP_TABLE_CAP` and `MIN_MTU`,
    were never public;
  - only the wire constants stay public (see Added).
  - `Transport::mtu`'s rustdoc now states the smallest MTU core can use (85
    bytes to carry every valid observation).
- **Eleven types are `#[non_exhaustive]`.**
  - Enums (match with a wildcard): `ObserveError`, `StoreError`, `SendError`,
    `Event`, `NodeEvent`, `DropReason`, `SigAlgo`.
  - Structs (no struct literals outside core): `Observation`, `Dest`,
    `Signature`, `TrimPolicy`.
  - Build an `Observation` with `Observation::broadcast` or
    `Observation::addressed`, and a `TrimPolicy` with
    `TrimPolicy::default()` plus field assignment.
- **`Node::drain_events` returns `NodeEvent<T::Addr>`.** This replaces the fleet
  milestone's unreleased `ConfidenceEvent`.
- **`Node::store_mut` is removed.** It was added by the unreleased fleet fixes
  so the service could reach `LogStore::flush`. Use `Node::flush`, and
  `Node::set_wall_clock` / `Node::peer_time_estimate` for a `WallClockStore`.
  Read access through `Node::store` stays.
- **MSRV.** `rust-version = "1.82.0"`. CI builds tines-core on 1.82, with and
  without default features and for `thumbv7em-none-eabihf`. It runs no tests
  there, because a dev-dependency needs edition 2024. From now on an MSRV bump
  is a minor release.

### Added

- **Handoff receipts.** The new `Ack` frame carries up to `ACK_IDS_CAP` (90)
  ids. Acks are sent as the exchange runs, and are accepted only from a peer
  the id was sent to during the current contact.
  - An ack for an addressed observation moves custody: the sender releases
    its copy (`Store::release`) and never offers it again.
  - An ack for a broadcast is confidence only. It surfaces as
    `NodeEvent::Delivered { peer, id }` from `Node::drain_events`.
- **Counted drop reporting.** A frame this node can't use isn't dropped
  silently.
  - The exchange counts it under a `DropReason`: `VersionMismatch`,
    `Malformed`, or `IdentityChanged` (F6). The count is keyed by the
    sender's transport address.
  - `Node::drain_events` reports each counter as
    `NodeEvent::Dropped { addr, reason, count }`, after the queued
    `Delivered` events.
  - The counter table is bounded. When it's full, further drops go to one
    overflow counter, reported with `addr: None` and the reason of the first
    drop that overflowed since the last drain. Draining empties the table,
    keys included.
  - An expired addressed observation that's refused by policy is *not*
    counted.
- **`Store::flush(now) -> Result<(), StoreError>`** and **`Store::is_empty`**,
  both with safe defaults, plus **`Node::flush(now)`**. `flush` is the store's
  durability point and its time anchor for ages after a restart. Flushing
  twice is harmless, and an earlier `now` is clamped. Core never flushes on
  its own.
- **`WallClockStore`** (spec decision H). An opt-in supertrait of `Store`, for
  backends that hold data across power-off. It has an associated
  `FoldReport`, `set_wall_clock(wall_now, now)` and `peer_time_estimate(now)`.
  `LogStore` implements it; `MemStore` doesn't.
- **`Node::set_wall_clock`** and **`Node::peer_time_estimate`**. They exist
  when `S: WallClockStore` and delegate to it, so no `&mut Store` escapes the
  facade. Core forwards the wall time and never reads it.
- **`Dest`, `Signature` and `SigAlgo`**, and the constructors
  `Observation::broadcast`, `Observation::addressed` and
  `Observation::with_observed_at`. The two constructors panic on an
  oversized topic or payload; `Node::observe` and `Node::observe_to` return
  `ObserveError::TooLarge` instead.
- **`tines_core::frame_sender(bytes) -> Option<NodeId>`**: a frame's claimed
  sender, for transports that bind a connection to an identity.
- **Public wire constants:** `WIRE_VERSION`, `MAX_TOPIC`, `MAX_PAYLOAD`,
  `MAX_FRAME`, `SUMMARY_BYTES`, `ACK_IDS_CAP`, `MAX_SIG` and `BLOOM_K`.
- **`tines_core::store_conformance`**, behind the `conformance` feature and
  `std`-free. A suite that lets any backend prove the §4 `Store` contract.
  `MemStore` and `LogStore` both run it.
- The `conformance` feature also enables a `#[doc(hidden)]` `conformance`
  module of frame encoders and local limits for the fuzz harness. It isn't
  part of the supported API.

### Changed

- **Addressed delivery is a custody chain.** It used to be epidemic
  forwarding.
  - A message has one custodian at a time, which hands it on and releases it
    when the next holder acks.
  - A lost ack can leave two custodians for a while; the protocol never drops
    a message.
  - Bounce records carry a direction, so only a record of an actual send
    validates an ack.
- **Core never trusts a backend's filtering.** It re-checks `dest` and
  `is_released` on every custody candidate. It re-checks `dest` when building
  a summary, rebuilding a broadcast diff, and sending a broadcast.
- **Core hardening** (spec §10):
  - **F6:** a peer's `NodeId` is pinned by its first frame in a contact. A
    later frame with another `from` is dropped and counted as
    `IdentityChanged`. The pin resets on `Lost`, and on a repeat `Contact`
    for a known peer.
  - **F8:** a peer with no inbound frame for `PEER_IDLE_MS` (30 s) is evicted
    as if `Lost`. That includes a peer that only ever sent `Contact`. Idle
    peers are swept before a peer is added and at the start of every
    `next_frame`, and a peer that resumes later is a new implicit contact.
  - **F9:** at most one received Summary per peer is acted on per
    `SUMMARY_INTERVAL_MS`. A summary is accepted once at least half the
    interval has passed, to allow for poll jitter (spec decision L). The
    send-queue rebuild skips ids the peer's filter says it holds
    (`Bloom::might_contain`).
  - **F13:**
    - the event queue drops the oldest event when full;
    - a removed peer's pending acks are purged, and only that peer's;
    - frames, and reassembled observations, must be canonical.
- **Receipts are scoped to the contact.** Broadcast-send records for an
  address are dropped when its contact starts or ends. A broadcast whose send
  fails has its record undone, so its ack isn't accepted.

### Fixed

- A receiver whose store returned `Err` (for example `Full`) still acked, so
  the sender released custody of a message nobody kept. Now nothing is acked
  unless it was stored, addressed or broadcast (F1).
- From the 1.0 review:
  - Silent and contact-only peers were never evicted, so eight spoofed
    addresses could still lock out every newcomer (S1, F8).
  - The drop table never freed its keys. After 32 distinct
    `(addr, reason)` pairs, every drop went to the overflow slot until reboot
    (S7).
  - Losing one peer purged every peer's pending acks (S9).
  - Broadcast receipts weren't limited to this contact, and a failed send
    could still be confirmed (S10).
  - A store whose broadcast iterator yielded addressed ids could leak them
    into a summary or forward them as broadcasts (S11). A test store that
    overrides no filtering now pins the re-checks (B8).
  - A repeat `Contact` didn't reset the F6 pin.
  - A reassembled observation was decoded with trailing bytes allowed.
  - The summary rate limit could ignore an on-time summary measured a
    millisecond early.
  - `DropReason::IdentityChanged` carried a variant-level `#[non_exhaustive]`,
    so code outside the crate couldn't match it.

## Hosts 0.4.0 — Unreleased

`tines`, `tines-store`, `tines-bt`, `tines-ros2` and `tines-sim` move to 0.4.0
and depend on `tines-core = "1"`. Like tines-core 1.0.0, nothing here is
tagged or published yet.

The 0.3.0 release shipped only `tines-core` and `tines-sim`. The four other
host crates were added afterwards, during the fleet milestone, and carried the
workspace version 0.3.0 in the tree without being released. The breaks below
are measured against that in-tree 0.3.0. This release also carries the fleet
milestone's host work (F2–F5, F17).

### Breaking

- **Log format v2** (tines-store).
  - The file starts with a `"TINESLOG"` header and a format version (now 2,
    for tines-core 1.0's `Observation` encoding).
  - Each record has a sync marker, a length and a CRC-32C, and ids are
    recomputed on load.
  - Opening a log of any other version is an error that names the fix
    (delete the log). There's no migration.
- **`LogStore::flush` is the `Store::flush(now)` trait method** (tines-store).
  The inherent `flush()` is gone. Call `Node::flush(now)`; the marker now
  uses the caller's tick.
- **`LogStore::open(path, now)` quarantines what it loads** (tines-store).
  Observations already in the log are held back until the host feeds
  trusted wall time (see Added).
- **`tines::serve`** takes a fifth argument, `peer_time_window_ms`.
  **`tines::handle`** takes `observed_at: Option<u64>` before `now`.
- **Service config** (tines):
  - unknown keys are rejected (`deny_unknown_fields`);
  - `trusted_clock` accepts only `"system"`; any other value is a config
    error;
  - the `first_seq` field is gone (seqs come from the store's boot counter).

### Added

- **Trusted wall time in the hosts** (spec §9, decisions G and J). A host
  opens its store before it knows the time, then adopts a wall time once per
  run.
  - **Local source first.** The service's `trusted_clock = "system"`, or
    tines-ros2's `trust_system_clock` / `trust_wall_clock`.
  - **Otherwise a peer, by decision J.**
    - The host collects `Node::peer_time_estimate` for `peer_time_window_ms`
      (default 10 000) from the first plausible estimate.
    - It then adopts the latest estimate.
    - The store only offers estimates within its
      `LogStoreBuilder::plausible_wall_ms` range, so a forged one can't hide
      the honest ones.
  - **Plausible range.** A wall time is adopted only if it's between
    2026-01-01Z (`tines::trust::WALL_FLOOR_MS`) and about 2126
    (`WALL_CEILING_MS`). That applies to the system clock and to peers.
  - **Flush on trust.** Adopting the time flushes the store at once, so a
    marker carrying the wall time reaches disk.
  - Until trust, observations carry `observed_at: None`; after it, the
    host's wall time.
- **`tines::trust`**: `WALL_FLOOR_MS`, `WALL_CEILING_MS`,
  `PLAUSIBLE_WALL_MS`, `DEFAULT_PEER_TIME_WINDOW_MS`, `is_plausible_wall_ms`,
  `system_wall_ms`, `adopt_wall_clock` and `PeerTimeWindow`. `HostClock`
  gains `anchor_wall`, `wall_ms` and `is_trusted`.
- **Service config keys** (tines):
  - `trusted_clock`;
  - `peer_time_window_ms` (default 10000);
  - `unknown_downtime_age_ms` (default: the LogStore default, 24 h).
- **`TinesNode` trust methods** (tines-ros2): `trust_system_clock`,
  `trust_wall_clock`, `set_peer_time_window_ms` and `has_trusted_time`. They
  give a ROS2-only fleet a local trusted-time source.
- **LogStore quarantine and fold** (tines-store).
  - `LogStore` implements `WallClockStore`.
  - `set_wall_clock` folds the downtime into held ages, once, and returns a
    `FoldReport`, which `fold_report` reads back later.
  - **Downtime known:** held ages gain the wall-clock downtime.
  - **Downtime unknown:** held addressed observations expire, and held
    broadcasts are floored at `unknown_downtime_age_ms`.
  - Quarantined entries are exempt from every trim rule (spec decision L).
  - `peer_time_estimate` returns the latest estimate among observations
    stored since this open.
- **`LogStoreBuilder`** (tines-store):
  - `set_unknown_downtime_age_ms` (default 24 h);
  - `plausible_wall_ms` (default unbounded; both hosts pass
    `tines::trust::PLAUSIBLE_WALL_MS`).
- **Snapshots** (tines-store, spec decision I).
  - `open`, after its tick fold, rewrites the log as a snapshot. So does
    `set_wall_clock`, after its fold. A snapshot holds every live observation
    at its current age, a `Release` for every released id still held, and a
    closing clock marker.
  - The snapshot goes to `<log>.snapshot-tmp` in the same directory. That
    file is fsynced, locked, renamed over the log, and then the directory is
    fsynced. A leftover temp file is removed on open.
  - The fold's results and age lineage then survive any number of reboots.
    Each snapshot also compacts the log.
- **New host crates** (first released in this line, see above):
  - `tines-store`: `LogStore`, a persistent backend;
  - `tines-bt`: a BLE transport over BlueZ;
  - `tines`: a standalone service with Unix-socket JSON IPC;
  - `tines-ros2`: a ROS2 node, with rclrs behind the `ros2` feature.

### Changed

- `LogStore` persists a boot counter, and `first_seq()` returns
  `boot_count << 32`. Both hosts seed `Node::new` from it.
- `LogStore::release` writes a `Release` record immediately.
- `LogStore::open` takes an exclusive lock and creates the file `0600`.
- `LogStore::flush` and `set_wall_clock` clamp an earlier `now` to the store's
  time anchor, so `stored_at` and clock markers never move backwards.
- A log whose first header was torn while it was being created (shorter than
  the header, starting with the magic) opens as a new log. Any other short
  file is still refused.
- tines-bt rewrite:
  - L2CAP SeqPacket channels, one frame per packet.
  - Every node advertises and scans; the smaller `NodeId` dials and the
    larger accepts.
  - Connections are bound to the advertised `NodeId`, and frames whose
    claimed sender differs (`tines_core::frame_sender`) are dropped.
  - Connections emit `Contact` and `Lost`.
  - Live connections are capped by tines-bt's own `MAX_PEERS` (8).
- tines-sim's property tests run 64 cases again, and the bounce-overflow
  property checks that the table really overflowed.

### Fixed

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
- From the 1.0 review:
  - `LogStore` didn't implement `Store::flush`, so `Node::flush` wrote no
    marker and fsynced nothing (B1).
  - Fold results and age lineage lived only in memory. An expired message
    came back after the next reboot, and ages reset on a third boot (B2).
    Snapshots fix both.
  - A forged `observed_at = u64::MAX` overflowed: a panic in debug, a time
    near 1970 in release (B3). The arithmetic saturates, and the hosts
    bound every wall time they adopt.
  - A flush between open and the fold made the fold count that time twice
    (S2).
  - `flush` didn't clamp an earlier `now` (S3).
  - tines-ros2 had no local trusted-time source (S4).
  - A quiet reboot lost everything, because nothing flushed after trust
    was set (S5).
  - The service trusted a 1970 system clock and ignored config typos (S6).

### Known issues

- The service talks to exactly one configured `peer`. The multi-peer
  allowlist the fleet-fixes spec describes isn't wired up (F3).
- Neither host flushes on a timer or at shutdown (F18).
  - The service trims and flushes only after 64 poll rounds that each stored
    something new.
  - tines-ros2 flushes only when it adopts trusted time, and never trims.
- Peer time is unauthenticated until 1.1. An adopted time isn't corrected
  while the host runs (F19).

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
