# Changelog

All notable changes to the tines crates. Versions follow
[Semantic Versioning](https://semver.org/); before 1.0, a minor bump may break
the API.

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
