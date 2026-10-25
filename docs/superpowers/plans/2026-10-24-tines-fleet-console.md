# tines Fleet Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A desktop console (`tines-gui`) that operates tines nodes in two containerized modes — a Bluetooth container talking to real robots, and a simulated fleet of containers exchanging frames over a new IPC-bus transport — with the service IPC widened to expose the full node surface.

**Architecture:** Three new pieces plus widenings. `tines-bus` (workspace member): a `Transport` impl over a host Unix-socket bus daemon. `tines-gui` (standalone crate outside the workspace, like `fuzz/`): egui/eframe launcher + one process per container, speaking the service IPC and driving docker via CLI. The `tines` service gains a transport choice and a widened IPC (addressed observe, contact/lost/dropped events, store listing). Core gains exactly two additive `NodeEvent` variants.

**Tech Stack:** Rust 1.99.0 (pinned), tines-core (no_std, untouched except Task 1), bluer/BlueZ, std Unix sockets, egui/eframe 0.36 (glow, wayland), docker CLI subprocess.

**Spec:** `docs/superpowers/specs/2026-10-24-tines-fleet-console-design.md` — the plan argues from the spec, so the spec travels with it; executors read both.

## Global Constraints

- Repo rules (AGENTS.md): TDD (failing test → fail → implement → pass → commit), conventional commits, `sh scripts/ci.sh` green before every commit, specs/plans are read-only history, README tracks code, THREAT_MODEL.md updated when a change touches a finding.
- Toolchain: `1.99.0`, edition 2021, workspace resolver 2; `thumbv7em-none-eabihf` target installed.
- `tines-core` stays `no_std`, no async/threads/I/O, no panic on untrusted input; the ONLY core change in this work is Task 1 (two additive `NodeEvent` variants, enum is `#[non_exhaustive]`).
- Wire constants frozen: `MAX_FRAME=1500`, `WIRE_VERSION=4`; the bus carries core wire frames verbatim inside a bus-local envelope (no wire-format change).
- Service IPC: length-prefixed (u32 LE) JSON, 1 MiB cap, topics/payloads/ids as lowercase hex on the wire; malformed input → `Error` response, never a panic. Service `Config` keeps `deny_unknown_fields`.
- The `Node` is never in the GUI process; the GUI only speaks IPC to service processes.
- `tines-gui` is a standalone crate at repo root (NOT a workspace member; precedent: `fuzz/`). `tines-bus` IS a workspace member.
- eframe features exactly `glow`, `default_fonts`, `wayland`; `vsync = false`; opaque window painted with the omarchy theme (`~/.local/state/omarchy/current/theme/colors.toml`, keys `background`/`foreground`/`accent`, directory watch via `notify` — copy the mechanism from `~/src/aleph-keyring/crates/aleph-gui`).
- Docker is driven as a CLI subprocess (`docker` binary); no docker SDK.
- New public names in `tines-core` (Task 1) are called out in the commit message and AGENTS.md.

## Review Focus

The five failure modes most likely to bite a person using this software, with the test that pins each, owned by the task that owns the code:

1. **A bus client lies about who it is** (frame's declared `from` ≠ its registered NodeId) — the receiving transport must drop the frame (bt's F6 rule), and the daemon must drop/close on a routing-header mismatch. → Task 3 tests.
2. **`observe` with `to` but no `lifetime_ms`** (or vice versa) — must be an `Error`, not a half-addressed observation. → Task 2 tests.
3. **A huge store breaks `List`** — the response must cap at `LIST_CAP` entries and set `truncated: true`, never exceed the 1 MiB frame cap. → Task 2 tests.
4. **A config that pairs the wrong transport with the wrong keys** (bt without `peer`, bus without `bus_socket`, unknown key) — startup error, no silent fallback. → Task 4 tests.
5. **BLE reconnect storms** (bt re-fires `Contact` on every reconnection) — `NodeEvent::Contact` fires only for a newly-added peer, so the console feed isn't flooded; `Lost` fires only for a peer that was present. → Task 1 tests.

---

### Task 1: Core — `NodeEvent::Contact` / `NodeEvent::Lost`

**Files:**
- Modify: `tines-core/src/exchange.rs` (the `NodeEvent` enum, `on_event`, `add_peer`/`remove_peer` call sites)
- Test: tests in `tines-core/src/exchange.rs`'s `#[cfg(test)]` module

**Interfaces:**
- Produces: `NodeEvent<A>::Contact { addr: A }` and `NodeEvent<A>::Lost { addr: A }` — additive variants on the `#[non_exhaustive]` enum. Semantics: emitted from `Exchange::on_event` when a transport `Event::Contact(addr)` adds a NEW peer (repeat `Contact` for a known peer restarts its idle clock and emits nothing), and when `Event::Lost(addr)` removes a peer that was present (`Lost` for an unknown addr emits nothing). Idle eviction (`evict_idle`, F8) stays silent. The variant carries the transport `Addr` (core never maps addr→NodeId); for both bt and bus the addr IS the `NodeId`.
- Consumes: nothing new; `NodeEvent` already derives `Clone, Copy, Debug, PartialEq`.

- [ ] **Step 1: Write the failing tests** in `tines-core/src/exchange.rs`:

`contact_for_new_peer_emits_event` — `on_event(Event::Contact(addr))` then `drain_events()` contains exactly `Contact { addr }`; a second `on_event(Event::Contact(addr))` for the same addr drains empty.
`lost_for_known_peer_emits_event` — `Contact` then `Lost(addr)` drains `Lost { addr }`; `Lost` for a never-seen addr drains empty.
`idle_eviction_emits_nothing` — drive `next_frame` with `now` past `PEER_IDLE_MS` so the peer is evicted; `drain_events()` is empty.
`contact_event_rides_the_drop_oldest_queue` — fill the event queue past `EVENT_QUEUE_CAP` with `Delivered`-style pushes, then a `Contact`; the drained oldest event is gone, the `Contact` survives (heap-safe drop-oldest behavior, no panic).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-core contact_for_new_peer_emits_event lost_for_known_peer_emits_event idle_eviction_emits_nothing contact_event_rides_the_drop_oldest_queue`
Expected: compile errors (variants don't exist) / assertion failures.

- [ ] **Step 3: Implement** — add the two variants to `NodeEvent<A>` with the doc comments from the spec (decision C); in `on_event`, replace the `Contact`/`Lost` arms to push into `self.events` with the drop-oldest pattern already used by `Delivered` (`let _ = self.events.push(...)`), gated on new-peer/known-peer as above. Check peer presence via the peers table before `add_peer`/`remove_peer`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines-core && sh scripts/check-no-std.sh && cargo clippy -p tines-core --all-targets -- -D warnings`
Expected: all green; the thumbv7em no_std build still passes.

- [ ] **Step 5: Commit**

```bash
git add tines-core/src/exchange.rs
git commit -m "feat(core): emit Contact/Lost NodeEvents (decision C)"
```

---

### Task 2: Service IPC widening

**Files:**
- Modify: `tines/src/ipc.rs` (Request/Response enums, new out types, `handle`)
- Test: `tines/tests/ipc.rs` (existing `MockTransport` already surfaces one `Contact`)

**Interfaces:**
- Consumes: Task 1's `NodeEvent::Contact/Lost`; `Node::observe_to(to: NodeId, lifetime_ms: u64, topic, payload, observed_at, now) -> Result<ObservationId, ObserveError>`; `Store::for_each_id` + `Store::get(id, now)`.
- Produces (all new variants are additive; the existing four operations keep their exact wire shape):
  - `Request::Observe { topic, payload, to: Option<String>, lifetime_ms: Option<u64> }` — `to` and `lifetime_ms` both set → `observe_to`; both unset → `observe`; exactly one set → `Response::Error { message: "to and lifetime_ms must be set together" }`.
  - `Request::List` → `Response::List { observations: Vec<ObservationOut>, truncated: bool }` with `LIST_CAP = 1024` entries max; `truncated` is `true` when more exist. `ObservationOut { id: String, observer: String, topic: String, payload: String, to: Option<String>, lifetime_ms: Option<u64>, age_ms: u64, observed_at: Option<u64> }` (all hex where binary; `observed_at` from the stored observation).
  - `Response::Events` gains two fields (existing `events` field unchanged): `contacts: Vec<ContactOut>` and `drops: Vec<DropOut>`. `ContactOut { peer: String, joined: bool }` (hex addr; `joined: false` = lost). `DropOut { peer: Option<String>, reason: String, count: u32 }` (`None` = overflow slot). Drops now surface here AND on stderr (stderr line stays).
  - `Status` unchanged.

- [ ] **Step 1: Write the failing tests** in `tines/tests/ipc.rs`:

`observe_addressed_roundtrip` — `Observe { to, lifetime_ms }` → `Response::Observe { id }`; then `List` shows that observation with matching `to`/`lifetime_ms`.
`observe_half_addressed_is_error` — `to` set, `lifetime_ms` none → `Error`; and vice versa.
`list_returns_all_observations` — observe one broadcast + one addressed → `List` has both, fields correct, `truncated` false.
`list_caps_at_list_cap` — observe `LIST_CAP + 1` small observations in a loop → `List` has exactly `LIST_CAP` entries and `truncated: true`.
`events_include_contacts` — fresh `serve` with the mock (its first poll surfaces `Contact`) → `Events` response has `contacts == [ContactOut { peer: <mock peer hex>, joined: true }]`.
`events_include_lost_and_drops` — push `Event::Lost(peer)` into the mock's pending queue and inject a garbage frame; `Events` reports `contacts` with `joined: false` and a `drops` entry with the right `reason`.
`existing_events_field_unchanged` — the Delivered path (mock auto-acks) still fills `events` exactly as before.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines --test ipc observe_addressed_roundtrip list_returns_all_observations events_include_contacts`
Expected: FAIL (unknown fields/variants).

- [ ] **Step 3: Implement** — extend the enums and add the `handle` arms in `tines/src/ipc.rs`; encode the drop path (`NodeEvent::Dropped`) into `DropOut` instead of stderr-only; keep the existing `_ => None` arm for future variants. Reuse the existing `encode_hex`/`decode_hex`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines && cargo clippy -p tines --all-targets -- -D warnings`
Expected: PASS; existing ipc tests still green (wire compat for the four old ops).

- [ ] **Step 5: Commit**

```bash
git add tines/src/ipc.rs tines/tests/ipc.rs
git commit -m "feat(host): widen service IPC — addressed observe, List, contact/drop events"
```

---

### Task 3: `tines-bus` crate — bus transport + daemon

**Files:**
- Create: `tines-bus/Cargo.toml` (deps: `tines-core = { path = "../tines-core", version = "1" }`; std only, no serde), `tines-bus/src/lib.rs`, `tines-bus/src/transport.rs`, `tines-bus/src/daemon.rs`, `tines-bus/src/bin/tines-busd.rs`
- Test: `tines-bus/tests/transport.rs`, `tines-bus/tests/daemon.rs`
- Modify: `Cargo.toml` (workspace members += `tines-bus`)

**Interfaces:**
- Consumes: `tines_core::transport::{Event, FrameBuf, SendError, Transport}`, `tines_core::frame_sender`, `tines_core::consts::MAX_FRAME`, `NodeId`.
- Produces:
  - `pub trait BusLink` (the narrow I/O seam, mirroring bt's `Radio`): `fn poll(&mut self) -> Option<BusEvent>; fn send(&mut self, to: &NodeId, data: &[u8]) -> Result<(), BusError>; fn mtu(&self) -> usize;` with `BusEvent::{ Joined(NodeId), Left(NodeId), Frame(NodeId, FrameBuf), Closed }` and `pub enum BusError { Io(std::io::Error), Unreachable }`. The real link (daemon client) lives in the same crate, thread-driven, NOT part of the pure logic.
  - `pub struct BusTransport` — `pub fn new(me: NodeId, allowlist: Option<NodeId>, socket: impl AsRef<Path>) -> std::io::Result<BusTransport>`; `impl Transport for BusTransport { type Addr = NodeId; ... }`. Poll maps `Joined`/`Left` → `Event::Contact`/`Event::Lost`, `Frame` → `Event::Frame` after the spoofing check (`frame_sender(frame) == peer`), `Closed` → `Event::Lost` for every bound peer. Frames/events from a non-allowlisted peer are dropped (allowlist `None` = any peer). `send` to a non-allowlisted or unknown peer → `SendError::Unreachable`; outbound frames must declare `me` as sender.
  - `pub fn daemon::serve(socket: impl AsRef<Path>) -> std::io::Result<()>` — bind (removing a stale socket file first), accept, run the protocol below. Binary `tines-busd` takes the socket path as argv[1].
  - **Bus envelope (pinned, bus-local — the core frame inside is untouched):** every message is `[u32 LE length][payload]`, length counting payload bytes only.
    - Hello (client→daemon, first message): payload = 16-byte `NodeId` (len 16). A second hello for an already-registered id replaces the old connection (old socket closed).
    - Data (client→daemon): payload = `[16B from][16B to][frame]`. Daemon drops (and counts on stderr) any message whose `from` ≠ that socket's registered id, or whose `to` is not registered; otherwise forwards the envelope verbatim to `to`'s socket. Frames longer than `MAX_FRAME` close the connection.
    - Control (daemon→client): payload = `[1B kind][16B NodeId]` (len 17); kind `0 = joined`, `1 = left`; sent to every other registered client on join, and to all remaining on leave. EOF/error from a client's socket = leave + close, daemon stays up.
- The daemon is single-threaded `poll`-based over nonblocking sockets; per-connection state = registered id (`Option<NodeId>`), read buffer. No frame content is decoded by the daemon beyond the routing header.

- [ ] **Step 1: Write the failing tests**

`tines-bus/tests/transport.rs` (spawn `daemon::serve` on a thread with a tempfile dir socket, then drive pure `BusTransport`s; two nodes, allowlist `None`):
- `two_transports_exchange_frames` — A.send(to=B, frame declaring `from`=A) → B.poll yields `Event::Frame(A, bytes)` with identical bytes.
- `spoofed_frame_is_dropped` — B's link delivers a frame declaring `from`=C (not B's bound peer); B.poll does not yield it.
- `join_and_leave_become_contact_and_lost` — A connects first; when B connects, A.poll yields `Event::Contact(B)`; when B drops, A yields `Event::Lost(B)`.
- `allowlist_filters` — A with allowlist `Some(B)`: frames/join events for C never surface; `send` to C → `SendError::Unreachable`.

`tines-bus/tests/daemon.rs` (drive raw sockets against the daemon):
- `duplicate_hello_replaces` — second connection helloing an already-registered id kills the first (its read returns EOF).
- `mismatched_from_is_dropped` — a socket registered as A sends a data envelope with `from`=B: the addressed peer receives nothing; the daemon remains alive for the next message.
- `unknown_dest_is_dropped` — data to an unregistered id: dropped, sender stays connected.
- `oversized_frame_closes_connection` — a data envelope with a frame larger than `MAX_FRAME`: daemon closes that connection only.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines-bus`
Expected: FAIL (crate does not exist).

- [ ] **Step 3: Implement** the crate: pure `BusTransport` logic over the `BusLink` trait in `transport.rs`; the daemon and its real socket link in `daemon.rs`; the thin `tines-busd` bin. Add `tines-bus` to the workspace members.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines-bus && cargo clippy -p tines-bus --all-targets -- -D warnings && cargo fmt --all -- --check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock tines-bus/
git commit -m "feat(bus): tines-bus transport and daemon over a host IPC bus"
```

---

### Task 4: Service transport choice + config moves to the lib

**Files:**
- Modify: `tines/Cargo.toml` (add `tines-bus` path dep), `tines/src/main.rs` (config parse + node construction), `tines/src/lib.rs` (new modules)
- Create: `tines/src/config.rs` (the `Config`/`TrustedClock`/`TransportKind` types, moved from `main.rs` so clients can parse generated configs), `tines/src/transport.rs` (`HostTransport`)
- Test: config tests in `tines/src/config.rs` (moved), new integration test `tines/tests/service_bus.rs`

**Interfaces:**
- Consumes: Task 3's `BusTransport`; `BtTransport` as today.
- Produces:
  - `tines::config::Config` (pub, `deny_unknown_fields`): existing keys unchanged except `peer: Option<String>`; new keys `transport: Option<TransportKind>` (default `bt`; `TransportKind` serde-rename lowercase: `bt` | `bus`) and `bus_socket: Option<String>`.
  - Validation (pub fn on `Config`, called by `main` before anything else): `transport = bt` + `peer = None` → error `"transport bt requires peer"`; `transport = bus` + `bus_socket = None` → error `"transport bus requires bus_socket"`; bus + `peer = Some(..)` = allowlist of one, bus + `None` = any peer.
  - `tines::transport::HostTransport` — `pub enum HostTransport { Bt(BtTransport), Bus(BusTransport) }` with `impl Transport for HostTransport { type Addr = NodeId; ... }` delegating to the inner transport.
  - `main.rs` builds `HostTransport` per config (bt: `BluerRadio::new(me, peer)` + `BtTransport::new(me, peer, radio)`; bus: `BusTransport::new(me, peer, &config.bus_socket)`) and hands it to the unchanged `serve`. Everything else in `main` is untouched.

- [ ] **Step 1: Write the failing tests**

Config tests (moved + new, in `tines/src/config.rs`): existing parse tests keep passing (minimal bt config, `trusted_clock`, unknown field, peer-time window); new: `bus_config_parses` (transport "bus", bus_socket, no peer), `bt_without_peer_is_invalid`, `bus_without_socket_is_invalid`, `unknown_transport_is_rejected`, `transport_defaults_to_bt`.

`tines/tests/service_bus.rs` (the sim-fleet deliverable, no docker): spawn the real `tines-busd` on a tempdir socket; spawn two real `tines` service processes (`env!("CARGO_BIN_EXE_tines")`) with generated bus configs (`trusted_clock: "system"`); connect each socket with the length-prefixed framing; `Observe` on node A; poll `Belief` on node B until it sees the payload (deadline ~5 s); `List` on B contains it; `Events` on both eventually reports `Contact { joined: true }`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tines --test service_bus`
Expected: FAIL (no `service_bus.rs`, config types not in lib).

- [ ] **Step 3: Implement** — move `Config`/`TrustedClock` into `tines/src/config.rs` (pub, re-export from `lib.rs`), add `TransportKind` + validation, add `HostTransport` in `tines/src/transport.rs`, rewire `main.rs` (`tines::config::Config`, the validation call, `HostTransport` construction). `peer` becomes `Option<String>`; the bt branch errors via validation.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tines && cargo clippy -p tines --all-targets -- -D warnings`
Expected: PASS — including `service_bus.rs` (two real services converge over the bus) and all pre-existing ipc tests.

- [ ] **Step 5: Commit**

```bash
git add tines/
git commit -m "feat(host): service transport choice (bt|bus) and lib-level config"
```

---

### Task 5: Runtime docker image

**Files:**
- Create: `Dockerfile.tines`
- Modify: `.dockerignore` (verify it excludes `target/`, `.git/`, `tines-gui/`)

**Interfaces:**
- Produces: image tag `tines-stack:latest`, built with `docker build -f Dockerfile.tines -t tines-stack:latest .`. Two stages:
  - build: `FROM ubuntu:24.04`; apt: `build-essential pkg-config cmake curl ca-certificates libclang-dev clang libdbus-1-dev libudev-dev`; rustup minimal (profile minimal, default host); `WORKDIR /workspace`; `COPY Cargo.toml Cargo.lock ./` then each workspace crate dir (`tines-core`, `tines-store`, `tines-bt`, `tines-bus`, `tines-sim`, `tines-ros2`, `tines` — every member's Cargo.toml must be present for cargo to resolve); `RUN cargo build --release -p tines -p tines-bus`.
  - runtime: `FROM ubuntu:24.04`; apt: `libdbus-1-3 libudev1 bluez dbus`; `COPY --from=build` the two release binaries to `/usr/local/bin/`; `ENTRYPOINT ["tines"]` (config path comes as the container arg; no default `CMD`).
- The same image serves both modes; BT mode's `--privileged`/host-network/D-Bus flags are runtime args in Task 7.

- [ ] **Step 1: Write the failing verification** — nothing to test in Rust; the check is the build + smoke below.

- [ ] **Step 2: Verify it fails**

Run: `docker build -f Dockerfile.tines -t tines-stack:latest .`
Expected: FAIL (no Dockerfile).

- [ ] **Step 3: Implement** the Dockerfile as pinned above.

- [ ] **Step 4: Verify it passes**

Run: `docker build -f Dockerfile.tines -t tines-stack:latest .` then smoke: `docker run --rm -d --name tines-smoke tines-stack:latest /data/config.json` against a host-written bt config in a mounted dir → `sleep 2; docker logs tines-smoke` contains `tines: listening on`; then `docker stop tines-smoke`.
Expected: build succeeds; smoke log line appears. (The radio-less bt service must still start and serve IPC — no adapter is not fatal.)

- [ ] **Step 5: Commit**

```bash
git add Dockerfile.tines
git commit -m "feat(docker): runtime image for the tines service"
```

---

### Task 6: `tines-gui` skeleton — IPC client, config generation, theme

**Files:**
- Create: `tines-gui/Cargo.toml` (deps: `tines = { path = "../tines" }`, `eframe = { version = "0.36", default-features = false, features = ["glow", "default_fonts", "wayland"] }`, `egui = "0.36"`, `serde`, `serde_json`, `notify = "8"`; dev-deps: `egui_kittest = "0.36"` with the `wgpu` feature, `tempfile`), `tines-gui/src/main.rs`, `tines-gui/src/ipc_client.rs`, `tines-gui/src/config.rs`, `tines-gui/src/theme.rs`
- Test: `tines-gui/tests/ipc_client.rs`, `tines-gui/tests/config.rs`
- Modify: `scripts/ci.sh` (append gui checks mirroring the fuzz lines: `cargo fmt --manifest-path tines-gui/Cargo.toml -- --check`, `cargo clippy --manifest-path tines-gui/Cargo.toml --all-targets -- -D warnings`, `cargo test --manifest-path tines-gui/Cargo.toml`)

**Interfaces:**
- Consumes: `tines::{Request, Response}` and `tines::config::Config` (Task 2/4); the service's wire framing (u32 LE length + JSON).
- Produces:
  - `tines_gui::ipc_client::IpcClient` — `pub fn spawn(socket: PathBuf) -> io::Result<IpcClient>` (connects on a worker thread; queues requests); `pub fn request(&self, req: Request) -> Result<u64, ClientError>` (encodes + queues, returns a monotonically increasing message id); `pub fn try_recv(&self) -> Option<(u64, Response)>` (nonblocking); `pub fn reconnect(&self)` (re-dials; drops nothing). `pub enum ClientError { Io(io::Error), Json(serde_json::Error) }`. Request→response correlation by id; responses arrive out of order safely (client tags each frame with the id — **client-side framing**: `[u32 LE len][u32 LE msg_id][Request JSON]`, response `[u32 LE len][u32 LE msg_id][Response JSON]`; the msg_id sits inside the length-prefixed body, so the service-side protocol is untouched and the client keeps its own correlation). Timeouts are the GUI's business (the client only reports what arrives).
  - `tines_gui::config::{fleet_dir, node_dir, generate_node_id, NodeConfig}` — `pub fn fleet_dir(fleet: &str) -> PathBuf` = `$XDG_STATE_HOME|~/.local/state` + `tines-console/fleets/<fleet>` (created `0700`); `pub fn node_dir(fleet: &str, node: &str) -> PathBuf`; `pub fn generate_node_id() -> String` (32 lowercase hex from 16 bytes of `/dev/urandom`); `pub struct NodeConfig { node_id: String, peer: Option<String>, log_path: String, socket_path: String, transport: String, bus_socket: Option<String>, trusted_clock: Option<String> }` serializing to EXACTLY the service `Config` schema (unknown keys are fatal on the service side). `write(fleet, node) -> io::Result<PathBuf>` writes `<node_dir>/config.json`.
  - `tines_gui::theme::OmarchyTheme` — the aleph-keyring mechanism: load `~/.local/state/omarchy/current/theme/colors.toml` keys `background`/`foreground`/`accent`, `notify` watch on the `current` directory, repaint on change.

- [ ] **Step 1: Write the failing tests**

`tines-gui/tests/config.rs` — `generated_config_parses_as_service_config`: build a `NodeConfig` for bt and for bus, serialize, and deserialize each with `serde_json` into `tines::config::Config` (proves the schema matches the service exactly); `node_id_is_32_hex_chars`; `fleet_dir_is_0700`.

`tines-gui/tests/ipc_client.rs` — a test-local fake server (std `UnixListener`) speaking the service framing: `client_roundtrips_observe` (server decodes `Request::Observe`, replies `Response::Observe`; client's `try_recv` yields the matching id); `client_survives_server_restart` (kill listener, `reconnect`, next request round-trips).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --manifest-path tines-gui/Cargo.toml`
Expected: FAIL (crate does not exist).

- [ ] **Step 3: Implement** the three modules + the crate manifest. `main.rs` for now: parse args, if none print `"tines-gui: launcher"` and exit 0 (Task 8 wires the real UI).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --manifest-path tines-gui/Cargo.toml && cargo clippy --manifest-path tines-gui/Cargo.toml --all-targets -- -D warnings && cargo fmt --manifest-path tines-gui/Cargo.toml -- --check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tines-gui/ scripts/ci.sh
git commit -m "feat(gui): tines-gui skeleton — IPC client, config generation, omarchy theme"
```

---

### Task 7: Docker orchestration + bus-daemon supervision

**Files:**
- Create: `tines-gui/src/docker.rs`, `tines-gui/src/fleet.rs`
- Test: `tines-gui/tests/docker.rs` (command-building only — no real docker), `tines-gui/tests/fleet.rs`

**Interfaces:**
- Consumes: Task 6's `config` module; `tines-busd` binary on PATH (built in Task 3).
- Produces:
  - `tines_gui::docker::Docker` — `pub fn image() -> &'static str` = `"tines-stack:latest"`; `pub fn build_image(&self) -> Result<(), DockerError>` (`docker build -f Dockerfile.tines -t tines-stack:latest .` from the repo root); `pub fn start_bt(&self, name: &str, fleet: &str) -> Result<ContainerId, DockerError>` — flags pinned: `docker run -d --name tines-bt-<name> --privileged --net=host -v /run/dbus/system_bus_socket:/run/dbus/system_bus_socket -v <fleet_dir>/<name>:/data tines-stack:latest tines /data/config.json`; `pub fn start_sim(&self, name: &str, fleet: &str)` — same minus privileged/net-host/dbus, name `tines-sim-<name>`; `pub fn stop(&self, id: &ContainerId)`; `pub fn remove(&self, id: &ContainerId)`; `pub fn logs(&self, id: &ContainerId, tail: u32) -> Result<String, DockerError>`; `pub fn is_running(&self, id: &ContainerId) -> bool` (docker inspect exit status). `pub struct ContainerId(pub String);` `pub enum DockerError { NotInstalled, Spawn(io::Error), Exit { code: i32, stderr: String } }`. All commands run via `std::process::Command`, args as pinned Vecs (never shell-interpolated).
  - `tines_gui::fleet::Fleet` — `pub fn create(fleet: &str) -> io::Result<Fleet>` (mkdir 0700 via Task 6 `fleet_dir`; spawn `tines-busd <fleet_dir>/bus.sock` as a child, stdout/stderr inherited); `pub fn bus_socket(&self) -> PathBuf`; `pub fn add_node(&self, name: &str, node_id: Option<&str>, mode: Mode, peer: Option<&str>) -> io::Result<NodeSpec>` (writes the config JSON; bus mode sets `bus_socket` = `/data/bus.sock` — the in-container path, since the fleet dir is mounted at `/data`; log/socket paths `/data/<name>/log`, `/data/<name>/sock`); `pub fn stop_bus(&mut self)` (kill child). `pub enum Mode { Bt, Sim }`; `pub struct NodeSpec { pub name: String, pub dir: PathBuf }`. On drop without `stop_bus`, the daemon child is killed.

- [ ] **Step 1: Write the failing tests**

`tines-gui/tests/docker.rs` — the exact command vectors: `start_bt_args_match_pinned_flags`, `start_sim_args_match_pinned_flags` (assert the full argv), `stop_and_remove_args`.
`tines-gui/tests/fleet.rs` — `fleet_dir_layout` (dirs exist, bus.sock parent exists, daemon child spawned), `add_node_bus_config_mounts_data` (config JSON: `transport == "bus"`, `bus_socket == "/data/bus.sock"`, `socket_path == "/data/<name>/sock"`, and it parses as `tines::config::Config`), `add_node_bt_config_has_peer`, `generated_id_roundtrips`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --manifest-path tines-gui/Cargo.toml`
Expected: FAIL (modules do not exist).

- [ ] **Step 3: Implement** `docker.rs` and `fleet.rs` exactly as pinned.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --manifest-path tines-gui/Cargo.toml && cargo clippy --manifest-path tines-gui/Cargo.toml --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tines-gui/
git commit -m "feat(gui): docker orchestration and bus-daemon supervision"
```

---

### Task 8a: Launcher UI

**Files:**
- Create: `tines-gui/src/launcher.rs`
- Modify: `tines-gui/src/main.rs` (arg dispatch: no args → launcher; `--node <dir> <container-id> <mode>` → node window (Task 8b))
- Test: `tines-gui/tests/launcher.rs` (egui_kittest)

**Interfaces:**
- Consumes: Tasks 6/7 (`Fleet`, `Docker`, `config`, theme).
- Produces: `pub struct Launcher { fleet: Option<Fleet>, docker: Docker, containers: Vec<ContainerEntry>, children: Vec<std::process::Child>, actions: VecDeque<LauncherAction> }` with `pub struct ContainerEntry { name: String, id: ContainerId, mode: Mode }` and `pub enum LauncherAction { StartContainer { name: String, mode: Mode }, StopContainer(ContainerId), RemoveContainer(ContainerId), OpenWindow { name: String, dir: PathBuf, id: ContainerId, mode: Mode }, BuildImage }`. `pub fn update(&mut self, ctx: &egui::Context)` renders: mode picker (Bt / Sim), fleet name field + create, node list (name, mode, running state), per-node Open/Stop/Remove buttons, image build button, log tail pane for the selected container. State changes and subprocess spawns happen ONLY by draining `actions` in `main.rs` (keeps the UI testable headless).

- [ ] **Step 1: Write the failing tests** (`egui_kittest`, no real docker/daemon — inject a `Fleet`-less state and assert on `actions`):

`create_fleet_action_queues_spawn` — type a fleet name, click Create → `actions` contains `StartContainer`-preceding spawn intent (assert via the fleet field set + action drain helper).
`open_window_queues_spawn` — click a node's Open → `actions` contains `OpenWindow` with the right name/dir.
`stop_queues_stop_action` — click Stop → `StopContainer` with the right id.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --manifest-path tines-gui/Cargo.toml --test launcher`
Expected: FAIL.

- [ ] **Step 3: Implement** `launcher.rs` + the `main.rs` dispatch + action drain (spawn children with `--node`; build image on `BuildImage`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --manifest-path tines-gui/Cargo.toml --test launcher && cargo clippy --manifest-path tines-gui/Cargo.toml --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tines-gui/
git commit -m "feat(gui): launcher — fleet creation, container controls, window spawn"
```

---

### Task 8b: Node window UI

**Files:**
- Create: `tines-gui/src/window.rs`
- Modify: `tines-gui/src/main.rs` (the `--node` arm builds the window app)
- Test: `tines-gui/tests/window.rs` (egui_kittest)

**Interfaces:**
- Consumes: Task 6's `IpcClient`, Task 2's widened protocol, Task 8a's arg dispatch.
- Produces: `pub struct NodeWin { client: IpcClient, name: String, mode: Mode, transport: TransportKind, status: Option<StatusData>, beliefs: VecDeque<BeliefOut>, feed: VecDeque<FeedEntry>, store_rows: Vec<ObservationOut>, last_id: Option<String> }` with `pub enum FeedEntry { Contact { peer: String, joined: bool }, Delivered { peer: String, id: String }, Dropped { peer: Option<String>, reason: String, count: u32 } }`. `pub fn update(&mut self, ctx: &egui::Context)` renders the spec's layout: status strip (node id from `Status`, next_seq, stored, transport from the launcher-passed arg — NOT from the wire), observe panel (topic, payload, optional recipient + lifetime, text-with-hex-toggle), belief panel, store browser (from `List`), events feed, container panel (`docker logs` via `Docker`, stop/remove buttons). All I/O goes through the `IpcClient`; the UI thread never blocks (requests queue, responses drain each frame; `Events` polled every 500 ms). BT windows show the single-peer hint from the spec.
- `eframe` app wiring follows the wayland-gui skill: `NativeOptions { vsync: false, ... }`, `with_transparent(false)`, `OmarchyTheme` colors painted every frame.

- [ ] **Step 1: Write the failing tests** (egui_kittest against `NodeWin::update` with the fake server from Task 6's test helpers moved into a shared test util):

`status_strip_shows_node_id` — after the fake server answers `Status`, the strip text contains the hex id.
`observe_button_queues_observe` — type topic/payload, click Send → the client queue carries `Request::Observe` with hex-encoded topic/payload and no `to`.
`addressed_fields_queue_observe_to` — fill recipient + lifetime → `Request::Observe` carries `to`/`lifetime_ms`.
`feed_shows_contact` — fake server answers `Events` with a contact → feed contains the peer hex.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --manifest-path tines-gui/Cargo.toml --test window`
Expected: FAIL.

- [ ] **Step 3: Implement** `window.rs` and the `--node` arm.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --manifest-path tines-gui/Cargo.toml --test window && cargo clippy --manifest-path tines-gui/Cargo.toml --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Manual verification** — `cargo run --manifest-path tines-gui/Cargo.toml`; build the image; create a 2-node sim fleet; confirm both windows show contact events, an observation typed in one appears in the other's store and beliefs; then stop and remove the fleet. Screenshot the window and match chrome to the desktop theme (wayland-gui skill).

- [ ] **Step 6: Commit**

```bash
git add tines-gui/
git commit -m "feat(gui): node window — observe, beliefs, store, events, container panel"
```

---

### Task 9: Docs and housekeeping

**Files:**
- Modify: `AGENTS.md` (crate layout: add `tines-bus`; note `tines-gui` as a standalone local tool like `fuzz/`; note decision C in the core section), `README.md` (console + bus transport sections tracking the code), `CHANGELOG.md` (Unreleased: bus transport, widened IPC, transport choice, console), `THREAT_MODEL.md` (F3: bus drops the single-peer limit as a test medium; new entries: bus daemon as a host attack surface — 0700 fleet dir, routing-header checks; IPC widening — new request kinds keep the Error posture)

**Interfaces:** none — prose only, tracking code that exists.

- [ ] **Step 1:** Write the doc edits (nothing to test).
- [ ] **Step 2:** Run the full gate: `sh scripts/ci.sh` — everything green, including the new gui lines.
- [ ] **Step 3: Commit**

```bash
git add AGENTS.md README.md CHANGELOG.md THREAT_MODEL.md
git commit -m "docs: fleet console, bus transport, widened IPC"
```

---

## Final Verification (before considering the work done)

- `sh scripts/ci.sh` fully green (workspace + gui + fuzz lines).
- `cargo test --manifest-path tines-gui/Cargo.toml` green.
- Manual run from Task 8b Step 5 completed: sim fleet of 2 converges, contacts visible, addressed observe visible, BT mode smoke (container starts, window opens, radio optional).
- `docker build -f Dockerfile.tines -t tines-stack:latest .` green.
- Design doc + plan committed with the code; CHANGELOG entry present.
