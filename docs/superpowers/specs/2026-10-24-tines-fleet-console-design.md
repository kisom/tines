# tines fleet console + simulated fleet — design

Date: 2026-10-24
Status: approved by Kyle (2026-10-24) — decision C: add
`NodeEvent::Contact`/`NodeEvent::Lost` to core (see "The how")

## Intent

A desktop GUI (new `tines-gui` crate) that operates tines nodes in two
containerized modes:

1. **Bluetooth**: a container with the host Bluetooth adapter passed
   through runs the full robot stack; its node talks to real tines
   robots over BLE.
2. **Simulated fleet**: containers on the host run the full stack with a
   new IPC-bus transport (new `tines-bus` crate); they exchange tines
   frames with each other over a host bus.

The GUI is a console: a launcher screen plus one window per container.
Each window exposes the whole node control surface — observe (including
addressed), belief, store listing, events (contacts, drops, confirmed
deliveries), status — so tines can be exercised without hand-rolled JSON.

## Why

- Testing tines today means hand-crafting length-prefixed JSON to a Unix
  socket; contacts and custody can only be watched in stderr.
- The two-device BLE check (`tines-bt/README.md`) has never been
  recorded; a BT container plus a console makes it a routine manual test.
- The only exchange transports are BLE (real radio) and tines-sim's
  in-process memory transport; nothing lets separate processes exchange
  frames. A simulated fleet on one machine needs a host IPC transport.
- No runtime container image exists (`Dockerfile.ros2` is a CI
  build-check).
- Kyle wants to test tines functionality and interact with tines-enabled
  robots (hexapod on a Jetson running ROS2, Pi-class robots).

## Goals

- G1. `tines-gui`: an egui/eframe Wayland app following this desktop's
  conventions (the `wayland-gui` skill): a launcher, plus one GUI process
  per container.
- G2. BT mode: a containerized full stack with host Bluetooth passthrough;
  the container's node is a real radio peer for robots in range.
- G3. Sim mode: a new `tines-bus` host transport crate over a host IPC
  bus; containers plug in and exchange frames; fleet size is
  operator-chosen.
- G4. The `tines` service config gains a transport choice (`bt` default,
  `bus`); existing config files keep working.
- G5. Widened service IPC: addressed observe (`to` + `lifetime_ms` via
  `Node::observe_to`), contact/lost events, dropped-frame events, and a
  store listing; the existing four operations are unchanged.
- G6. GUI windows expose every node operation; the docker lifecycle
  (create, start, stop, remove, logs) is managed from the GUI.
- G7. A runtime container image in the repo (new Dockerfile); both modes
  run from the same image with different runtime flags.

## Non-goals

- `tines-core` changes limited to one additive pair (decision C, approved
  2026-10-24): `NodeEvent::Contact`/`NodeEvent::Lost` variants, so the
  console's event feed can show live contacts. Nothing else in core
  changes: it stays `no_std`, and the 1.0 branch is unaffected (the enum
  is non-exhaustive). The bus is a host crate implementing the existing
  `Transport` trait; the IPC widening is in the `tines` service crate.
- No `tines-sim` changes: the simulator remains the protocol test
  harness; the simulated fleet runs real services over the bus transport.
- No ROS2 integration this round: containers run the service (Pi-class
  load). `tines-ros2` is untouched; a ROS2-backed container is future
  work.
- Not a fleet deployment tool: the console is for development, testing
  and interacting; systemd units and production deployment stay out of
  scope.
- No node in the GUI process: the `Node` is not `Send` and the GUI never
  hosts one; it only speaks IPC to containerized services.

## Anti-goals

- No weakening of the trust boundary: the widened IPC keeps the existing
  posture — malformed input gets an `Error` response, never a panic;
  frame caps stay; hex-on-the-wire stays.
- No silent config breakage: `transport` defaults to `bt`; existing
  configs parse unchanged; unknown keys are still rejected.
- No radio in the GUI process; no BLE access outside the BT container.
- No X11: eframe features `glow`/`default_fonts`/`wayland` only.
- No docker SDK dependency: docker is driven as a subprocess (CLI).

## Constraints

- AGENTS.md applies unchanged: spec-first, TDD with
  `sh scripts/ci.sh` before commit, conventional commits. New crates go
  in the workspace and the AGENTS.md crate layout.
- The `Node` is not `Send`; the service's inline poll architecture is
  unchanged. `tines-bus` is a `Transport` implementation and must not
  require `Send` of the node (same as `BtTransport`).
- The bus carries the core's wire frames (`MAX_FRAME` = 1500 and friends
  are unchanged).
- Docker is available on this desktop; the GUI shells out to it.
- The console follows `wayland-gui` conventions: theme watch on
  `~/.local/state/omarchy/current`, `vsync = false`, opaque window
  painted with the theme background.

## The how (decided with Kyle)

### Core change (decision C, approved 2026-10-24)

Add `NodeEvent::Contact` / `NodeEvent::Lost` variants to core, emitted
from the exchange's `on_event` when the transport reports a contact
starting or ending. The enum is non-exhaustive and the variants ride the
existing drop-oldest event queue, so this is additive and changes no
wire format. This is the only core change in this work.

### Modes

- **Launcher**: two choices — Bluetooth (real robots) and Simulated
  fleet. From it: create containers (name them, choose or generate node
  ids), see the running fleet, open any node's window.
- **One GUI process per container** (Kyle: "one GUI instance per
  container"): the launcher spawns a child process of the same binary in
  node-window mode, so a hung container can't freeze the fleet view.
- **BT mode**: `docker run` privileged, host network, D-Bus access
  (bluer/BlueZ needs the system bus and the hci device). Service config:
  node id, peer (the service still takes one — F3 remainder; the GUI
  exposes exactly what the service supports), log path on a named
  volume, socket path on a mounted directory. The GUI connects to the
  socket on the host filesystem.
- **Sim mode**: the launcher starts a bus daemon on the host, then N
  containers, each running the service with the bus transport and the
  socket directory mounted. GUI windows drive each node.

- **Bus access control** (wntrmute's call): the bus daemon's socket lives
  in a per-fleet directory under the operator's XDG state directory,
  created `0700`. Same-user local processes are already inside the trust
  boundary (THREAT_MODEL); the bus adds no new posture.
- **Status strip transport** (wntrmute's call): the launcher knows the
  transport because it wrote the container's config; it passes it to the
  window process. `Status` on the wire is unchanged.
- **Addressed panel in BT mode** (wntrmute's call): the panel stays in
  every window — `observe_to` is a valid node operation whatever the
  transport. BT windows show a hint that the transport currently
  exchanges with exactly one configured peer, so an addressed
  observation to anyone else will sit until its lifetime expires.

### The bus transport (`tines-bus`, new crate)

- Implements `Transport` with `Addr = NodeId`, exactly like `tines-bt`:
  frames are the core wire frames; a frame's declared sender is checked
  against the bound peer (same address-spoofing rule).
- **Topology** (wntrmute's call; Kyle can override): hub-and-spoke. A
  small bus daemon (a bin in the crate) binds a Unix socket in a
  per-fleet directory on the host; each node connects once. The bus fans
  every frame out to all connected nodes — a shared medium, like radio —
  and reports joins/leaves so the transport can emit `Contact`/`Lost`.
- **Peers**: the bus transport enforces core's `MAX_PEERS` (8), as
  tines-bt does; an empty allowlist means any bus peer (the bus is a
  test medium, so it drops bt's single-peer F3 limit).
- **Framing**: length-prefixed core frames over the socket; no new wire
  format.
- **Contact model**: any frame from a bus peer is an implicit contact
  (core already treats unknown addresses that way); daemon disconnect →
  `Lost` for every peer.

### Service changes (`tines` crate)

- **Config**: new optional `transport` key, `"bt"` (default) or `"bus"`;
  with `bus`, a `bus_socket` path key. `peer` stays required for `bt`;
  for `bus` it is an optional allowlist (empty = any peer).
- **IPC widening** (backward-compatible: new tagged variants; the
  existing four operations are untouched):
  - `Observe` gains optional `to` (hex `NodeId`) and `lifetime_ms`
    (present together or not at all) → `Node::observe_to`; without them,
    the existing `observe` path.
  - `Events` also returns contact/lost events (peer hex + kind) and
    dropped-frame events (reason + count), alongside confirmed
    deliveries. This changes the current D3 surfacing (drops to stderr
    only) so the console can show them; the stderr line stays.
  - New `List` request → the stored observations (id, observer, topic,
    payload, addressed destination, age), capped so a response fits the
    1 MiB frame cap.
  - `Status` unchanged.
- Trusted-time behavior is unchanged; the console shows service stderr
  via `docker logs`.

### The GUI (`tines-gui`, new crate)

- **Window layout**: status strip (node id, next seq, stored count,
  transport, socket path); observe panel (topic, payload, optional
  recipient + lifetime, broadcast default; text mode with a hex toggle —
  the wire is hex, the operator should not have to hex-encode by hand);
  belief panel (query a topic; list known topics from `List`); store
  browser (`List` view with refresh); events feed (contacts, lost,
  delivered, dropped, live); container panel (`docker logs` tail,
  stop/remove/restart).
- **Config generation**: the launcher writes each container's service
  config JSON (matching the `deny_unknown_fields` schema), generates
  random node ids unless the operator supplies them, and manages
  volume/socket paths under a per-fleet state directory.
- **IPC client**: the GUI speaks the widened IPC through a client module
  implementing the length-prefixed JSON framing. The request/response
  types are public in the `tines` crate (lib + bin), so the GUI can
  depend on the lib and share the types (builder's call, leaning yes).

### Docker image

- New `Dockerfile.tines`: builds the workspace, installs BlueZ runtime
  dependencies; the entrypoint runs the `tines` service with a mounted
  config. Both modes use this image; BT mode adds the privileged/host
  network and D-Bus/hci plumbing at run time.

## What's left to the builder

- Exact JSON schemas for the new IPC variants (within the constraints
  above).
- Bus daemon protocol details (framing, join/leave messages, fan-out).
- egui layout specifics and the theme integration.
- Docker run flags per mode (device passthrough, volume layout, image
  build/tag flow).
- Launcher supervision of child processes and daemons (PID tracking,
  cleanup on exit).

## Success looks like

- Sim mode: N containers exchange over the bus; broadcast observations
  converge in every node; an addressed observation with custody relay is
  watchable in the events feeds.
- BT mode: the container's node exchanges with a real tines robot over
  BLE — the two-device check from `tines-bt/README.md` is finally run
  and recorded from the console.
- Every operation in the widened IPC is reachable from a window without
  hand-rolled JSON.
