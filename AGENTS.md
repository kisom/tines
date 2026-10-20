# AGENTS.md — tines

Instructions for AI agents working on **tines**: opportunistic shared memory
for mostly-offline robots. Read this before touching code.

## What tines is

Small robots collect facts about the world while mostly offline. When two of
them happen to meet in range, they swap what they know so each one's picture of
the world improves. No robot depends on receiving anything, but there is no
reason not to share.

`tines` is the library that does this: a store of timestamped-by-age
observations, a belief layer derived from them, and a delay-tolerant exchange
protocol over any datagram link. Read the full intent, goals, anti-goals, and
architecture in the **spec first**:

- **Core design (authoritative):** `docs/superpowers/specs/2026-10-16-tines-design.md`
- **Addressed delivery (authoritative):** `docs/superpowers/specs/2026-10-18-tines-addressed-delivery.md`
- **Fleet deployment v1 (authoritative):** `docs/superpowers/specs/2026-10-18-tines-fleet-deployment-design.md` — supersedes the addressed-delivery spec's epidemic forwarding (custody chain with handoff receipts).
- **Fleet fixes (done):** `docs/superpowers/specs/2026-10-19-tines-fleet-fixes.md` — security-assessment findings F1–F5 and F17.
- **tines-core 1.0 (next, core):** `docs/superpowers/specs/2026-10-18-tines-1-0-design.md` — wire version, public API, signature slot, core hardening (§10).
- **Host hardening (next, hosts):** `docs/superpowers/specs/2026-10-19-tines-host-hardening.md` — findings F10–F12.
- **Implementation plans:** `docs/superpowers/plans/`

The plans argue from the specs. Where a plan is silent, the **spec rules**.
Do not edit the specs or the plans — they are design history, not source.
`README.md` is the user-facing overview and should track the code.
`THREAT_MODEL.md` is the living security assessment: when a change fixes or
changes a finding, update its status there (§1 and §10) with the commit.
`CHANGELOG.md` records each release.

## Architecture (three layers)

```
 memory — "my priors": beliefs per topic, derived from storage under a policy
 storage — append-only log of raw observations; local trimming policy
 comms   — contact-driven exchange of observations; transport trait + adapters
```

Only observations cross the wire. Memory is a pure function of storage plus a
policy and is never transmitted. See the spec's "Architecture" section for the
`Observation`, `Store`, `BeliefPolicy`, `Transport`, and exchange protocol
details.

## Workflow

Changes are TDD: write the failing test → see it fail → implement → see it
pass → commit.

1. Keep the public API small. New public names in `tines-core` (types, trait
   methods, functions) need a reason; call them out when you add them.
2. Before committing, run the full gate, which is what CI runs:
   `sh scripts/ci.sh` (fmt, clippy and rustdoc with warnings as errors, tests
   with and without `alloc`, and the `thumbv7em-none-eabihf` build).
3. Use conventional commit messages (`fix(core): …`, `ci: …`, `docs: …`).

The `Node` facade is core, not application glue: an application on any target
should only need to construct a `Node` with its `NodeId`, store, and transport,
call `poll()` in its loop, and `observe()` when it sees something.

## Hard constraints (non-negotiable in `tines-core`)

- `tines-core` is `#![no_std]`; `alloc` is a cargo feature (default on). With
  `--no-default-features` it must build for `thumbv7em-none-eabihf`. Only
  `MemStore` may require `alloc`.
- No async, no threads, no I/O, and no panic on untrusted input in core. Every
  frame from the wire is untrusted: malformed bytes are dropped, never
  unwrapped.
- Core never maps a transport `Addr` to a `NodeId`; every frame carries the
  sender's `NodeId`.
- Identity hash covers `(observer, seq, topic, payload, to)` **only**.
  `age_ms`, `observed_at` and `lifetime_ms` are excluded.
- Addressed observations (`to = Some`) never appear in a summary Bloom
  filter, and an addressed observation past its `lifetime_ms` is never
  stored or forwarded.
- `first_seq` passed to `Node::new` must not repeat across reboots
  (`boot_count << 32`, with a persisted boot counter; `LogStore::first_seq`
  provides it). A repeated seq after reboot collides in the ID hash and must
  dedupe silently (`insert` returns `Ok(false)`).
- Receipts: a receive the store rejects (`Err`, e.g. `Full`) is never acked,
  addressed or broadcast; an addressed duplicate whose local copy is released
  is never acked; an `Ack` is accepted only from a peer the id was sent to
  this contact.
- `NodeId = [u8; 16]`, `ObservationId = [u8; 16]`, `Tick = u64` (local
  monotonic ms, supplied on every call).

### Review Focus (each covered by a test; keep it that way)

1. Reused `seq` after reboot → store dedupes, `insert` returns `Ok(false)`.
2. Oversized `topic`/`payload` at `observe()` → `Err(TooLarge)`, never truncate/panic.
3. Garbage frames → dropped, no panic, no store change.
4. Frame from an address with no prior `Contact` → treated as implicit contact.
5. Tick going backwards → age saturates at stored value, never underflow.
6. Every emitted frame fits the transport's `mtu()`, including summaries.
7. Store-full receive → no `Ack`; custody stays with the sender (unit test and
   the full-store sim property).
8. Released addressed duplicate → no `Ack`.

### Compile-time constants (`tines-core/src/consts.rs`)

`MAX_TOPIC=64`, `MAX_PAYLOAD=1024`, `MAX_PEERS=8`, `SUMMARY_BYTES=512`,
`BLOOM_K=3`, `SUMMARY_INTERVAL_MS=2000`, `REASSEMBLY_TIMEOUT_MS=5000`,
`SEND_QUEUE_CAP=256`, `MAX_FRAME=1500`, `ACK_IDS_CAP=90`,
`EVENT_QUEUE_CAP=64`, `BCAST_SENT_CAP=256`, `ACK_BUFFER_CAP=256`. Match the
exact values above.

## Crate layout

```
tines/
  tines-core/   no_std core: Observation, Store/Transport/BeliefPolicy traits,
                exchange state machine, Node facade, in-memory store, reference
                policies
  tines-sim/    std: simulator, contact traces, convergence, relay and custody
                property tests
  tines-store/  std: LogStore, the persistent append-only backend
  tines-bt/     std: BLE transport over BlueZ (bluer)
  tines/        std: standalone service, Unix-socket JSON IPC
  tines-ros2/   std: ROS2 node; rclrs behind the `ros2` feature
```

Only `tines-core` is headed for 1.0; the host crates stay 0.x (1.0 spec, D4).
They're std-only and may use threads and I/O, but they're still the trust
boundary for radio, IPC and disk input (see `THREAT_MODEL.md`).

`tines-ros2` with `--features ros2` is outside the default gate: check it
with the ROS2 Docker job (`Dockerfile.ros2`, `scripts/check-ros2.sh`).

## Not started

`tines-udp`, `tines-sqlite`, `tines-cli`, and `tines-serial` (from the core
spec's crate layout) belong to a future plan. Don't start them unless asked.
