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

- **Spec (authoritative):** `docs/superpowers/specs/2026-10-16-tines-design.md`
- **Implementation plan (how to build it):** `docs/superpowers/plans/2026-10-16-tines-core-and-sim.md`

The plan argues from the spec. Where the plan is silent, the **spec rules**.
Do not edit the spec or the plan — they are design history, not source.

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

This project is built by working the implementation **task-by-task, in order**.
Each task is TDD: write the failing test → see it fail → implement → see it
pass → commit. Do not skip ahead — later tasks depend on the exact names and
signatures earlier tasks produce. Tick the task checkboxes as you go.

For each task:

1. Read its section in the plan.
2. Implement to the interfaces the plan pins; don't invent new public names.
3. Before committing, run:
   - `cargo test --workspace`
   - `cargo clippy --workspace -- -D warnings`
   - `sh scripts/check-no-std.sh` (for any core change)
4. Commit with the **exact** message the plan gives for that task.

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
- Identity hash covers `(observer, seq, topic, payload)` **only**. `age_ms` and
  `observed_at` are excluded.
- `Node::next_seq()` must be persisted by the application across reboots
  (`boot_count << 32 | counter`); a repeated seq after reboot collides in the
  ID hash and must dedupe silently (`insert` returns `Ok(false)`).
- `NodeId = [u8; 16]`, `ObservationId = [u8; 16]`, `Tick = u64` (local
  monotonic ms, supplied on every call).

### Review Focus (each pinned to a task with a test)

1. Reused `seq` after reboot → store dedupes, `insert` returns `Ok(false)`. (Task 3)
2. Oversized `topic`/`payload` at `observe()` → `Err(TooLarge)`, never truncate/panic. (Task 10)
3. Garbage frames → dropped, no panic, no store change. (Tasks 2, 6, 7, 13)
4. Frame from an address with no prior `Contact` → treated as implicit contact. (Task 7)
5. Tick going backwards → age saturates at stored value, never underflow. (Task 3)

### Compile-time constants (`tines-core/src/consts.rs`)

`MAX_TOPIC=64`, `MAX_PAYLOAD=1024`, `MAX_PEERS=8`, `SUMMARY_BYTES=512`,
`BLOOM_K=3`, `SUMMARY_INTERVAL_MS=2000`, `REASSEMBLY_TIMEOUT_MS=5000`,
`SEND_QUEUE_CAP=256`, `MAX_FRAME=1500`. Match the exact values above.

## Crate layout and current state

```
tines/
  tines-core/   no_std core: Observation, Store/Transport/BeliefPolicy traits,
                exchange state machine, Node facade, in-memory store, reference
                policies
  tines-sim/    std: simulator, contact traces, convergence property tests
```

State of the plan (`docs/superpowers/plans/...tines-core-and-sim.md`):

- **Done & committed:** Tasks 1–6 (workspace + Observation identity, wire
  frames, Store/MemStore, salted Bloom summary, Transport trait, fragmentation
  and reassembly).
- **Implemented, not committed:** Task 7 — `tines-core/src/exchange.rs` plus the
  `lib.rs` changes. Commit it with the Task 7 message before starting Task 8.
- **Pending:** Task 8 (belief policies), 9 (`PairTransport` test helper),
  10 (Node facade + `tests/pair.rs` convergence test), 11–13 (simulator world,
  harness, property tests), 14 (`scripts/ci.sh` + README).

The `std` simulator is the test harness for the protocol; its convergence and
order-independence property tests are the main correctness evidence.

## Not in scope for this plan

`tines-udp`, `tines-sqlite`, `tines-cli`, and `tines-serial` belong to a second
plan. Do not start them here. The README lives in Task 14 of the plan — it will
be produced by the implementer; do not author it separately.
