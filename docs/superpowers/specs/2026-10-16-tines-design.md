# tines — opportunistic shared memory for mostly-offline robots

Design document. 2026-10-16.

## Intent

Small robots doing their work collect facts about the world. They are
mostly offline. When two of them happen to be in range, they should swap
what they know so that each robot's picture of the world improves over
time. No robot *depends* on receiving anything — it can do its job on its
own observations — but there is no reason not to share.

`tines` is the library that does this: a store of timestamped
observations, a belief layer derived from them, and a delay-tolerant
exchange protocol that runs over whatever radio or link the robot has.

The name is from Vinge's *A Fire Upon the Deep*: the Tines only form a
mind when close enough to hear each other.

## Goals

- A robot's knowledge is a monotonically growing set of observations;
  meeting another robot can only add to it.
- Any two robots holding the same set of observations have identical
  storage, regardless of the order or path by which they got them.
- Exchange is opportunistic, session-free, and tolerant of being cut off
  at any byte. A half-finished exchange leaves both sides valid.
- The same core code runs on a Jetson-class Linux robot, an MCU-class
  robot, and inside a simulator, with only the transport and storage
  adapters differing.
- The semantics of an observation's payload are opaque to storage and
  transport. Only the memory layer interprets payloads, and the
  interpretation can change without touching stored data.

## Non-goals

- Guaranteed or timely delivery to any particular node. There is no
  "destination"; every node is a sink.
- Routing, custody transfer, or any multi-hop forwarding logic beyond
  "I carry what I have and give it to whoever I meet". Classic Bundle
  Protocol (RFC 9171) routing and custody are explicitly not in scope.
- Consensus. Robots may hold different beliefs about the same topic;
  that is acceptable and expected.
- Security, authentication, or encryption in v1. Observer IDs are
  asserted, not proven. (Noted as future work; the ID scheme should not
  preclude signing later.)
- Defining the observation payload schema. That is the application's
  problem; `tines` ships a byte-string payload and a topic key.

## Anti-goals

Things that would make this a failure even if it technically works:

- Sync behaviour that depends on contact order (robot A ends up with a
  different store depending on whether it met B or C first).
- A core crate that silently requires an allocator, `std`, threads, or a
  filesystem, making the MCU target an afterthought.
- A memory layer that must be synced, or whose state cannot be rebuilt
  from storage alone.
- Exchange that requires a reliable, ordered, long-lived connection.

## Constraints (decided)

- **Language:** Rust.
- **Targets:** Jetson-class Linux (primary), MCU-class (`no_std`, likely
  no allocator), and an in-process simulator for development and test.
- **Core crate is `no_std`.** `alloc` is an optional feature. Storage and
  transport are traits; the core contains no I/O.
- **PHY-agnostic.** The core never sees a radio. It sees a transport trait
  (below). Adapters exist outside the core.
- **Observations are opaque, timestamped, observer-attributed, and keyed
  by topic.** The payload shape is deliberately undecided (one candidate
  is a timestamped RSO observation; it does not matter to this design).

## Architecture

Three layers, each a separate concern, top to bottom:

```
 ┌──────────────────────────────────────────┐
 │ memory      — "my priors": beliefs per   │  derived, local, never synced
 │               topic, computed from       │
 │               storage under a policy     │
 ├──────────────────────────────────────────┤
 │ storage     — append-only log of raw     │  synced by set-union
 │               observations; local        │
 │               trimming policy            │
 ├──────────────────────────────────────────┤
 │ comms       — contact-driven exchange    │  PHY-agnostic; transport
 │               of observations            │  trait + adapters
 └──────────────────────────────────────────┘
```

Only observations cross the wire. Memory is a pure function of storage
plus a policy.

### Observation (the unit of everything)

```
Observation {
    id:          ObservationId   // content-derived hash (see below)
    observer:    NodeId          // who saw it
    seq:         u64             // observer-local, never repeats
    age_ms:      u64             // time since observed; mutable, grows per hop
    observed_at: Option<u64>     // absolute ms since epoch, if observer had a clock
    topic:       Topic           // key for grouping/trimming, e.g. "door/3"
    payload:     bytes           // opaque to tines
}
```

Decisions:

- `id` is a hash over `(observer, seq, topic, payload)`. Two robots that
  observe the same thing produce two observations; de-duplication is
  by identity, not by meaning. Union is therefore idempotent and
  order-independent. An observation is an *event*, not a fact: the same
  robot seeing the same chair twice is two observations. Nothing in the
  design requires an ID to be reproducible across reboots.
- `observer` (`NodeId`) is a 128-bit value supplied by the application
  when the node is constructed. It must be stable across reboots; the
  core does not provision or persist it. Adapters may derive it from a
  hardware unique ID (MCU chip UID, Jetson serial) or read it from
  config. *(My call.)*
- `time` is carried as **age, not a timestamp**, following RFC 9171's
  Bundle Age Block. Every observation carries `age_ms: u64`, the elapsed
  time since it was observed. Each holder increments it by its own
  holding time (monotonic tick; every target has one) before forwarding.
  An observer that has a real clock may additionally set
  `observed_at: Option<u64>` (ms since Unix epoch). A receiver with a
  clock can pin a clockless observation at `now − age_ms`. Precision
  degrades by each hop's tick error; `tines` does not attempt clock
  sync. *(Kyle's suggestion; the RFC mapping and the optional absolute
  field are my call.)*
- Because `age_ms` is mutable, it is **not** in the identity hash. The
  hash covers `(observer, seq, topic, payload)` where `seq` is an
  observer-local monotonic counter that must not repeat across reboots
  (persisted, or `boot_count << 32 | counter`). *(My call.)*
- `topic` is a short byte string. Its structure (hierarchy, separators)
  is application-defined but `tines` supports prefix matching for
  trimming and queries.
- Observations are immutable. There is no update or delete at the
  protocol level — only local trimming.
- Fixed upper bounds on `topic` and `payload` length are a core
  configuration constant so the MCU target can use fixed-size buffers.
  *(My call: 64 bytes topic, 1 KiB payload default; configurable at
  compile time.)*

### Storage layer

Responsibilities: hold observations, answer "do you have X", enumerate
what's held, enforce a local trimming policy.

```
trait Store {
    fn insert(&mut self, obs: &Observation) -> Result<Inserted, StoreError>;
    fn contains(&self, id: &ObservationId) -> bool;
    fn get(&self, id: &ObservationId) -> Option<Observation>;
    fn ids(&self) -> impl Iterator<Item = ObservationId>;
    fn by_topic(&self, prefix: &[u8]) -> impl Iterator<Item = Observation>;
    fn trim(&mut self, policy: &TrimPolicy) -> usize;
}
```

Decisions:

- Storage is append-only from the protocol's point of view. `trim` is
  the only removal and is purely local.
- Trimming is policy, not semantics: keep the last N per topic, drop
  older than T, stay under B bytes. Trimming never merges or rewrites
  observations.
- A trimmed observation may be re-received from a peer later. *(My call:
  accept it; the trim policy will drop it again. Keeping a tombstone set
  to refuse re-receipt is a possible later optimisation, bounded in size,
  and must not be required for correctness.)*
- Implementations: SQLite-backed (Jetson), flash ring buffer or
  fixed-size table (MCU), in-memory (simulator and tests). The core
  ships the in-memory one.

### Memory layer

Responsibilities: answer "what do I believe about topic X" by folding the
relevant observations under a policy.

```
trait BeliefPolicy<B> {
    fn fold(&self, me: NodeId, obs: impl Iterator<Item = Observation>) -> B;
}
```

Decisions:

- Memory is derived. It can be a cache, but it must be rebuildable from
  storage at any time and is never transmitted.
- Policies are pluggable. Examples the application might implement:
  most recent wins; own observations outrank others'; per-observer trust
  weights; observation-count thresholds; a confidence that decays with
  age. `tines` ships a `LatestWins` and a `SelfFirst` policy as
  references; anything that needs to parse the payload belongs to the
  application.
- Memory is where disagreement between observations is handled. Storage
  never resolves conflicts.

### Communications layer

Responsibilities: notice a peer, find out what the peer has that we lack
(and vice versa), transfer it, survive being interrupted.

#### Transport trait (the PHY boundary)

```
trait Transport {
    type Addr;
    fn poll(&mut self) -> Option<Event<Self::Addr>>;   // Contact(addr) | Lost(addr) | Frame(addr, bytes)
    fn send(&mut self, to: &Self::Addr, frame: &[u8]) -> Result<(), SendError>;
    fn mtu(&self) -> usize;
}
```

Decisions:

- Datagram semantics, unreliable, unordered, bounded size. Everything
  above assumes frames can be lost or duplicated.
- The core is driven by polling (`poll` / `send`) so it fits both a
  bare-metal superloop and an async runtime via a thin shim. *(My call:
  no async in the core.)*
- Contact detection is the transport's job (beacon heard, link up, sim
  says "in range"). The core reacts to `Contact` and `Lost`.
- `Addr` is a routing handle only; the core never maps it to a `NodeId`.
  Every protocol frame carries the sender's `NodeId` in its header, and
  per-peer exchange state is keyed by `NodeId`. An address that changes
  mid-contact (DHCP, roaming) therefore costs nothing. *(My call.)*
- Adapters planned: UDP broadcast/unicast over ad-hoc Wi-Fi (Jetson),
  serial-attached LoRa/packet radio, in-process simulator.

#### Exchange protocol

On `Contact(peer)`, both sides run the same symmetric procedure; neither
is the initiator in any semantic sense.

1. **Summarise.** Send a compact summary of held observation IDs.
2. **Diff.** On receiving the peer's summary, compute which held IDs the
   peer appears to lack and send those observations.
3. **Repeat** while the contact lasts, re-summarising periodically so
   observations received from a third party mid-contact also propagate.

Decisions:

- Every message is self-contained and idempotent. Receiving the same
  observation twice is a no-op; receiving a summary out of order is
  harmless. There is no session state that must survive to keep the
  store consistent.
- Summaries must fit in a small number of frames even when the store is
  large. *(My call: an inverted Bloom filter / IBLT-style structure or a
  per-topic-prefix hash tree; the builder picks, with the requirement
  that a false "peer has it" costs only a delayed propagation, never an
  inconsistency.)*
- Observations are sent smallest-first or oldest-first? *(My call:
  prioritise the peer's probable interest if the application supplies a
  priority function; otherwise newest-first, on the theory that short
  contacts should carry the freshest data.)*
- Fragmentation: observations may exceed MTU on narrow radios. The core
  fragments and reassembles; a partially received observation is
  discarded, never stored.
- Rate: the core emits at most one frame per `poll` so the application
  controls pacing and duty cycle.
- **Convergence is best-effort per contact and eventual over repeated
  contacts.** A single contact makes no completeness promise; it may be
  cut off at any frame. The guarantee is: given enough cumulative
  contact time and non-permanent loss, two nodes' stores converge
  (modulo trimming). No "final authoritative diff" or end-of-contact
  handshake exists. This follows directly from sharing being
  non-load-bearing. *(My call, consistent with the stated intent.)*

#### Simulator

A `std` crate wrapping the core with:

- N nodes, each with its own store and clock skew.
- A contact model: positions and ranges over time, or an explicit
  contact-trace file.
- A loss/duplication model per link.
- Assertions: after the trace, every pair of nodes that were ever
  transitively connected hold the same store (modulo trimming); no node's
  store depends on contact order.

The simulator is also the test harness for the protocol.

## Crate layout (my call)

```
tines/
  tines-core/      no_std; Observation, Store trait, Transport trait,
                   BeliefPolicy trait, exchange state machine, Node
                   facade, in-memory Store, reference policies
  tines-sim/       std; simulator, contact traces, property tests
  tines-udp/       std; UDP adapter (Jetson)
  tines-serial/    no_std-compatible; framed serial adapter
  tines-sqlite/    std; SQLite Store
  tines-cli/       std; inspect a store, replay a trace, run a node
```

The `Node` facade — the thing that owns a `Store`, a `Transport`, and the
exchange state machine, and exposes `observe()`, `poll()`, and
`belief()` — is core, not application glue. An application on any
target should need only: construct a `Node` with its `NodeId`, store,
and transport; call `poll()` in its loop; call `observe()` when it sees
something.

## v1 scope (my call)

Ships: `tines-core`, `tines-sim`, `tines-udp`, `tines-sqlite`,
`tines-cli`. The simulator's convergence property tests pass; two Jetsons
(or two Linux processes) on one Wi-Fi segment exchange observations via
UDP and persist them in SQLite.

Deferred: `tines-serial` and the MCU flash store. The `no_std` guarantee
is enforced in v1 by building `tines-core` for a `thumbv7em-none-eabihf`
target in CI with the in-memory store, so the MCU path is not an
afterthought even though no MCU adapter ships.

## Left to the builder

- Hash function for `ObservationId` (should be cheap on MCU; BLAKE2s or
  SipHash-128 are reasonable).
- Summary data structure and its parameters.
- Wire encoding (postcard or a hand-rolled fixed layout; must be
  `no_std`).
- Fragment header layout and reassembly timeout.
- SQLite schema.
- MCU flash store layout (can be a stub in v1 as long as the trait
  boundary is honoured).

## Open questions (not blocking)

- Payload schema. Deliberately out of scope; an RSO observation is one
  candidate.
- Whether topics should carry a schema/version tag so the memory layer
  can skip payloads it cannot parse. *(Leaning yes, as a convention in
  the topic string rather than a protocol field.)*
- Observer authentication (signing). Out of scope for v1; the ID hash
  should be chosen so a signature can be appended without changing the
  ID.
