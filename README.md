# tines — opportunistic shared memory for mostly-offline robots

Small robots doing their work collect facts about the world. They are mostly
offline, so when two of them happen to meet in range they swap what they know so
that each robot's picture of the world improves over time. No robot *depends* on
receiving anything — it can do its job on its own observations — but there is no
reason not to share.

![tines](tines.jpg)

`tines` is the library that does this: a store of timestamped-by-age
observations, a belief layer derived from them, and a delay-tolerant exchange
protocol that runs over whatever datagram link the robot has. The name is from
Vinge's *A Fire Upon the Deep*: the Tines only form a mind when close enough to
hear each other.

## Contents

- [Design at a glance](#design-at-a-glance)
- [Architecture](#architecture)
- [Observations](#observations)
- [The exchange protocol](#the-exchange-protocol)
- [Addressed delivery](#addressed-delivery)
- [Receipts](#receipts)
- [Usage](#usage)
- [Writing a transport](#writing-a-transport)
- [Writing a store](#writing-a-store)
- [The simulator](#the-simulator)
- [Running on a robot](#running-on-a-robot)
- [Crate layout](#crate-layout)
- [Building and CI](#building-and-ci)
- [Security](#security)
- [Where to read more](#where-to-read-more)

## Design at a glance

- **Sharing is opportunistic, never load-bearing.** A contact may be cut off
  at any frame. Two nodes' stores converge given enough cumulative contact
  time; no single contact promises anything.
- **Order doesn't matter.** Storage is synced by set union over content-derived
  ids, so a node ends up with the same store whether it met B or C first.
- **The MCU is a first-class target.** `tines-core` is `#![no_std]`, has no
  async, threads, or I/O, and builds without an allocator for
  `thumbv7em-none-eabihf`. Every buffer is fixed-size.
- **Untrusted input never panics.** Every frame off the wire is decoded
  defensively; malformed bytes are dropped.
- **The application sets the pace.** `Node::poll` emits at most one frame per
  call, so duty cycle and airtime are the application's call.
- **No trust layer yet.** Anyone in radio range can read and inject. See
  [Security](#security).

## Architecture

Three layers, each a separate concern, top to bottom:

```mermaid
flowchart TB
    subgraph node["Node&lt;S, T&gt; — the application facade"]
        direction TB
        memory["<b>memory</b><br/>beliefs per topic, computed<br/>from storage under a BeliefPolicy"]
        storage["<b>storage</b> — Store trait<br/>append-only set of observations,<br/>keyed by content id; local trimming"]
        comms["<b>comms</b> — Exchange<br/>contact-driven summary / diff /<br/>send over a Transport"]
        memory -- "derived from (pure function)" --> storage
        comms -- "inserts received / reads to send" --> storage
    end
    app(["application"]) -- "observe() / belief()" --> node
    app -- "poll()" --> node
    comms <-- "frames" --> transport[["Transport<br/>(UDP, LoRa, serial, sim…)"]]
```

| Layer   | Holds                              | Crosses the wire? |
|---------|------------------------------------|-------------------|
| memory  | beliefs, recomputed on demand      | never             |
| storage | raw observations                   | yes, by set union |
| comms   | per-peer exchange state            | carries the frames |

Only observations cross the wire. Memory is a pure function of storage plus a
policy and is never transmitted, so two robots can hold different beliefs about
the same facts — that is expected, not a bug.

## Observations

An `Observation` is the unit of storage and exchange:

| Field         | Meaning                                                        | In the id? |
|---------------|----------------------------------------------------------------|:----------:|
| `observer`    | `NodeId` (`[u8; 16]`) of the robot that saw it                 | ✓ |
| `seq`         | the observer's sequence number                                 | ✓ |
| `topic`       | what it is about, ≤ 64 bytes (`b"door/3"`)                      | ✓ |
| `payload`     | opaque bytes, ≤ 1024                                           | ✓ |
| `to`          | `None` for broadcast, or a recipient `NodeId`                  | ✓ |
| `age_ms`      | how old the observation is                                     |   |
| `observed_at` | optional absolute timestamp, if the robot has a clock          |   |
| `lifetime_ms` | validity window, present exactly when `to` is                  |   |

The **content id** is the first 16 bytes of Blake2s-256 over the ✓ fields.
Because age and timestamps are excluded, the same fact arriving by two
different paths, at two different ages, dedupes to one entry.

### Time is relative

Robots don't share a clock, so the protocol carries *age*, not time. Each call
takes a local monotonic `Tick` (milliseconds), and a store reports an
observation's current age as:

```
current_age = age_ms_at_insert + (now − stored_at)      // saturating
```

When an observation is sent, it goes out with its current age, so age keeps
accumulating across hops. A tick that goes backwards (after a reboot, say)
saturates rather than underflowing.

### Sequence numbers must survive reboots

`seq` is part of the id, so a robot must not reuse one for different content.
Pass `Node::new` a `first_seq` that is higher than any seq used before,
typically `boot_count << 32`, with a boot counter persisted across reboots.
`tines-store`'s `LogStore` keeps that counter for you: seed the node with
`store.first_seq()`. A repeated `seq` with identical content simply dedupes
(`insert` returns `Ok(false)`).

## The exchange protocol

When two robots come into range, both run the same symmetric procedure;
neither is the initiator.

```mermaid
sequenceDiagram
    autonumber
    participant A as Robot A
    participant B as Robot B
    Note over A,B: Transport reports Contact on both sides
    A->>B: Summary (salted Bloom filter of A's broadcast ids)
    B->>A: Summary (salted Bloom filter of B's broadcast ids)
    Note over A: diff: queue what B appears to lack, youngest first
    Note over B: diff: queue what A appears to lack, youngest first
    A->>B: Obs (or Frag × n)
    B->>A: Obs (or Frag × n)
    A->>B: Obs …
    Note over A,B: every 2 s: re-summarise with a new salt,<br/>so ids learned mid-contact propagate too
    A-->>B: Summary (new salt)
    Note over A,B: Lost — or the contact simply ends mid-frame
```

- **Summaries** are 512-byte salted Bloom filters (k = 3). On a narrow link
  the filter is folded down to the largest power of two that fits the MTU. A
  false "peer has it" only delays propagation; the salt changes every round, so
  a different set of ids is affected each time.
- **Diffs** are sent youngest-first, on the theory that short contacts should
  carry the freshest data. Each peer's queue holds up to 256 ids.
- **Every frame is self-contained and idempotent.** Duplicates, reordering and
  loss are all harmless; there is no session state to corrupt.

### Wire frames

Frames are [postcard](https://docs.rs/postcard)-encoded and each carries the
sender's `NodeId`, so the core never needs to map transport addresses to
robots.

```mermaid
classDiagram
    class Summary {
        from: NodeId
        salt: u32
        bits: ≤512 bytes
    }
    class Obs {
        from: NodeId
        obs: Observation
    }
    class Frag {
        from: NodeId
        id: ObservationId
        index: u8
        count: u8
        data: bytes
    }
    class Ack {
        from: NodeId
        ids: ≤90 ObservationIds
    }
```

A transport that binds a connection to a peer's identity can check a frame's
claimed sender with `tines_core::wire::frame_sender`, without decoding the
whole frame.

### Fragmentation

An observation that doesn't fit the transport's MTU is split into up to 64
`Frag` frames (64 bytes of each frame are reserved for the header). Each peer
has one stream in flight at a time, and it is finished before anything else
starts for that peer. All fragments of a stream are cut from one encoding, so
the receiver can always reassemble them. A partial observation is never
stored; the receiver discards it after 5 s.

```mermaid
flowchart LR
    obs["Observation<br/>(e.g. 1 KB payload)"] --> enc["postcard encode<br/>once per stream"]
    enc --> f0["Frag 0/8"] & f1["Frag 1/8"] & fd["…"] & f7["Frag 7/8"]
    f0 & f1 & fd & f7 -. "lossy link,<br/>any order" .-> re["Reassembler<br/>(per peer)"]
    re -- "all 8 present,<br/>id checks out" --> st[("store")]
    re -- "timeout / bad id" --> drop(["discarded"])
```

### What one `poll()` does

`Node::poll` drains inbound transport events, then emits **at most one**
frame:

```mermaid
flowchart TD
    start(["poll(now)"]) --> drain["Drain transport events:<br/>Contact → add peer<br/>Lost → drop peer<br/>Frame → decode, store, update peer"]
    drain --> due{"Summary due<br/>for some peer?"}
    due -- yes --> sum["Send Summary"]
    due -- no --> rr["Pick next peer, round-robin"]
    rr --> inflight{"Fragment stream<br/>in flight?"}
    inflight -- yes --> frag["Send next fragment"]
    inflight -- no --> turn{"Whose turn?"}
    turn -- custody --> addr["Start an addressed message<br/>(deliver, else forward)"]
    turn -- broadcast --> diff["Start the front of the<br/>broadcast diff"]
    addr -. "nothing to send" .-> diff
    diff -. "nothing to send" .-> addr
    sum & frag & addr & diff --> send["transport.send()"]
    send -- "Busy / Unreachable" --> undo["Roll back:<br/>same frame next poll"]
    send -- ok --> done([return])
    undo --> done
```

## Addressed delivery

Most observations are broadcasts: everyone gets everything. An observation can
also be **addressed** to one robot with `Node::observe_to`, e.g. a tasking from
a base station. Addressed messages travel along a **custody chain**: a message
has one custodian at a time, and each handoff is confirmed by a receipt before
the old custodian lets go.

```mermaid
sequenceDiagram
    participant Base
    participant R1 as Robot 1
    participant R2 as Robot 2
    participant R3 as Robot 3 (recipient)
    Base->>Base: observe_to(R3, lifetime 60 s, "task", …)
    Note over Base,R1: contact
    Base->>R1: Obs to=R3 (forward)
    R1->>Base: Ack
    Note over Base: releases its copy:<br/>no longer offered to anyone
    Note over R1,R2: later contact
    R1->>R2: Obs to=R3 (forward)
    R2->>R1: Ack
    Note over R2,R3: later contact
    R2->>R3: Obs to=R3 (delivery)
    R3->>R2: Ack
    Note over R3: shows up in R3's belief()
    Note over Base,R3: anyone still holding it drops it once age > lifetime
```

- A custodian offers the message to the recipient when it meets it
  (**delivery**), and otherwise to any peer it meets (**forwarding**).
  Delivery is preferred. Custody and broadcast take turns for each peer, so
  neither can starve the other.
- The receiver acks only once it has stored the message. When the ack comes
  back, the sender **releases** its copy: the copy stays in its store until
  it expires or is trimmed, but is never offered again.
- If an ack is lost, the sender keeps offering the message, so a message can
  briefly have two custodians. That costs airtime, never the message. A
  custodian that fails after handing the message on does lose it; that's
  accepted.
- Addressed messages **never appear in a summary**, so their existence doesn't
  leak to unrelated robots through gossip.
- **Bounce suppression:** within one contact, a robot never offers a message
  back to the peer it came from, or to a peer that already has it.
- **Expiry is local and silent.** A message older than its `lifetime_ms` is
  never forwarded and never stored. There are no end-to-end delivery reports
  and no cancellation messages.
- `belief()` sees broadcasts and messages addressed *to me*. Messages carried
  for others exist only to be forwarded.

## Receipts

Each robot acknowledges what it receives with `Ack` frames, sent as it goes
during a contact (up to 90 ids per frame), for both kinds of observation:

| Kind      | What an ack means to the sender                                        |
|-----------|-------------------------------------------------------------------------|
| addressed | custody has moved: release my copy                                      |
| broadcast | confidence only: "peer P has observation X"; nothing changes in storage |

An ack is accepted only from a peer the id was actually sent to during the
current contact. A robot never acks an observation it failed to store (for
example, because its store is full), and never acks an addressed duplicate
it has already released, since that would hand custody back to a robot that
no longer carries the message.

Broadcast confirmations surface as `NodeEvent<A>::Delivered { peer, id }` from
`Node::drain_events`. Frames the peer could not use are counted under a
`DropReason` (`VersionMismatch` or `Malformed`) and surface as
`NodeEvent::Dropped { reason, count }` (see the `drop` reporting in the `tines`
service and the ROS2 node). The event queue is bounded (64) plus a small
counted-drop table, so drops are always reported even under a chatty peer —
events are telemetry, and losing one never loses data.

## Usage

`tines-core` exposes a single application-facing facade, `Node<S, T>`, generic
over any `Store` and `Transport`:

```rust
use tines_core::{LatestWins, MemStore, Node, NodeId, TrimPolicy};

let me: NodeId = [7; 16];
let boot_count = 0u64; // persisted and incremented on every boot
let store = MemStore::new();
let transport = /* any Transport implementation */;

// first_seq must not repeat across reboots. With tines-store's LogStore,
// pass store.first_seq() instead.
let mut node: Node<MemStore, _> = Node::new(me, boot_count << 32, store, transport);

// Record something you saw. observed_at is an optional absolute clock
// (ms since epoch); `now` is a local monotonic Tick on every call.
let id = node
    .observe(b"door/3", b"open", Some(1_700_000_000_000), 0)
    .expect("observation fits the size limits");

// Address a tasking to one robot; it is carried until delivered or 60 s old.
node.observe_to([9; 16], 60_000, b"task", b"inspect door/3", None, 0)
    .expect("observation fits the size limits");

// Drive the exchange in your loop. Returns how many new observations were
// stored this round.
let stored = node.poll(500);

// Ask what you believe about a topic prefix, under a belief policy
// (e.g. the youngest observation wins).
let belief = node.belief(b"door", &LatestWins, 500);

// Which peers have confirmed holding which of your broadcasts, and which
// frames the peer dropped and counted.
for event in node.drain_events() {
    match event {
        tines_core::NodeEvent::Delivered { peer, id } => {
            println!("peer {peer:?} has {id:?}");
        }
        tines_core::NodeEvent::Dropped { addr, reason, count } => {
            println!("dropped {count} frame(s) from {addr:?}: {reason:?}");
        }
    }
}

// Keep the store bounded.
node.trim(
    &TrimPolicy {
        keep_per_topic: Some(4),
        max_age_ms: Some(3_600_000),
        max_count: Some(10_000),
    },
    500,
);
```

`observe` rejects an oversized topic or payload with `ObserveError::TooLarge`;
nothing is ever truncated.

### Belief policies

A `BeliefPolicy` folds the matching observations, one at a time and in no
particular order, into a single belief. Nothing is buffered, which matters
with 1 KB payloads on an MCU. Two policies ship:

| Policy       | Picks                                                                   |
|--------------|-------------------------------------------------------------------------|
| `LatestWins` | the youngest observation; ties broken by `(observer, seq)`              |
| `SelfFirst`  | my own youngest observation if I have one, otherwise like `LatestWins` |

Write your own by implementing `BeliefPolicy::step` (for example, a majority
vote or a weighted average over payloads). Because beliefs are recomputed from
storage, changing policy never requires touching stored data.

### Trimming

`Store::trim` applies a `TrimPolicy` locally. In order, it keeps the N youngest
observations per topic, drops anything older than `max_age_ms`, then drops the
oldest observations until `max_count` remain. Trimmed ids aren't tombstoned:
if a peer offers one again later, it's accepted.

## Writing a transport

A transport is the boundary with the physical link. It only has to move
datagrams and report who is in range:

```rust
pub trait Transport {
    type Addr: Clone + PartialEq + core::fmt::Debug;
    fn poll(&mut self) -> Option<Event<Self::Addr>>; // Contact | Lost | Frame
    fn send(&mut self, to: &Self::Addr, frame: &[u8]) -> Result<(), SendError>;
    fn mtu(&self) -> usize;                           // ≤ 1500
}
```

- **Datagram semantics.** Frames may be lost, duplicated or reordered; the
  protocol assumes nothing more.
- **Contact detection is the transport's job:** a beacon heard, a link
  coming up, or the simulator saying "in range". A frame from an address with
  no prior `Contact` is treated as an implicit contact.
- **`Addr` is only a routing handle.** It can change mid-contact (DHCP,
  roaming) without harm.
- **Report back-pressure honestly.** Return `SendError::Busy` when the radio
  can't take a frame right now; the exchange will offer the same frame again
  on the next poll. `TooLarge` tells the exchange to give that frame up.
- **Driving `Exchange` without `Node`?** `Exchange::next_frame` assumes the
  frame it returns gets sent. If `send` fails, call
  `Exchange::on_send_error(&err)` before the next `next_frame`, or the frame
  is treated as delivered. `Node::poll` does this for you.

## Writing a store

`MemStore` (behind the default `alloc` feature) is the reference backend. A
flash or SQLite backend implements the `Store` trait:

| Method                  | Contract                                                                 |
|-------------------------|--------------------------------------------------------------------------|
| `insert`                | `Ok(true)` if new; `Ok(false)` for a known id, leaving the existing entry (and its age) untouched; `Err(StoreError::Full)` when out of room |
| `contains`              | whether an id is stored                                                  |
| `get`                   | the observation with `age_ms` advanced by `now − stored_at`, saturating if `now` is earlier |
| `len`                   | number of stored observations                                            |
| `for_each_id`           | every stored id, any order                                               |
| `for_each_broadcast_id` | ids with `to == None`; these feed the summary                            |
| `for_each_addressed_id` | ids with `to == Some(_)`; scanned for custody on every poll              |
| `release`               | mark an id as handed off, so it's never offered again; a no-op if the id isn't stored |
| `is_released`           | whether an id has been released; a re-inserted observation starts un-released |
| `for_each_topic`        | age-adjusted observations whose topic starts with a prefix               |
| `trim`                  | apply a `TrimPolicy` (per-topic, then age, then count); return how many were removed |

The store must remember when each observation was inserted (`stored_at`, in
the caller's `Tick`s) so it can report current ages. `insert` may return
`Err(StoreError::Full)`; the exchange then doesn't ack, so the sender keeps
custody.

`for_each_addressed_id` has a default implementation that visits every id,
which is correct but makes the exchange read every broadcast observation on
each poll just to find the few addressed ones. Override it if your backend can
filter by `to` cheaply, e.g. with an index or a separate table.

## The simulator

`tines-sim` runs N nodes over a scripted contact trace with seeded loss and
duplication, and is the protocol's test harness:

```rust
use tines_sim::{Contact, ContactTrace, Sim, WorldConfig};

let mut trace = ContactTrace::default();
trace.contacts.push(Contact { start_ms: 0, end_ms: 5_000, a: 0, b: 1 });
trace.contacts.push(Contact { start_ms: 10_000, end_ms: 15_000, a: 1, b: 2 });

let cfg = WorldConfig { nodes: 3, mtu: 512, loss: 0.1, dup: 0.05, seed: 42 };
let mut sim = Sim::new(cfg, trace.clone());
sim.observe(0, b"door/3", b"open");
sim.run_until(trace.end());

assert!(sim.converged(&[0, 1, 2])); // 0 and 2 never met, but 1 carried it
```

```mermaid
gantt
    title The contact trace above
    dateFormat x
    axisFormat %S s
    section 0 ↔ 1
    in range :0, 5000
    section 1 ↔ 2
    in range :10000, 15000
```

The property tests (`tines-sim/tests/props.rs`) generate random traces and
check that:

- every pair of nodes that was ever transitively connected ends up with the
  same broadcast store;
- no store depends on the order of contacts;
- a store only ever grows during a run (union is monotone);
- garbage frames never panic and never change a store;
- an unexpired addressed message always has a holder, a custodian or its
  recipient, including when some robots' stores are full.

`tines-sim/tests/relay.rs` adds scripted relay scenarios: a message relayed
down a custody chain to its recipient, one that expires before it gets there
and is never stored, and a released duplicate that isn't acked.

## Running on a robot

`tines-core` is only the library. The host crates put it on a Linux robot
(a Pi, or a Jetson running ROS2):

```mermaid
flowchart LR
    subgraph robot["One robot"]
        direction TB
        clients(["IPC clients"]) -- "Unix socket,<br/>length-prefixed JSON" --> svc["tines<br/>(service)"]
        ros(["ROS2 nodes"]) -- "/tines/… topics" --> rosnode["tines-ros2<br/>(TinesNode)"]
        svc & rosnode --> core["tines-core<br/>Node"]
        core --> log[("tines-store<br/>LogStore")]
        core --> bt["tines-bt<br/>BtTransport"]
    end
    bt <-- "BLE L2CAP" --> other(["other robots"])
```

A robot runs either the service or the ROS2 node, not both: each owns its
`Node` and its log.

- **`tines-store`**: `LogStore`, an append-only log file.
  - Each record is checksummed (CRC-32C). Recovery skips a corrupt record and
    truncates only a torn tail.
  - Releases are written as they happen.
  - A boot counter provides `first_seq()`.
  - `flush()` writes a clock marker, so ages carry across a restart, and
    fsyncs the log.
  - The file is created `0600` and locked, so only one process can open it.
- **`tines-bt`**: BLE over BlueZ, using L2CAP SeqPacket channels (one
  frame per packet).
  - Every robot advertises its `NodeId` and scans. The smaller `NodeId`
    dials and the larger accepts.
  - A connection is bound to the advertised `NodeId`, and frames claiming
    any other sender are dropped.
  - Connections produce `Contact` and `Lost` events. See
    [`tines-bt/README.md`](tines-bt/README.md).
- **`tines`**: the standalone service, for robots without ROS2.
  - It runs the exchange on a poll loop and answers `Observe`, `Belief`,
    `Events` and `Status` requests on a Unix socket.
  - It's configured by a JSON file: `node_id`, `peer`, `log_path`,
    `socket_path` and `poll_interval_ms`.
- **`tines-ros2`**: a ROS2 node.
  - It publishes each tines topic under `/tines/` and confirmed deliveries on
    `/tines/receipts`.
  - It turns messages on those topics into observations.
  - rclrs is behind the `ros2` feature, so the crate builds without a ROS2
    toolchain.

The hosts use a monotonic millisecond clock that starts at 0 when the
process does. Neither host sets `observed_at` yet.

**Current limits** (tracked in [`THREAT_MODEL.md`](THREAT_MODEL.md)):
- The service talks to one configured `peer`.
- The hosts don't yet flush on a timer.
- The host-hardening work (trim defaults, bounded channels, socket
  permissions, ROS2 isolation) hasn't landed.

The two-device BLE check in `tines-bt/README.md` hasn't been recorded yet.

## Crate layout

```
tines/
  tines-core/   no_std core: Observation, Store / Transport / BeliefPolicy
                traits, the exchange state machine, the `Node` facade, the
                in-memory store, and the reference belief policies.
                `alloc` is a default feature; without it, tines-core builds
                for thumbv7em-none-eabihf (only MemStore needs alloc).
  tines-sim/    std: the simulator, contact traces, and the convergence /
                order-independence / custody property tests.
  tines-store/  std: LogStore, the persistent append-only backend.
  tines-bt/     std: the BLE transport (BlueZ via bluer).
  tines/        std: the standalone service with Unix-socket IPC.
  tines-ros2/   std: the ROS2 node (rclrs behind the `ros2` feature).
```

Fixed limits, all compile-time constants in `tines-core/src/consts.rs`:

| Constant                | Value  | Meaning                                  |
|-------------------------|--------|------------------------------------------|
| `MAX_TOPIC`             | 64     | topic bytes                              |
| `MAX_PAYLOAD`           | 1024   | payload bytes                            |
| `MAX_PEERS`             | 8      | peers tracked at once                    |
| `SUMMARY_BYTES`         | 512    | full Bloom summary size                  |
| `BLOOM_K`               | 3      | hash functions per id                    |
| `SUMMARY_INTERVAL_MS`   | 2000   | re-summary period per peer               |
| `REASSEMBLY_TIMEOUT_MS` | 5000   | partial observation discard time         |
| `SEND_QUEUE_CAP`        | 256    | ids queued per peer                      |
| `MAX_FRAME`             | 1500   | largest frame (and MTU) supported        |
| `ACK_IDS_CAP`           | 90     | ids in one `Ack` frame                   |
| `EVENT_QUEUE_CAP`       | 64     | confirmed deliveries awaiting `drain_events` |
| `BCAST_SENT_CAP`        | 256    | broadcast sends remembered per peer, to validate acks |
| `ACK_BUFFER_CAP`        | 256    | received ids waiting to be acked, per kind |

## Building and CI

The toolchain is pinned in `rust-toolchain.toml`. `scripts/ci.sh` runs the full
gate — formatting, clippy and docs (warnings are errors), the test suite with
and without `alloc`, and the `no_std` build for `thumbv7em-none-eabihf`:

```sh
sh scripts/ci.sh
```

GitHub Actions runs the same script on every push to `master` and on every
pull request.

## Security

tines has **no trust layer yet**, by design. Anyone in radio range can read
everything, inject observations under any identity, and take custody of
addressed messages. That's acceptable only where the radio environment is
trusted. Signatures are planned for 1.1, with room for them reserved in the
1.0 wire format.

[`THREAT_MODEL.md`](THREAT_MODEL.md) has the full assessment: trust
boundaries, adversaries, findings, and which spec fixes each one.

## Where to read more

The specs are authoritative; the plans argue from them.

- **Core design:** `docs/superpowers/specs/2026-10-16-tines-design.md`
- **Addressed delivery:** `docs/superpowers/specs/2026-10-18-tines-addressed-delivery.md`
  (its epidemic forwarding is superseded by the fleet spec's custody chain)
- **Fleet deployment:** `docs/superpowers/specs/2026-10-18-tines-fleet-deployment-design.md`
- **Fleet fixes:** `docs/superpowers/specs/2026-10-19-tines-fleet-fixes.md`
- **Host hardening (next for the hosts):** `docs/superpowers/specs/2026-10-19-tines-host-hardening.md`
- **tines-core 1.0 (next for the core):** `docs/superpowers/specs/2026-10-18-tines-1-0-design.md`
- **Threat model:** [`THREAT_MODEL.md`](THREAT_MODEL.md)
- **Plans:** `docs/superpowers/plans/`
- **Changes:** [`CHANGELOG.md`](CHANGELOG.md)
