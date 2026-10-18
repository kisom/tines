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

Only observations cross the wire. Memory is a pure function of storage plus a
policy and is never transmitted; storage is synced by set-union (order- and
path-independent); comms is a contact-driven exchange of observations over a
`Transport` trait.

## Usage

`tines-core` exposes a single application-facing facade, `Node<S, T>`, generic
over any `Store` and `Transport`. Persist `Node::next_seq()` across reboots
(`boot_count << 32 | counter`) and pass it back as the initial `first_seq`; a
repeated `seq` after reboot collides in the content-id hash and dedupes silently.

```rust
use tines_core::{LatestWins, MemStore, Node, NodeId};

let me: NodeId = [7; 16];
let boot_count = 0u64;
let counter = 3u64;
let store = MemStore::new();
let transport = /* any Transport implementation */;

// first_seq must survive reboot: boot_count << 32 | counter
let mut node: Node<MemStore, _> = Node::new(
    me,
    boot_count << 32 | counter,
    store,
    transport,
);

// Record something you saw. observed_at is an optional absolute clock
// (ms since epoch); `now` is a local monotonic Tick on every call.
let id = node
    .observe(b"door/3", b"open", Some(1_700_000_000_000), 0)
    .expect("observation fits the size limits");

// Drive the exchange in your loop. Returns how many new observations were
// stored this round.
let stored = node.poll(500);

// Ask what you believe about a topic prefix, under a belief policy
// (e.g. the youngest observation wins).
let belief = node.belief(b"door", &LatestWins, 500);
```

## Crate layout

```
tines/
  tines-core/   no_std core: Observation, Store / Transport / BeliefPolicy
                traits, the exchange state machine, and the `Node` facade.
                `std` is optional; `tines-core` builds for `thumbv7em-none-eabihf`
                with `--no-default-features`.
  tines-sim/    std: the simulator, contact traces, and the convergence /
                order-independence property tests.
```

## CI

`scripts/ci.sh` runs the full gate — formatting, clippy, the test suite, and the
`no_std` build — and is safe to drop into a CI config:

```sh
sh scripts/ci.sh
```

## Where to read more

The spec (authoritative) and the implementation plan that argues from it:

- **Spec:** `docs/superpowers/specs/2026-10-16-tines-design.md`
- **Plan:** `docs/superpowers/plans/2026-10-16-tines-core-and-sim.md`
