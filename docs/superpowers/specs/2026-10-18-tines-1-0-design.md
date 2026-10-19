# tines-core 1.0 — design

Date: 2026-10-18
Status: draft v4. Kyle decided D1–D5 and the follow-ups Q1–Q3
(2026-10-18). §2b records the 1.1 direction on age and authenticity. Not yet
reviewed.
Builds on: `2026-10-16-tines-design.md`, `2026-10-18-tines-addressed-delivery.md`,
`2026-10-18-tines-fleet-deployment-design.md` (assumed fully implemented)

## Intent

Make `tines-core` something a fleet can depend on for years:

- a wire format that says which version it is,
- a public API small enough to keep stable,
- a `Store` trait that third parties can implement without being broken by
  the next release,
- evidence (fuzzing, pinned vectors, semver checks) that the untrusted-input
  and compatibility promises hold.

1.0 is a promise, not a feature release. Nothing here adds behaviour to the
exchange; it removes, hides, pins, documents, and reserves space on the
wire for signatures, which ship in 1.1.

## Why

The fleet milestone puts tines on real robots that are mostly offline. A
robot that misses an update keeps running old code for weeks, so two problems
that don't matter in the simulator start to matter:

- **Wire drift.** Today a frame has no version. A node running a different
  encoding either drops every frame as `Malformed` with no indication why, or
  (worse) decodes a frame it misunderstands. Fleets need to fail loudly and
  safely.
- **API drift.** 0.3.0 had to break every out-of-tree `Store` impl to add
  `release`. Each such break forces every host crate (`tines-store`,
  `tines-bt`, `tines-ros2`, `tines`) to move in lockstep. A frozen,
  extensible surface stops that.

No robot runs tines yet, and none will be deployed with a pre-1.0
`tines-core`. 1.0 is the first version that ships on a robot, so there is no
installed base to migrate (D5).

## Goals

1. **Versioned wire format.** Every frame starts with a version byte. A node
   drops frames of any other version, and malformed frames, and reports
   both (D3).
2. **A stable, minimal public API** for `tines-core`: what an application, a
   `Store` backend and a `Transport` adapter need, and nothing else.
3. **A settled `Store` trait** whose future additions can always be
   non-breaking.
4. **A reserved signature slot** on every observation, with a defined
   `Signature` type, so 1.1 can add signing and verification without
   changing the wire format (D1).
5. **Evidence:** fuzzing of the untrusted-input path, pinned wire and
   identity vectors, an MSRV checked in CI, and `cargo-semver-checks` gating
   releases after 1.0.
6. **Complete docs:** every public item documented (`#![deny(missing_docs)]`),
   plus a written compatibility policy.

## Non-goals

- New protocol behaviour. Custody, receipts, summaries and fragmentation stay
  exactly as the fleet milestone leaves them.
- **Signing and verification: 1.1.** 1.0 defines the `Signature` type and
  carries it on the wire, untouched. It never creates, checks or acts on a
  signature. 1.1 adds signing, verification and key handling as a
  non-breaking minor release with significant new features (§2a). 1.1
  starts with a written threat model covering, among other things, whether
  `age_ms` must be authenticated (§2b).
- Version negotiation or multi-version nodes. A 1.x node speaks wire v1
  only; see "Compatibility policy".
- 1.0 for the other crates. `tines-sim`, `tines-store`, `tines-bt`,
  `tines-ros2` and `tines` stay 0.x and version independently (D4).
- Migrating pre-1.0 stores. Pre-1.0 `tines-store` logs are wiped, not
  migrated (D5).
- Topic schema/version tags. The core spec's open question stays a naming
  convention in the topic string, owned by applications.
- LoRa efficiency (`FRAG_OVERHEAD`) — still a `tines-lora` concern.

## Anti-goals

- A 1.0 that has to go to 2.0 the first time a robot gets a new radio. Local
  capacities (`MAX_PEERS`, queue caps) must be changeable in a minor release.
- A 1.1 that has to break the wire to add signatures. That's why the slot
  is reserved now.
- Freezing internals. `Exchange`, `Peer`, `Bloom`, `Reassembler` and `Frame`
  layout are implementation, not API.
- A version byte that lets old and new nodes misread each other's frames.

## Constraints

- All existing core invariants hold: `#![no_std]`, `alloc` optional, builds
  for `thumbv7em-none-eabihf` without default features, no async/threads/I/O,
  no panic on untrusted input, `Addr` never mapped to `NodeId` by core.
- The identity hash (`observer, seq, topic, payload, to`) does **not**
  change. Its pinned vector carries over from 0.x unchanged. The signature
  is excluded from the id, as the core spec planned.
- 0.x and 1.0 nodes can't talk (1.0 frames carry a version byte 0.x
  doesn't expect). That doesn't matter: no robot runs 0.x (D5).
- `rust-toolchain.toml` stays pinned for CI; MSRV is a separate, lower floor.

## The how

### 1. Wire envelope, version byte, and drop reporting

- Every frame on the wire is `[WIRE_VERSION: u8] ‖ postcard(Frame)`.
  `WIRE_VERSION` is a public constant.
- Decode: a frame whose first byte isn't `WIRE_VERSION` is dropped before
  postcard sees it. Empty frames are dropped.
- **The v1 byte value is chosen so pre-1.0 nodes reject it.** 0.x decodes
  the first byte as postcard's `Frame` variant tag (0–3 after the fleet
  milestone). Any value ≥ 4 is an unknown variant and decodes as
  `Malformed`. Conversely, a 0.x frame starts with 0–3, which a 1.0 node
  rejects as a version mismatch. No robot runs 0.x, but test rigs and
  simulators might, so this costs nothing to get right. Pick a value well
  clear of small tags (e.g. `0xA1`).
- **Every drop is reported (D3).** A frame dropped at the envelope or in
  decoding raises a `NodeEvent::Dropped { reason }`, where `reason` is a
  `#[non_exhaustive]` `DropReason`:
  - `VersionMismatch { version: u8 }`: an un-updated or foreign peer.
  - `Malformed`: the version was right but the body didn't decode, or
    decoded to something core must reject (e.g. a `Signature` whose length
    doesn't fit its algorithm, §2a).

  An operator then sees "met an un-updated robot" or "a peer is sending
  garbage" instead of a silent non-exchange. With the version byte,
  "Malformed" from a v1 peer means a bug or an attack, not drift. The
  report carries no `NodeId`, since the sender can't be trusted before
  decode. Reports are rate-limited per transport address so one peer can't
  flood the event queue (builder's call how; the limit is a local
  constant).
- Expired addressed observations are *not* drops. They're well-formed and
  are discarded by policy; reporting them would be noise.
- `SUMMARY_OVERHEAD` and `FRAG_OVERHEAD` absorb the extra byte. Every frame
  still fits `mtu()` (Review Focus #6).
- **What a version bump means:** any change to the encoding of an existing
  variant, the meaning of an existing field, or a wire constant (§5) is a new
  `WIRE_VERSION` and a semver-major release. Appending a *new* `Frame`
  variant at the end is **not** a bump. Older 1.x nodes decode an unknown
  tag as `Malformed`, drop it and report it, which is the same safe
  degradation `Ack` relied on. New variants must therefore be optional to
  the protocol: a peer that drops them still interoperates correctly.

### 2. Observation: fixed shape, invariant enforced by type

Today `to: Option<NodeId>` and `lifetime_ms: Option<u64>` are separate
public fields, and `reject()` has to drop the two malformed combinations.
1.0 makes the invalid states unrepresentable (D2):

```rust
#[non_exhaustive]
pub struct Observation {
    pub observer: NodeId,
    pub seq: u64,
    pub age_ms: u64,
    pub observed_at: Option<u64>,
    pub topic: Topic,
    pub payload: Payload,
    pub dest: Option<Dest>,          // replaces to + lifetime_ms
    pub sig: Option<Signature>,      // reserved; signing is 1.1 (§2a)
}

#[non_exhaustive]
pub struct Dest { pub to: NodeId, pub lifetime_ms: u64 }
```

- Fields stay readable (`Store` backends and belief policies need them), but
  `#[non_exhaustive]` stops outside crates from building the struct
  literally, so fields can be added in 1.x. Construction goes through
  `Observation::broadcast(observer, seq, topic, payload)` and
  `Observation::addressed(observer, seq, topic, payload, to, lifetime_ms)`,
  plus `with_observed_at`. Applications normally never build one —
  `Node::observe`/`observe_to` do. 1.0 has no public way to set `sig`; 1.1
  adds it.
- The id hash reads `dest.map(|d| d.to)` and is byte-for-byte the same as
  0.x; the pinned vector proves it. `sig` is excluded from the id.
- `reject()` keeps only the expiry check; the malformed-pair branch and its
  tests go away (the pairing is now enforced by the type, including on
  decode).

### 2a. The signature slot (D1)

1.0 reserves the slot and fixes the type; **1.1 adds signing and
verification.** That's a non-breaking minor release with significant new
features. The rustdoc for `Signature`, `SigAlgo` and `Observation::sig`
must say so in those words, so nobody mistakes the 1.0 slot for a security
feature.

```rust
#[non_exhaustive]
pub struct Signature {
    pub version: u8,                         // signature format version
    pub algo: SigAlgo,                       // u8 on the wire
    pub bytes: heapless::Vec<u8, MAX_SIG>,   // the signature itself
}

#[non_exhaustive]
#[repr(u8)]
pub enum SigAlgo {
    Ed25519 = 1,
    // 0 is reserved (never valid). PQC algorithms get new ids later.
}
```

- **Coverage rule (normative):** a signature covers the whole observation,
  and MUST also cover its own `version` and `algo` bytes. The second part
  is what prevents fallback attacks: an attacker can't relabel a signature
  as an older format version or a weaker algorithm without invalidating
  it. The signed message is
  `version ‖ algo ‖ observer ‖ seq ‖ observed_at ‖ topic ‖ payload ‖ dest`
  (`dest` including `lifetime_ms`), in a canonical, length-prefixed
  encoding that 1.1 pins with a test vector.
  - **One field is necessarily unsigned: `age_ms`.** Every relay advances
    it, so no observer signature can cover it. A relay can therefore make an
    observation look older or younger than it is. 1.1's docs must say so,
    and belief policies that care should weigh `observed_at` (signed) where
    the observer has a clock. Whether age needs authenticating some other
    way is an open 1.1 question with its own threat model (§2b).
  - `sig` itself is excluded (a signature can't cover itself).
  - The signed bytes are *not* the 16-byte id. A 128-bit truncated hash
    gives only 64-bit collision resistance, which is too weak to sign, and
    the id also leaves out `observed_at` and `lifetime_ms`.
- **The wire carries `algo` as a raw `u8`, not as a postcard enum tag.** A
  postcard enum would make a 1.0 node reject any observation carrying an
  algorithm id it doesn't know, so it would drop an observation it could
  otherwise store and forward. Instead `SigAlgo` decodes through a `u8`
  with a catch-all (`SigAlgo::Unknown(u8)`, or an equivalent the builder
  picks). Unknown algorithms are carried and forwarded untouched. Only
  `algo = 0` and a length that's wrong for a *known* algorithm (Ed25519
  must be exactly 64 bytes) are `Malformed`.
- **Unknown versions are carried too.** 1.0 checks `version` only for
  `Malformed`-level sanity (the builder pins the rule; e.g. `version = 0` is
  invalid). It never rejects a well-formed signature it can't interpret,
  for the same reason as unknown algorithms.
- Core never creates, checks or strips a signature in 1.0. `insert` stores
  `sig` with the observation, and every send carries it. Relays don't
  re-sign.
- **Which key verifies it** is 1.1's problem. The natural binding is
  `observer` → public key through a fleet key registry the host supplies.
  1.0 doesn't need a key id on the wire because the observer `NodeId` is
  already there.
- **Known gap for 1.1:** dedupe is first-copy-wins, so an unsigned or
  forged-signature copy that arrives first would shadow the real one. 1.1
  needs "replace if better signed" semantics in `Store` (e.g. a defaulted
  `replace_sig`, allowed under the §4 extension rule because the safe
  default — keep the first copy — loses only the upgrade).

### 2b. Age, authenticity, and the 1.1 threat model (direction, not 1.0 work)

None of this changes 1.0. It records what 1.1 has to settle before it
designs signing, so the 1.0 slot doesn't get mistaken for the whole answer.

**1.1 needs a written threat model before choosing a mechanism.** In
particular it must decide whether `age_ms` needs to be authenticated, by
whom, and against which attackers. Two attackers are worth separating:

- **Outsider:** no fleet key. Can listen, inject frames, replay frames,
  and change frames in transit on the radio.
- **Insider:** a fleet robot with a valid key that is compromised or buggy.
  It can already drop, withhold or delay anything it carries; custody
  already accepts carrier failure.

**What the observer's signature can't protect.** `age_ms` is advanced by
every holder, so the observer's §2a signature can't cover it. If it stays
unauthenticated, here is what a forged age breaks today:

| Forged age | Where core uses it | Effect |
|---|---|---|
| Lower (looks fresher) | `LatestWins` picks the smallest age | Stale world state beats a newer observation; robots plan on old facts. |
| Lower | `reject`: addressed expiry is `age_ms > lifetime_ms` | An expired directive (a task) lives again and is delivered after its lifetime. |
| Lower | `TrimPolicy::max_age_ms`, `keep_per_topic` keep the youngest | Stale data survives trimming and pushes out fresher data. |
| Higher (looks staler) | `LatestWins` | A fresh observation loses to an older one, which suppresses it. |
| Higher | addressed expiry | A live directive is rejected on arrival. For an insider that's the same as dropping it (accepted carrier failure); for an outsider it's a new way to kill custody traffic. |
| Higher | trim | Fresh data is trimmed early. |
| Either | dedupe keeps the first copy's age lineage (`insert` returns `Ok(false)` and leaves the stored age) | The first forged copy fixes the age at that receiver for good. A later honest copy can't correct it. |

The threat model should weigh these against what each attacker can
already do. An insider can already drop and delay, so age forgery adds
mainly the *freshness* attacks: stale facts looking new, and expired
directives coming back. An outsider can do all of the above unless frames
are authenticated hop by hop.

**Candidate mechanisms** (for 1.1 to choose between, after the threat
model):

- **Per-hop age statement.** The sending relay signs
  `(observation id, age_ms, relay NodeId)`. The receiver checks it, then
  discards it, and signs its own when it forwards. Size doesn't grow per
  hop: about 80 bytes per transfer (relay id + 64-byte signature). It
  costs one signature per send and one check per receive.
  - Stops outsiders.
  - Makes insider lies attributable to a specific robot, but doesn't
    prevent them.
  - Doesn't need the `Observation` slot: 1.1 can add it as a new frame
    variant appended to `Frame`, which §1 allows in a minor release.
- **Authenticated link (likely preferred).** A short authenticated
  handshake at contact start, then a short authentication tag on every
  frame. It protects ages, `Ack`s (today guarded only by "accept from the
  address we sent to"), summaries and fragments, for one signature
  exchange per contact.
  - Stops outsiders.
  - Says nothing about insiders.
  - Also a frame-level addition, so it fits 1.x.
- **Absolute-time cross-check.** `observed_at` is covered by the observer's
  signature (§2a). A receiver whose clock roughly agrees with the
  observer's can flag an `age_ms` that disagrees with
  `now − observed_at` by more than an allowed skew. It costs nothing on the
  wire, but needs clocks, which v1 robots may not have.

The observer's signature (§2a) is needed under every option: it's the only
thing that keeps the *content* checkable after any number of hops.

### 3. Public API surface

Public in 1.0 (everything else becomes `pub(crate)`):

| Item | Why it's public |
|---|---|
| `Node`, `ObserveError` | the facade applications use |
| `NodeEvent` (non-exhaustive enum; replaces `ConfidenceEvent`), `DropReason` | `Delivered { peer, id }`, `Dropped { reason }`; room for more |
| `Observation`, `Dest`, `Signature`, `SigAlgo`, `NodeId`, `ObservationId`, `Tick`, `Topic`, `Payload` | the data model |
| `Store`, `StoreError`, `TrimPolicy`, `MemStore` (`alloc`) | backends and their reference impl |
| `Transport`, `Event`, `SendError`, `FrameBuf` | transport adapters |
| `BeliefPolicy`, `LatestWins`, `SelfFirst` | the memory layer |
| `consts` (wire constants + local caps, §5), `WIRE_VERSION` | sizing |

Made private: `Exchange`, `Peer`, `InFlight`, `Bloom` (including the
public `salt` field), `Reassembler`, `fragment`, `Frame`, `WireError`,
`FRAG_OVERHEAD`. Nothing outside `tines-core` uses them except one
`tines-sim` test that touches `Exchange` directly; it moves to `Node`.

- **All public enums are `#[non_exhaustive]`**: `ObserveError`,
  `StoreError`, `SendError`, `Event`, `NodeEvent`, `DropReason`,
  `SigAlgo`. Adding an error case, an event, a drop reason or an algorithm
  becomes a minor release. (Transports can still *construct* `Event`
  variants; only exhaustive matches outside core need a wildcard.)
- **Public structs with public fields are `#[non_exhaustive]`** where they
  may grow: `Observation`, `Dest`, `Signature`, `TrimPolicy`
  (`TrimPolicy::default()` + field assignment still works).
- `Node` keeps its current methods: `new`, `id`, `next_seq`, `observe`,
  `observe_to`, `poll`, `drain_events`, `belief`, `trim`, `store`,
  `transport_mut`. `drain_events` returns `NodeEvent`s.
- Every remaining public item is documented; `#![deny(missing_docs)]` goes in
  `tines-core/src/lib.rs` (26 items are missing docs as of the fleet
  milestone; most of them are the fields of the now-private types).

### 4. The `Store` trait, settled

The trait after the fleet milestone (`insert`, `contains`, `get`, `len`,
`for_each_id`, `for_each_broadcast_id`, `for_each_addressed_id`, `release`,
`is_released`, `for_each_topic`, `trim`) is the 1.0 trait, with these
changes:

- **`is_empty()`** gains a default (`len() == 0`), dropping the
  `len_without_is_empty` allow.
- **Release semantics are documented as best-effort.** `release` stays
  infallible (`()`): if a persistent backend fails to record a release, the
  worst case after reboot is a resumed copy, i.e. a duplicate custodian.
  That's safe, never a lost message, so a `Result` would only add an error
  path nobody can act on.
- **Stores keep `sig` verbatim.** `get` returns the signature it was given;
  a backend that drops or alters it is non-conforming.
- **The exchange never trusts a backend's filtering.** Whether a backend's
  `for_each_addressed_id` skips released or broadcast ids is an
  optimisation only; core re-checks `dest` and `is_released` on every
  candidate (already true after the fleet milestone; 1.0 pins it with a test
  that uses a store which overrides nothing).
- **Rule for 1.x:** any method added to `Store`, `Transport` or
  `BeliefPolicy` after 1.0 must have a default whose behaviour is *safe*: it
  can lose performance or telemetry but never data or custody. A method that
  can't have such a default is a 2.0 change. This is the rule `release`
  violated in 0.3.0.
- A shared conformance test suite, `tines_core::store_conformance` (behind
  a `conformance` feature, `std`-free), lets backends prove the contract:
  - dedupe,
  - age lineage,
  - saturating ticks,
  - release surviving `get`,
  - `sig` round-tripping,
  - trim order,
  - no tombstones.

  `MemStore` and `tines-store` both run it. This is new public API, called
  out per AGENTS.md: it's what makes a third-party `Store` trustworthy.

### 5. Constants: wire vs local

`consts.rs` splits into two documented groups:

- **Wire constants**, frozen for wire v1: changing any is a `WIRE_VERSION`
  bump. These are `MAX_TOPIC`, `MAX_PAYLOAD`, `MAX_FRAME`, `SUMMARY_BYTES`,
  `BLOOM_K`, `ACK_IDS_CAP` and `MAX_SIG` (= 64, Ed25519; Q1), plus the
  private `MAX_FRAGMENTS`. Post-quantum signatures don't fit in 64 bytes, so
  adding one is a wire v2 / tines-core 2.0 change.
- **Local capacities and timings**, which can change in a minor release
  because they never cross the wire: `MAX_PEERS`, `SEND_QUEUE_CAP`,
  `ACK_BUFFER_CAP`, `BCAST_SENT_CAP`, `EVENT_QUEUE_CAP`,
  `SUMMARY_INTERVAL_MS`, `REASSEMBLY_TIMEOUT_MS`, and the drop-report rate
  limit.

AGENTS.md's constant list is updated to show the split.

A signed observation is up to 67 bytes bigger (`version` + `algo` + length +
64). Fragmentation already handles observations that don't fit one frame,
so nothing else changes. The fuzz and MTU tests must include
maximum-size, signed observations.

### 6. Bounce-table overflow, pinned

The bounce table (`SEND_QUEUE_CAP` entries, shared across peers, drop-oldest)
can overflow during a large sync. The 1.0 spec pins what overflow costs:

- A lost **Sent** record makes the peer's ack get rejected, so the
  custodian keeps its copy. The result is a duplicate custodian, which is
  safe.
- A lost **Received** record may let us offer an id back to the peer that
  gave it to us. That peer either released it (and doesn't ack, per the
  released-duplicate rule) or still holds custody (and acks, and we
  release). Either way at least one holder remains. The cost is airtime.
- Requirement: a sim property drives the table past capacity across
  `MAX_PEERS` peers and checks the "always ≥ 1 holder" invariant and that
  every candidate still gets a turn (the custody cursor). Whether to give
  each peer a fair share of the table instead of one shared FIFO is the
  builder's call; it's a local change, not a wire one.

### 7. Evidence

- **Fuzzing.** A `fuzz/` crate (cargo-fuzz, outside the workspace, nightly)
  with one main target that drives a real `Node` + `MemStore` through a
  scripted `Transport`. The fuzz input is decoded into a sequence of
  `Contact`/`Lost`/`Frame(bytes)`/`Tick(delta, may go backwards)`/`observe`
  steps. After each step it checks:
  - no panic,
  - every sent frame is no larger than `mtu()` (randomised per run),
  - no addressed observation is stored past its lifetime,
  - every rejected frame produced a `Dropped` report (up to the rate limit),
  - stored signatures round-trip unchanged.

  Fuzzing through `Node` keeps every internal private. It runs as a
  scheduled CI job (not a PR gate, since the toolchain is pinned stable),
  with the corpus checked in.
- **Stable twin.** The same harness runs as a proptest in the normal test
  suite with a bounded case count, so the PR gate still exercises it on the
  pinned toolchain.
- **Pinned vectors** in `tines-core/tests/vectors.rs`:
  - the identity hash (existing vector, unchanged),
  - one golden encoding per `Frame` variant including the version byte,
  - an observation carrying an Ed25519-shaped signature, and one carrying
    an unknown algorithm id, both of which must decode,
  - a 0.x-shaped frame that must be rejected as a version mismatch.

  A change to any of these vectors is a wire-format change by definition.
- **MSRV.** `rust-version` is set in `[workspace.package]` to the lowest
  version that builds `tines-core` (found with `cargo-msrv`, not guessed).
  CI builds and tests `tines-core` on that version as well as the pinned
  one. Raising MSRV is a minor release, called out in the CHANGELOG.
- **Semver gate.** After 1.0.0 is published, `cargo-semver-checks` against
  the last published `tines-core` runs in `scripts/ci.sh`.

### 8. Compatibility policy (goes in README and the crate docs)

- **Wire:** every 1.x release speaks wire v1. Only new optional `Frame`
  variants may be added, plus new `SigAlgo` ids and signature versions
  within the 64-byte `MAX_SIG` budget. A wire-v2 change, including any
  post-quantum signature, ships as tines-core 2.0.
- **Identity:** `Observation::id` never changes within a major version, and
  should not change across majors unless the hash itself is broken.
- **API:** standard semver, enforced by `cargo-semver-checks`; the
  extension rule for traits in §4; MSRV bumps are minor.
- **Local behaviour** (capacities, timings, scheduling order, which
  candidate goes first) can change in a minor release as long as the
  protocol properties hold:
  - custody: always ≥ 1 holder,
  - receipts never shorten world state,
  - every frame fits `mtu()`.
- **Signatures:** 1.0 carries them; 1.1 signs and verifies them, without
  breaking the wire or the API.

### 9. tines-store and the upgrade (D5)

`tines-store` (0.x) moves to `tines-core = "1"` and its log format follows
the new `Observation` encoding. There is no migration:

- The log gains a format header. Opening a log with a missing or older
  header is an error that names the fix (delete the log). Silently
  misparsing an old log is not allowed.
- Hosts wipe pre-1.0 logs on upgrade. That's acceptable because no robot
  runs tines yet, and none will be deployed with a pre-1.0 `tines-core`.
- The header is what makes a later migration possible if one is ever
  needed. Adding it is cheap now and expensive later.

## Decisions (Kyle, 2026-10-18)

- **D1. Reserve the signature slot now: yes.** `Signature { version: u8,
  algo: SigAlgo (u8), bytes }`; it must cover `version` and `algo`. Ed25519
  is the only algorithm defined; PQC ids come later. Signing and
  verification are 1.1, and the docs say so (§2a).
- **D2. `Dest` instead of `to` + `lifetime_ms`: yes, as recommended** (§2).
- **D3. Drop and report: yes**, and malformed frames are reported the same
  way as version mismatches (§1).
- **D4. Only `tines-core` goes 1.0: yes.**
- **D5. No `tines-store` migration.** No robot runs tines yet, and none will
  ship with a pre-1.0 `tines-core`; wiping logs is accepted (§9).

## Follow-up decisions (Kyle, 2026-10-18)

- **Q1. `MAX_SIG = 64`: agreed.** Ed25519 fits. Every observation
  reserves `MAX_SIG` bytes inline, including on the MCU, and sizing for
  post-quantum signatures (Falcon-512 is ~666 bytes, ML-DSA-44 is 2,420)
  would cost that RAM for an algorithm nobody uses yet. A post-quantum
  algorithm is therefore a wire v2 / tines-core 2.0 change. Smaller new
  algorithms can still be added in 1.x through new `algo` ids.
- **Q2. Coverage: the whole observation, plus `version` and `algo`.** The
  `version`/`algo` requirement exists to stop fallback attacks (§2a).
  `age_ms` is the one field that can't be signed, because relays change it
  (§2a).
- **Q3. Malformed frames are reported the same way as version
  mismatches**, through `NodeEvent::Dropped { reason }` (§1).

## Left to the builder

- The exact `WIRE_VERSION` value, within the rules in §1.
- How drop reports are rate-limited per peer, and the limit's value.
- The `SigAlgo` catch-all representation and the `version` sanity rule
  (§2a).
- Fair-share vs shared FIFO for the bounce table (§6).
- Shape of the fuzz input decoder and the conformance suite's API.
- Order of work. Suggested:
  1. vectors (they pin the starting point),
  2. the envelope and drop reports,
  3. `Dest`, then `Signature`,
  4. the API narrowing,
  5. docs, fuzzing and MSRV,
  6. the 1.0.0 release, with `cargo-semver-checks` switched on.

## Done means

- `sh scripts/ci.sh` is green with `#![deny(missing_docs)]`, an MSRV job and
  the vector tests.
- The fuzz target has run for an agreed budget (e.g. 24 CPU-hours) with no
  findings, and the corpus is committed.
- README has a "Stability" section with the §8 policy, including the 1.1
  signature note; CHANGELOG has a 1.0.0 entry listing every break from 0.3.
- AGENTS.md lists this spec, the wire/local constant split and the trait
  extension rule.
- `tines-store` refuses pre-1.0 logs with a clear error (§9).
- `tines-core` 1.0.0 is published, and the host crates depend on `"1"`.
