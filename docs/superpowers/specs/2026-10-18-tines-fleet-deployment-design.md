# tines fleet deployment v1 — design

Date: 2026-10-18
Status: approved by Kyle (2026-10-18); round-4 fixes applied

## Intent

Take tines from the simulator onto real robots. Kyle's fleet: a hexapod on a
Jetson running ROS2, plus smaller Pi-class robots, mostly offline — they meet
opportunistically over short-range radios, exchange observations, and part.
tines is the shared memory each robot uses for navigation and planning, and
the courier that moves destined data (tasks, telemetry home) across the fleet.

## Why

tines-core + tines-sim (M1), polish (M2), and addressed store-and-forward
(M2a) are done and green. This milestone answers the deployment questions:
what software runs where, how robots physically connect, and what a handoff
means. It adds one new protocol element — a handoff receipt, "I received
these observations" — which gives the fleet a custody signal for addressed
data and delivery confidence for world state, and it lands the first real
transport (BLE) plus the host layer (a ROS2 node, a standalone service, and
a persistent store backend).

## Goals

1. Handoff receipts in tines-core: a peer acknowledges the observations it
   received, on both classes.
2. A real BLE transport crate (tines-bt) implementing the core Transport
   trait on Linux (BlueZ).
3. tines-ros2: a self-contained ROS2 node that owns the tines memory and
   bridges ROS2 topics ↔ observations.
4. tines: a standalone service with its own IPC for robots without ROS2.
5. Receipt semantics: addressed → custody release (the carrier hands its
   copy over and stops forwarding it); world state → confidence events only
   (data stays in the world view).
6. A persistent store backend (host-side, std): the store survives reboot —
   world state and in-flight addressed custody come back after a power
   cycle.

## Non-goals

- No receiver selectivity: the bloom-summary push stays the catalog. Revisit
  only if robots need to decline carrying data.
- No custody-to-home for world state: it floods and ages out; home does not
  become an archive that lets the fleet forget.
- No LoRa transport in v1. Core already fragments observations
  (FRAG_OVERHEAD = 64 B per fragment) and folds summaries to the transport
  MTU; the real LoRa problem is efficiency — the 64-byte overhead is a
  quarter of a 255-byte LoRa packet. That is tines-lora tuning, not a core
  change.
- No end-to-end delivery confirmation for addressed data. This revises the
  addressed-delivery spec's "no ack" only to add hop-by-hop custody transfer,
  not end-to-end delivery reports.
- No persistence in tines-core or on no_std targets: core stays I/O-free;
  persistence in this milestone is a Linux host-side backend. MCU flash
  persistence is a later milestone.
- No wire-format versioning in this milestone. Versioning and the rest of
  the 1.0 spec-update gap list are deferred; the canonical list lives here
  (one line each) and is mirrored as a Chinook note: wire version byte
  (lands with the Ack frame); narrower public API (Peer fields,
  Bloom::salt, #[non_exhaustive] on the public enums, all-pub Observation);
  fuzzing the frame decoder; bounce-table overflow behavior; room for
  future signatures; rustdoc on the undocumented public items; rust-version
  field; settle the Store trait surface before 1.0.

## Anti-goals

- A tines that can't run on a Pi without ROS2 (hence the standalone `tines`).
- ROS2 leaking into tines-core: core stays no_std and platform-free.
- Receipts making world state forget early — world state is the shared world
  view and must survive handoffs.
- A probabilistic custody receipt: releasing custody on a false positive
  loses a destined message for good. Custody receipts are exact.

## Constraints

- tines-core invariants are unchanged: no_std, no async/threads/I/O in core,
  no panics on untrusted input; MAX_FRAME=1500, MAX_PAYLOAD=1024,
  MAX_TOPIC=64, SUMMARY_BYTES=512, BLOOM_K=3, SEND_QUEUE_CAP=256, and the
  addressed-delivery invariants (to/lifetime_ms pairing, addressed never in
  bloom/broadcast diff, expired never stored).
- BLE needs a Linux Bluetooth stack (BlueZ); Jetson and Pi both ship BT+BLE.
- tines-ros2 must be a plain ROS2 node; no core changes for ROS2's sake.
- Receipts are wire frames: untrusted, malformed receipts are dropped as Err.
- New core state stays fixed-size and allocation-free outside MemStore: the
  pending-ack buffers and the event queue are fixed-capacity and drop-oldest
  when full. Both drops are safe — a dropped ack just means the sender keeps
  carrying; a dropped event is missed telemetry, not lost data.

## The how (decided with Kyle)

- World state (`to = None`) floods to every encounter via bloom summary +
  diff, and lives in each robot's store until it ages out (lifetime/trim).
  It is the robots' world view for navigation and planning: nothing drops it
  early, no receipt shortens its life.
- Destined data (`to = Some(id)`) rides the existing addressed path: delivery
  to the recipient, or custody-chain relay (this supersedes the
  addressed-delivery spec's epidemic forwarding — see the addressed receipt
  below); dies at delivery or expiry.
- Receipts exist for both classes:
  - addressed: an exact receipt releases custody — the carrier stops
    forwarding the message to anyone; the acking peer becomes the next
    carrier. This supersedes the addressed-delivery spec's multi-hop
    epidemic forwarding: a message has one custodian at a time. Extra
    copies appear when a carrier hands it to several peers before the first
    ack comes back, or when an ack is lost (the contact ends before the ack
    arrives, the pending-ack buffer evicts it, or the bounce record is
    evicted); those yield duplicate custodians — safe, never a lost
    message. The originator does not deliver directly
    even if it later meets the recipient — delivery rides on the custody
    chain.
    Carrier failure after a transfer loses the message for good — Kyle
    confirmed this is accepted. If robustness ever demands it, the fallback
    is the epidemic reading (acks suppress resends per peer; carriers keep
    copies until expiry); not built now.
  - world state: a receipt is confidence — the sender fires a delivery event
    ("peer P received obs X") when a valid ack arrives; the store is
    unchanged.
- Ack eligibility: B acks every observation it stored or already had
  (insert returning Ok(false) counts — a duplicate means B has it). B never
  acks a rejected, expired, or store-full receive. Exception: an addressed
  duplicate whose local copy is released is not acked — a released holder
  no longer carries the message, so acking would hand custody back and
  strand it (the custodian releases, the released holder never forwards).
  The custodian re-offers at later contacts: airtime cost, not correctness.
  A recipient acking its own mail is unaffected (it never forwards
  addressed messages — the forwarding scan skips to == me).
- Ack validation: an Ack for id X is accepted only from an address X was
  actually sent to during the current contact. The bounce table today mixes
  directions (send-path and receive-path records are indistinguishable), so
  it cannot prove an id was sent; bounce records gain a direction, and ack
  validation matches only send-path records, while bounce suppression keeps
  matching both. Broadcast sends are not in the bounce table, so the
  exchange keeps a separate, fixed-size record of broadcast (id, addr) sends
  used only for ack validation; eviction there costs nothing but confidence
  telemetry. This limits forgery to ids an attacker was genuinely sent;
  wire-level trust is out of scope for v1.
- Ack sending: acks are sent as-you-go during the exchange — core has no
  end-of-contact handshake, so an ack held for contact-end would never
  leave. B coalesces pending (id, addr) pairs and emits consecutive Ack
  frames to each addr; a pending Ack goes out after a due summary and before
  further addressed/broadcast traffic, so short contacts still deliver
  receipts. Each Ack frame is split to fit the transport's mtu(). Addressed
  acks and broadcast acks live in separate fixed buffers so a world-state
  flood cannot evict custody acks.
- Transport: BLE. Advertising is the rendezvous discovery; connections carry
  the frame exchange.
- Persistence: tines-store is an append-only observation log with trim
  records, index rebuilt at startup, constructed with the current Tick so it
  can rebase ages at load. On load it rebases every stored_at to load time
  and folds the pre-reboot elapsed time (measured against the last persisted
  clock marker) into age_ms, so ages continue instead of saturating. Time
  spent powered off is invisible (no RTC in v1) and extends addressed
  lifetimes by at most the downtime — accepted for v1. Released addressed
  observations persist a released flag (tombstones until trim), so a reboot
  does not resume forwarding them. next_seq persists in tines-store metadata;
  the core contract (boot_count << 32 | counter) is unchanged.
- Release is a Store-level operation: the Store trait gains `release(id)`
  and `is_released(id)` — new required methods, a breaking change to a trait
  others implement, so the crates ship as 0.3.0 with a CHANGELOG entry (no
  safe no-op default exists: a silent no-op would resume forwarding). The
  plan must call both out per AGENTS.md. `for_each_addressed_id` skips
  released messages; MemStore and tines-store honor the flag (tines-store
  persists it, MemStore tracks it in RAM). The exchange's custody scan also
  checks `is_released(id)` in its predicate — the same way it re-checks `to`
  today — so a backend that keeps the default `for_each_addressed_id`
  (which visits every id) cannot quietly forward released copies: the
  store's skip is an optimization, never load-bearing.
- Trim may drop an in-custody addressed message before its lifetime expires
  — equivalent to carrier failure (accepted). Hosts should size
  max_count/keep_per_topic so custody traffic survives until its lifetime
  runs out rather than being count-trimmed.
- Software shape: tines-core (receipts) + tines-bt + tines-store
  (persistent backend, std) + tines-ros2 (self-contained, owns the
  memory) + tines (standalone, non-ROS2) + tines-sim (test rig).

## Left to the builder (my calls, marked)

- Receipt wire format: a new `Ack` frame carrying a list of content-IDs,
  split across frames to fit mtu(); settled with TDD. `Ack` is added as the
  last variant of `Frame` so postcard's existing variant tags do not shift.
- Pending-ack buffers: two fixed buffers of SEND_QUEUE_CAP (id, addr)
  entries in core — one for addressed acks (custody release), one for
  broadcast acks (confidence). Overflow drops the oldest entries — safe: a
  dropped addressed ack means the sender keeps carrying; a dropped broadcast
  ack only loses a confidence event.
- New fixed-size state gets named constants in consts.rs with pinned values
  (the broadcast-send record, the two pending-ack buffers, the event
  queue); the plan names and pins them, and AGENTS.md's constant list is
  updated when they land.
- World-state confidence events: the SENDER fires one event per acked id
  when a valid Ack arrives; Node gains a fixed-size event queue (drop-oldest
  when full) that hosts drain. New public API on Node — the plan must call
  this out per AGENTS.md.
- Released-but-kept: an acked addressed observation stays in the store with
  a persisted released flag, excluded from custody/forwarding, trimmed
  normally; re-offers of it dedupe silently and are not acked (the
  custodian keeps custody and re-offers at the next contact).
- Sim properties to pin: an ack releases a carrier's copy only if the acking
  peer actually holds the message (stored it, or already had it and still
  carries it — never on a rejected/expired receive, never from a released
  holder); a released carrier no longer forwards it; world-state receipts
  fire events and never mutate the store; with no carrier failures, every
  unexpired addressed message is always held by at least one carrier or by
  its recipient.
- tines-ros2: rclrs; tines topic names are hex-escaped into a `/tines/`
  prefix namespace, plus `/tines/receipts` for receipts; topics are created
  lazily per live tines topic, bounded by the store's topic set under trim
  and a configurable cap.
- `tines` IPC: a Unix domain socket with a small protocol (revisit if the
  Pi targets need otherwise).
- BLE: 16-bit service UUID + flags + NodeId in service data = 25 bytes,
  which fits legacy 31-byte advertising (a 128-bit UUID does not: 39
  bytes); extended advertising is an option, not a requirement. 16-bit
  service UUIDs are SIG-assigned; an unassigned one works for a private
  fleet but could collide with other products — noted, accepted. tines-bt's
  Addr is the advertised NodeId; frames whose `from` differs from the
  connection's advertised NodeId are dropped at the transport. Connection
  symmetry is broken by a fixed rule: the robot with the numerically
  smaller NodeId acts as central/initiator. L2CAP CoC preferred (its MTU
  covers 1500-byte frames); on the GATT fallback tines-bt reports the real
  MTU through mtu() and lets core's summary folding and fragmentation do
  the fitting — tines-bt never splits frames itself.
- tines-store replaces the planned tines-sqlite (an append-only log fits the
  storage layer; SQLite is unnecessary). tines-udp, tines-cli, tines-serial
  stay not-started per AGENTS.md.
- Host model: exactly one tines instance per device — tines-ros2 on ROS2
  robots, standalone tines elsewhere; they never coexist on one machine.
- When this spec lands, AGENTS.md gains it in the authoritative-spec list
  with a note that it supersedes the addressed-delivery spec's epidemic
  forwarding.
- The `tines` crate/binary keeps its name per Kyle's explicit call, despite
  the project-name collision the reviewer noted.
- Sequencing within the milestone: receipts → tines-bt → tines-store →
  tines-ros2 → tines.

## Review resolutions

Round 1 (builder check, 7 questions), round 2 (HANDOFF.md, six numbered
points plus smaller ones) and round 3 (HANDOFF.md) are resolved as recorded
below.

1. Custody release scope — Kyle decided: the single-copy custody chain,
   superseding epidemic forwarding. Epidemic-with-per-peer-release
   is recorded as the future fallback if robustness becomes an issue.
2. Ages after reboot — tines-store rebases stored_at and folds pre-reboot
   elapsed into age_ms at load; downtime is invisible (no RTC), accepted;
   next_seq persists in tines-store metadata.
3. Ack sending rule and size — as-you-go (no contact-end handshake),
   coalesced, fixed buffers with (id, addr) entries, drop-oldest overflow,
   frames split to mtu(), addressed acks never evicted by broadcast floods.
4. Ack eligibility — stored and duplicates acked (except released
   duplicates — see #17); rejected/expired/full never acked.
5. Forged acks — accepted only for ids sent to that address this contact;
   addressed via the bounce table, broadcast via a separate broadcast-send
   record. A scope-limiting rule, since v1 has no trust layer.
6. Wire versioning — deferred to the 1.0 spec update; the 1.0 gap list
   lives in the spec itself (under the versioning non-goal), mirrored as a
   Chinook note.
7. Confidence events — sender-side, fired when a valid Ack arrives;
   fixed-size event queue on Node.
8. Released messages — persisted released flag via a new `Store::release(id)`
   trait method; tombstones until trim; no post-reboot re-forwarding;
   duplicates dedupe silently and released duplicates are not acked
   (see #17).
9. BLE feasibility — 16-bit UUID advertising fits legacy frames; transport
   drops from/Addr mismatches; mtu() reporting plus core folding on the
   GATT fallback; smaller-NodeId-initiates rule restored.
10. LoRa non-goal — rewritten: fragmentation and folding already exist; the
    issue is FRAG_OVERHEAD efficiency, a tines-lora concern.
11. ROS2 — rclrs, hex-escaping under `/tines/`, lazy per-topic creation with
    a cap.
12. Crate names — tines-store replaces tines-sqlite; udp/cli/serial
    untouched; `tines` naming is Kyle's call.
13. Housekeeping — status line updated; "M3" replaced with "this milestone".
14. (Round 2) Ack-then-trim — documented as equivalent to carrier failure,
    with host sizing guidance for max_count/keep_per_topic.
15. (Round 2) Restored v1 decisions the rewrite dropped: BLE initiator rule,
    one-instance-per-device, ack scheduling order.
16. (Round 2) Smaller points — Ack as the last Frame variant (postcard tags
    stay stable); new no-carrier-failure custody property; duplicate-ack
    wording fixed; 16-bit UUID caveat noted (gap-list location superseded
    by #18); tines-store takes the current Tick.
17. (Round 3) Released duplicates are not acked — a released holder no
    longer carries the message, so acking would hand custody back and can
    strand it. `Store::is_released(id)` lands alongside `release(id)`.
18. (Round 3) The 1.0 gap list now lives in the spec itself (one line per
    gap, under the versioning non-goal); Chinook keeps a mirror.
19. (Round 3) Bounce records gain a direction bit so ack validation can
    match only send-path records.
20. (Round 3) Extra-copy sources spelled out (multi-handoff, lost ack,
    evictions) — duplicate custodians, never a lost message.
21. (Round 3) `Store::release`/`is_released` are breaking changes: the
    crates ship as 0.3.0 with a CHANGELOG entry; settling the Store trait
    surface is on the 1.0 gap list.
22. (Round 3) Smaller points: new constants named and pinned in consts.rs;
    "persist the flag" reworded to "honor the flag"; AGENTS.md update
    scheduled for landing; round counts corrected.
