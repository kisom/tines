# tines — Addressed Delivery (store-and-forward)

Date: 2026-10-18
Status: draft
Extends: `docs/superpowers/specs/2026-10-16-tines-design.md`

## Intent

Robots are mostly offline. Today tines gossips observations to every node it
meets — every fact spreads to everyone, and every node is a sink for its own
world-picture. That covers shared *facts* but not *directives*: there is no way
for one robot to address a message to a specific recipient, or to have an
intermediary carry it there.

The motivating scenario: a survey drone (A) detects an object of interest that
needs the instrument robot C to investigate. A and C may never meet directly.
When A meets B, A hands B the tasking addressed to C; B carries it until it
meets C (or another node that can keep it moving); C eventually receives it and
recognizes it as already seen. A keeps its own copy and, if it ever meets C,
delivers the details directly.

A second scenario: a robot that needs retrieval or repair can emit a message
*about itself* — either broadcast (`to = none`) or addressed to a known entity
(its "control"/home base).

## Goals

1. A node can address a message to a specific recipient (`to = NodeId`).
2. Multi-hop, epidemic store-and-forward: any node that isn't the recipient can
   carry the message and forward it to any peer, until it reaches the recipient
   or expires.
3. Best-effort: no delivery acknowledgment; the originator keeps its own copy.
4. Lifetime-bounded: every addressed message carries a lifetime; carriers drop
   it once expired.
5. Dedup by content ID: the recipient (and every carrier) recognizes an
   already-seen message and ignores duplicates.
6. Local-drop cancellation: a node that is done with a message simply stops
   relaying it; nothing propagates.

## Non-goals / anti-goals

- No delivery receipts or custody-transfer acknowledgments. A never learns
  whether C got it.
- No hop-count limit. Lifetime is the only bound for now; a TTL/hop bound can be
  added later.
- No routing intelligence. Forwarding is pure epidemic — no notion of "closer to
  the destination."
- No first-class "about" field. The subject stays the `topic` (a naming
  convention the application owns).
- No change to the existing broadcast gossip path.
- The simulation GUI (mark a node ready/not, visually interact with memory) is a
  separate milestone (M3), out of scope here.

## Constraints

- `tines-core` remains `#![no_std]`; `alloc` is feature-gated (default on) and
  only `MemStore` may require it. Must build for `thumbv7em-none-eabihf` with
  `--no-default-features`.
- No async, threads, I/O, or panics on untrusted wire input.
- The content-ID hash changes: `to` becomes part of identity (a breaking change
  to the pinned vector from the M1 spec).
- Addressed messages never appear in a bloom summary (which would leak their
  existence and cause them to be gossiped).
- `lifetime_ms` is excluded from the content ID, like `age_ms`/`observed_at`.

## Data model

`Observation` gains two fields:

- `to: Option<NodeId>` — recipient. `None` = broadcast (today's gossip);
  `Some(C)` = addressed to C.
- `lifetime_ms: Option<u64>` — validity window for addressed messages;
  invariant `lifetime_ms.is_some() == to.is_some()`. Set at creation, carried
  unchanged, excluded from identity.

`observer` remains "from whom" (the originator). "about whom/what" remains the
`topic`.

The content ID becomes
`first-16-bytes(Blake2s-256(observer || seq_le || topic_len_le || topic ||
payload_len_le || payload || to))`, where `to = None` hashes as a distinct fixed
marker. Consequence: the same payload addressed to two different recipients is
two messages (distinct IDs), and a relay can hold both.

## Delivery model

Two disjoint paths.

**Broadcast (`to = None`)** — unchanged. Bloom-summary/diff gossip; every node
absorbs what it hears.

**Addressed (`to = Some(C)`)** — a separate, targeted path:

- A carrier holds the message in a bounded custody buffer (not the main store,
  not the bloom).
- On contact with a peer P: if P is C, deliver it (C absorbs into its store,
  deduped by ID); otherwise forward it — P becomes another carrier (multi-hop
  epidemic).
- Loop prevention: content-ID dedup makes re-delivery a no-op; a node does not
  re-forward a message to the peer it just received it from (immediate-bounce
  suppression).

**Invariant (non-negotiable):** only broadcast observations (`to = None`) are
summarized in a bloom. Addressed messages — to me or to others — are never
summarized, so their existence never leaks to unrelated nodes via gossip.

## Lifetime and expiry

An addressed message is valid for `lifetime_ms` from creation. A carrier
computes its current age with the existing machinery
(`current_age = age_ms + (now - stored_at)`) and drops it once
`current_age > lifetime_ms`. Expiry is local and silent. Age already accumulates
per hop (each relay re-encodes the age-adjusted observation), so a lifetime
measured from observation is consistent across every carrier.

## Cancellation

A node that is done with a message — the recipient, or a carrier that no longer
cares — simply stops relaying it. No cancellation signal propagates through the
network.

## Decisions vs. my calls

**Kyle decided:** `to` is part of identity (content ID); multi-hop epidemic
forwarding; lifetime (per-message, time-based) rather than hop-count; best-effort
with no ack; local-drop cancellation; "about" stays the topic; `observer` is
"from whom."

**My calls** (the builder-check surfaced five open questions; all resolved below
without needing further input — none changed a Kyle decision):

- **Custody is a filtered view over the store, not a separate core buffer.** A
  `MAX_CUSTODY`-sized heapless buffer of full messages would need ~64 × 1 KB ≈
  72 KB of core static RAM, breaking the no-alloc MCU constraint. Instead,
  addressed messages live in the (backend-owned, alloc-gated) store alongside
  broadcast facts; the "custody set" is just "stored observations with
  `to = Some(other)` not yet expired." Storage cost is the backend's, exactly as
  `Store`/`MemStore` already work.
- **Originator is a carrier.** The originator's own addressed message lives in
  that same view and is forwarded like any carrier's copy; it never enters the
  broadcast set and never feeds the bloom.
- **Delivery rides the custody path.** A carrier forwards on contact; when the
  peer is the recipient, the message is absorbed into its store (deduped by ID,
  excluded from the bloom). No separate "direct send" path.
- **Expired-never-stored is a correctness rule.** No node — carrier or recipient
  — ever stores an addressed message whose lifetime has expired; expiry is
  checked before insertion.
- **The app-facing view is broadcast + to-me.** `for_each_topic`/belief see only
  `to = None` and `to = Some(me)` observations; `to = Some(other)` messages are
  invisible to queries and exist only to be forwarded.
- **Convergence is broadcast-scoped.** The simulator's `converged`/
  `connected_components_converge` properties apply to broadcast observations
  only; addressed delivery gets its own properties (recipient receives a live
  message along a connected path; non-recipients dedup; expired never stored).
- **Bounce suppression is per-contact:** a node does not re-forward a message to
  the peer it just received it from within the same contact (content-ID dedup
  covers correctness otherwise).
- **Scheduling:** under the ≤1-frame-per-poll budget, the exchange interleaves
  broadcast (summary/diff) and custody (addressed forward); custody must not
  starve broadcast. Exact priority is left to the plan.
- **"control"/"home base"** are application-level names resolved to a `NodeId`
  before the message is created; the core sees only `NodeId`s.
- **`lifetime_ms` semantics:** measured from observation, excluded from identity,
  `Option` with the `Some`-iff-addressed invariant.

## What's left to the builder

- Exact wire-format changes (`to`, `lifetime_ms`) and the new content-ID hash
  layout + an updated known-vector test.
- The store/custody-buffer layout and the summary filter that excludes addressed
  messages.
- The exchange changes: targeted + epidemic forwarding on contact, bounce
  suppression, expiry.
- The `Node` facade API for addressing a message with a recipient and lifetime.
- A simulator scenario reproducing the relay chain (A → B → C, C dedups), plus
  property tests for expiry and epidemic spread.
