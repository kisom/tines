//! Compile-time constants for the tines workspace.
//!
//! Every value here is a compile-time `const` so it can drive generic
//! parameters (e.g. `heapless::Vec` capacities) and be optimized away.
//!
//! They split into two groups (§5): **wire** constants, frozen for wire v1 —
//! changing any is a `WIRE_VERSION` bump because it changes how a v1 peer
//! decodes or interprets a frame — and **local** constants, which never cross
//! the wire and can change in a minor release. Each const is tagged `wire` or
//! `local`. The private `MAX_FRAGMENTS` in `crate::frag` is also a wire
//! constant; see its note there.

// ---------------------------------------------------------------------------
// Wire constants — frozen for wire v1 (any change is a `WIRE_VERSION` bump)
// ---------------------------------------------------------------------------

/// WIRE. The first byte of every frame on the wire; changing it is a
/// `WIRE_VERSION` bump. `crate::wire::Frame::decode` rejects a leading byte
/// that differs (`WireError::VersionMismatch`), and [`crate::wire::frame_sender`]
/// emits this. `4` is the first value in the `4..=0x7F` range that fits a
/// single-byte postcard enum tag and sits above the 0.x tag values (`0..=3`).
pub const WIRE_VERSION: u8 = 4;

/// WIRE. Decoder capacity bound: a peer rejects any topic longer than this.
pub const MAX_TOPIC: usize = 64;

/// WIRE. Decoder capacity bound: a peer rejects any payload longer than this.
pub const MAX_PAYLOAD: usize = 1024;

/// WIRE. Both sides must agree; this signature slot is reserved on every
/// observation (`[crate::observation::Signature]`) and carried verbatim,
/// never signed or verified until 1.1. `64` fits Ed25519.
pub const MAX_SIG: usize = 64;

/// WIRE. Per-key summary capacity a peer expects when decoding a summary.
pub const SUMMARY_BYTES: usize = 512;

/// WIRE. Both sides must agree on the number of hash functions to read a
/// Bloom filter in a summary.
pub const BLOOM_K: usize = 3;

/// WIRE. Decoder capacity bound (`Frag.data` is an inline `Vec<u8, MAX_FRAME>`),
/// so a peer cannot decode a bigger frame; a known 2.0 trigger (R3).
pub const MAX_FRAME: usize = 1500;

/// WIRE. Decoder capacity bound on an `Ack` receipt's content-ID list.
pub const ACK_IDS_CAP: usize = 90;

// ---------------------------------------------------------------------------
// Local constants — never cross the wire (safe to change in a minor release)
// ---------------------------------------------------------------------------

/// LOCAL. Node-local peer table; never sent on the wire.
pub const MAX_PEERS: usize = 8;

/// LOCAL. Emission cadence; local scheduling only.
pub const SUMMARY_INTERVAL_MS: u64 = 2000;

/// LOCAL. Inbound reassembly timer; local only.
pub const REASSEMBLY_TIMEOUT_MS: u64 = 5000;

/// LOCAL. Outbound delivery buffer size; local only.
pub const SEND_QUEUE_CAP: usize = 256;

/// LOCAL. BLE floor / MTU-sweep lower bound: below this a fragment header
/// leaves no room for payload. Used by the frag sweep
/// (`crate::frag`) and the BLE transport clamp; never carried on the wire.
pub const MIN_MTU: usize = 23;

/// LOCAL. In-node `Exchange` `events` queue of `NodeEvent`s; never leaves the
/// node (unlike the wire `ACK_IDS_CAP`).
pub const EVENT_QUEUE_CAP: usize = 64;

/// LOCAL. `Exchange` counted-drop table of distinct `(addr, DropReason)`
/// pairs. Once full, further counted drops fold into a single overflow counter
/// (reported with no address); see [`crate::exchange::NodeEvent::Dropped`].
pub const DROP_TABLE_CAP: usize = 32;

/// LOCAL. `Exchange` per-peer `bcast_sent` table of broadcast ids dispatched
/// to a peer.
pub const BCAST_SENT_CAP: usize = 256;

/// LOCAL. Pending ack-id buffer (`acks` for addressed, `bcast_acks` for
/// broadcast) before the next Ack is emitted; overflow drops the oldest entry.
pub const ACK_BUFFER_CAP: usize = 256;

/// LOCAL. Idle eviction window: a peer with no inbound frame for longer than
/// this many ticks is removed as if Lost (`crate::exchange`). Never crosses
/// the wire; local only.
pub const PEER_IDLE_MS: u64 = 30_000;
