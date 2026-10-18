//! Compile-time constants for the tines workspace.
//!
//! Every value here is a compile-time `const` so it can drive generic
//! parameters (e.g. `heapless::Vec` capacities) and be optimized away.

/// Maximum topic length in bytes.
pub const MAX_TOPIC: usize = 64;

/// Maximum payload length in bytes.
pub const MAX_PAYLOAD: usize = 1024;

/// Maximum number of peers a node tracks.
pub const MAX_PEERS: usize = 8;

/// Maximum size, in bytes, of a per-key summary.
pub const SUMMARY_BYTES: usize = 512;

/// Number of hash functions used by the per-key Bloom filter.
pub const BLOOM_K: usize = 3;

/// Interval, in milliseconds, between summary emissions.
pub const SUMMARY_INTERVAL_MS: u64 = 2000;

/// Timeout, in milliseconds, for inbound frame reassembly.
pub const REASSEMBLY_TIMEOUT_MS: u64 = 5000;

/// Maximum number of frames buffered for outbound delivery.
pub const SEND_QUEUE_CAP: usize = 256;

/// Maximum UDP frame size in bytes.
pub const MAX_FRAME: usize = 1500;

/// Maximum number of content-IDs an `Ack` receipt may carry.
pub const ACK_IDS_CAP: usize = 90;

/// Cap on the `Exchange` `events` queue of `ConfidenceEvent`s (one per
/// broadcast id a peer has confirmed).
pub const EVENT_QUEUE_CAP: usize = 64;

/// Cap on the `Exchange` per-peer `bcast_sent` table of broadcast ids
/// dispatched to a peer.
pub const BCAST_SENT_CAP: usize = 256;
