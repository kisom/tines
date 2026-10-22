//! tines-core: content-defined Observation identity and shared-memory core.
#![no_std]
#![deny(missing_docs)]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod belief;
mod bloom;
#[cfg(feature = "conformance")]
#[doc(hidden)]
pub mod conformance;
pub mod consts;
mod exchange;
mod frag;
#[cfg(feature = "alloc")]
pub mod mem_store;
pub mod node;
pub mod observation;
pub mod store;
#[cfg(feature = "conformance")]
pub mod store_conformance;
pub mod transport;
mod wire;

pub use belief::{BeliefPolicy, LatestWins, SelfFirst};
pub use exchange::{DropReason, NodeEvent};
pub use node::{Node, ObserveError};
pub use observation::{
    Dest, NodeId, Observation, ObservationId, Payload, SigAlgo, Signature, Tick, Topic,
};
pub use store::{Store, StoreError, TrimPolicy, WallClockStore};
pub use transport::{Event, FrameBuf, SendError, Transport};
pub use wire::frame_sender;

#[cfg(feature = "alloc")]
pub use mem_store::MemStore;

#[cfg(test)]
extern crate std;

// The pinned wire vectors (spec §7) need the crate-private `Frame` and
// `WireError`, so they compile as a unit-test module rather than as an
// integration test (`autotests = false` in Cargo.toml keeps Cargo from also
// building them as one).
#[cfg(test)]
#[path = "../tests/vectors.rs"]
mod vectors;
