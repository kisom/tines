//! tines-core: content-defined Observation identity and shared-memory core.
#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod bloom;
pub mod consts;
pub mod frag;
pub mod observation;
pub mod store;
#[cfg(feature = "alloc")]
pub mod mem_store;
pub mod transport;
pub mod wire;

pub use bloom::Bloom;
pub use frag::{fragment, Reassembler};
pub use observation::{Observation, NodeId, ObservationId, Payload, Tick, Topic};
pub use store::{Store, TrimPolicy, StoreError};
pub use transport::{Event, FrameBuf, SendError, Transport};
pub use wire::{Frame, WireError};

#[cfg(feature = "alloc")]
pub use mem_store::MemStore;

#[cfg(test)]
extern crate std;
