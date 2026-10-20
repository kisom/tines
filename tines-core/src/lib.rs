//! tines-core: content-defined Observation identity and shared-memory core.
#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod belief;
mod bloom;
pub mod consts;
mod exchange;
mod frag;
#[cfg(feature = "alloc")]
pub mod mem_store;
pub mod node;
pub mod observation;
pub mod store;
pub mod transport;
mod wire;

pub use belief::{BeliefPolicy, LatestWins, SelfFirst};
pub use exchange::{DropReason, NodeEvent};
pub use node::{Node, ObserveError};
pub use observation::{
    Dest, NodeId, Observation, ObservationId, Payload, SigAlgo, Signature, Tick, Topic,
};
pub use store::{Store, StoreError, TrimPolicy};
pub use transport::{Event, FrameBuf, SendError, Transport};
pub use wire::frame_sender;

#[cfg(feature = "alloc")]
pub use mem_store::MemStore;

#[cfg(test)]
extern crate std;
