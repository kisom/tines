//! tines-core: content-defined Observation identity and shared-memory core.
#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod consts;
pub mod observation;

pub use observation::{Observation, NodeId, ObservationId, Payload, Tick, Topic};

#[cfg(test)]
extern crate std;
