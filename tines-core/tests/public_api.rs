//! Compile probe for the tines-core 1.0 public surface (Task 5).
//!
//! Every `use` path here must keep resolving against `tines_core`. A future
//! change that removes any of these items from the public API — or drops a
//! crate-root re-export that Task 5 established — fails to build here, which
//! is exactly what the probe guards against. CI compiles it on the workspace
//! both with and without the default (alloc) features.
#![allow(unused_imports)]

// Re-exported at the crate root by Task 5's `tines_core::lib`.
use tines_core::consts::WIRE_VERSION;
use tines_core::{
    belief::{BeliefPolicy, LatestWins, SelfFirst},
    node::{Node, ObserveError},
    observation::{
        Dest, NodeId, Observation, ObservationId, Payload, SigAlgo, Signature, Tick, Topic,
    },
    store::{Store, StoreError, TrimPolicy},
    transport::{Event, FrameBuf, SendError, Transport},
    DropReason, NodeEvent,
};
use tines_core::{consts, frame_sender};

// `MemStore` lives behind the `alloc` feature.
#[cfg(feature = "alloc")]
use tines_core::MemStore;
