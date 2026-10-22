//! tines-core: content-defined Observation identity and shared-memory core.
//!
//! # Stability
//!
//! tines-core 1.0 guarantees, within the 1.x line:
//!
//! - **Wire:** every 1.x release speaks wire v1 ([`consts::WIRE_VERSION`]).
//!   Only new optional frame variants may be added, plus new [`SigAlgo`] ids
//!   and signature versions within the 64-byte [`consts::MAX_SIG`] budget. A
//!   wire-v2 change, including any post-quantum signature or a
//!   [`consts::MAX_FRAME`] above 1500, ships as tines-core 2.0.
//! - **Identity:** [`Observation::id`] never changes within a major version,
//!   and should not change across majors unless the hash itself is broken.
//! - **API:** standard semver; MSRV bumps are minor. Public types are
//!   `#[non_exhaustive]` where they may grow. A method added to [`Store`],
//!   [`Transport`] or [`BeliefPolicy`] after 1.0 must have a default whose
//!   behaviour is safe: it may lose performance or telemetry, never data or
//!   custody. Wall-clock folds live in the opt-in [`WallClockStore`] trait,
//!   not in [`Store`]. The `#[doc(hidden)]` `conformance` module is not part
//!   of the API.
//! - **Local behaviour** (capacities, timings, scheduling order, which
//!   candidate goes first) can change in a minor release as long as the
//!   protocol properties hold: custody always keeps at least one holder,
//!   receipts never shorten world state, and every frame fits
//!   [`Transport::mtu`]. That's why the local constants are crate-private.
//! - **Signatures:** 1.0 carries them ([`Observation::sig`]); 1.1 signs and
//!   verifies them, without breaking the wire or the API.
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
