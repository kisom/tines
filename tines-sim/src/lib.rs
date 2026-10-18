//! tines-sim: a std simulator for the tines shared-memory library.
//!
//! `SimWorld` is a shared, reference-counted simulation of nodes connected by a
//! `ContactTrace`. `SimTransport` implements the `tines_core::Transport` trait
//! on top of that world: outbound frames are enqueued into per-node inboxes,
//! and `SimWorld::advance` turns contact-state changes into `Contact`/`Lost`
//! events. Loss and duplication are driven by a deterministic, seeded RNG so
//! that convergence behaviour is reproducible.

#![deny(warnings)]

pub mod sim;
pub mod trace;
pub mod transport;
pub mod world;

pub use sim::{Sim, SimNode};
pub use trace::{Contact, ContactTrace};
pub use transport::SimTransport;
pub use world::{SimWorld, WorldConfig};
