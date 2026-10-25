//! The tines bus transport: exchange wire frames between nodes over a host
//! IPC bus (Unix sockets), identity-preserving (`Addr = NodeId`).
//!
//! [`BusTransport`] is the tines [`Transport`] implementation; [`daemon`] is
//! the hub that routes frames between all connected nodes. The two sides of
//! a fleet talk through one shared daemon socket.

#![deny(missing_docs)]

pub mod daemon;
mod transport;

pub use transport::{BusError, BusEvent, BusLink, BusTransport, DaemonLink};
