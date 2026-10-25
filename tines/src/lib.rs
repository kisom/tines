//! The standalone `tines` service.
//!
//! Hosts a `Node` over a Unix-domain socket and serves a
//! small request/response IPC for `observe` / `belief` / `events` / `status`.
//! One instance runs per device — the binary (`src/main.rs`) wires a
//! `Node<LogStore, HostTransport>` for real BLE or the host bus, while tests
//! drive the same [`serve`] / [`handle`] path with a mock transport so no
//! radio is required.
//!
//! The IPC protocol is documented in [`ipc`]; the on-disk configuration in
//! [`config`].
#![deny(missing_docs)]

pub mod clock;
pub mod config;
pub mod ipc;
pub mod transport;
pub mod trust;

pub use clock::HostClock;
pub use config::{Config, TransportKind, TrustedClock};
pub use ipc::{
    handle, serve, BeliefOut, ContactOut, DropOut, EventOut, ObservationOut, Request, Response,
    LIST_CAP,
};
pub use transport::HostTransport;
