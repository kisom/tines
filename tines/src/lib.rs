//! The standalone `tines` service.
//!
//! Hosts a `Node` over a Unix-domain socket and serves a
//! small request/response IPC for `observe` / `belief` / `events` / `status`.
//! One instance runs per device — the binary (`src/main.rs`) wires a
//! `Node<LogStore, BtTransport>` for real BLE, while tests drive the same
//! [`serve`] / [`handle`] path with a mock transport so no radio
//! is required.
//!
//! The IPC protocol is documented in [`ipc`].
#![deny(missing_docs)]

pub mod ipc;

pub use ipc::{handle, serve, BeliefOut, EventOut, Request, Response};
