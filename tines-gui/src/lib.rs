//! The tines fleet console.
//!
//! A launcher plus one window per container: each window drives a running
//! `tines` service over its Unix-socket IPC. This crate holds the pieces
//! shared by those windows — the IPC client ([`ipc_client`]), the
//! config-generation half of the launcher ([`config`]), and the omarchy
//! theme mechanism ([`theme`]).
#![deny(missing_docs)]

pub mod config;
pub mod docker;
pub mod fleet;
pub mod ipc_client;
pub mod launcher;
pub mod theme;
pub mod window;
