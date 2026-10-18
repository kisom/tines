//! tines-store — persistent append-only `LogStore` backend.
//!
//! This crate is the host-side (std) persistence layer for tines. The `Store`
//! trait, `TrimPolicy`, and `StoreError` live in `tines-core` (which stays
//! `no_std` and I/O-free); everything durable lives here.

#![forbid(unsafe_code)]

pub mod log;

pub use log::LogStore;
