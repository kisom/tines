//! tines-store — persistent `LogStore` backend: an append-only log, rewritten
//! as a snapshot at open and after the wall-clock fold.
//!
//! This crate is the host-side (std) persistence layer for tines. The `Store`
//! trait, `TrimPolicy`, and `StoreError` live in `tines-core` (which stays
//! `no_std` and I/O-free); everything durable lives here.

#![forbid(unsafe_code)]

pub mod log;

pub use log::LogStore;
