//! cargo-fuzz entry point for the `tines-core` node harness.
//!
//! [`fuzz::run_input`] reads the mode and MTU from the first three bytes and
//! drives the shared [`fuzz::Harness`] — the same oracle the proptest twin in
//! `tines-core/tests/fuzz_twin.rs` runs, so a property checked here is
//! checked there too. Adversarial-mode inputs also get the §10 peer-table
//! recovery probe.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if let Some(mut h) = fuzz::run_input(data) {
        if h.mode() == fuzz::Mode::Adversarial {
            h.check_peer_table_recovers();
        }
    }
});
