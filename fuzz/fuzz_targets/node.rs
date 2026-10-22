//! cargo-fuzz entry point for the `tines-core` node harness.
//!
//! Decodes arbitrary bytes into [`fuzz::Step`]s and drives the shared
//! [`fuzz::Harness`] — the same oracle the proptest twin in
//! `tines-core/tests/fuzz_twin.rs` runs, so any bug caught here is caught there
//! too (and vice versa).
#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let steps = fuzz::decode_steps(data);
    // Randomize the MTU per corpus entry so different runs probe different wire
    // bounds (the §7 "every sent frame ≤ mtu" check then catches clamps that
    // fail at a specific MTU). First byte seeds it; an empty input falls back
    // to 128.
    let mtu = 128usize + (data.first().copied().unwrap_or(0) as usize) % 512;
    if !steps.is_empty() {
        fuzz::Harness::new(mtu).run(&steps);
    }
});
