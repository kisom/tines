#!/bin/sh
set -e
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo clippy --locked -p tines-core --no-default-features --all-targets -- -D warnings
# The backend conformance suite lives behind the `conformance` feature so it
# stays out of the default/std builds, but every backend crate still runs it.
cargo clippy --locked -p tines-core --features conformance --all-targets -- -D warnings
cargo clippy --locked -p tines-store --features conformance --all-targets -- -D warnings
cargo clippy --locked -p tines-sim --features conformance --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc --locked -p tines-core --no-deps --no-default-features
cargo test --locked --workspace
cargo test --locked -p tines-core --features conformance
cargo test --locked -p tines-store --features conformance
cargo test --locked -p tines-sim --features conformance
cargo test --locked -p tines-core --no-default-features
sh scripts/check-no-std.sh
