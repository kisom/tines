#!/bin/sh
set -e
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo clippy --locked -p tines-core --no-default-features --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc --locked -p tines-core --no-deps --no-default-features
cargo test --locked --workspace
cargo test --locked -p tines-core --no-default-features
sh scripts/check-no-std.sh
