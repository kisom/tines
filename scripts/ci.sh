#!/bin/sh
set -e
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
cargo test --workspace
sh scripts/check-no-std.sh
