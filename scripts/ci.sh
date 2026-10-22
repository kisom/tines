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
# The std-free conformance suite also runs with `--no-default-features`, so the
# no_std + conformance combination is actually exercised (not just by inspection).
cargo test --locked -p tines-core --no-default-features --features conformance
sh scripts/check-no-std.sh
# The fuzz crate sits outside the workspace and only *runs* on nightly
# (cargo-fuzz, see .github/workflows/fuzz.yml), but it must keep resolving and
# compiling on the pinned stable toolchain, or the scheduled job rots unseen.
cargo fmt --manifest-path fuzz/Cargo.toml -- --check
cargo clippy --locked --manifest-path fuzz/Cargo.toml --all-targets -- -D warnings
# Post-1.0.0 release gate: every 1.x change to tines-core must stay
# semver-compatible with the published 1.0.0. Skipped where the tool is not
# installed (`cargo install cargo-semver-checks --locked`); the GitHub Actions
# `ci` job installs a prebuilt binary so the gate is enforced there.
if command -v cargo-semver-checks >/dev/null 2>&1; then
    cargo semver-checks check-release -p tines-core --baseline-version 1.0.0
fi
