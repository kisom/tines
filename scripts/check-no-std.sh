#!/bin/sh
set -e
rustup target add thumbv7em-none-eabihf
cargo build -p tines-core --no-default-features --target thumbv7em-none-eabihf
