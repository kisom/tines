#!/bin/sh
set -e
target=thumbv7em-none-eabihf
if ! rustup target list --installed | grep -qx "$target"; then
    rustup target add "$target"
fi
cargo build --locked -p tines-core --no-default-features --target "$target"
