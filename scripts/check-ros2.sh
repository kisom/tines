#!/usr/bin/env bash
#
# Build the rclrs-backed ROS2 node for the `tines-ros2` crate.
#
# This compiles the `ros2` feature, which pulls in `rclrs` and its dependency
# on the ROS2 C libraries. It therefore REQUIRES a ROS2 toolchain installed
# (a sourced `colcon`/`/opt/ros/<distro>/setup.bash` and `rclrs` on the
# registry); it is NOT run in CI.
#
# The default (ROS2-free) build and tests — including `topic_name` escaping —
# are exercised in CI via `cargo test --workspace` and need no ROS2 toolchain.
#
# Usage:
#   source /opt/ros/<distro>/setup.bash   # any ROS2 distro rclrs supports
#   ./scripts/check-ros2.sh
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Building tines-ros2 with the ros2 feature (requires a ROS2 toolchain)..."
cargo build -p tines-ros2 --features ros2

echo "OK: tines-ros2 (ros2 feature) built against ROS2."
