//! tines-ros2: a ROS2 front-end for the tines content-defined gossip stack.
//!
//! The crate splits cleanly in two:
//!
//! * **Pure, ROS2-free** (built by default): [`topic_name`] escapes an
//!   arbitrary tines topic into a valid ROS2 topic name under `/tines/`, and
//!   the tines-side wiring composes [`tines_core::Node`],
//!   [`tines_store::LogStore`], and [`tines_bt::BtTransport`] with no ROS2
//!   dependency. This is what builds and is tested without a ROS2 toolchain.
//!
//! * **rclrs-backed** (feature `ros2`): `TinesNode`, a self-contained ROS2 node
//!   that publishes each tines topic as a ROS2 topic and publishes confirmed
//!   deliveries to [`crate::RECEIPTS_TOPIC`] for [`tines_core::NodeEvent`]s, and
//!   subscribes for new observations. Compiled only behind
//!   `#[cfg(feature = "ros2")]` so the default build never needs rclrs.
//!
//! See [`check-ros2.sh`](../../scripts/check-ros2.sh) for building the rclrs
//! node against a ROS2 toolchain.
#![warn(missing_docs)]

pub mod topics;

/// ROS2 topic carrying confirmed [`tines_core::NodeEvent`]s (one per
/// broadcast a peer confirmed holding). Always the hex-escaped literal below.
pub const RECEIPTS_TOPIC: &[u8] = b"/tines/receipts";

pub use topics::topic_name;

#[cfg(feature = "ros2")]
pub mod node;

#[cfg(feature = "ros2")]
pub use node::TinesNode;
