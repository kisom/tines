//! ROS2 topic naming for tines.
//!
//! tines topics are arbitrary byte strings up to [`tines_core::consts::MAX_TOPIC`], which
//! may contain characters ROS2 cannot put directly in a topic name (`/`,
//! control bytes, non-ASCII, …). [`topic_name`] hex-escapes the bytes into a
//! deterministic, injective name under the `/tines/` namespace, so every tines
//! topic maps to exactly one valid ROS2 topic and back.

use core::fmt::Write;

/// The namespace every hex-escaped tines topic lives under.
const TINES_PREFIX: &str = "/tines/";

/// Escapes a tines topic into a valid ROS2 topic name under `/tines/`.
///
/// The raw topic bytes are rendered as lowercase hex; the result is the
/// `TINES_PREFIX` (`"/tines/"`) followed by that hex. Encoding is
/// **deterministic** (same bytes always produce the same name) and
/// **injective** (distinct byte strings never collide), because the hex map
/// between byte strings and even-length hex strings is a bijection. The empty
/// topic therefore maps to just `/tines/`.
///
/// ```
/// assert_eq!(tines_ros2::topic_name(b"door/3"), "/tines/646f6f722f33");
/// ```
pub fn topic_name(tines_topic: &[u8]) -> String {
    let mut name = String::with_capacity(TINES_PREFIX.len() + tines_topic.len() * 2);
    name.push_str(TINES_PREFIX);
    for &byte in tines_topic {
        // `write!` into a `String` never errors, so the `Err` arm is unreachable.
        let _ = write!(name, "{byte:02x}");
    }
    name
}
