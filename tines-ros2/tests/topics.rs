//! Integration tests for [`tines_ros2::topic_name`] (pure, default features).

use tines_core::consts::MAX_TOPIC;
use tines_ros2::topic_name;

/// Every arbitrary input — control bytes, high/non-ASCII bytes, slashes, and
/// even the empty topic — escapes into a valid ROS2 topic name under
/// `/tines/` consisting solely of lowercase hex digits.
#[test]
fn topic_name_escapes() {
    let inputs: [&[u8]; 5] = [
        b"door/3".as_ref(),
        b"".as_ref(),
        [0u8, 1, 2, 250, 128, 255].as_ref(),
        "café — naïve ✓".as_bytes(),
        &vec![7u8; MAX_TOPIC][..],
    ];

    for input in inputs {
        let name = topic_name(input);
        // Valid ROS2 topic names are absolute (start with `/`) and contain no
        // illegal characters; here the tail is pure hex, so both hold.
        assert!(
            name.starts_with("/tines/"),
            "topic must live under the /tines/ namespace: {name}"
        );
        let hex = &name["/tines/".len()..];
        assert_eq!(
            hex.len(),
            input.len() * 2,
            "each byte must become exactly two hex digits"
        );
        // Each byte maps to exactly its two lowercase hex digits.
        for (byte, pair) in input.iter().zip(hex.as_bytes().chunks(2)) {
            assert_eq!(
                pair,
                format!("{byte:02x}").into_bytes(),
                "byte {byte:02x} must escape to its lowercase hex {byte:02x}"
            );
        }
    }
}

/// Encoding is deterministic and injective across the full range of topic
/// lengths up to [`MAX_TOPIC`]: distinct byte strings never collide.
#[test]
fn topic_name_roundtrip() {
    // Deterministic: identical input yields identical output.
    assert_eq!(topic_name(b"door/3"), topic_name(b"door/3"));

    // Injective: every distinct-length input maps to a distinct name. Hex of
    // distinct byte strings is distinct, and the shared `/tines/` prefix cannot
    // cause a cross-length collision (the hex tail has length `2 * len`).
    for len in 0..=MAX_TOPIC {
        // A prefix of distinct bytes: `len == a` yields a name never seen at any
        // other `len`, and distinct `len` values cannot collide.
        let input: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        assert!(topic_name(&input).starts_with("/tines/"));
    }

    // Cross-check: distinct byte strings of *equal* length never collide.
    let mut names = std::collections::BTreeSet::new();
    for seed in 0..=255u8 {
        let input: Vec<u8> = (0..MAX_TOPIC).map(|i| seed.wrapping_add(i as u8)).collect();
        let name = topic_name(&input);
        assert!(name.starts_with("/tines/"));
        assert!(
            names.insert(name),
            "injectivity violated: seed {seed} collided"
        );
    }

    assert_eq!(names.len(), 256, "256 distinct equal-length names expected");
}
