//! Integration tests for tines-bt advertising and the transport's mismatch-drop
//! rule.
//!
//! Every test uses a mock [`Radio`] and never touches a real BLE radio, so the
//! suite runs on a CI box with no Bluetooth stack.

use std::collections::VecDeque;

use tines_bt::{adv, BtTransport, Radio, RadioError, RadioEvent};
use tines_core::observation::NodeId;
use tines_core::transport::{Event, Transport};

/// A canned [`Radio`] backed by queued events and a configurable MTU.
struct MockRadio {
    events: VecDeque<RadioEvent>,
    sent: Vec<(NodeId, Vec<u8>)>,
    mtu: usize,
}

impl MockRadio {
    fn new(mtu: usize) -> Self {
        MockRadio {
            events: VecDeque::new(),
            sent: Vec::new(),
            mtu,
        }
    }

    fn push(&mut self, ev: RadioEvent) {
        self.events.push_back(ev);
    }
}

impl Radio for MockRadio {
    fn poll(&mut self) -> Option<RadioEvent> {
        self.events.pop_front()
    }

    fn send(&mut self, _me: NodeId, to: &NodeId, data: &[u8]) -> Result<(), RadioError> {
        self.sent.push((*to, data.to_vec()));
        Ok(())
    }

    fn mtu(&self) -> usize {
        self.mtu
    }
}

#[test]
fn advertising_payload_fits_31_bytes() {
    let p = adv::advertise([7u8; 16]);
    assert_eq!(p.len(), adv::ADVERTISING_BUFFER_LEN);

    // First record is the flags AD: length 2, type 0x01, data 0x06.
    assert_eq!((p[0], p[1], p[2]), (2, 0x01, 0x06));

    // Second record is service data, 16-bit UUID: length 19, type 0x16.
    assert_eq!((p[3], p[4]), (19, 0x16));

    // Every meaningful byte lies within the legacy 31-byte advertising limit
    // (the 16-byte NodeId ends at byte 23; the rest is zero padding).
    let last = p.iter().rposition(|&b| b != 0).unwrap();
    assert_eq!(last, 22);
}

#[test]
fn advertising_payload_roundtrips_node_id() {
    let node: NodeId = [
        0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa,
        0xbb,
    ];
    let p = adv::advertise(node);
    assert_eq!(adv::decode_node_id(&p), Some(node));
}

#[test]
fn from_mismatch_is_dropped() {
    let me: NodeId = [1u8; 16];
    let peer: NodeId = [2u8; 16]; // the connection's advertised NodeId
    let spoof: NodeId = [9u8; 16]; // claimed source that does not match `peer`

    // A single spoofed frame: the transport drops it and emits nothing.
    let mut radio = MockRadio::new(23);
    radio.push(RadioEvent::Data {
        from: spoof,
        data: vec![0xCA, 0xFE],
    });
    let mut t = BtTransport::new(me, peer, radio);
    assert_eq!(
        t.poll(),
        None,
        "a frame from a non-advertised id yields no Event::Frame"
    );

    // A legitimate frame from `peer` is delivered unchanged.
    let mut good = MockRadio::new(23);
    good.push(RadioEvent::Data {
        from: peer,
        data: vec![0x01, 0x02],
    });
    let mut t = BtTransport::new(me, peer, good);
    match t.poll() {
        Some(Event::Frame(from, data)) => {
            assert_eq!(from, peer);
            assert_eq!(&data[..], &[0x01, 0x02]);
        }
        other => panic!("expected a Frame from {peer:?}, got {other:?}"),
    }

    // And a spoofed frame sitting before a legit one is simply skipped.
    let mut mixed = MockRadio::new(23);
    mixed.push(RadioEvent::Data {
        from: spoof,
        data: vec![0xFF],
    });
    mixed.push(RadioEvent::Data {
        from: peer,
        data: vec![0x03],
    });
    let mut t = BtTransport::new(me, peer, mixed);
    match t.poll() {
        Some(Event::Frame(from, data)) => {
            assert_eq!(from, peer);
            assert_eq!(&data[..], &[0x03]);
        }
        other => panic!("expected the Frame from {peer:?}, got {other:?}"),
    }
}

#[test]
fn smaller_node_id_is_central() {
    let a: NodeId = [0u8; 16];
    let b: NodeId = [1u8; 16];
    assert!(tines_bt::is_central(a, b));
    assert!(!tines_bt::is_central(b, a));
}

#[test]
fn mtu_is_clamped_to_max_frame() {
    // A radio that advertises an L2CAP-CoC-style MTU larger than MAX_FRAME must
    // still report MAX_FRAME to honour the Transport contract.
    let radio = MockRadio::new(4096);
    let t = BtTransport::new([1u8; 16], [2u8; 16], radio);
    assert_eq!(t.mtu(), tines_core::consts::MAX_FRAME);
}

#[test]
fn send_enforces_mtu() {
    let radio = MockRadio::new(23);
    let mut t = BtTransport::new([1u8; 16], [2u8; 16], radio);
    // A frame larger than the 23-byte MTU must be rejected as TooLarge.
    let big = vec![0u8; 24];
    assert_eq!(
        t.send(&[2u8; 16], &big),
        Err(tines_core::transport::SendError::TooLarge)
    );
}
