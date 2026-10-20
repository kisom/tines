//! Integration tests for tines-bt advertising and the transport's mismatch-drop
//! rule.
//!
//! Every test uses a mock [`Radio`] and never touches a real BLE radio, so the
//! suite runs on a CI box with no Bluetooth stack.

use std::collections::VecDeque;

use tines_bt::{adv, BtTransport, Radio, RadioError, RadioEvent};
use tines_core::consts::WIRE_VERSION;
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

/// A framed envelope: a `WIRE_VERSION` byte, a variant tag, then the 16-byte
/// `from`. This is the layout `frame_sender` peeks — the transport delivers this
/// whole envelope to core (which decodes it), so the `from` must be stamped with
/// the sender's id.
fn frame(from: NodeId, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(2 + 16 + payload.len());
    b.push(WIRE_VERSION); // wire version byte
    b.push(0u8); // postcard externally-tagged variant tag
    b.extend_from_slice(&from);
    b.extend_from_slice(payload);
    b
}

#[test]
fn from_mismatch_is_dropped() {
    let me: NodeId = [1u8; 16];
    let peer: NodeId = [2u8; 16]; // the connection's advertised NodeId (bound Addr)
    let spoof: NodeId = [9u8; 16]; // a declared sender that does not match `peer`

    // A legitimate frame from `peer` (envelope stamped with `peer`) is delivered
    // unchanged, envelope included — core decodes the whole thing.
    let mut good = MockRadio::new(23);
    good.push(RadioEvent::Data {
        from: peer,
        data: frame(peer, &[0x01, 0x02]),
    });
    let mut t = BtTransport::new(me, peer, good);
    match t.poll() {
        Some(Event::Frame(from, data)) => {
            assert_eq!(from, peer);
            assert_eq!(&data[..], &frame(peer, &[0x01, 0x02])[..]);
        }
        other => panic!("expected a Frame from {peer:?}, got {other:?}"),
    }

    // A frame whose envelope `from` (peeked by frame_sender) is `spoof`, not the
    // bound peer, is dropped by the from-check — even though the event claims the
    // bound peer as its source.
    let mut mismatched = MockRadio::new(23);
    mismatched.push(RadioEvent::Data {
        from: peer,
        data: frame(spoof, &[0xCA, 0xFE]),
    });
    let mut t = BtTransport::new(me, peer, mismatched);
    assert_eq!(
        t.poll(),
        None,
        "a frame whose decoded `from` differs from the bound Addr is dropped"
    );

    // And a mismatched frame sitting before a legit one is simply skipped; the
    // legit one is delivered.
    let mut mixed = MockRadio::new(23);
    mixed.push(RadioEvent::Data {
        from: peer,
        data: frame(spoof, &[0xFF]),
    });
    mixed.push(RadioEvent::Data {
        from: peer,
        data: frame(peer, &[0x03]),
    });
    let mut t = BtTransport::new(me, peer, mixed);
    match t.poll() {
        Some(Event::Frame(from, data)) => {
            assert_eq!(from, peer);
            assert_eq!(&data[..], &frame(peer, &[0x03])[..]);
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
