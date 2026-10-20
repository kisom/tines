//! Integration tests for the tines-bt transport, covering the F3 properties:
//! the `from`/`Addr` check, `Contact`/`Lost`, multi-peer filtering, NodeId
//! binding (UUID-checked advertisement + inbound-address lookup), and the
//! central-peripheral role decision.
//!
//! Every test uses a mock [`Radio`] and never touches a real BLE radio, so the
//! suite runs on a CI box with no Bluetooth stack.

use std::collections::{BTreeSet, HashMap, VecDeque};

use tines_bt::{adv, resolve_bound, BtTransport, Radio, RadioError, RadioEvent};
use tines_core::observation::NodeId;
use tines_core::transport::{Event, Transport};

use bluer::{Address, AddressType, Uuid, UuidExt};

/// A canned [`Radio`] backed by queued events and a configurable MTU.
struct MockRadio {
    events: VecDeque<RadioEvent>,
    _sent: Vec<(NodeId, Vec<u8>)>,
    mtu: usize,
}

impl MockRadio {
    fn new(mtu: usize) -> Self {
        MockRadio {
            events: VecDeque::new(),
            _sent: Vec::new(),
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
        self._sent.push((*to, data.to_vec()));
        Ok(())
    }

    fn mtu(&self) -> usize {
        self.mtu
    }
}

impl Drop for MockRadio {
    fn drop(&mut self) {
        // Drain the queue so a caller that only polls a couple of times does not
        // leave events stranded behind the dead drop.
        self.events.clear();
    }
}

/// Build a minimal frame: one leading tag byte, then the 16-byte `from`.
/// `frame_sender` reads `from` from bytes 1..17, so this matches the wire.
fn frame(from: NodeId) -> Vec<u8> {
    let mut b = vec![0u8; 1 + 16];
    b[1..1 + 16].copy_from_slice(&from);
    b
}

#[test]
fn from_check_and_peer_filter_drop_frames() {
    let me: NodeId = [1u8; 16];
    let peer: NodeId = [2u8; 16]; // the connection's advertised NodeId (bound Addr)
    let spoof: NodeId = [9u8; 16]; // a declared sender that is not the bound peer
    let other: NodeId = [3u8; 16]; // a different bound peer entirely

    // A clean frame from `peer` is delivered unchanged.
    let mut good = MockRadio::new(23);
    good.push(RadioEvent::Data {
        from: peer,
        data: frame(peer),
    });
    let mut t = BtTransport::new(me, peer, good);
    match t.poll() {
        Some(Event::Frame(from, data)) => {
            assert_eq!(from, peer);
            assert_eq!(&data[..], &frame(peer)[..]);
        }
        other => panic!("expected a Frame from {peer:?}, got {other:?}"),
    }

    // From-check violation: the frame's declared `from` (in the event) is the
    // bound peer, but the frame's actual `from` bytes say `spoof`. This is an
    // address-spoofing violation — dropped via the peeked `frame_sender`.
    let mut spoofed = MockRadio::new(23);
    spoofed.push(RadioEvent::Data {
        from: peer,
        data: frame(spoof),
    });
    let mut t = BtTransport::new(me, peer, spoofed);
    assert_eq!(
        t.poll(),
        None,
        "a frame whose decoded `from` differs from the bound Addr is dropped"
    );

    // Peer filter: a frame tagged as coming from a *different* bound peer is
    // dropped (a multi-peer radio emits events for every connection; the
    // transport only surfaces its own bound peer).
    let mut other_peer = MockRadio::new(23);
    other_peer.push(RadioEvent::Data {
        from: other,
        data: frame(other),
    });
    let mut t = BtTransport::new(me, peer, other_peer);
    assert_eq!(t.poll(), None, "a frame from a non-bound peer is dropped");

    // A spoofed frame sitting before a good one is simply skipped; the good one
    // is delivered.
    let mut mixed = MockRadio::new(23);
    mixed.push(RadioEvent::Data {
        from: peer,
        data: frame(spoof),
    });
    mixed.push(RadioEvent::Data {
        from: peer,
        data: frame(peer),
    });
    let mut t = BtTransport::new(me, peer, mixed);
    match t.poll() {
        Some(Event::Frame(from, data)) => {
            assert_eq!(from, peer);
            assert_eq!(&data[..], &frame(peer)[..]);
        }
        other => panic!("expected the clean Frame from {peer:?}, got {other:?}"),
    }
}

#[test]
fn contact_and_lost_emitted() {
    let me: NodeId = [1u8; 16];
    let peer: NodeId = [2u8; 16];

    let mut radio = MockRadio::new(23);
    radio.push(RadioEvent::Contact { addr: peer });
    radio.push(RadioEvent::Data {
        from: peer,
        data: frame(peer),
    });
    radio.push(RadioEvent::Lost { addr: peer });

    let mut t = BtTransport::new(me, peer, radio);

    assert_eq!(t.poll(), Some(Event::Contact(peer)), "up → Contact");
    assert!(matches!(t.poll(), Some(Event::Frame(..))), "a frame passes");
    assert_eq!(t.poll(), Some(Event::Lost(peer)), "end → Lost");
    assert_eq!(t.poll(), None, "no more events once the connection is gone");
}

#[test]
fn frame_boundaries_preserved_at_transport() {
    // Two back-to-back frames arrive as two separate Data events; the
    // transport must surface them as two separate Frames, never merged.
    let me: NodeId = [1u8; 16];
    let peer: NodeId = [2u8; 16];
    let frame_a = frame(peer);
    let mut frame_b = frame(peer);
    frame_b[0] = 0xFF; // differ from frame_a via the tag byte, so `from` stays == peer
    let a_expected = frame_a.clone();
    let b_expected = frame_b.clone();

    let mut radio = MockRadio::new(23);
    radio.push(RadioEvent::Data {
        from: peer,
        data: frame_a,
    });
    radio.push(RadioEvent::Data {
        from: peer,
        data: frame_b,
    });

    let mut t = BtTransport::new(me, peer, radio);

    match t.poll() {
        Some(Event::Frame(_, a)) => assert_eq!(&a[..], &a_expected[..]),
        other => panic!("expected the first frame, got {other:?}"),
    }
    match t.poll() {
        Some(Event::Frame(_, b)) => assert_eq!(&b[..], &b_expected[..]),
        other => panic!("expected the second frame, got {other:?}"),
    }
    assert_eq!(t.poll(), None);
}

#[test]
fn is_central_by_node_id_order() {
    let a: NodeId = [0u8; 16];
    let b: NodeId = [1u8; 16];
    let big: NodeId = [255u8; 16];
    assert!(tines_bt::is_central(a, b)); // smaller dials
    assert!(!tines_bt::is_central(b, a));
    assert!(tines_bt::is_central(b, big));
    assert!(!tines_bt::is_central(big, b));
}

#[test]
fn resolve_bound_rejects_unknown_inbound() {
    // An empty scan map: no address is eligible, so an inbound from an unknown
    // address binds to nothing and must be rejected.
    let empty: HashMap<Address, (NodeId, AddressType)> = HashMap::new();
    let unknown = Address::new([0xAA; 6]);
    assert_eq!(resolve_bound(&empty, &unknown), None);

    // A peer we discovered and bound: its own address resolves.
    let node: NodeId = [4u8; 16];
    let mut map = HashMap::new();
    map.insert(Address::new([0xBB; 6]), (node, AddressType::LePublic));
    let addr = Address::new([0xBB; 6]);
    assert_eq!(resolve_bound(&map, &addr), Some(node));
}

#[test]
fn decode_ad_node_id_requires_tines_uuid() {
    let me: NodeId = [7u8; 16];
    let node: NodeId = [5u8; 16];
    let allow: BTreeSet<NodeId> = BTreeSet::new(); // empty = any tines advertiser

    // An EIR-less service-data blob (wrong UUID) is rejected.
    let wrong_uuid: HashMap<Uuid, Vec<u8>> =
        HashMap::from([(Uuid::from_u16(0x180F), node.to_vec())]);
    assert_eq!(adv::decode_ad_node_id(&wrong_uuid, me, &allow), None);

    // No service data at all is rejected.
    let empty: HashMap<Uuid, Vec<u8>> = HashMap::new();
    assert_eq!(adv::decode_ad_node_id(&empty, me, &allow), None);

    // The tines UUID with a valid NodeId is decoded.
    let good: HashMap<Uuid, Vec<u8>> =
        HashMap::from([(Uuid::from_u16(adv::SERVICE_UUID), node.to_vec())]);
    assert_eq!(adv::decode_ad_node_id(&good, me, &allow), Some(node));
}

#[test]
fn decode_ad_node_id_honours_allowlist() {
    let me: NodeId = [7u8; 16];
    let allowed: NodeId = [5u8; 16];
    let denied: NodeId = [6u8; 16];
    let service: HashMap<Uuid, Vec<u8>> =
        HashMap::from([(Uuid::from_u16(adv::SERVICE_UUID), allowed.to_vec())]);

    // Allowlist containing `allowed`: the advertiser is eligible.
    let mut allow = BTreeSet::new();
    allow.insert(allowed);
    assert_eq!(adv::decode_ad_node_id(&service, me, &allow), Some(allowed));

    // Allowlist not containing this advertiser: rejected.
    let deny: BTreeSet<NodeId> = BTreeSet::from([denied]);
    assert_eq!(adv::decode_ad_node_id(&service, me, &deny), None);
}
