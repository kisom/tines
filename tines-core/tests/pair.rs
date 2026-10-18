mod common;
use common::PairTransport;

use std::collections::BTreeSet;
use tines_core::observation::Payload;
use tines_core::transport::{Event, FrameBuf, SendError, Transport};
use tines_core::{consts, LatestWins, MemStore, Node, ObserveError, Store};

#[test]
fn pair_transport_connect_then_frame() {
    let (mut a, mut b) = PairTransport::pair(1500);
    a.connect();
    assert_eq!(a.poll(), Some(Event::Contact(1)));
    a.send(&1, b"hi").unwrap();
    assert_eq!(
        b.poll(),
        Some(Event::Frame(0, FrameBuf::from_slice(b"hi").unwrap()))
    );
}

#[test]
fn observe_assigns_increasing_seq() {
    let (a, _b) = PairTransport::pair(1500);
    let mut node = Node::new([1u8; 16], 10, MemStore::new(), a);
    node.observe(b"door/1", b"open", None, 0).unwrap();
    node.observe(b"door/2", b"closed", None, 0).unwrap();
    // next_seq advances by one per observe(): first_seq + 2.
    assert_eq!(node.next_seq(), 12);
}

#[test]
fn observe_rejects_oversized_topic_and_payload() {
    let (a, _b) = PairTransport::pair(1500);
    let mut node = Node::new([1u8; 16], 0, MemStore::new(), a);

    // Review Focus 2: oversized topic and payload are rejected whole.
    let oversized_topic = vec![0u8; consts::MAX_TOPIC + 1];
    assert_eq!(
        node.observe(&oversized_topic, b"ok", None, 0),
        Err(ObserveError::TooLarge)
    );

    let oversized_payload = vec![0u8; consts::MAX_PAYLOAD + 1];
    assert_eq!(
        node.observe(b"ok", &oversized_payload, None, 0),
        Err(ObserveError::TooLarge)
    );

    // Neither was stored; the store is unchanged, nothing truncated.
    assert_eq!(node.store().len(), 0);
}

#[test]
fn two_nodes_converge_over_pair_transport() {
    let (a, b) = PairTransport::pair(1500);
    let mut na = Node::new([1u8; 16], 0, MemStore::new(), a);
    let mut nb = Node::new([2u8; 16], 0, MemStore::new(), b);

    na.observe(b"door/1", b"open", None, 0).unwrap();
    na.observe(b"door/2", b"closed", None, 0).unwrap();
    na.observe(b"bat/1", b"up", None, 0).unwrap();
    nb.observe(b"door/3", b"open", None, 0).unwrap();
    nb.observe(b"bat/2", b"down", None, 0).unwrap();

    na.transport_mut().connect();
    nb.transport_mut().connect();

    let mut t = 0u64;
    for _ in 0..1000 {
        na.poll(t);
        nb.poll(t);
        t += 10;
    }

    assert_eq!(na.store().len(), 5);
    assert_eq!(nb.store().len(), 5);

    let mut ids_a = BTreeSet::new();
    na.store().for_each_id(&mut |id: &[u8; 16]| {
        ids_a.insert(*id);
    });
    let mut ids_b = BTreeSet::new();
    nb.store().for_each_id(&mut |id: &[u8; 16]| {
        ids_b.insert(*id);
    });
    assert_eq!(ids_a, ids_b, "both stores must hold identical id sets");
}

#[test]
fn belief_latest_wins_across_nodes() {
    let (a, b) = PairTransport::pair(1500);
    let mut na = Node::new([1u8; 16], 0, MemStore::new(), a);
    let mut nb = Node::new([2u8; 16], 0, MemStore::new(), b);

    // Same topic "door", different observers. a is older, b is younger.
    na.observe(b"door", b"early", Some(0), 0).unwrap();
    nb.observe(b"door", b"late", Some(500), 500).unwrap();

    na.transport_mut().connect();
    nb.transport_mut().connect();

    let mut t = 0u64;
    for _ in 0..1000 {
        na.poll(t);
        nb.poll(t);
        t += 10;
    }

    let now = 100_000u64;
    // LatestWins must pick b's younger observation on both nodes. Local
    // observe stores at `now` (b's stored_at=500, a's stored_at=0), so b's
    // is younger everywhere it is held; a re-received copy has a slightly
    // different age, so compare by identity, not byte-equality.
    let belief = nb
        .belief(b"door", &LatestWins, now)
        .expect("door belief exists");
    assert_eq!(
        belief.observer, [2; 16],
        "nb belief should be b's observation"
    );
    assert_eq!(belief.seq, 0);
    assert_eq!(belief.payload, Payload::from_slice(b"late").unwrap());
    assert_eq!(belief.observed_at, Some(500));

    // a's node, looking at the same topic, agrees on the same observation.
    let belief_a = na.belief(b"door", &LatestWins, now).unwrap();
    assert_eq!(
        belief_a.observer, [2; 16],
        "na belief should be b's observation"
    );
    assert_eq!(belief_a.payload, Payload::from_slice(b"late").unwrap());
    assert_eq!(belief_a.observed_at, Some(500));
}

#[test]
fn cut_off_mid_exchange_leaves_both_valid_and_resumes() {
    let (a, b) = PairTransport::pair(1500);
    let mut na = Node::new([1u8; 16], 0, MemStore::new(), a);
    let mut nb = Node::new([2u8; 16], 0, MemStore::new(), b);

    // a publishes 200 observations with 1 KiB payloads each.
    let big = vec![0u8; 1024];
    for _ in 0..200 {
        na.observe(b"big", &big, None, 0).unwrap();
    }

    na.transport_mut().connect();
    nb.transport_mut().connect();

    let mut t = 0u64;
    for _ in 0..20 {
        na.poll(t);
        nb.poll(t);
        t += 10;
    }

    // Cut the link mid-exchange, then reconnect.
    na.transport_mut().disconnect();
    nb.transport_mut().disconnect();
    na.transport_mut().connect();
    nb.transport_mut().connect();

    let mut done = false;
    for _ in 0..20000 {
        na.poll(t);
        nb.poll(t);
        t += 10;
        if nb.store().len() == 200 {
            done = true;
            break;
        }
    }
    assert!(done, "exchange must resume after reconnect");
    assert_eq!(nb.store().len(), 200);
}

#[test]
fn third_party_observation_propagates_within_contact() {
    let (a, b) = PairTransport::pair(1500);
    let mut na = Node::new([1u8; 16], 0, MemStore::new(), a);
    let mut nb = Node::new([2u8; 16], 0, MemStore::new(), b);

    na.transport_mut().connect();
    nb.transport_mut().connect();

    let mut t = 0u64;
    for _ in 0..1000 {
        na.poll(t);
        nb.poll(t);
        t += 10;
    }
    // Converged (both empty), still in contact.

    // Inject a fresh observation into a.
    let id = na.observe(b"fresh", b"hi", None, t).unwrap();

    // Within ~SUMMARY_INTERVAL_MS*2 of polling, b must have received it.
    for _ in 0..400 {
        na.poll(t);
        nb.poll(t);
        t += 10;
    }
    assert!(
        nb.store().contains(&id),
        "fresh observation must propagate to b"
    );
    assert_eq!(nb.store().len(), 1);
}

#[test]
fn pair_transport_disconnect_enqueues_lost() {
    let (mut a, _b) = PairTransport::pair(1500);
    a.disconnect();
    assert_eq!(a.poll(), Some(Event::Lost(1)));
}

#[test]
fn pair_transport_send_rejects_unreachable() {
    let (mut a, _b) = PairTransport::pair(1500);
    // a's only peer is 1; sending to 2 is unreachable.
    assert_eq!(a.send(&2, b"hi"), Err(SendError::Unreachable));
}
