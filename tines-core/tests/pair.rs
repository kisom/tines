mod common;
use common::PairTransport;

use tines_core::transport::{Event, FrameBuf, Transport};

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
