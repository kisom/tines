//! Shared, test-only helper: two `PairTransport`s wired back-to-back as if they
//! were peers on a datagram link. A `send` on one side surfaces as a `Frame`
//! on the other; `connect`/`disconnect` enqueue `Contact`/`Lost` on the
//! caller's own queue only.
//!
//! Each side owns its own inbound queue (`inbox`) and holds a shared handle to
//! the other side's queue (`peer_inbox`), so outbound frames land in the peer's
//! queue — just like frames crossing a real link. `std` is fine here; this is a
//! test fixture, not `tines-core` proper.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use tines_core::transport::{Event, FrameBuf, SendError, Transport};

/// A pair of in-memory transports connected back-to-back for tests.
///
/// Addresses are `0` and `1`; the first handle handed back by [`PairTransport::pair`]
/// is `0`, the second is `1`.
pub struct PairTransport {
    /// Ourselves: `0` or `1`.
    me: u8,
    /// Address of the peer this side talks to.
    other: u8,
    /// Our own inbound queue.
    inbox: Rc<RefCell<VecDeque<Event<u8>>>>,
    /// The peer's inbound queue, where our `send`s land.
    peer_inbox: Rc<RefCell<VecDeque<Event<u8>>>>,
    /// Configured MTU.
    mtu: usize,
}

impl PairTransport {
    /// Build a connected pair. Returns `(addr 0, addr 1)`.
    pub fn pair(mtu: usize) -> (PairTransport, PairTransport) {
        let inbox0: Rc<RefCell<VecDeque<Event<u8>>>> = Rc::new(RefCell::new(VecDeque::new()));
        let inbox1: Rc<RefCell<VecDeque<Event<u8>>>> = Rc::new(RefCell::new(VecDeque::new()));

        let a = PairTransport {
            me: 0,
            other: 1,
            inbox: inbox0.clone(),
            peer_inbox: inbox1.clone(),
            mtu,
        };
        let b = PairTransport {
            me: 1,
            other: 0,
            inbox: inbox1.clone(),
            peer_inbox: inbox0.clone(),
            mtu,
        };
        (a, b)
    }

    /// Announce that the peer is reachable.
    ///
    /// A real contact is symmetric: when two peers meet, each independently
    /// observes it and calls `connect()` on its own transport, so this enqueues
    /// `Contact(other)` on *this* side's queue only. (Broadcasting onto the
    /// peer's queue as well would crowd out the peer's own `Contact` and any
    /// in-flight frames — the paired peer learns of the contact via its own
    /// `connect()`, as the test in `tests/pair.rs` exercises.)
    pub fn connect(&mut self) {
        let other = self.other;
        self.inbox.borrow_mut().push_back(Event::Contact(other));
    }

    /// Announce that the peer is unreachable.
    #[allow(dead_code)] // required by the transport interface; used by sim harness
    pub fn disconnect(&mut self) {
        let other = self.other;
        self.inbox.borrow_mut().push_back(Event::Lost(other));
    }
}

impl Transport for PairTransport {
    type Addr = u8;

    fn poll(&mut self) -> Option<Event<Self::Addr>> {
        self.inbox.borrow_mut().pop_front()
    }

    fn send(&mut self, to: &Self::Addr, frame: &[u8]) -> Result<(), SendError> {
        if *to != self.other {
            return Err(SendError::Unreachable);
        }
        let buf = FrameBuf::from_slice(frame).map_err(|_| SendError::TooLarge)?;
        let event = Event::Frame(self.me, buf);
        self.peer_inbox.borrow_mut().push_back(event);
        Ok(())
    }

    fn mtu(&self) -> usize {
        self.mtu
    }
}
