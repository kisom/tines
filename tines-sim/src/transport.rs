//! SimTransport: a `Transport` implementation over a shared `SimWorld`.
//!
//! `SimTransport` is a datagram pipe bound to one node (`me`). Outbound frames
//! are checked against the live contact graph and the world's MTU, then
//! subjected to a loss roll and — on success — a duplication roll. All of this
//! mutates the shared `SimWorld`'s per-node mailboxes and frame counters, so
//! every participant shares one view of the network.

use crate::world::SimWorld;
use rand::Rng;
use std::cell::RefCell;
use std::rc::Rc;
use tines_core::transport::{Event, FrameBuf, SendError, Transport};

/// A node's view of a shared `SimWorld`.
pub struct SimTransport {
    world: Rc<RefCell<SimWorld>>,
    me: usize,
}

impl SimTransport {
    /// Bind transport `me` to the shared `world`.
    pub fn new(world: Rc<RefCell<SimWorld>>, me: usize) -> Self {
        SimTransport { world, me }
    }
}

impl Transport for SimTransport {
    type Addr = usize;

    /// Pop the next queued event for this node, if any.
    fn poll(&mut self) -> Option<Event<Self::Addr>> {
        self.world.borrow_mut().inboxes[self.me].pop_front()
    }

    /// Attempt to deliver `frame` to `to`.
    ///
    /// Order of checks: reachability, then MTU, then the loss roll. A frame that
    /// survives loss is enqueued; a surviving frame is duplicated with the dup
    /// roll (counting each enqueue toward `frames_sent`).
    fn send(&mut self, to: &Self::Addr, frame: &[u8]) -> Result<(), SendError> {
        let mut w = self.world.borrow_mut();
        if !w.trace.in_contact(self.me, *to, w.prev_t) {
            return Err(SendError::Unreachable);
        }
        if frame.len() > w.cfg.mtu {
            return Err(SendError::TooLarge);
        }
        if w.rng.gen::<f64>() < w.cfg.loss {
            w.frames_dropped += 1;
            return Ok(());
        }
        let buf = FrameBuf::from_slice(frame).map_err(|_| SendError::TooLarge)?;
        let dup = w.rng.gen::<f64>() < w.cfg.dup;
        w.inboxes[*to].push_back(Event::Frame(self.me, buf.clone()));
        w.frames_sent += 1;
        if dup {
            w.inboxes[*to].push_back(Event::Frame(self.me, buf));
            w.frames_sent += 1;
        }
        Ok(())
    }

    /// Maximum frame size in bytes this transport can send.
    fn mtu(&self) -> usize {
        self.world.borrow().cfg.mtu
    }
}
