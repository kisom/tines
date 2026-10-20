//! World configuration and the shared `SimWorld`.
//!
//! `SimWorld` is the heart of the simulator: it owns the contact trace, a
//! per-node inbound mailbox, the last-advanced time, and a seeded RNG that
//! governs loss and duplication. It is always held behind `Rc<RefCell<..>>` so
//! that many `SimTransport`s can share a single world.

use crate::trace::ContactTrace;
use rand::{rngs::StdRng, SeedableRng};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tines_core::transport::{Event, FrameBuf};

/// World configuration for a `SimWorld`.
#[derive(Clone, Debug)]
pub struct WorldConfig {
    pub nodes: usize,
    pub mtu: usize,
    pub loss: f64,
    pub dup: f64,
    pub seed: u64,
}

/// A shared simulation of `nodes` connected by a `ContactTrace`.
///
/// Every node owns an inbound mailbox of `Event`s. `advance` walks every
/// unordered pair, emitting `Contact`/`Lost` where the contact graph changed
/// since the last advance. `SimTransport` uses the same mailboxes and the same
/// RNG (held here) to deliver and drop frames.
pub struct SimWorld {
    pub(crate) cfg: WorldConfig,
    pub(crate) trace: ContactTrace,
    pub(crate) inboxes: Vec<VecDeque<Event<usize>>>,
    pub(crate) prev_t: u64,
    /// Whether the world has been advanced at least once. Until the first
    /// `advance`, contact windows that are already active at t=0 have no
    /// transition to fire on, so the first advance emits a `Contact` for every
    /// pair that is already in contact (a contact "alive" at sim start is
    /// treated as having just made contact).
    pub(crate) primed: bool,
    pub(crate) rng: StdRng,
    pub(crate) frames_sent: u64,
    pub(crate) frames_dropped: u64,
}

impl SimWorld {
    /// Build a world with empty mailboxes, `prev_t = 0`, and a seeded RNG.
    pub fn new(cfg: WorldConfig, trace: ContactTrace) -> Rc<RefCell<SimWorld>> {
        let seed = cfg.seed;
        let inboxes = vec![VecDeque::new(); cfg.nodes];
        let world = SimWorld {
            cfg,
            trace,
            inboxes,
            prev_t: 0,
            primed: false,
            rng: StdRng::seed_from_u64(seed),
            frames_sent: 0,
            frames_dropped: 0,
        };
        Rc::new(RefCell::new(world))
    }

    /// Advance the world to time `t`, emitting `Contact`/`Lost` for every pair
    /// whose contact state changed since the previous advance.
    ///
    /// On the very first advance, every pair already in contact is seeded with
    /// a `Contact`: a contact window that predates the first advance has no
    /// transition to react to, so without this the peers would never be added.
    pub fn advance(&mut self, t: u64) {
        if !self.primed {
            self.primed = true;
            for a in 0..self.cfg.nodes {
                for b in (a + 1)..self.cfg.nodes {
                    if self.trace.in_contact(a, b, self.prev_t) {
                        self.inboxes[a].push_back(Event::Contact(b));
                        self.inboxes[b].push_back(Event::Contact(a));
                    }
                }
            }
        }
        for a in 0..self.cfg.nodes {
            for b in (a + 1)..self.cfg.nodes {
                let old = self.trace.in_contact(a, b, self.prev_t);
                let new = self.trace.in_contact(a, b, t);
                if !old && new {
                    self.inboxes[a].push_back(Event::Contact(b));
                    self.inboxes[b].push_back(Event::Contact(a));
                } else if old && !new {
                    self.inboxes[a].push_back(Event::Lost(b));
                    self.inboxes[b].push_back(Event::Lost(a));
                }
            }
        }
        self.prev_t = t;
    }

    /// Total frames successfully enqueued (duplication counts each enqueue).
    pub fn frames_sent(&self) -> u64 {
        self.frames_sent
    }

    /// Total frames dropped by the loss roll-out.
    pub fn frames_dropped(&self) -> u64 {
        self.frames_dropped
    }

    /// Inject a raw inbound `Frame` into `node`'s mailbox without going through
    /// the transport's reachability, MTU, or loss checks.
    ///
    /// A test seam: it lets tests feed arbitrary or malformed byte strings
    /// straight to a node's exchange, which is where the "never panic on
    /// untrusted wire input" guarantee lives. The frame is not validated here —
    /// a malformed frame is dropped by the receiving
    /// [`Node`](tines_core::Node) rather than stored.
    pub fn inject_frame(&mut self, node: usize, peer: usize, bytes: FrameBuf) {
        self.inboxes[node].push_back(Event::Frame(peer, bytes));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{SimWorld, WorldConfig};
    use crate::trace::{Contact, ContactTrace};
    use crate::transport::SimTransport;
    use std::cell::RefCell;
    use std::rc::Rc;
    use tines_core::transport::{Event, SendError, Transport};

    /// A 2-node world where nodes 0 and 1 are in contact during
    /// `[start, end)`, with the given loss/dup/mtu/seed.
    fn two_node_world(
        mtu: usize,
        loss: f64,
        dup: f64,
        seed: u64,
        start: u64,
        end: u64,
    ) -> Rc<RefCell<SimWorld>> {
        let cfg = WorldConfig {
            nodes: 2,
            mtu,
            loss,
            dup,
            seed,
        };
        let mut trace = ContactTrace::default();
        trace.contacts.push(Contact {
            start_ms: start,
            end_ms: end,
            a: 0,
            b: 1,
        });
        SimWorld::new(cfg, trace)
    }

    /// A snapshot of everything currently queued in `node`'s inbox.
    fn inbox(world: &Rc<RefCell<SimWorld>>, node: usize) -> Vec<Event<usize>> {
        world.borrow().inboxes[node].iter().cloned().collect()
    }

    /// The number of `Frame` events queued in `node`'s inbox.
    fn frame_count(world: &Rc<RefCell<SimWorld>>, node: usize) -> usize {
        world.borrow().inboxes[node]
            .iter()
            .filter(|e| matches!(e, Event::Frame(_, _)))
            .count()
    }

    #[test]
    fn advance_emits_contact_and_lost_at_window_edges() {
        let world = two_node_world(1500, 0.0, 0.0, 42, 100, 200);

        // Before the window opens, neither mailbox has anything.
        world.borrow_mut().advance(50);
        assert_eq!(inbox(&world, 0), Vec::<Event<usize>>::new());
        assert_eq!(inbox(&world, 1), Vec::<Event<usize>>::new());

        // At the start edge both peers learn of the contact.
        world.borrow_mut().advance(100);
        assert_eq!(inbox(&world, 0), vec![Event::Contact(1)]);
        assert_eq!(inbox(&world, 1), vec![Event::Contact(0)]);

        // At the end edge both peers learn of the loss.
        world.borrow_mut().advance(200);
        assert_eq!(inbox(&world, 0), vec![Event::Contact(1), Event::Lost(1)]);
        assert_eq!(inbox(&world, 1), vec![Event::Contact(0), Event::Lost(0)]);
    }

    #[test]
    fn send_outside_contact_is_unreachable() {
        let cfg = WorldConfig {
            nodes: 2,
            mtu: 1500,
            loss: 0.0,
            dup: 0.0,
            seed: 42,
        };
        let world = SimWorld::new(cfg, ContactTrace::default());
        let mut t0 = SimTransport::new(world.clone(), 0);
        assert_eq!(t0.send(&1, &[1, 2, 3]), Err(SendError::Unreachable));
    }

    #[test]
    fn send_over_mtu_is_too_large() {
        let world = two_node_world(8, 0.0, 0.0, 42, 0, 1000);
        let mut t0 = SimTransport::new(world.clone(), 0);
        assert_eq!(t0.send(&1, &[0u8; 9]), Err(SendError::TooLarge));
    }

    #[test]
    fn loss_1_drops_everything() {
        let world = two_node_world(1500, 1.0, 0.0, 42, 0, 1000);
        let mut t0 = SimTransport::new(world.clone(), 0);
        for _ in 0..100 {
            t0.send(&1, &[1, 2, 3]).unwrap();
        }
        assert_eq!(frame_count(&world, 1), 0);
        assert_eq!(world.borrow().frames_dropped(), 100);
    }

    #[test]
    fn dup_1_doubles_everything() {
        let world = two_node_world(1500, 0.0, 1.0, 42, 0, 1000);
        let mut t0 = SimTransport::new(world.clone(), 0);
        for _ in 0..10 {
            t0.send(&1, &[1, 2, 3]).unwrap();
        }
        assert_eq!(frame_count(&world, 1), 20);
    }

    #[test]
    fn same_seed_same_drops() {
        let cfg = WorldConfig {
            nodes: 2,
            mtu: 1500,
            loss: 0.5,
            dup: 0.0,
            seed: 1234,
        };
        let make_trace = || {
            let mut trace = ContactTrace::default();
            trace.contacts.push(Contact {
                start_ms: 0,
                end_ms: 1000,
                a: 0,
                b: 1,
            });
            trace
        };
        let w1 = SimWorld::new(cfg.clone(), make_trace());
        let w2 = SimWorld::new(cfg.clone(), make_trace());
        let mut t1 = SimTransport::new(w1.clone(), 0);
        let mut t2 = SimTransport::new(w2.clone(), 0);
        for _ in 0..50 {
            t1.send(&1, &[1]).unwrap();
            t2.send(&1, &[1]).unwrap();
        }
        assert_eq!(w1.borrow().frames_dropped(), w2.borrow().frames_dropped());
    }
}
