//! Addressed (relayed) delivery scenarios for the `Sim` harness.
//!
//! Unlike the broadcast convergence scenarios in `convergence.rs`, addressed
//! observations diverge by design: the recipient holds a tasking that the
//! carriers no longer need once they have forwarded it. These tests exercise
//! the relay path — an [`Sim::observe_to`] observation handed off hop-by-hop
//! along a contact chain — without asserting whole-graph convergence.
//!
//! * [`custody_chain_delivers_without_carrier_failure`] — an addressed task
//!   relayed hop-by-hop along an A→B→C chain reaches the recipient, and the
//!   custody chain releases at every hop: a carrier that gets an ack stops
//!   forwarding the id.
//! * [`released_duplicate_not_acked`] — a carrier that released custody on the
//!   recipient's ack reconnects with the recipient without pulling the
//!   delivered task back out of custody, so the message never vanishes.

use tines_core::{NodeId, Store};
use tines_sim::{Contact, ContactTrace, Sim, WorldConfig};

/// The recipient id. `Sim` assigns node `i` the id `[i as u8; 16]`, so node 2's
/// id is `[2; 16]` and the task in both tests below is addressed to it.
const RECIPIENT: NodeId = [2; 16];

/// The relay contact trace shared by both tests: `(0,1)` during `[0, 5000)`,
/// then `(1,2)` during `[10_000, 15_000)`. Node 0 and node 2 never share a
/// contact, so a task must be relayed through node 1 to reach node 2.
fn relay_trace() -> ContactTrace {
    let mut trace = ContactTrace::default();
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 5_000,
        a: 0,
        b: 1,
    });
    trace.contacts.push(Contact {
        start_ms: 10_000,
        end_ms: 15_000,
        a: 1,
        b: 2,
    });
    trace
}

/// The task observed at `t = 0` reaches the far end of the chain even though
/// node 0 and node 2 never met; it is relayed through node 1.
#[test]
fn relayed_task_reaches_recipient() {
    let trace = relay_trace();
    let end = trace.end();
    let cfg = WorldConfig {
        nodes: 3,
        mtu: 1500,
        loss: 0.0,
        dup: 0.0,
        seed: 0,
    };
    let mut sim = Sim::new(cfg, trace);
    let id = sim.observe_to(0, RECIPIENT, 60_000, b"task", b"investigate");
    sim.run_until(end);

    assert!(
        sim.nodes[2].store().contains(&id),
        "node 2 must hold the relayed task"
    );
}

/// An addressed task with a short lifetime never reaches the recipient: the
/// carrier drops it once its age passes the lifetime, so node 2 never stores
/// it after running well past expiry.
#[test]
fn carrier_drops_expired() {
    let trace = relay_trace();
    let end = trace.end();
    let cfg = WorldConfig {
        nodes: 3,
        mtu: 1500,
        loss: 0.0,
        dup: 0.0,
        seed: 0,
    };
    let mut sim = Sim::new(cfg, trace);
    let id = sim.observe_to(0, RECIPIENT, 10, b"task", b"investigate");
    sim.run_until(end + 10_000);

    assert!(
        !sim.nodes[2].store().contains(&id),
        "an addressed task past its lifetime must not survive at the recipient"
    );
}

/// An addressed task relayed hop-by-hop along a connected A→B→C contact chain
/// reaches the far end, and the custody chain releases at every hop: a carrier
/// that receives an ack stops forwarding the id. Neither hop fails — the
/// message lands with the recipient and no carrier keeps holding a copy it has
/// already handed off (the "carrier failure" the custody chain prevents).
///
/// The trace is the shared relay trace: `(0,1)` during `[0, 5000)`, then
/// `(1,2)` during `[10_000, 15_000)`, so the task must hop through node 1.
#[test]
fn custody_chain_delivers_without_carrier_failure() {
    let trace = relay_trace();
    let end = trace.end();
    let cfg = WorldConfig {
        nodes: 3,
        mtu: 1500,
        loss: 0.0,
        dup: 0.0,
        seed: 0,
    };
    let mut sim = Sim::new(cfg, trace);
    // Node 0 addresses the task to node 2, the far end of the chain.
    let id = sim.observe_to(0, RECIPIENT, 600_000, b"task", b"investigate");
    // Relay it down the chain (0→1 in the first window, 1→2 in the second)
    // and run a little past the end so node 1 can collect node 2's ack and
    // release before the last contact closes.
    sim.run_until(end + 2_000);

    // The recipient holds the delivered task, un-released.
    let c = sim.nodes[2].store();
    assert!(
        c.contains(&id) && !c.is_released(&id),
        "node 2 must hold the addressed task after the relay"
    );

    // Every carrier that relayed it still holds the copy but has released it
    // on the ack, so it no longer forwards the id (a released store is not
    // re-offered).
    for carrier in &sim.nodes[0..2] {
        let s = carrier.store();
        assert!(
            s.contains(&id) && s.is_released(&id),
            "carrier node {} must still hold the task but released",
            carrier.id()[0]
        );
    }
}

/// A carrier and its recipient both hold an addressed task mid-path; the
/// carrier releases custody once the recipient acks (the custody chain). When
/// the pair reconnects, the delivered task must not be lost: the recipient
/// keeps it, so custody never vanishes ("released-with-nobody-holding").
///
/// Two A↔B windows let the same pair reconnect after the first exchange: the
/// release happened over the first contact, the reconnect over the second.
#[test]
fn released_duplicate_not_acked() {
    let mut trace = ContactTrace::default();
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 5_000,
        a: 0,
        b: 1,
    });
    trace.contacts.push(Contact {
        start_ms: 10_000,
        end_ms: 15_000,
        a: 0,
        b: 1,
    });
    let end = trace.end();
    let cfg = WorldConfig {
        nodes: 2,
        mtu: 1500,
        loss: 0.0,
        dup: 0.0,
        seed: 0,
    };
    let mut sim = Sim::new(cfg, trace);
    // Node 0 addresses a task to node 1.
    let id = sim.observe_to(0, RECIPIENT, 600_000, b"task", b"investigate");
    // First window: node 1 receives it and acks, so node 0 releases custody on
    // the ack and node 1 is left holding the delivered task.
    sim.run_until(5_000);
    assert!(
        sim.nodes[0].store().is_released(&id),
        "the carrier must release custody on the recipient's ack"
    );
    assert!(
        sim.nodes[1].store().contains(&id),
        "the recipient must hold the task after delivery"
    );

    // Reconnect the pair and run the second window.
    sim.run_until(end + 2_000);

    // The task must stay with the recipient: reconnecting a released carrier
    // must not pull the delivered copy back out of custody, so at least one
    // node still holds it.
    let r = sim.nodes[1].store();
    assert!(
        r.contains(&id) && !r.is_released(&id),
        "the recipient must keep the delivered task after reconnecting"
    );
}
