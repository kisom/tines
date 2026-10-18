//! Addressed (relayed) delivery scenarios for the `Sim` harness.
//!
//! Unlike the broadcast convergence scenarios in `convergence.rs`, addressed
//! observations diverge by design: the recipient holds a tasking that the
//! carriers no longer need once they have forwarded it. These tests exercise
//! the relay path — an [`Sim::observe_to`] observation handed off hop-by-hop
//! along a contact chain — without asserting whole-graph convergence.

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
