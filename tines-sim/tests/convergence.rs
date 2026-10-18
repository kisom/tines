//! Deterministic convergence scenarios for the `Sim` harness.
//!
//! Every scenario is built from a `WorldConfig` + `ContactTrace`, then stepped
//! with `Sim` and asserted against. Because loss/dup/seed are seeded and the
//! contact graph is fixed, the outcomes below are reproducible run to run.

use tines_core::LatestWins;
use tines_sim::{Contact, ContactTrace, Sim, WorldConfig};

/// Build the world for test 1: two nodes, a single `[0, 5000)` contact.
#[test]
fn two_nodes_one_contact_converge() {
    let cfg = WorldConfig {
        nodes: 2,
        mtu: 1500,
        loss: 0.0,
        dup: 0.0,
        seed: 0,
    };
    let mut trace = ContactTrace::default();
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 5000,
        a: 0,
        b: 1,
    });

    let mut sim = Sim::new(cfg, trace);
    for i in 0..10 {
        sim.observe(0, format!("n0-{}", i).as_bytes(), format!("p{}", i).as_bytes());
    }
    for i in 0..10 {
        sim.observe(1, format!("n1-{}", i).as_bytes(), format!("p{}", i).as_bytes());
    }
    sim.run_until(5000);

    assert!(sim.converged(&[0, 1]), "both nodes must hold every observation");
    assert_eq!(sim.id_set(0).len(), 20, "each node stores 10 + 10 obs");
    assert_eq!(sim.id_set(1).len(), 20);
}

/// Test 2: a node two hops away must still have the first node's observations
/// even though the two never met directly.
#[test]
fn relay_through_middle_node() {
    let cfg = WorldConfig {
        nodes: 3,
        mtu: 1500,
        loss: 0.0,
        dup: 0.0,
        seed: 0,
    };
    let mut trace = ContactTrace::default();
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 3000,
        a: 0,
        b: 1,
    });
    trace.contacts.push(Contact {
        start_ms: 4000,
        end_ms: 7000,
        a: 1,
        b: 2,
    });

    let mut sim = Sim::new(cfg, trace);
    for i in 0..5 {
        sim.observe(0, format!("a-{}", i).as_bytes(), format!("a-p{}", i).as_bytes());
    }
    sim.run_until(7000);

    let from_0 = sim.id_set(0);
    let node_2 = sim.id_set(2);
    // 0 and 2 never shared a contact, yet 2 received everything through 1.
    for id in &from_0 {
        assert!(node_2.contains(id), "node 2 is missing node 0's observation {id:?}");
    }
    assert_eq!(from_0.len(), 5, "node 0 observed 5 things");
}

/// Test 3: two isolated components each converge internally but the whole graph
/// does not, and the components are reported exactly.
#[test]
fn disconnected_component_does_not_converge() {
    let cfg = WorldConfig {
        nodes: 4,
        mtu: 1500,
        loss: 0.0,
        dup: 0.0,
        seed: 0,
    };
    let mut trace = ContactTrace::default();
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 5000,
        a: 0,
        b: 1,
    });
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 5000,
        a: 2,
        b: 3,
    });

    let mut sim = Sim::new(cfg, trace);
    sim.observe(0, b"comp-A", b"value-A");
    sim.observe(2, b"comp-B", b"value-B");
    sim.run_until(5000);

    assert_eq!(
        sim.components(),
        vec![vec![0, 1], vec![2, 3]],
        "the contact graph splits into two pairs"
    );
    assert!(!sim.converged(&[0, 1, 2, 3]), "cross-component never converges");
    assert!(sim.converged(&[0, 1]), "component A converges");
    assert!(sim.converged(&[2, 3]), "component B converges");
}

/// Test 4: a lossy, duplicate-prone link still converges given enough time.
#[test]
fn lossy_link_still_converges_given_time() {
    let cfg = WorldConfig {
        nodes: 2,
        mtu: 1500,
        loss: 0.3,
        dup: 0.1,
        seed: 7,
    };
    let mut trace = ContactTrace::default();
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 60_000,
        a: 0,
        b: 1,
    });

    let mut sim = Sim::new(cfg, trace);
    for i in 0..50 {
        sim.observe(0, format!("l0-{}", i).as_bytes(), format!("l0-p{}", i).as_bytes());
        sim.observe(1, format!("l1-{}", i).as_bytes(), format!("l1-p{}", i).as_bytes());
    }
    sim.run_until(60_000);

    assert!(
        sim.converged(&[0, 1]),
        "a lossy link converges once the summary-driven retransmits drain the queue"
    );
    assert_eq!(sim.id_set(0).len(), 100);
    assert_eq!(sim.id_set(1).len(), 100);
}

/// Test 5: the belief age tracks true elapsed wall-clock time across two hops.
///
/// The observation is made at `t=1000` and the contact graph is
/// `[0,3000)` between 0-1 and `[4000,7000)` between 1-2, so by `t=7000` node 2
/// should believe `x` is roughly `7000 - 1000 = 6000` ms old.
#[test]
fn age_tracks_true_elapsed_across_hops() {
    let cfg = WorldConfig {
        nodes: 3,
        mtu: 1500,
        loss: 0.0,
        dup: 0.0,
        seed: 0,
    };
    let mut trace = ContactTrace::default();
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 3000,
        a: 0,
        b: 1,
    });
    trace.contacts.push(Contact {
        start_ms: 4000,
        end_ms: 7000,
        a: 1,
        b: 2,
    });

    let mut sim = Sim::new(cfg, trace);
    sim.run_until(1000);
    sim.observe(0, b"x", b"hello");
    sim.run_until(7000);

    let belief = sim
        .nodes
        .get(2)
        .and_then(|n| n.belief(b"x", &LatestWins, 7000))
        .expect("node 2 should hold a belief for \"x\"");

    let age = belief.age_ms;
    assert!(
        (age as i64 - 6000).abs() <= 20,
        "age {age} must be within +/-{} ms of 6000 (true elapsed across two hops)",
        2 * sim.step_ms
    );
}

/// Test 6: payloads larger than the MTU are fragmented across frames and still
/// converge.
#[test]
fn large_payloads_fragment_over_small_mtu() {
    let cfg = WorldConfig {
        nodes: 2,
        mtu: 256,
        loss: 0.0,
        dup: 0.0,
        seed: 9,
    };
    let mut trace = ContactTrace::default();
    trace.contacts.push(Contact {
        start_ms: 0,
        end_ms: 20_000,
        a: 0,
        b: 1,
    });

    let mut sim = Sim::new(cfg, trace);
    for i in 0..20 {
        let topic0 = format!("n0-{}", i);
        let topic1 = format!("n1-{}", i);
        let payload = vec![0xABu8; 1024];
        sim.observe(0, topic0.as_bytes(), &payload);
        sim.observe(1, topic1.as_bytes(), &payload);
    }
    sim.run_until(20_000);

    assert!(
        sim.converged(&[0, 1]),
        "1 KiB payloads fragmented over a 256-byte MTU still converge"
    );
    assert_eq!(sim.id_set(0).len(), 40);
    assert_eq!(sim.id_set(1).len(), 40);
}
