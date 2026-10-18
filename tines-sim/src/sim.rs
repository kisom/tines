//! Sim: a reference-counted harness over a shared `SimWorld`.
//!
//! `Sim` drives the world and its `Node`s as a discrete time-step machine. Each
//! [`Sim::step`] advances the clock, asks the world to emit any `Contact`/`Lost`
//! events, and polls every node once. Peers discover each other through those
//! events and exchange summaries; the harness exposes convenience views over the
//! resulting state (the per-node id set, global convergence, and the components
//! of the contact graph) for building deterministic convergence scenarios.

use crate::trace::ContactTrace;
use crate::transport::SimTransport;
use crate::world::{SimWorld, WorldConfig};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use tines_core::{MemStore, Node, NodeId, ObservationId, Store};

/// A single node as seen through the simulation transport.
pub type SimNode = Node<MemStore, SimTransport>;

/// A run of the simulation, owned by the test.
pub struct Sim {
    pub world: Rc<RefCell<SimWorld>>,
    pub nodes: Vec<SimNode>,
    pub t: u64,
    pub step_ms: u64,
}

impl Sim {
    pub fn new(cfg: WorldConfig, trace: ContactTrace) -> Sim {
        let n = cfg.nodes;
        let world = SimWorld::new(cfg, trace);
        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            nodes.push(Node::new(
                [i as u8; 16],
                0,
                MemStore::new(),
                SimTransport::new(world.clone(), i),
            ));
        }
        Sim {
            world,
            nodes,
            t: 0,
            step_ms: 10,
        }
    }

    pub fn step(&mut self) {
        self.t += self.step_ms;
        self.world.borrow_mut().advance(self.t);
        for node in &mut self.nodes {
            node.poll(self.t);
        }
    }

    pub fn run_until(&mut self, t_end: u64) {
        while self.t < t_end {
            self.step();
        }
    }

    pub fn observe(&mut self, node: usize, topic: &[u8], payload: &[u8]) -> ObservationId {
        self.nodes[node]
            .observe(topic, payload, None, self.t)
            .expect("observe")
    }

    pub fn observe_to(
        &mut self,
        node: usize,
        to: NodeId,
        lifetime_ms: u64,
        topic: &[u8],
        payload: &[u8],
    ) -> ObservationId {
        self.nodes[node]
            .observe_to(to, lifetime_ms, topic, payload, None, self.t)
            .expect("observe_to")
    }

    pub fn id_set(&self, node: usize) -> BTreeSet<ObservationId> {
        let mut set = BTreeSet::new();
        self.nodes[node].store().for_each_id(&mut |id| {
            set.insert(*id);
        });
        set
    }

    pub fn converged(&self, nodes: &[usize]) -> bool {
        if nodes.is_empty() {
            return true;
        }
        let first = self.id_set(nodes[0]);
        nodes.iter().all(|&i| self.id_set(i) == first)
    }

    pub fn components(&self) -> Vec<Vec<usize>> {
        let (n, contacts) = {
            let world = self.world.borrow();
            (
                world.cfg.nodes,
                world
                    .trace
                    .contacts
                    .iter()
                    .map(|c| (c.a, c.b))
                    .collect::<Vec<_>>(),
            )
        };

        let mut parent: Vec<usize> = (0..n).collect();
        for (a, b) in contacts {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                parent[ra] = rb;
            }
        }

        let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for node in 0..n {
            let root = find(&mut parent, node);
            groups.entry(root).or_default().push(node);
        }

        let mut result: Vec<Vec<usize>> = groups.into_values().collect();
        for group in &mut result {
            group.sort_unstable();
        }
        result.sort_by_key(|g| g[0]);
        result
    }
}

/// Iterative union-find root with path compression.
fn find(parent: &mut [usize], x: usize) -> usize {
    let mut root = x;
    while parent[root] != root {
        root = parent[root];
    }
    let mut cur = x;
    while parent[cur] != root {
        let next = parent[cur];
        parent[cur] = root;
        cur = next;
    }
    root
}
