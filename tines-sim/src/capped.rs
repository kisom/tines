//! A [`Store`] that delegates to a [`MemStore`] but rejects `insert` with
//! [`StoreError::Full`] once it has held `capacity` observations.
//!
//! `MemStore` is unbounded, so it never fills; this wrapper lets the simulator
//! exercise the full-store custody path (F1): a receiver whose store is full
//! must not ack a received observation, or custody is released for a message
//! nobody holds.

use tines_core::{MemStore, Observation, ObservationId, Store, StoreError, Tick, TrimPolicy};

/// A capacity-bounded [`Store`]: delegates to a [`MemStore`] but returns
/// `StoreError::Full` on `insert` once `capacity` observations are held.
pub struct CappedStore {
    capacity: usize,
    inner: MemStore,
}

impl CappedStore {
    /// A capped store holding at most `capacity` observations.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: MemStore::new(),
        }
    }
}

impl Default for CappedStore {
    fn default() -> Self {
        // Small enough that a node filling up on received observations rejects
        // later inserts, which is what drives the full-store ack path.
        Self::new(8)
    }
}

impl Store for CappedStore {
    fn insert(&mut self, obs: Observation, now: Tick) -> Result<bool, StoreError> {
        if self.inner.len() >= self.capacity {
            return Err(StoreError::Full);
        }
        self.inner.insert(obs, now)
    }

    fn contains(&self, id: &ObservationId) -> bool {
        self.inner.contains(id)
    }

    fn get(&self, id: &ObservationId, now: Tick) -> Option<Observation> {
        self.inner.get(id, now)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn for_each_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        self.inner.for_each_id(f)
    }

    fn for_each_broadcast_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        self.inner.for_each_broadcast_id(f)
    }

    fn for_each_addressed_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        self.inner.for_each_addressed_id(f)
    }

    fn release(&mut self, id: &ObservationId) {
        self.inner.release(id)
    }

    fn is_released(&self, id: &ObservationId) -> bool {
        self.inner.is_released(id)
    }

    fn for_each_topic(&self, prefix: &[u8], now: Tick, f: &mut dyn FnMut(&Observation)) {
        self.inner.for_each_topic(prefix, now, f)
    }

    fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize {
        self.inner.trim(policy, now)
    }
}
