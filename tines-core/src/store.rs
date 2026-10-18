//! Shared-memory observation store.
//!
//! This module defines the [`Store`] trait plus its [`TrimPolicy`] and
//! [`StoreError`] types. Everything here is `#![no_std]`-clean and does **not**
//! require the `alloc` feature — only concrete backends (such as `mem_store`,
//! which is compiled only with `alloc`) need allocation, so the trait, policy,
//! and error types remain available on `no_std` targets as well.

use crate::observation::{Observation, ObservationId, Tick};

/// Ejection-policy knobs consumed by [`Store::trim`].
///
/// Rules are applied in the order listed here: per-topic retention, then a
/// global age cutoff, then a global count cap. Any field left as `None` is
/// ignored.
#[derive(Clone, Debug, Default)]
pub struct TrimPolicy {
    /// Keep the `N` youngest (lowest current age) observations per exact topic.
    pub keep_per_topic: Option<usize>,
    /// Drop any observation whose current age (in ms) exceeds this value.
    pub max_age_ms: Option<u64>,
    /// Drop the oldest observations until the total count is at most this value.
    pub max_count: Option<usize>,
}

/// Errors returned by [`Store`] operations.
#[derive(Debug, PartialEq, Eq)]
pub enum StoreError {
    /// The store is full and cannot accept new observations.
    Full,
    /// A backend-internal failure.
    Backend,
}

/// A content-addressed, age-aware collection of [`Observation`]s.
///
/// Identity is the content-derived [`ObservationId`]; two observations that
/// differ only in age or `observed_at` share an id and dedupe on insert. Ages
/// are reported as *current* ages, advanced by the elapsed ticks since each
/// observation was stored.
#[allow(clippy::len_without_is_empty)]
pub trait Store {
    /// Insert an observation.
    ///
    /// Returns `Ok(true)` if it was newly inserted, or `Ok(false)` if an
    /// observation with the same id already exists (in which case the existing
    /// entry — and its stored age lineage — is left untouched).
    fn insert(&mut self, obs: Observation, now: Tick) -> Result<bool, StoreError>;

    /// Returns `true` if an observation with `id` is currently stored.
    fn contains(&self, id: &ObservationId) -> bool;

    /// Returns the observation with `id`, with `age_ms` advanced by
    /// `now - stored_at` (saturating at 0 elapsed when `now < stored_at`).
    fn get(&self, id: &ObservationId, now: Tick) -> Option<Observation>;

    /// Number of observations currently stored.
    fn len(&self) -> usize;

    /// Visit every stored observation id.
    fn for_each_id(&self, f: &mut dyn FnMut(&ObservationId));

    /// Visit the ids of observations that are not addressed to anyone
    /// (`to == None`), i.e. broadcasts. Order is unspecified.
    fn for_each_broadcast_id(&self, f: &mut dyn FnMut(&ObservationId));

    /// Visit the ids of observations addressed to someone (`to == Some`).
    /// Order is unspecified.
    ///
    /// The default visits every id, which is correct since callers still
    /// check `to`; backends that can filter cheaply should override it so the
    /// exchange does not read every broadcast on each poll.
    fn for_each_addressed_id(&self, f: &mut dyn FnMut(&ObservationId)) {
        self.for_each_id(f)
    }

    /// Mark the observation with `id` as released: it has been sent and must
    /// not be offered for exchange again.
    ///
    /// Releasing an id that is not currently stored is a no-op. The mark
    /// persists for the lifetime of the entry (until it is trimmed); a
    /// re-inserted observation starts un-released.
    fn release(&mut self, id: &ObservationId);

    /// Returns `true` if the observation with `id` has been released.
    fn is_released(&self, id: &ObservationId) -> bool;

    /// Visit every stored observation whose topic starts with `prefix`
    /// (an empty prefix matches all), each age-adjusted to `now`.
    fn for_each_topic(&self, prefix: &[u8], now: Tick, f: &mut dyn FnMut(&Observation));

    /// Apply [`TrimPolicy`] rules in order (per-topic, age, count) and return
    /// the number of observations removed. Trimmed ids are not tombstoned:
    /// re-inserting them later is accepted.
    fn trim(&mut self, policy: &TrimPolicy, now: Tick) -> usize;
}

/// Current (age-adjusted) age of a stored observation at tick `now`.
#[inline]
#[cfg(feature = "alloc")]
pub(crate) fn current_age(obs: &Observation, stored_at: Tick, now: Tick) -> u64 {
    obs.age_ms.saturating_add(now.saturating_sub(stored_at))
}
