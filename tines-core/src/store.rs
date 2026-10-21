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
///
/// `#[non_exhaustive]`: build via [`TrimPolicy::default`] and set fields, so a
/// future 1.x can add policy knobs without a breaking change.
#[non_exhaustive]
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
#[non_exhaustive]
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

    /// `true` if the store holds no observations.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Visit every stored observation id.
    fn for_each_id(&self, f: &mut dyn FnMut(&ObservationId));

    /// Visit the ids of observations that are not addressed to anyone
    /// (`dest` is `None`), i.e. broadcasts. Order is unspecified.
    fn for_each_broadcast_id(&self, f: &mut dyn FnMut(&ObservationId));

    /// Visit the ids of observations addressed to someone (`dest` is `Some`).
    /// Order is unspecified.
    ///
    /// The default visits every id, which is correct since callers still
    /// check `dest`; backends that can filter cheaply should override it so the
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
    ///
    /// Release is best-effort and infallible: it never returns an error, and a
    /// failed release is, at worst, a duplicate custodian on the next
    /// exchange — never a lost message.
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

    /// Best-effort durability flush, advancing each stored observation's age
    /// up to the caller's `now` and recording `now` as the flush clock.
    ///
    /// Flush must not lose messages, must never resurrect a released message,
    /// and is infallible by contract: a failed flush is, at worst, a
    /// duplicate custodian on the next exchange — never a lost message.
    /// Backends that cannot persist more cheaply default to a no-op that
    /// simply returns `Ok(())`.
    fn flush(&mut self, now: Tick) -> Result<(), StoreError> {
        let _ = now;
        Ok(())
    }
}

/// Current (age-adjusted) age of a stored observation at tick `now`.
#[inline]
#[cfg(feature = "alloc")]
pub(crate) fn current_age(obs: &Observation, stored_at: Tick, now: Tick) -> u64 {
    obs.age_ms.saturating_add(now.saturating_sub(stored_at))
}

/// A [`Store`] that a host can feed trusted wall-clock time into.
///
/// tines-core itself never deals with wall time — this trait is a host-facing
/// hook so a host can lift the quarantine on observations held across a
/// power-off, and estimate trusted time from a peer's observation, without a
/// general `&mut Store` backdoor (decision F in the spec). Only backends that
/// support a wall-clock fold implement it; `MemStore` and other pure-tick
/// stores do not.
pub trait WallClockStore: Store {
    /// Summary of a wall-clock fold, for the host's logs.
    type FoldReport;

    /// Fold trusted wall time into the store, lifting the quarantine on held
    /// observations. The host calls this exactly once, on the first trusted
    /// time.
    fn set_wall_clock(&mut self, wall_now: u64, now: Tick) -> Self::FoldReport;

    /// Best estimate of trusted wall time from a peer's observation stored
    /// since the last open, if any. `None` means stay quarantined.
    fn peer_time_estimate(&self, now: Tick) -> Option<u64>;
}
