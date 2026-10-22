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

    /// The store's durability point.
    ///
    /// A flush does two things:
    /// - everything inserted, released or trimmed so far becomes durable;
    /// - `now` becomes the time anchor for ages after a restart, so a
    ///   persistent backend can count the time between this flush and the
    ///   restart into each age.
    ///
    /// More of the contract:
    /// - Flushing twice is harmless.
    /// - A `now` earlier than the last anchor is clamped to it.
    /// - A flush doesn't change any age [`Self::get`] reports at `now` or
    ///   later, evict anything, or undo a release.
    /// - A failure returns [`StoreError::Backend`] and leaves the store
    ///   usable. What wasn't made durable is at risk only if the process
    ///   then stops before a later flush succeeds.
    /// - The application decides when to flush; core never calls `flush`
    ///   itself. On flash, every flush costs wear.
    ///
    /// The default does nothing and returns `Ok(())`, which is correct for a
    /// store that holds nothing across a restart, such as `MemStore`.
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

/// A [`Store`] that holds data across power-off and can take trusted wall
/// time from the host (spec §9, decision H).
///
/// A store that has been powered off can't tell how long it was off from
/// ticks alone, which restart on every boot. A backend that implements this
/// trait holds back what it loaded at start-up (its *quarantine*: not
/// offered, delivered or acked, but still deduped) until the host has
/// trusted wall time, then folds the downtime into those ages. Core never
/// reads or interprets a wall time: this trait only carries the host's value
/// to the backend, and the backend's estimate back to the host. Hosts reach
/// it through [`Node::set_wall_clock`](crate::Node::set_wall_clock) and
/// [`Node::peer_time_estimate`](crate::Node::peer_time_estimate), so no
/// `&mut Store` escapes the facade. Only backends that hold data across
/// power-off implement it; `MemStore` doesn't.
pub trait WallClockStore: Store {
    /// The backend's summary of what a fold did, for the host's logs.
    type FoldReport;

    /// Fold trusted wall time into the store: `wall_now` is the host's
    /// trusted wall time in ms since the Unix epoch, and `now` the current
    /// tick. Lifts the quarantine on what the store held at start-up.
    ///
    /// The fold runs once, when the host first has trusted time. A second
    /// call is a no-op that returns the first fold's report.
    fn set_wall_clock(&mut self, wall_now: u64, now: Tick) -> Self::FoldReport;

    /// An estimate of the current wall time (ms since the Unix epoch) from
    /// peers' observations stored since start-up that carry `observed_at`,
    /// or `None` if there are none yet.
    ///
    /// Peer time is unauthenticated until 1.1: a peer can claim any
    /// `observed_at`, so the host bounds the estimate before it passes it to
    /// [`Self::set_wall_clock`].
    fn peer_time_estimate(&self, now: Tick) -> Option<u64>;
}
