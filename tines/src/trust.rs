//! Trusted wall time for the hosts (spec §9, decisions G and J).
//!
//! A host opens its store before it knows the wall time, and holds what was
//! already in the log in quarantine until it adopts a trusted wall time. The
//! time comes, in order, from:
//!
//! 1. the robot's own trusted source (the service's `trusted_clock = "system"`,
//!    or `TinesNode::trust_system_clock` in tines-ros2), read by
//!    [`system_wall_ms`];
//! 2. otherwise, a peer's observation, through `Node::peer_time_estimate`.
//!    [`PeerTimeWindow`] collects estimates for a short window from the first
//!    plausible one and adopts the latest (decision J);
//! 3. otherwise the host stays quarantined; the robot's own fail-safe is
//!    outside tines.
//!
//! Every candidate passes [`is_plausible_wall_ms`] first (1.0 review, B3): a
//! forged `observed_at = u64::MAX`, or a board that booted before NTP and reads
//! 1970, is never adopted. [`adopt_wall_clock`] is the one place a host commits
//! to a wall time: it folds the store, anchors the [`HostClock`], and flushes so
//! a clock marker carrying the wall time reaches disk (review S5).
//!
//! The time is adopted once per run (decision J): neither path corrects it
//! while the host runs.

use tines_core::{Node, Tick, Transport};
use tines_store::log::FoldReport;
use tines_store::LogStore;

use crate::HostClock;

/// The earliest wall time a host will adopt: 2026-01-01T00:00:00Z, in ms since
/// the Unix epoch.
///
/// No tines host predates 2026, so an earlier reading is a clock that was
/// never set (a board without an RTC reads near 1970 until NTP syncs) or a
/// forged peer `observed_at`. Adopting it would stamp observations near 1970,
/// and peers would adopt those stamps in turn.
pub const WALL_FLOOR_MS: u64 = 1_767_225_600_000;

/// The latest wall time a host will adopt: [`WALL_FLOOR_MS`] plus 100 years
/// (36 525 days), about 2126-01-01.
///
/// A time ahead of the truth is the safe direction (spec §9: the fold
/// over-counts downtime, so things expire early), so the ceiling is not there
/// to catch a modestly fast clock. It catches values that can only be forged
/// or corrupt, such as `u64::MAX`, which would otherwise overflow every
/// `wall + elapsed` downstream. A century is beyond any deployment's life and
/// still far below where `u64` milliseconds overflow.
pub const WALL_CEILING_MS: u64 = WALL_FLOOR_MS + 36_525 * 24 * 60 * 60 * 1000;

/// The default peer-time window: how long a host keeps collecting peer
/// estimates after the first plausible one before adopting the latest
/// (decision J, about 10 s).
pub const DEFAULT_PEER_TIME_WINDOW_MS: u64 = 10_000;

/// Whether `wall_ms` (ms since the Unix epoch) lies within
/// [`WALL_FLOOR_MS`]`..=`[`WALL_CEILING_MS`], so a host may adopt it.
pub fn is_plausible_wall_ms(wall_ms: u64) -> bool {
    (WALL_FLOOR_MS..=WALL_CEILING_MS).contains(&wall_ms)
}

/// The system clock in ms since the Unix epoch, if it is plausible.
///
/// `None` when the system clock reads before the epoch or outside
/// [`is_plausible_wall_ms`] — typically a board that booted before its clock
/// was synced. The caller should then log it and fall back to the peer path,
/// never anchor 0 (review S6).
pub fn system_wall_ms() -> Option<u64> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let since = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let ms = u64::try_from(since.as_millis()).ok()?;
    is_plausible_wall_ms(ms).then_some(ms)
}

/// Adopt `wall` as the trusted wall time at tick `now`, once.
///
/// Runs the store's fold (`Node::set_wall_clock`), anchors `clock`, and flushes
/// the store so a marker carrying the wall time reaches disk right away (review
/// S5). Returns the fold report, or `None` — changing nothing — if `clock` is
/// already trusted or `wall` isn't plausible ([`is_plausible_wall_ms`]).
///
/// A failed flush is logged to stderr, not returned: trust is already
/// established in memory, and the next scheduled flush retries.
pub fn adopt_wall_clock<T>(
    node: &mut Node<LogStore, T>,
    clock: &mut HostClock,
    wall: u64,
    now: Tick,
) -> Option<FoldReport>
where
    T: Transport,
{
    if clock.is_trusted() || !is_plausible_wall_ms(wall) {
        return None;
    }
    let report = node.set_wall_clock(wall, now);
    clock.anchor_wall(wall, now);
    if let Err(e) = node.flush(now) {
        eprintln!("tines: store flush after adopting wall time failed: {e:?}");
    }
    Some(report)
}

/// Decision J's peer-time window.
///
/// Feed it the store's current estimate on every poll while the host is
/// untrusted ([`offer`](Self::offer)). The first plausible estimate opens the
/// window; while it is open the window keeps the latest plausible estimate,
/// advanced by local ticks so every candidate is compared at the same instant.
/// When the window has run its length it yields that estimate exactly once,
/// for the host to adopt with [`adopt_wall_clock`].
///
/// Implausible estimates ([`is_plausible_wall_ms`]) are ignored: they neither
/// open the window nor compete in it.
#[derive(Debug, Clone)]
pub struct PeerTimeWindow {
    /// How long the window stays open after the first plausible estimate.
    window_ms: u64,
    /// The tick of the first plausible estimate; `None` until one arrives.
    opened_at: Option<Tick>,
    /// The latest plausible estimate so far, and the tick it refers to.
    best: Option<(u64, Tick)>,
    /// Set once the window has yielded its estimate.
    done: bool,
}

impl PeerTimeWindow {
    /// A window that stays open `window_ms` after the first plausible
    /// estimate. `0` adopts the first plausible estimate immediately.
    pub fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            opened_at: None,
            best: None,
            done: false,
        }
    }

    /// Offer the store's current estimate (`None` if it has none) at tick
    /// `now`. Returns the estimate to adopt, valued at `now`, once the window
    /// has run its length; `None` before then and on every call after.
    pub fn offer(&mut self, estimate: Option<u64>, now: Tick) -> Option<u64> {
        if self.done {
            return None;
        }
        // Advance the held best to `now`, so it compares fairly with an
        // estimate the store values at `now`. A tick that went backwards
        // advances nothing.
        let held = self
            .best
            .map(|(wall, at)| wall.saturating_add(now.saturating_sub(at)));
        let fresh = estimate.filter(|&e| is_plausible_wall_ms(e));
        let best = match (held, fresh) {
            (Some(h), Some(f)) => h.max(f),
            (h, f) => h.or(f)?,
        };
        self.best = Some((best, now.max(self.best.map_or(now, |(_, at)| at))));
        let opened_at = *self.opened_at.get_or_insert(now);
        if now.saturating_sub(opened_at) < self.window_ms {
            return None;
        }
        self.done = true;
        // Advancing by ticks can carry a held estimate past the ceiling only
        // after a century of uptime; check anyway rather than adopt it.
        is_plausible_wall_ms(best).then_some(best)
    }

    /// Whether the window has opened (a plausible estimate has arrived) but
    /// not yet yielded.
    pub fn is_open(&self) -> bool {
        self.opened_at.is_some() && !self.done
    }
}

impl Default for PeerTimeWindow {
    fn default() -> Self {
        Self::new(DEFAULT_PEER_TIME_WINDOW_MS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const W: u64 = 1_800_000_000_000; // 2027-01-15, inside the bounds

    #[test]
    fn bounds_reject_epoch_and_overflow_values() {
        assert!(!is_plausible_wall_ms(0));
        assert!(!is_plausible_wall_ms(1_700_000_000_000)); // 2023
        assert!(!is_plausible_wall_ms(WALL_FLOOR_MS - 1));
        assert!(is_plausible_wall_ms(WALL_FLOOR_MS));
        assert!(is_plausible_wall_ms(W));
        assert!(is_plausible_wall_ms(WALL_CEILING_MS));
        assert!(!is_plausible_wall_ms(WALL_CEILING_MS + 1));
        assert!(!is_plausible_wall_ms(u64::MAX));
    }

    #[test]
    fn floor_is_2026_01_01_utc() {
        // 56 years from 1970, 14 of them leap years (1972..=2024).
        assert_eq!(WALL_FLOOR_MS, (56 * 365 + 14) * 86_400_000);
    }

    #[test]
    fn window_waits_then_adopts_latest() {
        let mut w = PeerTimeWindow::new(1_000);
        assert_eq!(w.offer(None, 0), None);
        assert!(!w.is_open());
        assert_eq!(
            w.offer(Some(W), 100),
            None,
            "first estimate opens the window"
        );
        assert!(w.is_open());
        assert_eq!(w.offer(Some(W + 5_000), 500), None, "a later, larger one");
        assert_eq!(w.offer(Some(W + 10), 900), None, "a smaller one loses");
        // Window closes at 100 + 1000: the best (W+5000 at tick 500) advanced
        // to tick 1100.
        assert_eq!(w.offer(Some(W + 600), 1_100), Some(W + 5_600));
        assert!(!w.is_open());
        assert_eq!(w.offer(Some(W + 99_999), 2_000), None, "adopts once");
    }

    #[test]
    fn held_estimate_advances_with_ticks() {
        let mut w = PeerTimeWindow::new(1_000);
        assert_eq!(w.offer(Some(W), 0), None);
        // The store no longer reports an estimate (e.g. the observation was
        // trimmed); the held one still advances and is adopted.
        assert_eq!(w.offer(None, 1_000), Some(W + 1_000));
    }

    #[test]
    fn implausible_estimates_never_open_or_win() {
        let mut w = PeerTimeWindow::new(0);
        assert_eq!(w.offer(Some(u64::MAX), 0), None);
        assert_eq!(w.offer(Some(1_000), 1), None);
        assert!(!w.is_open());
        assert_eq!(w.offer(Some(W), 2), Some(W), "zero window adopts at once");
    }

    #[test]
    fn implausible_estimate_mid_window_is_ignored() {
        let mut w = PeerTimeWindow::new(100);
        assert_eq!(w.offer(Some(W), 0), None);
        assert_eq!(w.offer(Some(u64::MAX), 50), None);
        assert_eq!(w.offer(Some(u64::MAX), 100), Some(W + 100));
    }

    #[test]
    fn tick_going_backwards_does_not_underflow() {
        let mut w = PeerTimeWindow::new(10);
        assert_eq!(w.offer(Some(W), 1_000), None);
        assert_eq!(w.offer(None, 500), None);
        assert_eq!(w.offer(None, 1_010), Some(W + 10));
    }
}
