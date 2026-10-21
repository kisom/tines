//! The host's single monotonic millisecond clock.
//!
//! [`HostClock`] is a single [`std::time::Instant`] captured at process start.
//! Its [`now`](HostClock::now) returns the milliseconds elapsed since that
//! capture as a [`u64`] tines `Tick`.
//!
//! It is monotonic, so an NTP step (forward or backwards) can't rewind it —
//! ages and addressed lifetimes keep advancing instead of saturating or
//! jumping. And it is expressed in milliseconds, the unit the tines `Tick`
//! expects: an observation with `lifetime_ms = 1000` expires after ~1 s of the
//! host's own ticks, not after ~1000 s (which is what a wall-clock *seconds*
//! clock, the old `now_ms`, produced).
//!
//! There is one clock per process. `LogStore` is opened with `clock.now()`
//! (which is `0` at startup, correct by construction), and the service uses
//! the same clock for every `observe`/`belief` tick, so the store and the
//! service never disagree about what "now" is.
//!
//! Wall-clock time (`SystemTime`, milliseconds since the Unix epoch) is used
//! only for `observed_at`, and only when the host is configured to trust its
//! own clock — never for the monotonic tick.

use std::time::Instant;

/// One process-wide monotonic millisecond clock.
///
/// Copy cheaply (a single [`Instant`]); pass it to whoever needs "now".
#[derive(Debug, Clone)]
pub struct HostClock {
    /// The instant this clock measures elapsed time from (process start).
    start: Instant,
    /// The tick at which the trusted wall clock was anchored. Only meaningful
    /// alongside [`Self::wall_at_set`].
    anchor_tick: u64,
    /// The trusted wall time (ms since the Unix epoch), anchored at
    /// [`Self::anchor_tick`]. `None` until the host establishes a trusted wall
    /// clock (system clock config or a peer), after which [`Self::wall_ms`]
    /// returns `Some(wall + (now - anchor_tick))` on every observe.
    wall_at_set: Option<u64>,
}

impl HostClock {
    /// Capture the clock at process start.
    ///
    /// `now()` is `0` immediately after this, and grows monotonically.
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
            anchor_tick: 0,
            wall_at_set: None,
        }
    }

    /// Milliseconds elapsed since [`HostClock::start`].
    ///
    /// `0` right after [`HostClock::start`]; monotonic thereafter.
    pub fn now(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Anchor the trusted wall clock, once.
    ///
    /// `wall` is the wall time (ms since the Unix epoch) the host trusted at
    /// tick `now`. This is called once — the first time the host establishes a
    /// trusted wall clock, either from its system clock
    /// (`trusted_clock = "system"`) or by deriving it from a peer's
    /// observation (`LogStore::peer_time_estimate`). From then on
    /// [`Self::wall_ms`] returns `Some(wall + (now' − now))`.
    ///
    /// A second call is a no-op: the anchor is fixed at the first trusted time.
    pub fn anchor_wall(&mut self, wall: u64, now: u64) {
        if self.wall_at_set.is_none() {
            self.wall_at_set = Some(wall);
            self.anchor_tick = now;
        }
    }

    /// The trusted wall time (ms since the Unix epoch) at tick `now`, if the
    /// host has a trusted wall clock. `None` before the first trusted time
    /// (R4: an observation made while quarantined carries `observed_at = None`).
    pub fn wall_ms(&self, now: u64) -> Option<u64> {
        self.wall_at_set
            .map(|wall| wall + now.saturating_sub(self.anchor_tick))
    }

    /// Whether the host has a trusted wall clock (i.e. trust has been
    /// established and `wall_ms` returns `Some`).
    pub fn is_trusted(&self) -> bool {
        self.wall_at_set.is_some()
    }
}

impl Default for HostClock {
    fn default() -> Self {
        Self::start()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::HostClock;

    /// The tines `Tick` is milliseconds, not seconds. Over a short real sleep
    /// the clock must advance by the size of that sleep (hundreds of ms),
    /// never by single digits as a wall-clock *seconds* clock would. Fast and
    /// non-flaky: a generous lower bound absorbs CI jitter without a long sleep.
    #[test]
    fn now_advances_in_milliseconds_not_seconds() {
        let clock = HostClock::start();
        let first = clock.now();
        std::thread::sleep(Duration::from_millis(150));
        let second = clock.now();
        let delta = second.saturating_sub(first);
        assert!(
            delta >= 120,
            "HostClock::now must advance in milliseconds: expected >=120 ms over a 150 ms sleep, got {delta} ms",
        );
        // And it must be ms since process start, not epoch seconds: no process
        // has lived a century, so the absolute value stays well under 60 s of
        // millis. This would catch a regression back to `as_secs()`.
        assert!(
            second < 60_000,
            "HostClock::now must be ms since process start, was {second}",
        );
    }

    #[test]
    fn now_is_zero_immediately_after_start() {
        let clock = HostClock::start();
        // No sleep: elapsed is a few microseconds, well under a millisecond.
        assert!(clock.now() < 1, "clock starts at 0, was {}", clock.now());
    }
}
