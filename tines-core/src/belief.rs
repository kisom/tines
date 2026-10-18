//! Belief policies: streaming fold over observations.
//!
//! A policy reduces an unordered stream of matching observations down to a
//! single accumulated `Belief` (an `Observation`) via [`BeliefPolicy::step`].
//! The fold is intentionally iterative rather than iterator-based so that a
//! caller (e.g. `Node::belief`) never has to buffer all observations at once —
//! a 1 KiB payload × N buffer is unacceptable on the MCU target.

use crate::observation::{NodeId, Observation};

/// Reduces an ordered stream of observations to a single accumulated belief.
pub trait BeliefPolicy {
    type Belief;
    /// Streaming fold: called once per matching observation (already
    /// age-adjusted to `now`), in no particular order. `acc` is `None` before
    /// the first call. Return the new accumulator.
    fn step(
        &self,
        me: NodeId,
        acc: Option<Self::Belief>,
        obs: &Observation,
    ) -> Option<Self::Belief>;
}

/// Latest observation wins: the observation with the smallest `age_ms`; ties
/// are broken deterministically by `(observer, seq)` ascending.
pub struct LatestWins;

/// Prefer the agent's own observation: the youngest of its own observations
/// if any exist, otherwise the same as [`LatestWins`]. An observation from
/// `me` beats any observation from another observer regardless of age.
pub struct SelfFirst;

impl BeliefPolicy for LatestWins {
    type Belief = Observation;

    fn step(
        &self,
        _me: NodeId,
        acc: Option<Self::Belief>,
        obs: &Observation,
    ) -> Option<Self::Belief> {
        match acc {
            None => Some(obs.clone()),
            Some(cur) => {
                if obs.age_ms < cur.age_ms
                    || (obs.age_ms == cur.age_ms
                        && (obs.observer, obs.seq) < (cur.observer, cur.seq))
                {
                    Some(obs.clone())
                } else {
                    Some(cur)
                }
            }
        }
    }
}

impl BeliefPolicy for SelfFirst {
    type Belief = Observation;

    fn step(
        &self,
        me: NodeId,
        acc: Option<Self::Belief>,
        obs: &Observation,
    ) -> Option<Self::Belief> {
        match acc {
            None => Some(obs.clone()),
            Some(cur) => {
                let obs_is_me = obs.observer == me;
                let cur_is_me = cur.observer == me;
                let winner = if obs_is_me && !cur_is_me {
                    obs.clone()
                } else if !obs_is_me && cur_is_me {
                    cur
                } else {
                    // Both from `me` or both from someone else: fall back to
                    // the LatestWins rule (youngest, tie broken by
                    // (observer, seq) ascending).
                    if obs.age_ms < cur.age_ms
                        || (obs.age_ms == cur.age_ms
                            && (obs.observer, obs.seq) < (cur.observer, cur.seq))
                    {
                        obs.clone()
                    } else {
                        cur
                    }
                };
                Some(winner)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::{Payload, Topic};

    fn obs(observer: [u8; 16], seq: u64, age_ms: u64) -> Observation {
        Observation {
            observer,
            seq,
            age_ms,
            observed_at: None,
            topic: Topic::from_slice(b"door/3").unwrap(),
            payload: Payload::from_slice(b"open").unwrap(),
        }
    }

    fn fold<P: BeliefPolicy>(policy: &P, me: NodeId, obs: &[Observation]) -> Option<P::Belief> {
        let mut acc: Option<P::Belief> = None;
        for o in obs {
            acc = policy.step(me, acc, o);
        }
        acc
    }

    fn fold_rev<P: BeliefPolicy>(policy: &P, me: NodeId, obs: &[Observation]) -> Option<P::Belief> {
        let mut acc: Option<P::Belief> = None;
        for i in (0..obs.len()).rev() {
            acc = policy.step(me, acc, &obs[i]);
        }
        acc
    }

    #[test]
    fn latest_wins_picks_smallest_age() {
        let me = [7; 16];
        let obs = [
            obs([10; 16], 1, 300),
            obs([11; 16], 2, 50),
            obs([12; 16], 3, 10),
            obs([13; 16], 4, 200),
        ];
        // Forward and reverse order must both select the youngest (age 10).
        assert_eq!(fold(&LatestWins, me, &obs).unwrap().seq, 3);
        assert_eq!(fold_rev(&LatestWins, me, &obs).unwrap().seq, 3);
    }

    #[test]
    fn latest_wins_none_on_empty() {
        let me = [7; 16];
        let obs: [Observation; 0] = [];
        assert_eq!(fold(&LatestWins, me, &obs), None);
        assert_eq!(fold_rev(&LatestWins, me, &obs), None);
    }

    #[test]
    fn latest_wins_tie_is_deterministic() {
        let me = [7; 16];
        // Same age, distinct (observer, seq): winner must be the smaller tuple
        // regardless of iteration order.
        let lo = obs([2; 16], 5, 100);
        let hi = obs([9; 16], 1, 100);
        let obs = [lo, hi];
        assert_eq!(fold(&LatestWins, me, &obs).unwrap().observer, [2; 16]);
        assert_eq!(fold_rev(&LatestWins, me, &obs).unwrap().observer, [2; 16]);
    }

    #[test]
    fn self_first_prefers_own_even_if_older() {
        let me = [7; 16];
        let mine = obs([7; 16], 1, 500);
        let theirs = obs([8; 16], 2, 10);
        let obs = [mine, theirs];
        // Own observation (age 500) beats theirs (age 10) regardless of order.
        assert_eq!(fold(&SelfFirst, me, &obs).unwrap().observer, [7; 16]);
        assert_eq!(fold_rev(&SelfFirst, me, &obs).unwrap().observer, [7; 16]);
    }

    #[test]
    fn self_first_falls_back_to_latest() {
        let me = [7; 16];
        // No observation belongs to `me`, so SelfFirst must match LatestWins.
        let a = obs([10; 16], 1, 300);
        let b = obs([11; 16], 2, 50);
        let c = obs([12; 16], 3, 10);
        let obs = [a, b, c];
        assert_eq!(fold(&SelfFirst, me, &obs).unwrap().seq, 3);
        assert_eq!(fold_rev(&SelfFirst, me, &obs).unwrap().seq, 3);
        assert_eq!(
            fold(&SelfFirst, me, &obs).unwrap(),
            fold(&LatestWins, me, &obs).unwrap()
        );
    }
}
