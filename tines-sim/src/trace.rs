//! Contact traces: a timeline of which nodes are within range of each other.

/// A single contact window between two nodes.
///
/// Nodes `a` and `b` are in contact during the half-open interval
/// `[start_ms, end_ms)`. Contacts are symmetric: a contact recorded as
/// `(a, b)` is equivalent to `(b, a)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Contact {
    pub start_ms: u64,
    pub end_ms: u64,
    pub a: usize,
    pub b: usize,
}

/// An ordered list of contact windows, as produced by the contact simulator.
#[derive(Clone, Debug, Default)]
pub struct ContactTrace {
    pub contacts: Vec<Contact>,
}

impl ContactTrace {
    /// Whether `a` and `b` are in contact at time `t`, for any contact
    /// involving the pair in either order.
    ///
    /// A contact counts during its half-open window: `start_ms <= t < end_ms`.
    pub fn in_contact(&self, a: usize, b: usize, t: u64) -> bool {
        self.contacts
            .iter()
            .any(|c| is_contact_pair(c, a, b) && in_window(c, t))
    }

    /// The last time any contact ends, or `0` if there are no contacts.
    pub fn end(&self) -> u64 {
        self.contacts.iter().map(|c| c.end_ms).max().unwrap_or(0)
    }
}

/// True when `c` links `a` and `b` in either order.
fn is_contact_pair(c: &Contact, a: usize, b: usize) -> bool {
    (c.a == a && c.b == b) || (c.a == b && c.b == a)
}

/// True when `t` falls inside the half-open window `[start, end)`.
fn in_window(c: &Contact, t: u64) -> bool {
    c.start_ms <= t && t < c.end_ms
}

#[cfg(test)]
mod tests {
    use super::{Contact, ContactTrace};

    #[test]
    fn in_contact_matches_both_orders() {
        let mut trace = ContactTrace::default();
        trace.contacts.push(Contact { start_ms: 100, end_ms: 200, a: 3, b: 7 });
        assert!(!trace.in_contact(3, 7, 50)); // before the window
        assert!(trace.in_contact(3, 7, 100)); // at the start edge
        assert!(trace.in_contact(7, 3, 150)); // mid-window, either order
        assert!(!trace.in_contact(3, 7, 200)); // at the end edge (half-open)
        assert!(!trace.in_contact(3, 7, 300)); // after the window
    }

    #[test]
    fn end_is_zero_when_empty() {
        assert_eq!(ContactTrace::default().end(), 0);
    }
}
