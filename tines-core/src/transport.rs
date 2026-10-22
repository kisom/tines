//! Transport trait and wire event types for tines-core.
//!
//! The `Transport` trait abstracts the byte pipe that carries `Frame`s between
//! peers. Implementations are `#![no_std]`-friendly: the only buffer type,
//! `FrameBuf`, is a fixed-capacity `heapless::Vec` that never touches the heap.

use crate::consts;

/// Inbound/outbound frame buffer: a `MAX_FRAME`-capacity stack buffer.
pub type FrameBuf = heapless::Vec<u8, { consts::MAX_FRAME }>;

/// An event observed on a `Transport` from a single peer.
///
/// `Frame` carries a full `MAX_FRAME` buffer inline, making it far larger than
/// `Contact`/`Lost`; the `large_enum_variant` lint is intentionally suppressed
/// to keep `FrameBuf` off the heap in the `no_std` core.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum Event<A> {
    /// A peer became reachable.
    Contact(A),
    /// A previously reachable peer became unreachable.
    Lost(A),
    /// A frame arrived from `A`.
    Frame(A, FrameBuf),
}

/// Reasons a `Transport::send` call can fail.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    /// The transport is busy with an in-flight send and cannot accept another.
    Busy,
    /// The destination peer is not currently reachable.
    Unreachable,
    /// The frame exceeds the transport's MTU and cannot be sent.
    TooLarge,
}

/// A byte pipe between peers, polled for inbound events and driven with `send`.
pub trait Transport {
    /// Address of a peer on this transport.
    type Addr: Clone + PartialEq + core::fmt::Debug;

    /// Poll for the next inbound event (contact, loss, or frame).
    fn poll(&mut self) -> Option<Event<Self::Addr>>;

    /// Attempt to deliver `frame` to `to`.
    fn send(&mut self, to: &Self::Addr, frame: &[u8]) -> Result<(), SendError>;

    /// Maximum frame size in bytes this transport can send (must be `<= MAX_FRAME`).
    ///
    /// The smallest MTU core can fully use is **85 bytes**: at 85 every valid
    /// observation, up to a maximum-size, signed, addressed one, fits in the
    /// 64 fragments a peer will reassemble. Below that core never emits an
    /// oversized frame, but it sends less:
    /// - below 85, the largest observations can't be sent;
    /// - below 66, nothing is fragmented, so only observations that fit
    ///   whole in one frame go out;
    /// - below 35, no receipt fits, so none is sent and every sender keeps
    ///   custody of what it sent;
    /// - below 26, no summary fits.
    fn mtu(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use super::{Event, SendError, Transport};
    use crate::consts;

    /// A no-op transport used to pin the trait's shape at compile time.
    struct NullTransport;

    impl Transport for NullTransport {
        type Addr = u8;

        fn poll(&mut self) -> Option<Event<Self::Addr>> {
            None
        }

        fn send(&mut self, _to: &Self::Addr, _frame: &[u8]) -> Result<(), SendError> {
            Ok(())
        }

        fn mtu(&self) -> usize {
            consts::MAX_FRAME
        }
    }

    #[test]
    fn null_transport_poll_is_none() {
        let mut t = NullTransport;
        assert!(t.poll().is_none());
        assert_eq!(t.mtu(), consts::MAX_FRAME);
    }
}
