//! The host's chosen transport: either the BLE radio ([`BtTransport`]) or the
//! host bus ([`BusTransport`]), per the service config.

use tines_bt::BtTransport;
use tines_bus::BusTransport;
use tines_core::{Event, NodeId, SendError, Transport};

/// The transport the service runs, selected by [`crate::config::Config`].
pub enum HostTransport {
    /// The BLE transport with the single configured peer.
    Bt(BtTransport),
    /// The host bus transport with the (optionally allowlisted) bus peers.
    Bus(BusTransport),
}

impl Transport for HostTransport {
    type Addr = NodeId;

    fn poll(&mut self) -> Option<Event<NodeId>> {
        match self {
            HostTransport::Bt(t) => t.poll(),
            HostTransport::Bus(t) => t.poll(),
        }
    }

    fn send(&mut self, to: &NodeId, frame: &[u8]) -> Result<(), SendError> {
        match self {
            HostTransport::Bt(t) => t.send(to, frame),
            HostTransport::Bus(t) => t.send(to, frame),
        }
    }

    fn mtu(&self) -> usize {
        match self {
            HostTransport::Bt(t) => t.mtu(),
            HostTransport::Bus(t) => t.mtu(),
        }
    }
}
