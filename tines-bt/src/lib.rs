//! tines-bt: a BLE `Transport` for tines over BlueZ, via the `bluer` crate.
//!
//! BLE radio addresses rotate and are not a stable identity, so `Addr` on this
//! transport is the peer's advertised `NodeId` (`tines_core::observation::NodeId`).
//! The advertising payload (see [`adv`]) carries that `NodeId` in service data,
//! so identity survives address rotation.
//!
//! Real radio I/O lives behind the small [`Radio`] trait (implemented over
//! `bluer` in [`transport::BluerRadio`]); the pure logic — advertising payload
//! assembly, the from/`Addr` mismatch-drop rule, and the central-peripheral
//! role decision — is unit-testable without hardware.

#![warn(missing_docs)]

pub mod adv;
mod transport;

pub use adv::{
    advertise, decode_node_id, ADVERTISING_BUFFER_LEN, ADV_FLAGS, NODE_ID_LEN, SERVICE_UUID,
};
pub use transport::{is_central, BluerRadio, BtTransport, Radio, RadioError, RadioEvent};
