//! [`BtTransport`]: a [`Transport`] implementation over BLE, with identity
//! (the peer's advertised [`NodeId`]) surviving BLE address rotation.
//!
//! Real radio I/O sits behind the small [`Radio`] trait, implemented over
//! `bluer`/BlueZ in [`BluerRadio`]. Everything here that does not touch the
//! radio — the from/`Addr` mismatch-drop rule and the central-peripheral role
//! decision — is pure and unit-testable without hardware.

use std::boxed::Box;

use tines_core::consts::MAX_FRAME;
use tines_core::observation::NodeId;
use tines_core::transport::{Event, FrameBuf, SendError, Transport};

/// The radio I/O seam for [`BtTransport`].
///
/// A real implementation ( `transport::BluerRadio`, over `bluer`/BlueZ) drives
/// the Bluetooth stack on a background task and surfaces its events here. Kept
/// as a narrow trait so the pure transport logic can be tested with a mock and
/// never needs a real radio.
pub trait Radio {
    /// The next raw radio event, or `None` if nothing is pending right now.
    fn poll(&mut self) -> Option<RadioEvent>;

    /// Deliver `data` to `to`, tagging the outbound frame with `me` (this
    /// device's advertised [`NodeId`]).
    fn send(&mut self, me: NodeId, to: &NodeId, data: &[u8]) -> Result<(), RadioError>;

    /// The current usable MTU in bytes (L2CAP Connection-Capable Channel when
    /// available, else the negotiated ATT MTU on the GATT fallback).
    fn mtu(&self) -> usize;
}

/// A raw event surfaced by a [`Radio`].
pub enum RadioEvent {
    /// A frame arrived from `from` (the peer's advertised [`NodeId`]).
    Data {
        /// The claimed source of the frame: the peer's advertised [`NodeId`].
        from: NodeId,
        /// The frame bytes carried by the event.
        data: Vec<u8>,
    },
    /// The peer disconnected; any prior [`Event::Lost`] address is now gone.
    Disconnected,
}

/// Why a [`Radio::send`] call can fail.
#[derive(Debug)]
pub enum RadioError {
    /// The radio is busy with an in-flight operation and cannot accept another.
    Busy,
    /// The peer is not currently reachable (no connection).
    Unreachable,
}

/// A BLE [`Transport`] whose address is the peer's advertised [`NodeId`].
///
/// Construct via [`BtTransport::new`], passing the device's own advertised id
/// (`me`), the peer's advertised id for the current connection (`peer`), and a
/// [`Radio`] (typically a `bluer`-backed `transport::BluerRadio`).
pub struct BtTransport {
    /// This device's advertised [`NodeId`]; tags every outbound frame.
    me: NodeId,
    /// The peer's advertised [`NodeId`] for the current connection. Used to drop
    /// spoofed/mismatched inbound frames; the peer's BLE address need not match.
    peer: NodeId,
    radio: Box<dyn Radio>,
}

impl BtTransport {
    /// Create a transport for a connection between `me` and `peer`.
    pub fn new(me: NodeId, peer: NodeId, radio: impl Radio + 'static) -> Self {
        BtTransport {
            me,
            peer,
            radio: Box::new(radio),
        }
    }

    /// Whether this node should act as the BLE central/initiator for the
    /// connection to `peer`. The numerically smaller [`NodeId`] initiates
    /// (see [`is_central`]).
    pub fn is_central(&self) -> bool {
        is_central(self.me, self.peer)
    }
}

/// The robot with the numerically smaller [`NodeId`] acts as the BLE
/// central/initiator.
///
/// NodeIds are distinct between peers, so this is a strict total order with no
/// tie; the `me == peer` case (which cannot occur for two distinct devices)
/// yields `false` — neither side initiates.
pub fn is_central(me: NodeId, peer: NodeId) -> bool {
    me < peer
}

impl Transport for BtTransport {
    /// The peer's advertised [`NodeId`]; identity survives BLE address rotation.
    type Addr = NodeId;

    /// Poll for the next inbound event.
    ///
    /// Inbound [`RadioEvent::Data`] frames whose `from` differs from the
    /// connection's advertised [`NodeId`] are dropped here — a protocol
    /// violation (address spoofing) at the transport, so core never observes a
    /// mismatch. Frames too large for a [`FrameBuf`] are skipped rather than
    /// aborting the poll.
    fn poll(&mut self) -> Option<Event<Self::Addr>> {
        loop {
            match self.radio.poll() {
                Some(RadioEvent::Data { from, data }) => {
                    if from != self.peer {
                        continue;
                    }
                    match FrameBuf::from_slice(&data) {
                        Ok(buf) => return Some(Event::Frame(from, buf)),
                        Err(_) => continue,
                    }
                }
                Some(RadioEvent::Disconnected) => {
                    let peer = self.peer;
                    self.peer = [0u8; 16];
                    return Some(Event::Lost(peer));
                }
                None => return None,
            }
        }
    }

    /// Attempt to deliver `frame` to `to`.
    fn send(&mut self, to: &Self::Addr, frame: &[u8]) -> Result<(), SendError> {
        if frame.len() > self.mtu() {
            return Err(SendError::TooLarge);
        }
        self.radio.send(self.me, to, frame).map_err(|e| match e {
            RadioError::Busy => SendError::Busy,
            RadioError::Unreachable => SendError::Unreachable,
        })
    }

    /// Maximum frame size in bytes this transport can send.
    ///
    /// Reports the radio's real MTU (L2CAP CoC when available, else the ATT
    /// MTU on the GATT fallback), clamped to `MAX_FRAME` per the
    /// [`Transport`] contract. Core folds summaries and fragments down to this.
    fn mtu(&self) -> usize {
        self.radio.mtu().min(MAX_FRAME)
    }
}

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;

use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{self as async_mpsc, UnboundedReceiver, UnboundedSender};

use bluer::{
    adv,
    l2cap::{SocketAddr, Stream as L2capStream},
    monitor::{self, Monitor, MonitorEvent, Pattern, RssiSamplingPeriod},
    AddressType, Uuid, UuidExt,
};

use crate::adv::SERVICE_UUID;

/// Fixed L2CAP PSM for the tines byte pipe. Shared with the peer (agreed
/// out-of-band): the L2CAP stream connects on this PSM on both sides, so the
/// numerically smaller [`NodeId`] (the central) is the one that lands the
/// connection first.
const TINES_L2CAP_PSM: u16 = 0x008D;

/// A [`bluer`]/BlueZ-backed [`Radio`].
///
/// Runs the async `bluer` D-Bus stack on a detached background thread: it
/// powers the adapter, advertises this device's [`NodeId`] in service data,
/// monitors for a peer advertising the tines service UUID, and on discovery
/// opens an L2CAP byte pipe on `TINES_L2CAP_PSM`. Inbound bytes are surfaced
/// as [`RadioEvent::Data`] tagged with the peer's [`NodeId`] (which the
/// transport already knows — the BLE address is only how we route to it, and it
/// may rotate); outbound bytes are written to the pipe. The usable MTU is read
/// from the L2CAP socket and reported through [`Radio::mtu`].
///
/// The D-Bus session lives for the lifetime of this radio; [`BluerRadio`]
/// detaches (rather than joins) its background thread on drop, because the run
/// loop blocks on BLE activity for as long as the transport is alive.
///
/// This backend establishes the L2CAP pipe from the initiating side (the smaller
/// [`NodeId`], per [`is_central`]); callers construct the transport only for the
/// peer they intend to talk to.
pub struct BluerRadio {
    event_rx: UnboundedReceiver<RadioEvent>,
    send_tx: UnboundedSender<Vec<u8>>,
    mtu: std::sync::Arc<AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl BluerRadio {
    /// Start the `bluer`/BlueZ radio for a connection between `me` (this device)
    /// and `peer` (the peer's advertised [`NodeId`]).
    pub fn new(me: NodeId, peer: NodeId) -> Self {
        let (event_tx, event_rx) = async_mpsc::unbounded_channel::<RadioEvent>();
        let (send_tx, send_rx) = async_mpsc::unbounded_channel::<Vec<u8>>();
        // Default to the transport's frame limit until the L2CAP socket reports a
        // negotiated MTU; [`BtTransport::mtu`] clamps to MAX_FRAME regardless.
        let mtu = Arc::new(AtomicUsize::new(MAX_FRAME));
        let mtu_for_run = Arc::clone(&mtu);

        let handle = thread::Builder::new()
            .name(format!("tines-bt-bluer-{}", node_hex(peer)))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build bluer runtime");
                let _ = rt.block_on(run(me, peer, event_tx, send_rx, mtu_for_run));
            })
            .expect("failed to spawn bluer radio thread");

        BluerRadio {
            event_rx,
            send_tx,
            mtu,
            handle: Some(handle),
        }
    }
}

impl Drop for BluerRadio {
    fn drop(&mut self) {
        // Detach the background radio thread: it owns the long-lived bluer D-Bus
        // session and would block a join while waiting on BLE activity.
        self.handle.take();
    }
}

impl Radio for BluerRadio {
    fn poll(&mut self) -> Option<RadioEvent> {
        self.event_rx.try_recv().ok()
    }

    fn send(&mut self, _me: NodeId, _to: &NodeId, data: &[u8]) -> Result<(), RadioError> {
        self.send_tx
            .send(data.to_vec())
            .map_err(|_| RadioError::Unreachable)
    }

    fn mtu(&self) -> usize {
        self.mtu.load(Ordering::Relaxed)
    }
}

/// Background async work: session, advertising, LE discovery, and the L2CAP
/// byte pipe. Returns once the connection drops.
async fn run(
    me: NodeId,
    peer: NodeId,
    event_tx: UnboundedSender<RadioEvent>,
    send_rx: UnboundedReceiver<Vec<u8>>,
    mtu: Arc<AtomicUsize>,
) -> Result<(), bluer::Error> {
    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    if let Err(e) = adapter.set_powered(true).await {
        log::warn!("tines-bt: failed to power adapter: {e:?}");
        return Ok(());
    }

    // Advertise our own NodeId in service data so a peer can recognise us and
    // learn our identity before connecting (address rotation aside).
    let advertisement = adv::Advertisement {
        advertisement_type: adv::Type::Peripheral,
        service_uuids: BTreeSet::from([Uuid::from_u16(SERVICE_UUID)]),
        service_data: BTreeMap::from([(Uuid::from_u16(SERVICE_UUID), me.to_vec())]),
        discoverable: Some(true),
        ..Default::default()
    };
    if adapter.advertise(advertisement).await.is_err() {
        log::warn!("tines-bt: failed to advertise; continuing with discovery");
    }

    // LE advertisement monitor: a peer advertising the tines service UUID. The
    // monitor yields the peer's (rotating) BLE address; the peer's identity is
    // the [`NodeId`] we already know (`peer`), which is what tags inbound frames.
    let manager = adapter.monitor().await?;
    let mut monitor = manager
        .register(Monitor {
            monitor_type: monitor::Type::OrPatterns,
            rssi_sampling_period: Some(RssiSamplingPeriod::First),
            patterns: Some(vec![Pattern {
                data_type: 0x16, // Service Data — 16-bit UUID
                start_position: 0,
                content: SERVICE_UUID.to_le_bytes().to_vec(),
            }]),
            ..Default::default()
        })
        .await?;

    while let Some(event) = monitor.next().await {
        if let MonitorEvent::DeviceFound(found) = event {
            let addr = found.device;
            let sa = SocketAddr::new(addr, AddressType::LePublic, TINES_L2CAP_PSM);
            match L2capStream::connect(sa).await {
                Ok(stream) => {
                    if let Ok(mtu_bytes) = stream.as_ref().recv_mtu() {
                        let m = mtu_bytes as usize;
                        mtu.store(m.clamp(MIN_MTU, MAX_FRAME), Ordering::Relaxed);
                    }
                    let (read_half, write_half) = stream.into_split();
                    let read_join = tokio::spawn(async move {
                        read_loop(peer, read_half, event_tx).await;
                    });
                    let write_join = tokio::spawn(async move {
                        write_loop(write_half, send_rx).await;
                    });
                    // The pipe is up. Block until either loop ends (disconnect),
                    // then tear down so the runtime can stop.
                    tokio::select! {
                        _ = read_join => {}
                        _ = write_join => {}
                    }
                    return Ok(());
                }
                Err(e) => {
                    log::trace!("tines-bt: connect to {addr:?} failed: {e:?}");
                }
            }
        }
    }
    Ok(())
}

/// Read inbound bytes off the L2CAP pipe and emit them as [`RadioEvent::Data`],
/// tagged with the peer's [`NodeId`].
async fn read_loop<P: AsyncRead + Unpin>(
    peer: NodeId,
    mut read_half: P,
    tx: UnboundedSender<RadioEvent>,
) {
    let mut buf = vec![0u8; MAX_FRAME];
    loop {
        match read_half.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                if tx
                    .send(RadioEvent::Data {
                        from: peer,
                        data: buf[..n].to_vec(),
                    })
                    .is_err()
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

/// Drain outbound bytes off the queue and write them to the L2CAP pipe.
async fn write_loop<P: AsyncWrite + Unpin>(mut write_half: P, mut rx: UnboundedReceiver<Vec<u8>>) {
    while let Some(bytes) = rx.recv().await {
        if write_half.write_all(&bytes).await.is_err() {
            break;
        }
    }
}

/// Smallest MTU we are willing to report (ATT's default minimum is 23).
const MIN_MTU: usize = 23;

/// Render a [`NodeId`] as hex for thread naming (avoids pulling in a hex crate).
fn node_hex(node: NodeId) -> String {
    use core::fmt::Write;
    let mut s = String::with_capacity(node.len() * 2);
    for b in node {
        let _ = write!(s, "{b:02x}");
    }
    s
}
