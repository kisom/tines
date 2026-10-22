//! [`BtTransport`]: a [`Transport`] implementation over BLE, with identity
//! (the peer's advertised [`NodeId`]) surviving BLE address rotation.
//!
//! Real radio I/O sits behind the small [`Radio`] trait, implemented over
//! `bluer`/BlueZ in [`BluerRadio`]. Everything here that does not touch the
//! radio — the from/`Addr` mismatch-drop rule, the central-peripheral role
//! decision, and the inbound-address → `NodeId` binding — is pure and
//! unit-testable without hardware.
//!
//! ## Framing
//!
//! The connection is a `bluer` [`l2cap::SeqPacket`] (a LE L2CAP Connection
//! Capable Channel): each received SDU is exactly one frame (one
//! [`RadioEvent::Data`]). The MTU is clamped to [`MAX_FRAME`] so one SDU is one
//! envelope.
//!
//! ## Roles
//!
//! Every node advertises *and* scans. To avoid two nodes both dialing, the
//! numerically-smaller [`NodeId`] dials (the central) and the larger accepts on
//! the [`SeqPacketListener`] bound to `TINES_L2CAP_PSM`.
//!
//! ## Binding
//!
//! The BLE address is only a routing key (it rotates), so the transport binds a
//! connection to the peer's advertised [`NodeId`]: the scanner decodes the
//! [`NodeId`] from a tines advertisement (UUID-checked via [`adv::decode_ad_node_id`])
//! and records `bt address → NodeId`; inbound connections are bound via the
//! scanned address (an unknown address is rejected).
//!
//! ## Multi-peer
//!
//! Up to [`MAX_PEERS`] connections may be live at once, one per bound
//! [`NodeId`]. A second connection to the same [`NodeId`] replaces the first.
//! The configured `peer` becomes an optional allowlist; empty means "any tines
//! advertiser".

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;

use futures::StreamExt;
use tokio::sync::mpsc::{self as async_mpsc, UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;

use bluer::{
    adv,
    l2cap::{self, SeqPacket, SeqPacketListener, SocketAddr},
    monitor::{self, Monitor, MonitorEvent, MonitorHandle, Pattern, RssiSamplingPeriod},
    Adapter, Address, AddressType, Session, Uuid, UuidExt,
};

use tines_core::consts::MAX_FRAME;
use tines_core::frame_sender;
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

    /// The current usable MTU in bytes (the L2CAP Connection-Capable Channel's
    /// negotiated MTU, clamped to [`MAX_FRAME`]).
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
    /// A connection to `addr` became live.
    Contact {
        /// The bound [`NodeId`] (the peer's advertised [`NodeId`]).
        addr: NodeId,
    },
    /// The connection to `addr` ended (EOF, error, or adapter loss).
    Lost {
        /// The bound [`NodeId`] whose connection ended.
        addr: NodeId,
    },
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

/// Resolve the bound [`NodeId`] for an inbound BLE `addr` from the scan map.
///
/// Returns `None` if the address was not seen advertising the tines service
/// UUID — an inbound connection from an unknown address binds to nothing and is
/// therefore rejected by the accept loop.
pub fn resolve_bound(
    scan_map: &HashMap<Address, (NodeId, AddressType)>,
    addr: &Address,
) -> Option<NodeId> {
    scan_map.get(addr).map(|(peer, _)| *peer)
}

impl Transport for BtTransport {
    /// The peer's advertised [`NodeId`]; identity survives BLE address rotation.
    type Addr = NodeId;

    /// Poll for the next inbound event.
    ///
    /// Inbound [`RadioEvent::Data`] frames are checked with [`frame_sender`]:
    /// the frame's *declared* sender (peeked from the envelope) must equal the
    /// frame's claimed `from`, and the `from` must equal this transport's bound
    /// [`NodeId`]. Both are dropped silently — the first is an address-spoofing
    /// violation, the second a frame for a different connection — so core never
    /// observes a mismatch. Frames too large for a [`FrameBuf`] are skipped
    /// rather than aborting the poll.
    fn poll(&mut self) -> Option<Event<Self::Addr>> {
        loop {
            match self.radio.poll() {
                Some(RadioEvent::Data { from, data }) => {
                    // From-check: the frame's declared sender (peeked) must equal
                    // the claimed `from`. A mismatch is spoofing — drop it.
                    if frame_sender(&data) != Some(from) {
                        continue;
                    }
                    if from != self.peer {
                        continue;
                    }
                    match FrameBuf::from_slice(&data) {
                        Ok(buf) => return Some(Event::Frame(from, buf)),
                        Err(_) => continue,
                    }
                }
                Some(RadioEvent::Contact { addr }) => {
                    if addr == self.peer {
                        return Some(Event::Contact(addr));
                    }
                }
                Some(RadioEvent::Lost { addr }) => {
                    if addr == self.peer {
                        return Some(Event::Lost(addr));
                    }
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
    /// Reports the radio's real MTU (the L2CAP Connection-Capable Channel's
    /// negotiated MTU, clamped to [`MAX_FRAME`]). Core folds summaries and
    /// fragments down to this.
    fn mtu(&self) -> usize {
        self.radio.mtu().min(MAX_FRAME)
    }
}

/// Smallest MTU we are willing to report (ATT's default minimum is 23).
const MIN_MTU: usize = 23;

/// Most live connections at once. Kept equal to tines-core's peer-table
/// capacity (8, a crate-private local constant since tines-core 1.0, decision
/// K): a connection beyond it would carry frames core's exchange has no slot
/// for. Both become configuration in a later release.
const MAX_PEERS: usize = 8;

/// L2CAP PSM the tines byte pipe runs on. Shared with the peer (agreed
/// out-of-band): both sides connect/listen on this PSM.
const TINES_L2CAP_PSM: u16 = 0x008D;

/// A [`bluer`]/BlueZ-backed [`Radio`].
///
/// Runs the async `bluer` D-Bus stack on a detached background thread: it
/// powers the adapter, advertises this device's [`NodeId`] in service data,
/// monitors for peers advertising the tines service UUID, and — critically —
/// *both* advertises and listens on `TINES_L2CAP_PSM` so either node can be
/// the initiator. On discovery it opens an L2CAP [`SeqPacket`] connection,
/// binds it to the peer's [`NodeId`] (from the advertisement), and replaces any
/// prior connection to that NodeId. Inbound bytes are surfaced as
/// [`RadioEvent::Data`] tagged with the bound [`NodeId`]; outbound bytes are
/// routed to the matching connection.
///
/// The D-Bus session lives for the lifetime of this radio; [`BluerRadio`]
/// detaches (rather than joins) its background thread on drop, because the run
/// loop blocks on BLE activity for as long as the transport is alive.
pub struct BluerRadio {
    event_rx: UnboundedReceiver<RadioEvent>,
    send_tx: UnboundedSender<(NodeId, Vec<u8>)>,
    mtu: Arc<AtomicUsize>,
    handle: Option<thread::JoinHandle<()>>,
}

impl BluerRadio {
    /// Start the `bluer`/BlueZ radio for a connection between `me` (this
    /// device) and `peer` (the peer's advertised [`NodeId`]).
    ///
    /// `peer` is this node's allowlist: only that NodeId's connections are
    /// accepted. With an empty allowlist the transport accepts any tines
    /// advertiser.
    pub fn new(me: NodeId, peer: NodeId) -> Self {
        let (event_tx, event_rx) = async_mpsc::unbounded_channel::<RadioEvent>();
        let (send_tx, send_rx) = async_mpsc::unbounded_channel::<(NodeId, Vec<u8>)>();
        // Default to the transport's frame limit until the L2CAP socket reports a
        // negotiated MTU; [`BtTransport::mtu`] clamps to MAX_FRAME regardless.
        let mtu = Arc::new(AtomicUsize::new(MAX_FRAME));
        let mtu_for_run = Arc::clone(&mtu);
        // This node only accepts connections to `peer`.
        let allow = Arc::new(BTreeSet::from([peer]));

        let handle = thread::Builder::new()
            .name(format!("tines-bt-bluer-{}", node_hex(peer)))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build bluer runtime");
                if let Err(e) = rt.block_on(run(me, allow, send_rx, mtu_for_run, event_tx)) {
                    log::error!("tines-bt: radio thread exited with error: {e}");
                } else {
                    log::info!("tines-bt: radio thread exited");
                }
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

    fn send(&mut self, _me: NodeId, to: &NodeId, data: &[u8]) -> Result<(), RadioError> {
        self.send_tx
            .send((*to, data.to_vec()))
            .map_err(|_| RadioError::Unreachable)
    }

    fn mtu(&self) -> usize {
        self.mtu.load(Ordering::Relaxed)
    }
}

/// Per-connection state held while the connection is live.
struct Conn {
    socket: Arc<SeqPacket>,
    /// Negotiated L2CAP MTU (bytes) — clamped at connect time.
    mtu: u16,
    /// Monotonic connection nonce; a newer connection to the same NodeId gets a
    /// higher nonce, so a stale read loop never removes the live one.
    nonce: u64,
}

/// State shared across a single radio's background tasks.
///
/// The fields are individually `Arc`-wrapped so the per-task tasks share them;
/// the struct itself is also wrapped in `Arc` so it can be handed off to the
/// detached `tokio` tasks that own the BLE session.
struct SessionState {
    /// Scanned `bt address → (bound NodeId, address type)`, for binding inbound
    /// connections and dialing outbound ones.
    scan_map: Arc<Mutex<HashMap<Address, (NodeId, AddressType)>>>,
    /// Live connections, one per bound [`NodeId`] (a second replaces the first).
    conns: Arc<Mutex<HashMap<NodeId, Conn>>>,
    /// The negotiated L2CAP MTU.
    mtu: Arc<AtomicUsize>,
    /// Allowed [`NodeId`]s; empty means "any tines advertiser".
    allow: Arc<BTreeSet<NodeId>>,
    /// Monotonic counter assigning a unique nonce to each connection.
    nonces: Arc<AtomicU64>,
    /// Fan-out channel for events; owned by the read loops and the write loop.
    events_tx: UnboundedSender<RadioEvent>,
    /// The adapter — used to look up discovered devices' service data.
    adapter: Arc<Adapter>,
    /// This device's advertised [`NodeId`].
    me: NodeId,
}

/// Background async work: session setup, advertising, LE discovery, listening,
/// connecting, and the L2CAP byte pipes. Returns once every task has exited.
async fn run(
    me: NodeId,
    allow: Arc<BTreeSet<NodeId>>,
    outbound: UnboundedReceiver<(NodeId, Vec<u8>)>,
    mtu: Arc<AtomicUsize>,
    events_tx: UnboundedSender<RadioEvent>,
) -> Result<(), bluer::Error> {
    let session = Session::new().await?;
    let adapter = Arc::new(session.default_adapter().await?);
    let state = Arc::new(SessionState {
        scan_map: Arc::new(Mutex::new(HashMap::new())),
        conns: Arc::new(Mutex::new(HashMap::new())),
        mtu: Arc::clone(&mtu),
        allow,
        nonces: Arc::new(AtomicU64::new(1)),
        events_tx,
        adapter: Arc::clone(&adapter),
        me,
    });

    // Power the adapter. A failure here is fatal: nothing else can work, so it
    // is returned (and logged by the radio thread) rather than swallowed.
    if let Err(e) = adapter.set_powered(true).await {
        log::error!("tines-bt: failed to power the Bluetooth adapter: {e}");
        return Err(e);
    }

    // Advertise our own NodeId in service data so a peer can recognise us and
    // learn our identity before connecting (address rotation aside). Advertising
    // is symmetric — the peer will connect to us on our listener if we cannot
    // dial — so a failure is surfaced at error level but not fatal.
    let advertisement = adv::Advertisement {
        advertisement_type: adv::Type::Peripheral,
        service_uuids: BTreeSet::from([Uuid::from_u16(crate::adv::SERVICE_UUID)]),
        service_data: BTreeMap::from([(Uuid::from_u16(crate::adv::SERVICE_UUID), me.to_vec())]),
        discoverable: Some(true),
        ..Default::default()
    };
    if adapter.advertise(advertisement).await.is_err() {
        log::error!("tines-bt: advertising failed; continuing with listening and discovery");
    }

    // LE advertisement monitor: a peer advertising the tines service UUID. The
    // monitor yields the peer's (rotating) BLE address; we look the peer up as
    // a `Device`, decode its [`NodeId`] (UUID-checked), and record it.
    let manager = adapter.monitor().await?;
    let monitor = manager
        .register(Monitor {
            monitor_type: monitor::Type::OrPatterns,
            rssi_sampling_period: Some(RssiSamplingPeriod::First),
            patterns: Some(vec![Pattern {
                data_type: 0x16, // Service Data — 16-bit UUID
                start_position: 0,
                content: crate::adv::SERVICE_UUID.to_le_bytes().to_vec(),
            }]),
            ..Default::default()
        })
        .await?;

    // Bind the listener (the accept role) on our PSM. A failure to bind means we
    // cannot accept, so it is fatal.
    let listener = l2cap::SeqPacketListener::bind(SocketAddr::new(
        Address::any(),
        AddressType::LePublic,
        TINES_L2CAP_PSM,
    ))
    .await
    .map_err(|e| {
        log::error!(
            "tines-bt: failed to bind the SeqPacket listener on PSM {TINES_L2CAP_PSM:#06X}: {e}"
        );
        e
    })?;

    let monitor_state = Arc::clone(&state);
    let accept_state = Arc::clone(&state);
    let connect_state = Arc::clone(&state);
    let write_state = Arc::clone(&state);
    let monitor_join = tokio::spawn(async move {
        monitor_task(monitor, monitor_state).await;
    });
    let accept_join = tokio::spawn(async move {
        accept_loop(listener, accept_state).await;
    });
    let connect_join = tokio::spawn(async move {
        connect_loop(connect_state).await;
    });
    let write_join = tokio::spawn(async move {
        write_loop(write_state, outbound).await;
    });

    tokio::select! {
        _ = monitor_join => {}
        _ = accept_join => {}
        _ = connect_join => {}
        _ = write_join => {}
    }
    Ok(())
}

/// Drive the LE advertisement monitor, recording discovered tines peers in the
/// scan map and forgetting them when they stop advertising.
async fn monitor_task(mut monitor: MonitorHandle, state: Arc<SessionState>) {
    while let Some(event) = monitor.next().await {
        match event {
            MonitorEvent::DeviceFound(found) => {
                let addr = found.device;
                if let Some((peer, atype)) = discover_peer(addr, state.clone()).await {
                    state.scan_map.lock().unwrap().insert(addr, (peer, atype));
                    log::trace!("tines-bt: discovered tines peer {peer:?} at {addr}");
                }
            }
            MonitorEvent::DeviceLost(found) => {
                state.scan_map.lock().unwrap().remove(&found.device);
            }
            // Other monitor events (RSSI changes, etc.) are ignored.
            _ => {}
        }
    }
}

/// Look a discovered `addr` up as a `Device`, decode its [`NodeId`] (checking
/// the tines service UUID and this node's allowlist), and return it with the
/// device's address type. Returns `None` for non-tines or disallowed advertisers.
async fn discover_peer(addr: Address, state: Arc<SessionState>) -> Option<(NodeId, AddressType)> {
    let device = state.adapter.device(addr).ok()?;
    let service_data = match device.service_data().await.ok()? {
        Some(service_data) => service_data,
        None => return None,
    };
    let peer = crate::adv::decode_ad_node_id(&service_data, state.me, &state.allow)?;
    let atype = device.address_type().await.ok()?;
    Some((peer, atype))
}

/// The accept role: bind `listener` on our PSM, and for each accepted connection
/// resolve the peer's bound [`NodeId`] and replace any prior connection to it.
async fn accept_loop(listener: SeqPacketListener, state: Arc<SessionState>) {
    loop {
        match listener.accept().await {
            Ok((socket, peer_sa)) => {
                let peer_addr = peer_sa.addr;
                let bound = resolve_bound(&state.scan_map.lock().unwrap(), &peer_addr);
                match bound {
                    Some(peer) => {
                        if state.allow.contains(&peer) {
                            // A second connection to an already-bound NodeId
                            // replaces the first (handled inside register_and_read);
                            // a brand-new peer is only admitted while under the
                            // concurrent-connection cap.
                            let under_cap = state.conns.lock().unwrap().contains_key(&peer)
                                || state.conns.lock().unwrap().len() < MAX_PEERS;
                            if under_cap {
                                register_and_read(state.clone(), peer, socket).await;
                                log::trace!("tines-bt: accepted connection from {peer:?} (bound)");
                            } else {
                                log::trace!("tines-bt: peer {peer:?} over MAX_PEERS ({MAX_PEERS}); closing new connection");
                            }
                        } else {
                            log::trace!("tines-bt: rejecting inbound from {peer:?} (not allowed)");
                        }
                    }
                    None => {
                        log::trace!("tines-bt: rejecting inbound from unknown address {peer_addr}");
                    }
                }
            }
            Err(e) => {
                log::error!("tines-bt: SeqPacketListener accept error: {e}");
                return;
            }
        }
    }
}

/// The central/initiator role: periodically dial peers we have discovered that
/// this node should connect to (smaller [`NodeId`]) and are not already live.
async fn connect_loop(state: Arc<SessionState>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;

        // Collect eligible candidates without holding any lock across an await.
        let candidates = {
            let scan = state.scan_map.lock().unwrap();
            let allow = &*state.allow;
            scan.iter()
                .filter(|(_addr, (peer, _))| ad_eligible(*peer, state.me, allow))
                .map(|(addr, (peer, atype))| (*addr, *peer, *atype))
                .collect::<Vec<_>>()
        };

        for (addr, peer, atype) in candidates {
            if !is_central(state.me, peer) {
                continue;
            }
            if state.conns.lock().unwrap().contains_key(&peer) {
                continue;
            }
            if state.conns.lock().unwrap().len() >= MAX_PEERS {
                log::trace!(
                    "tines-bt: connected peers over MAX_PEERS ({MAX_PEERS}); skipping {peer:?}"
                );
                continue;
            }
            let sa = SocketAddr::new(addr, atype, TINES_L2CAP_PSM);
            match SeqPacket::connect(sa).await {
                Ok(socket) => {
                    register_and_read(state.clone(), peer, socket).await;
                    log::trace!("tines-bt: connected to {peer:?} at {addr} (central)");
                }
                Err(e) => {
                    log::trace!("tines-bt: connect to {peer:?} failed: {e}");
                }
            }
        }
    }
}

/// Insert `socket` for `peer` into the connection map, replacing any prior
/// connection, and return its [`Arc<SeqPacket>`] and nonce.
async fn register_connection(
    state: Arc<SessionState>,
    peer: NodeId,
    socket: SeqPacket,
) -> (Arc<SeqPacket>, u64) {
    let mtu = mtu_bytes(&socket);
    let nonce = state.nonces.fetch_add(1, Ordering::Relaxed);
    let socket = Arc::new(socket);
    if let Some(old) = state.conns.lock().unwrap().insert(
        peer,
        Conn {
            socket: socket.clone(),
            mtu,
            nonce,
        },
    ) {
        // The replaced connection is no longer referenced here; its read loop's
        // nonce check surfaces the Lost event for the stale connection.
        drop(old.socket);
    }
    state.mtu.store(mtu as usize, Ordering::Relaxed);
    (socket, nonce)
}

/// Register `socket` for `peer` (replacing any prior connection) and spawn its
/// read loop.
async fn register_and_read(state: Arc<SessionState>, peer: NodeId, socket: SeqPacket) {
    let (conn, nonce) = register_connection(state.clone(), peer, socket).await;
    tokio::spawn(async move {
        read_loop(peer, conn, state, nonce).await;
    });
}

/// The read loop for a single connection: read one SDU at a time (one frame
/// each) and emit [`RadioEvent::Data`]. On EOF/error it removes the connection
/// unless a newer connection with a higher nonce has replaced it, then emits
/// [`RadioEvent::Lost`].
async fn read_loop(peer: NodeId, socket: Arc<SeqPacket>, state: Arc<SessionState>, nonce: u64) {
    let mut buf = vec![0u8; state.mtu.load(Ordering::Relaxed)];
    while let Ok(n) = socket.recv(&mut buf).await {
        if n == 0 {
            break;
        }
        if state
            .events_tx
            .send(RadioEvent::Data {
                from: peer,
                data: buf[..n].to_vec(),
            })
            .is_err()
        {
            break;
        }
    }

    // Connection ended. Remove it only if it is still the live one for this
    // NodeId (a newer connection replaced it, so leave that one in place).
    let mut guard = state.conns.lock().unwrap();
    if guard.get(&peer).map(|c| c.nonce) == Some(nonce) {
        guard.remove(&peer);
        let _ = state.events_tx.send(RadioEvent::Lost { addr: peer });
    }
}

/// Drain outbound frames and write each to its destination connection.
async fn write_loop(state: Arc<SessionState>, mut outbound: UnboundedReceiver<(NodeId, Vec<u8>)>) {
    while let Some((to, data)) = outbound.recv().await {
        // Read the connection (dropping the guard before awaiting so it isn't
        // held across the send's await point — a MutexGuard is not Send).
        let (socket, mtu) = match state.conns.lock().unwrap().get(&to) {
            Some(conn) => (Some(Arc::clone(&conn.socket)), conn.mtu as usize),
            None => (None, 0),
        };
        if let Some(socket) = socket {
            if socket.send(&data[..mtu.min(data.len())]).await.is_err() {
                log::trace!("tines-bt: send to {to:?} failed");
            }
        } else {
            log::trace!("tines-bt: no connection to {to:?}; dropping frame");
        }
    }
}

/// Whether the advertisement for `peer` is eligible for this node to consider.
///
/// A node never dials itself, and only considers peers in the allowlist (an
/// empty allowlist accepts any tines advertiser — the NodeId check is already
/// done by [`crate::adv::decode_ad_node_id`]).
fn ad_eligible(peer: NodeId, me: NodeId, allow: &BTreeSet<NodeId>) -> bool {
    peer != me && (allow.is_empty() || allow.contains(&peer))
}

/// The negotiated L2CAP MTU for `socket`, clamped to our limits.
fn mtu_bytes(socket: &SeqPacket) -> u16 {
    socket
        .send_mtu()
        .map(|m| m.clamp(MIN_MTU, MAX_FRAME) as u16)
        .unwrap_or(MAX_FRAME as u16)
}

/// Render a [`NodeId`] as hex for thread naming (avoids pulling in a hex crate).
fn node_hex(node: NodeId) -> String {
    use core::fmt::Write;
    let mut s = String::with_capacity(node.len() * 2);
    for b in node {
        let _ = write!(s, "{b:02x}");
    }
    s
}
