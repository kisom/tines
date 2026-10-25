//! The bus transport: a [`Transport`] whose peers are other tines nodes
//! connected to the same host bus daemon (see [`crate::daemon`]).
//!
//! The pure state machine ([`BusTransport`]) is generic over a narrow I/O
//! seam, [`BusLink`], mirroring how `tines-bt` splits logic from radio. The
//! real link ([`DaemonLink`]) is a thread-driven client of the bus daemon;
//! it lives in this crate but is not part of the pure logic.

use std::io::{self, Write};
use std::path::Path;
use std::sync::mpsc;

use tines_core::consts::MAX_FRAME;
use tines_core::{frame_sender, Event, FrameBuf, SendError, Transport};

use crate::daemon;
pub use tines_core::NodeId;

/// An event observed on the bus link.
#[derive(Debug)]
pub enum BusEvent {
    /// A peer registered with the daemon.
    Joined(NodeId),
    /// A registered peer left (or its connection was replaced).
    Left(NodeId),
    /// A data envelope arrived, attributed to the daemon-certified sender.
    Frame(NodeId, Box<FrameBuf>),
    /// The link itself closed (daemon gone, socket error, or local shutdown).
    Closed,
}

/// Errors a [`BusLink`] can report from `send`.
#[derive(Debug)]
pub enum BusError {
    /// The underlying socket/channel failed.
    Io(io::Error),
    /// The destination is not reachable on this link.
    Unreachable,
}

/// The narrow I/O seam behind [`BusTransport`].
///
/// `send` receives the *frame bytes only*; the link stamps the envelope with
/// the sender's identity, so a caller cannot forge who a frame is from.
pub trait BusLink {
    /// Poll for the next link event, if any is ready.
    fn poll(&mut self) -> Option<BusEvent>;

    /// Queue `data` for delivery to `to`.
    fn send(&mut self, to: &NodeId, data: &[u8]) -> Result<(), BusError>;

    /// Largest frame this link can carry (its transport MTU).
    fn mtu(&self) -> usize;
}

/// A thread-driven client of the bus daemon.
///
/// A writer thread owns the socket (sending the hello first, then queued
/// envelopes) and a reader thread owns a clone, decoding inbound envelopes
/// into [`BusEvent`]s. Dropping the link drops the writer's queue, which
/// shuts the socket down, which unblocks the reader — no explicit shutdown
/// protocol needed.
#[derive(Debug)]
pub struct DaemonLink {
    me: NodeId,
    out_tx: mpsc::Sender<Vec<u8>>,
    ev_rx: mpsc::Receiver<BusEvent>,
}

impl DaemonLink {
    /// Connect to the daemon at `socket` and hello as `me`.
    ///
    /// The connect retries briefly because a freshly spawned daemon binds
    /// asynchronously.
    pub fn connect(me: NodeId, socket: &Path) -> io::Result<DaemonLink> {
        let mut stream = retry_connect(socket)?;
        let mut reader = stream.try_clone()?;

        let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
        let (ev_tx, ev_rx) = mpsc::channel::<BusEvent>();

        // Writer thread: hello first, then queued envelopes. On queue
        // disconnect (the transport was dropped) it shuts the socket down,
        // which also unblocks the reader thread.
        let mut hello = Vec::with_capacity(4 + 16);
        hello.extend_from_slice(&16u32.to_le_bytes());
        hello.extend_from_slice(&me);
        std::thread::spawn(move || {
            if stream.write_all(&hello).is_err() {
                let _ = stream.shutdown(std::net::Shutdown::Both);
                return;
            }
            while let Ok(env) = out_rx.recv() {
                if stream.write_all(&env).is_err() {
                    break;
                }
            }
            let _ = stream.shutdown(std::net::Shutdown::Both);
        });

        // Reader thread: decode envelopes into events.
        std::thread::spawn(move || {
            loop {
                let payload = match daemon::read_envelope(&mut reader) {
                    Ok(Some(p)) => p,
                    Ok(None) | Err(_) => {
                        let _ = ev_tx.send(BusEvent::Closed);
                        return;
                    }
                };
                let ev = match payload.len() {
                    17 => match payload[0] {
                        0 => BusEvent::Joined(id_of(&payload[1..17])),
                        _ => BusEvent::Left(id_of(&payload[1..17])),
                    },
                    n if n >= 32 => {
                        let from = id_of(&payload[0..16]);
                        let frame = match FrameBuf::from_slice(&payload[32..]) {
                            Ok(f) => f,
                            // Overlong frame: protocol violation, treat as
                            // a dead link.
                            Err(_) => {
                                let _ = ev_tx.send(BusEvent::Closed);
                                return;
                            }
                        };
                        BusEvent::Frame(from, Box::new(frame))
                    }
                    _ => continue, // unknown message shape: ignore
                };
                if ev_tx.send(ev).is_err() {
                    return;
                }
            }
        });

        Ok(DaemonLink { me, out_tx, ev_rx })
    }
}

impl BusLink for DaemonLink {
    fn poll(&mut self) -> Option<BusEvent> {
        self.ev_rx.try_recv().ok()
    }

    fn send(&mut self, to: &NodeId, data: &[u8]) -> Result<(), BusError> {
        // Envelope: [u32 LE payload len][from][to][frame].
        let mut env = Vec::with_capacity(4 + 32 + data.len());
        env.extend_from_slice(&((32 + data.len()) as u32).to_le_bytes());
        env.extend_from_slice(&self.me);
        env.extend_from_slice(to);
        env.extend_from_slice(data);
        self.out_tx
            .send(env)
            .map_err(|_| BusError::Io(io::Error::new(io::ErrorKind::BrokenPipe, "bus link closed")))
    }

    fn mtu(&self) -> usize {
        MAX_FRAME
    }
}

/// Connect to `socket`, retrying briefly while a just-spawned daemon binds.
fn retry_connect(socket: &Path) -> io::Result<std::os::unix::net::UnixStream> {
    let mut last_err = None;
    for _ in 0..40 {
        match std::os::unix::net::UnixStream::connect(socket) {
            Ok(s) => return Ok(s),
            Err(e) => last_err = Some(e),
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::ConnectionRefused, "bus daemon never bound")
    }))
}

fn id_of(bytes: &[u8]) -> NodeId {
    let mut id = [0u8; 16];
    id.copy_from_slice(bytes);
    id
}

/// The bus transport: tines `Transport` over a [`BusLink`].
///
/// `type Addr = NodeId` — the daemon certifies each message's sender, so bus
/// peer identity is as strong as the other transports'.
pub struct BusTransport<L: BusLink = DaemonLink> {
    me: NodeId,
    allowlist: Option<NodeId>,
    peers: Vec<NodeId>,
    link: L,
    closed: bool,
}

impl BusTransport<DaemonLink> {
    /// Create a bus transport connected to the daemon at `socket`.
    ///
    /// `allowlist` pins the one peer this transport may see and reach
    /// (`None` = every bus peer), mirroring the single-peer allowlist of the
    /// bt transport.
    pub fn new(
        me: NodeId,
        allowlist: Option<NodeId>,
        socket: impl AsRef<Path>,
    ) -> io::Result<BusTransport<DaemonLink>> {
        let link = DaemonLink::connect(me, socket.as_ref())?;
        Ok(BusTransport {
            me,
            allowlist,
            peers: Vec::new(),
            link,
            closed: false,
        })
    }
}

impl<L: BusLink> BusTransport<L> {
    /// Whether `peer` passes the allowlist (and is not ourselves).
    fn allows(&self, peer: &NodeId) -> bool {
        *peer != self.me && self.allowlist.is_none_or(|a| a == *peer)
    }
}

impl<L: BusLink> Transport for BusTransport<L> {
    type Addr = NodeId;

    fn poll(&mut self) -> Option<Event<NodeId>> {
        loop {
            match self.link.poll()? {
                BusEvent::Joined(addr) => {
                    if !self.allows(&addr) || self.peers.contains(&addr) {
                        continue;
                    }
                    self.peers.push(addr);
                    return Some(Event::Contact(addr));
                }
                BusEvent::Left(addr) => {
                    if let Some(pos) = self.peers.iter().position(|p| *p == addr) {
                        self.peers.remove(pos);
                        return Some(Event::Lost(addr));
                    }
                }
                BusEvent::Frame(peer, frame) => {
                    if !self.allows(&peer) {
                        continue;
                    }
                    // The daemon certifies the envelope's `from`; the frame's
                    // own declared sender must agree with it.
                    if frame_sender(frame.as_slice()) != Some(peer) {
                        continue;
                    }
                    return Some(Event::Frame(peer, *frame));
                }
                BusEvent::Closed => {
                    if self.closed {
                        return None;
                    }
                    self.closed = true;
                    // Every bound peer is gone; drain one `Lost` per poll.
                    return self.peers.pop().map(Event::Lost);
                }
            }
        }
    }

    fn send(&mut self, to: &NodeId, frame: &[u8]) -> Result<(), SendError> {
        if !self.allows(to) || !self.peers.contains(to) {
            return Err(SendError::Unreachable);
        }
        if frame.len() > self.mtu() {
            return Err(SendError::TooLarge);
        }
        match self.link.send(to, frame) {
            Ok(()) => Ok(()),
            // The only failure modes left are a dead link (nothing is
            // reachable any more) and a bad destination.
            Err(_) => Err(SendError::Unreachable),
        }
    }

    fn mtu(&self) -> usize {
        self.link.mtu()
    }
}
