//! IPC over a Unix-domain socket.
//!
//! The [`serve`] fn hosts a tines [`Node`] and speaks a tiny
//! request/response protocol to each connecting peer. A single instance is the
//! control plane for its device; external tools (or another tines Node, in
//! later tasks) drive the node through six operations: `observe`, `belief`,
//! `events`, `status`, and `list` (the fleet console adds addressed `observe`
//! and contact/drop reporting to the `events` response).
//!
//! # Wire format
//!
//! Length-prefixed JSON, one message per direction:
//!
//! ```text
//!   [u32 : little-endian byte length] [u8 * length : a JSON frame]
//! ```
//!
//! Each frame is one of the [`Request`] or [`Response`] enums, tagged by a
//! `"kind"` field (serde internally tagged). Frames are capped at 1 MiB. A
//! longer length prefix gets an error response, but its body isn't consumed,
//! so the connection is out of sync afterwards; the host-hardening spec makes
//! it close the connection instead (`THREAT_MODEL.md` F11). There is no
//! framing negotiation: the first byte after the length prefix is `{`.
//!
//! Observations are content-addressed, so IDs are exchanged as lowercase hex
//! (the hex form of the 16-byte `ObservationId`). Topics and payloads are
//! passed to `observe` the same way: as hex.
//!
//! # Clock
//!
//! `now` is the tines tick passed to `observe`/`belief`. The service reads it
//! from its own monotonic millisecond [`HostClock`] (an `Instant` captured at
//! process start; see [`crate::clock`]) and ignores any caller-supplied value,
//! so the wire request carries no timestamp. The store is opened with the same
//! clock's `now` (0 at startup), so the service and the store agree on "now".

use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::time::Duration;

use tines_core::{Node, NodeEvent, NodeId, Store, Transport, TrimPolicy};
use tines_store::LogStore;

use crate::trust::{adopt_wall_clock, PeerTimeWindow};
use crate::HostClock;

const MAX_FRAME: usize = 1 << 20; // 1 MiB cap on a single JSON frame

/// Cap on the number of `poll` rounds the exchange runs per iteration. Without
/// it, a busy peer that keeps the exchange full could starve the IPC listener
/// of its accept loop.
const POLL_CAP: u64 = 64;

/// Max entries a `List` response returns; `truncated` reports when more exist.
pub const LIST_CAP: usize = 1024;

/// A confirmed delivery handed out by the IPC `events` request.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct EventOut {
    /// Hex of the `ObservationId` this confidence concerns.
    pub id: String,
    /// Hex of the `NodeId` of the peer that acknowledged.
    pub peer: String,
}

/// A contact change handed out by the IPC `events` request: a transport
/// `Contact` (`joined: true`) or `Lost` (`joined: false`) for one peer.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ContactOut {
    /// Hex of the peer's `NodeId`.
    pub peer: String,
    /// `true` for a new contact, `false` for a loss.
    pub joined: bool,
}

/// A counted drop handed out by the IPC `events` request.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DropOut {
    /// Hex of the peer the dropped frame came from, or `None` for the
    /// overflow slot (which has no address to name).
    pub peer: Option<String>,
    /// The drop reason, debug-formatted (e.g. `"VersionMismatch { version: 255 }"`).
    pub reason: String,
    /// The number of frames folded into this counter since the last drain.
    pub count: u32,
}

/// One stored observation handed out by the IPC `list` request.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ObservationOut {
    /// Hex of the `ObservationId`.
    pub id: String,
    /// Hex of the observer's `NodeId`.
    pub observer: String,
    /// Hex of the topic.
    pub topic: String,
    /// Hex of the payload.
    pub payload: String,
    /// Hex of the destination, or `None` for a broadcast.
    pub to: Option<String>,
    /// The addressed lifetime in ms, or `None` for a broadcast.
    pub lifetime_ms: Option<u64>,
    /// How old the observation is, in ms (see `Observation::age_ms`).
    pub age_ms: u64,
    /// The observer's wall-clock time, ms since the Unix epoch, or `None`
    /// when the observer had no trusted clock.
    pub observed_at: Option<u64>,
}

/// A belief returned by the IPC `belief` request.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct BeliefOut {
    /// Hex of the `ObservationId`.
    pub id: String,
    /// Hex of the topic.
    pub topic: String,
    /// Hex of the payload.
    pub payload: String,
}

/// An IPC request. See the module docs for the wire format.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "kind")]
pub enum Request {
    /// Insert an observation under `topic`.
    Observe {
        /// Hex of the topic bytes to observe.
        topic: String,
        /// Hex of the observation payload.
        payload: String,
        /// Hex of the destination `NodeId` for an addressed observation.
        /// Must be set together with `lifetime_ms`.
        #[serde(default)]
        to: Option<String>,
        /// The addressed lifetime in ms. Must be set together with `to`.
        #[serde(default)]
        lifetime_ms: Option<u64>,
    },
    /// Read the latest belief for `topic`.
    Belief {
        /// Hex of the topic bytes to query.
        topic: String,
    },
    /// Drain outstanding confirmed deliveries, contact changes, and counted
    /// drops.
    Events,
    /// Report the node's identity and log depth.
    Status,
    /// List every stored observation (capped at [`LIST_CAP`]).
    List,
}

/// An IPC response. See the module docs for the wire format.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "kind")]
pub enum Response {
    /// The inserted observation's `id` (hex).
    Observe {
        /// Hex of the `ObservationId`.
        id: String,
    },
    /// The latest belief for the topic, if any.
    Belief {
        /// The stored belief, or `None` if the topic is unknown.
        obs: Option<BeliefOut>,
    },
    /// Confirmed deliveries, contact changes, and counted drops drained from
    /// the exchange.
    Events {
        /// The drained confirmed deliveries.
        events: Vec<EventOut>,
        /// The drained contact changes (joined or lost), in order.
        #[serde(default)]
        contacts: Vec<ContactOut>,
        /// The drained counted drops.
        #[serde(default)]
        drops: Vec<DropOut>,
    },
    /// The node's `id`, next tines `next_seq`, and observations `stored`.
    Status {
        /// Hex of the node's own `NodeId`.
        id: String,
        /// Next tines sequence number the node will use.
        next_seq: u64,
        /// Number of observations in the store.
        stored: u32,
    },
    /// Every stored observation, up to [`LIST_CAP`] entries.
    List {
        /// The stored observations.
        observations: Vec<ObservationOut>,
        /// `true` when more observations exist than fit in
        /// [`LIST_CAP`] entries.
        truncated: bool,
    },
    /// The request was malformed or rejected.
    Error {
        /// A human-readable explanation.
        message: String,
    },
}

/// Handle one IPC request against a node.
///
/// This is the single point where a request becomes a node operation;
/// [`serve`](fn@serve) decodes frames, calls `handle`, and re-encodes the
/// response. Kept generic over the store/transport so tests can drive it with a
/// mock transport.
///
/// `observed_at` is the host's trusted wall time (ms since the Unix epoch) at
/// which the observation was made, or `None` if the host has no trusted wall
/// clock yet (R4: an observation made while quarantined carries `None`).
pub fn handle<S, T>(
    node: &mut Node<S, T>,
    req: Request,
    observed_at: Option<u64>,
    now: u64,
) -> Response
where
    S: Store + 'static,
    T: Transport<Addr = NodeId>,
{
    match req {
        Request::Observe {
            topic,
            payload,
            to,
            lifetime_ms,
        } => {
            let topic = match decode_hex(&topic) {
                Ok(t) => t,
                Err(e) => return error(e),
            };
            let payload = match decode_hex(&payload) {
                Ok(t) => t,
                Err(e) => return error(e),
            };
            let result = match (to, lifetime_ms) {
                (None, None) => node.observe(&topic, &payload, observed_at, now),
                (Some(to), Some(lifetime_ms)) => {
                    let to = match decode_hex(&to) {
                        Ok(t) => t,
                        Err(e) => return error(e),
                    };
                    let to: NodeId = match to.try_into() {
                        Ok(id) => id,
                        Err(_) => {
                            return error("to must be a 16-byte NodeId (32 hex chars)");
                        }
                    };
                    node.observe_to(to, lifetime_ms, &topic, &payload, observed_at, now)
                }
                // An addressed observation needs both a destination and a
                // lifetime; half-addressed is almost certainly a client bug.
                _ => return error("to and lifetime_ms must be set together"),
            };
            match result {
                Ok(id) => Response::Observe {
                    id: encode_hex(&id),
                },
                Err(e) => error(format!("observe failed: {e:?}")),
            }
        }
        Request::Belief { topic } => {
            let topic = match decode_hex(&topic) {
                Ok(t) => t,
                Err(e) => return error(e),
            };
            match node.belief(&topic, &tines_core::LatestWins, now) {
                Some(obs) => Response::Belief {
                    obs: Some(BeliefOut {
                        id: encode_hex(&obs.id()),
                        topic: encode_hex(&obs.topic),
                        payload: encode_hex(&obs.payload),
                    }),
                },
                None => Response::Belief { obs: None },
            }
        }
        Request::Events => {
            let mut events = Vec::new();
            let mut contacts = Vec::new();
            let mut drops = Vec::new();
            for ev in node.drain_events() {
                match ev {
                    NodeEvent::Delivered { peer, id } => events.push(EventOut {
                        id: encode_hex(&id),
                        peer: encode_hex(&peer),
                    }),
                    NodeEvent::Contact { addr } => contacts.push(ContactOut {
                        peer: encode_hex(&addr),
                        joined: true,
                    }),
                    NodeEvent::Lost { addr } => contacts.push(ContactOut {
                        peer: encode_hex(&addr),
                        joined: false,
                    }),
                    // Drops surface here AND on stderr (they are the whole
                    // point of D3; the stderr line stays for `tines` logs).
                    NodeEvent::Dropped {
                        addr,
                        reason,
                        count,
                    } => {
                        eprintln!("tines: drop: {reason:?} (count={count}, addr={addr:?})");
                        drops.push(DropOut {
                            peer: addr.map(|a| encode_hex(&a)),
                            reason: format!("{reason:?}"),
                            count,
                        });
                    }
                    // `NodeEvent` is non-exhaustive: ignore any future variant.
                    _ => {}
                }
            }
            Response::Events {
                events,
                contacts,
                drops,
            }
        }
        Request::List => {
            let mut ids = Vec::with_capacity(LIST_CAP.min(node.store().len()));
            let mut truncated = false;
            node.store().for_each_id(&mut |id| {
                if ids.len() < LIST_CAP {
                    ids.push(*id);
                } else {
                    truncated = true;
                }
            });
            let mut observations = Vec::with_capacity(ids.len());
            for id in &ids {
                if let Some(obs) = node.store().get(id, now) {
                    observations.push(ObservationOut {
                        id: encode_hex(&obs.id()),
                        observer: encode_hex(&obs.observer),
                        topic: encode_hex(&obs.topic),
                        payload: encode_hex(&obs.payload),
                        to: obs.dest.as_ref().map(|d| encode_hex(&d.to)),
                        lifetime_ms: obs.dest.as_ref().map(|d| d.lifetime_ms),
                        age_ms: obs.age_ms,
                        observed_at: obs.observed_at,
                    });
                }
            }
            Response::List {
                observations,
                truncated,
            }
        }
        Request::Status => Response::Status {
            id: encode_hex(&node.id()),
            next_seq: node.next_seq(),
            stored: node.store().len() as u32,
        },
    }
}

/// The serve loop's state between polls: the host clock, the peer-time window,
/// and the poll-round count that schedules trim and flush.
struct HostState {
    clock: HostClock,
    window: PeerTimeWindow,
    rounds: u64,
}

// Drive the exchange until no new observations arrive or the per-iteration cap
// (`POLL_CAP`) is reached, then — on schedule — trim and flush.
//
// The node isn't `Send` (the real `BtTransport` carries a `dyn Radio` with no
// `Send` bound), so this can't run on its own thread; it runs inline in
// [`serve`], fed by the socket read timeouts.
fn poll_exchange<T>(node: &mut Node<LogStore, T>, host: &mut HostState)
where
    T: Transport<Addr = NodeId> + 'static,
{
    while node.poll(host.clock.now()) > 0 && host.rounds < POLL_CAP {
        host.rounds += 1;
    }

    // Peer-time trust (spec §9, decision J). Trusted time comes from a local
    // source first — `main` anchors a trusted system clock at startup — and
    // otherwise from a peer. While untrusted, re-query the store's estimate
    // (drawn only from observations stored since this open; quarantined data
    // is invisible) on every poll, and let the window pick the latest
    // plausible one; adopt it once, when the window closes. With no peer time
    // the host stays quarantined: the robot's own fail-safe is outside tines.
    //
    // The estimate scans the store, O(n) on every poll. That's acceptable
    // only because it stops for good once the clock is trusted.
    if !host.clock.is_trusted() {
        let now = host.clock.now();
        let was_open = host.window.is_open();
        let adopted = host.window.offer(node.peer_time_estimate(now), now);
        if !was_open && host.window.is_open() {
            eprintln!("tines: peer wall time seen, collecting estimates before adopting");
        }
        if let Some(est) = adopted {
            match adopt_wall_clock(node, &mut host.clock, est, now) {
                Some(report) => eprintln!(
                    "tines: trusted wall clock from peer (estimate {est} ms, downtime {:?} ms)",
                    report.downtime_ms
                ),
                None => eprintln!("tines: peer wall time {est} ms rejected"),
            }
        }
    }

    // On a schedule: trim stale observations and flush the store. `Node::flush`
    // delegates to `LogStore`'s `Store::flush` impl, the durability point; a
    // failure is logged and the next scheduled flush retries.
    if host.rounds >= POLL_CAP {
        host.rounds = 0;
        node.trim(&TrimPolicy::default(), host.clock.now());
        match node.flush(host.clock.now()) {
            Ok(()) => {}
            Err(e) => eprintln!("tines: store flush failed: {e:?}"),
        }
    }
}

/// Serve this process's IPC socket and run its exchange.
///
/// `serve` answers IPC and drives the exchange inline on a poll loop fed by the
/// socket read timeouts (see `poll_exchange`). Because the node isn't `Send`
/// (the real `BtTransport` carries a `dyn Radio` with no `Send` bound), the
/// exchange can't move to a spawned thread — it runs inline here, between
/// connections and while a connection is idle.
///
/// `serve` is generic over the transport (so tests can drive it over an
/// in-memory link) but concrete on the on-disk [`LogStore`], whose `flush` is
/// the trim/flush hook. The real binary passes `Node<LogStore, BtTransport>`
/// and the tests pass `Node<LogStore, mock>`.
///
/// `poll_interval_ms` is the read-timeout cadence that feeds the poll loop; the
/// default (20 ms) keeps the exchange responsive without busy-polling.
///
/// `clock` is anchored already if the host trusts a local clock. Otherwise
/// `serve` takes its wall time from peers: from the first plausible estimate
/// it collects for `peer_time_window_ms` (decision J; default
/// [`DEFAULT_PEER_TIME_WINDOW_MS`](crate::trust::DEFAULT_PEER_TIME_WINDOW_MS)),
/// then adopts the latest, once.
pub fn serve<T>(
    mut node: Node<LogStore, T>,
    socket_path: impl AsRef<Path>,
    clock: HostClock,
    poll_interval_ms: u64,
    peer_time_window_ms: u64,
) -> std::io::Result<()>
where
    T: Transport<Addr = NodeId> + 'static,
{
    let socket_path = socket_path.as_ref().to_path_buf();
    // Clear a stale socket file left by a previous run so bind() can't fail.
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)?;

    // `accept` blocks, so run it in a dedicated thread that just forwards
    // accepted streams over a channel. That lets the main loop yield to the
    // exchange poll loop (via `recv_timeout`) instead of blocking forever. The
    // accept thread never touches the node, so the transport need not be
    // `Send` here — only the main loop borrows it.
    let (tx, rx) = std::sync::mpsc::channel::<UnixStream>();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            if stream
                .and_then(|s| {
                    tx.send(s)
                        .map_err(|_| std::io::Error::other("receiver dropped"))
                })
                .is_err()
            {
                break;
            }
        }
    });

    let mut host = HostState {
        clock,
        window: PeerTimeWindow::new(peer_time_window_ms),
        rounds: 0,
    };
    loop {
        match rx.recv_timeout(Duration::from_millis(poll_interval_ms.max(1))) {
            Ok(mut stream) => {
                // A read timeout on the connection lets an idle client yield to
                // the poll loop instead of stalling the exchange forever.
                stream
                    .set_read_timeout(Some(Duration::from_millis(poll_interval_ms.max(1))))
                    .expect("connection read timeout");
                serve_stream(&mut node, &mut stream, &mut host)?;
            }
            // Timeout: run the exchange poll loop instead of blocking.
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                poll_exchange(&mut node, &mut host);
            }
            // The accept thread is gone; stop serving.
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Ok(());
            }
        }
    }
}

/// Serve a single accepted connection until the peer disconnects, driving the
/// exchange on every idle read.
fn serve_stream<T>(
    node: &mut Node<LogStore, T>,
    stream: &mut UnixStream,
    host: &mut HostState,
) -> io::Result<()>
where
    T: Transport<Addr = NodeId> + 'static,
{
    loop {
        match read_request(stream) {
            Ok(request) => {
                let now = host.clock.now();
                // Stamp the observation with the trusted wall time if the host
                // has one; `None` before the first trusted time (R4).
                let observed_at = host.clock.wall_ms(now);
                let response = handle(node, request, observed_at, now);
                if write_response(stream, &response).is_err() {
                    // A client that vanished mid-connection shouldn't take
                    // down the whole service; just drop this socket.
                    break;
                }
                // Drive the exchange after every request so a busy client that
                // connects back-to-back can't starve it (the WouldBlock path
                // below only fires once the client goes idle).
                poll_exchange(node, host);
            }
            // Peer closed the connection cleanly.
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            // Idle read: drive the exchange rather than blocking forever.
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                poll_exchange(node, host);
            }
            // A malformed frame isn't a disconnect: tell the client and keep
            // serving this connection. The frame can't be identified, so the
            // error is generic.
            Err(_) => {
                if write_response(stream, &error("malformed request")).is_err() {
                    break;
                }
            }
        }
    }
    Ok(())
}
fn read_request<S: Read>(stream: &mut S) -> std::io::Result<Request> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame {len} bytes exceeds {MAX_FRAME}"),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Write one length-prefixed JSON frame to a stream.
fn write_response<S: Write>(stream: &mut S, resp: &Response) -> std::io::Result<()> {
    let body = serde_json::to_vec(resp)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if body.len() > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "response too large",
        ));
    }
    let len = (body.len() as u32).to_le_bytes();
    stream.write_all(&len)?;
    stream.write_all(&body)?;
    stream.flush()
}

fn error(msg: impl std::fmt::Display) -> Response {
    Response::Error {
        message: msg.to_string(),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err(format!("odd-length hex string: {}", s.len()));
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_val(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        other => Err(format!("invalid hex char: {other:02x}")),
    }
}
