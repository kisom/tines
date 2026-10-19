//! IPC over a Unix-domain socket.
//!
//! The [`serve`] fn hosts a tines [`Node`] and speaks a tiny
//! request/response protocol to each connecting peer. A single instance is the
//! control plane for its device; external tools (or another tines Node, in
//! later tasks) drive the node through four operations: `observe`, `belief`,
//! `events`, and `status`.
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
//! `"kind"` field (serde externally-tagged). Frames are capped at 1 MiB; a
//! longer frame is treated as a malformed request and the connection is
//! dropped. There is no framing negotiation — the first byte after the length
//! prefix is `"`.
//!
//! Observations are content-addressed, so IDs are exchanged as lowercase hex
//! (the hex form of the 16-byte `ObservationId`). Topics and payloads are
//! passed to `observe` the same way: as hex.
//!
//! # Sequence numbers
//!
//! `now` is the tines tick passed to `observe`/`belief`; the service reads it
//! from its own wall clock (`now_ms()`) and ignores any caller-supplied value,
//! so the wire request carries no timestamp.

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tines_core::{Node, Store, Transport};

const MAX_FRAME: usize = 1 << 20; // 1 MiB cap on a single JSON frame

/// A confidence event handed out by the IPC `events` request.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct EventOut {
    /// Hex of the `ObservationId` this confidence concerns.
    pub id: String,
    /// Hex of the `NodeId` of the peer that acknowledged.
    pub peer: String,
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
    },
    /// Read the latest belief for `topic`.
    Belief {
        /// Hex of the topic bytes to query.
        topic: String,
    },
    /// Drain outstanding confidence events.
    Events,
    /// Report the node's identity and log depth.
    Status,
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
    /// Confidence events drained from the exchange.
    Events {
        /// The drained confidence events.
        events: Vec<EventOut>,
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
pub fn handle<S, T>(node: &mut Node<S, T>, req: Request, now: u64) -> Response
where
    S: Store + 'static,
    T: Transport,
{
    match req {
        Request::Observe { topic, payload } => {
            let topic = match decode_hex(&topic) {
                Ok(t) => t,
                Err(e) => return error(e),
            };
            let payload = match decode_hex(&payload) {
                Ok(t) => t,
                Err(e) => return error(e),
            };
            match node.observe(&topic, &payload, None, now) {
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
            let events = node
                .drain_events()
                .into_iter()
                .map(|ev| EventOut {
                    id: encode_hex(&ev.id),
                    peer: encode_hex(&ev.peer),
                })
                .collect();
            Response::Events { events }
        }
        Request::Status => Response::Status {
            id: encode_hex(&node.id()),
            next_seq: node.next_seq(),
            stored: node.store().len() as u32,
        },
    }
}

/// Serve IPC on `socket_path` until the listener dies or a response write
/// fails. Blocks; each accepted connection is served request-by-request.
///
/// `Node<S, T>` may be any concrete store/transport, so the real binary passes
/// `Node<LogStore, BtTransport>` and tests pass a mock.
pub fn serve<S, T>(node: Node<S, T>, socket_path: impl AsRef<Path>) -> std::io::Result<()>
where
    S: Store + 'static,
    T: Transport + 'static,
{
    let node = Arc::new(Mutex::new(node));
    let path: PathBuf = socket_path.as_ref().to_path_buf();

    // Clear a stale socket file left by a previous run so bind() can't fail.
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;

    for stream in listener.incoming() {
        let mut stream = stream?;
        // Serve this connection request-by-request until the peer cleanly
        // disconnects, then move on to the next connection.
        loop {
            match read_request(&mut stream) {
                Ok(req) => {
                    let now = now_ms();
                    let resp = {
                        // Recover from a poisoned lock so one panicking
                        // handler can't take down the daemon.
                        let mut guard =
                            node.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        handle(&mut guard, req, now)
                    };
                    if write_response(&mut stream, &resp).is_err() {
                        // A client that vanished mid-connection shouldn't
                        // take down the whole service; just drop this socket.
                        break;
                    }
                }
                // A clean disconnect (peer closed the socket) ends the loop.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                // A malformed frame isn't a disconnect: tell the client and
                // keep serving this connection rather than dropping it. The
                // frame can't be identified, so the error is generic.
                Err(_) => {
                    if write_response(&mut stream, &error("malformed request")).is_err() {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Read one length-prefixed JSON frame from a stream.
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
