//! An async, queue-based client for the `tines` service IPC.
//!
//! The wire protocol is the service's own: `[u32 LE len][JSON]`. The client
//! tags frames with a monotonically increasing message id **on the
//! client-side queue only** — `request` enqueues `[id, JSON body]` and the
//! worker thread writes `[len][JSON]` to the socket, so the service-side
//! protocol is untouched. The service answers one request per connection in
//! order, so the worker pairs responses with the outstanding ids FIFO; the
//! GUI polls [`IpcClient::try_recv`] for `(id, response)` pairs.
//!
//! The worker owns the socket and re-dials forever: a dead service never
//! blocks `request` (requests queue), and [`IpcClient::reconnect`] forces a
//! re-dial without dropping queued requests. If the connection dies between
//! a write and its response, the request is replayed after the re-dial —
//! observe is the dominant verb here, and re-observing beats losing data.

use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use tines::{Request, Response};

/// Errors the client can report.
#[derive(Debug)]
pub enum ClientError {
    /// A socket-level failure (including a dead worker thread).
    Io(io::Error),
    /// A request failed to serialize.
    Json(serde_json::Error),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Io(e) => write!(f, "ipc client io: {e}"),
            ClientError::Json(e) => write!(f, "ipc client json: {e}"),
        }
    }
}

impl std::error::Error for ClientError {}

/// Shared state between the handle and the worker thread.
struct Shared {
    /// Set by [`IpcClient::reconnect`]; the worker redials and clears it.
    force_reconnect: AtomicBool,
}

/// A queue-based client for one service socket.
pub struct IpcClient {
    out: Sender<(u64, Vec<u8>)>,
    rx: Receiver<(u64, Response)>,
    next_id: Arc<AtomicU64>,
    shared: Arc<Shared>,
    _socket: PathBuf,
}

impl IpcClient {
    /// Spawn the worker thread and return the client. The socket is dialed
    /// on the worker thread, so this succeeds even while the service is
    /// down.
    pub fn spawn(socket: PathBuf) -> io::Result<IpcClient> {
        let (out_tx, out_rx) = mpsc::channel();
        let (resp_tx, resp_rx) = mpsc::channel();
        let next_id = Arc::new(AtomicU64::new(0));
        let shared = Arc::new(Shared {
            force_reconnect: AtomicBool::new(false),
        });
        let worker_shared = Arc::clone(&shared);
        let worker_socket = socket.clone();
        thread::Builder::new()
            .name("tines-gui-ipc".into())
            .spawn(move || worker(worker_socket, out_rx, resp_tx, worker_shared))?;
        Ok(IpcClient {
            out: out_tx,
            rx: resp_rx,
            next_id,
            shared,
            _socket: socket,
        })
    }

    /// Encode and queue a request; returns the message id it will be
    /// answered under.
    pub fn request(&self, req: Request) -> Result<u64, ClientError> {
        let body = serde_json::to_vec(&req).map_err(ClientError::Json)?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.out
            .send((id, body))
            .map_err(|e| ClientError::Io(io::Error::new(io::ErrorKind::BrokenPipe, e)))?;
        Ok(id)
    }

    /// Nonblocking: the next completed `(message id, response)`, if any.
    pub fn try_recv(&self) -> Option<(u64, Response)> {
        self.rx.try_recv().ok()
    }

    /// Force the worker to drop its current connection and re-dial. Queued
    /// requests are kept and replayed after the new dial.
    pub fn reconnect(&self) {
        self.shared.force_reconnect.store(true, Ordering::Relaxed);
    }
}

/// Write one `[u32 LE len][body]` frame to the stream.
fn write_frame(stream: &mut UnixStream, body: &[u8]) -> io::Result<()> {
    let len = (body.len() as u32).to_le_bytes();
    stream.write_all(&len)?;
    stream.write_all(body)?;
    stream.flush()
}

/// Read one `[u32 LE len][body]` frame from the stream.
///
/// Mirrors the service's reader: a length prefix above [`MAX_FRAME`] is a
/// protocol violation (`InvalidData`) rather than an allocation, so a torn
/// or hostile frame kills the connection (worker re-dials) instead of the
/// process.
fn read_frame(stream: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame {len} bytes exceeds {MAX_FRAME}"),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

/// 1 MiB cap on a single JSON frame, matching the service's own reader.
const MAX_FRAME: usize = 1 << 20;

/// The worker loop: dial, serve requests in order, redial forever.
fn worker(
    socket: PathBuf,
    out: Receiver<(u64, Vec<u8>)>,
    resp: Sender<(u64, Response)>,
    shared: Arc<Shared>,
) {
    let mut backlog: VecDeque<(u64, Vec<u8>)> = VecDeque::new();
    loop {
        // Dial (keeping the queue drained while we wait for the service).
        let mut stream = loop {
            match UnixStream::connect(&socket) {
                Ok(s) => break s,
                Err(_) => {
                    while let Ok(x) = out.try_recv() {
                        backlog.push_back(x);
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        };

        // Serve this connection until it dies or reconnect() asks.
        loop {
            if shared.force_reconnect.swap(false, Ordering::Relaxed) {
                break;
            }
            let (id, body) = if let Some(x) = backlog.pop_front() {
                x
            } else {
                match out.recv_timeout(Duration::from_millis(50)) {
                    Ok(x) => x,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => return,
                }
            };

            // A reconnect() may have landed while this request waited in the
            // channel: redial first rather than write on a connection that
            // may already be dead.
            if shared.force_reconnect.swap(false, Ordering::Relaxed) {
                backlog.push_front((id, body));
                break;
            }

            if write_frame(&mut stream, &body).is_err() {
                backlog.push_front((id, body));
                break;
            }
            match read_frame(&mut stream) {
                Ok(buf) => {
                    let response = serde_json::from_slice::<Response>(&buf).unwrap_or_else(|e| {
                        Response::Error {
                            message: format!("unparseable service response: {e}"),
                        }
                    });
                    let _ = resp.send((id, response));
                }
                Err(_) => {
                    // The connection died mid-exchange; replay the request
                    // on the next dial rather than dropping it.
                    backlog.push_front((id, body));
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_frame_accepts_a_valid_frame() {
        let body = b"{\"ok\":true}";
        let mut frame = Vec::new();
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(body);
        assert_eq!(read_frame(&mut &frame[..]).unwrap(), body);
    }

    #[test]
    fn read_frame_rejects_an_oversized_len_prefix() {
        // A torn stream could yield any u32; the cap keeps it from becoming
        // an allocation.
        let mut frame = ((MAX_FRAME as u32) + 1).to_le_bytes().to_vec();
        frame.extend_from_slice(&[0u8; 8]);
        let err = read_frame(&mut &frame[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
