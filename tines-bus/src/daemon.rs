//! The bus daemon: a single-threaded, nonblocking hub that routes envelopes
//! between connected clients.
//!
//! # Protocol
//!
//! Every message is `[u32 LE length][payload]`, the length counting payload
//! bytes only (bus-local; the core frames inside are untouched).
//!
//! - **Hello** (client→daemon, first message): payload = 16-byte `NodeId`.
//!   A hello for an already-registered id replaces the old connection (the
//!   old socket is closed).
//! - **Data** (client→daemon): payload = `[16B from][16B to][frame]`. The
//!   daemon drops (and counts on stderr) any message whose `from` is not that
//!   socket's registered id, or whose `to` is not registered; otherwise it
//!   forwards the envelope verbatim to `to`'s socket. Frames longer than
//!   [`MAX_FRAME`] close the connection.
//! - **Control** (daemon→client): payload = `[1B kind][16B NodeId]`; kind
//!   `0 = joined`, `1 = left`; sent to every other registered client on join,
//!   and to all remaining on leave.
//!
//! EOF or an error from a client's socket is a leave plus close; the daemon
//! itself stays up. No frame content is decoded beyond the routing header.

use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::time::Duration;

use tines_core::consts::MAX_FRAME;
use tines_core::NodeId;

/// Routing-header size (from + to) of a data envelope.
const HEADER_BYTES: usize = 32;

/// Longest envelope payload the daemon will buffer or forward.
const MAX_PAYLOAD: usize = MAX_FRAME + HEADER_BYTES;

fn id_of(bytes: &[u8]) -> NodeId {
    let mut id = [0u8; 16];
    id.copy_from_slice(bytes);
    id
}

fn control(kind: u8, id: NodeId) -> Vec<u8> {
    let mut env = Vec::with_capacity(4 + 17);
    env.extend_from_slice(&17u32.to_le_bytes());
    env.push(kind);
    env.extend_from_slice(&id);
    env
}

/// Read one envelope's payload from `s`; `Ok(None)` means clean EOF.
pub(crate) fn read_envelope(s: &mut UnixStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    let n = s.read(&mut len_buf)?;
    if n == 0 {
        return Ok(None);
    }
    if n != 4 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "short envelope length header",
        ));
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_PAYLOAD {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "envelope over the size cap",
        ));
    }
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload)?;
    Ok(Some(payload))
}

struct Conn {
    id: Option<NodeId>,
    stream: UnixStream,
    buf: Vec<u8>,
    /// Set to evict this connection on the next apply pass.
    close_pending: bool,
}

/// A queued action from processing one message.
enum Action {
    /// Close the connection at `idx`; if it was registered, announce a leave.
    Close { idx: usize },
    /// Write a prepared envelope to the connection at `to`.
    Forward { to: usize, env: Vec<u8> },
}

/// Serve the bus on `socket` until the process dies.
///
/// A stale socket file from a previous run is removed first so `bind` cannot
/// fail on it.
pub fn serve(socket: impl AsRef<Path>) -> io::Result<()> {
    let socket = socket.as_ref().to_path_buf();
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    listener.set_nonblocking(true)?;

    let mut conns: Vec<Conn> = Vec::new();
    loop {
        // Accept everything pending.
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    conns.push(Conn {
                        id: None,
                        stream,
                        buf: Vec::new(),
                        close_pending: false,
                    });
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }

        // Read + parse each connection, queueing actions.
        let mut actions: Vec<Action> = Vec::new();
        for i in 0..conns.len() {
            let (closed, msgs) = {
                let c = &mut conns[i];
                let mut closed = false;
                let mut chunk = [0u8; 4096];
                loop {
                    match c.stream.read(&mut chunk) {
                        Ok(0) => {
                            closed = true;
                            break;
                        }
                        Ok(n) => {
                            c.buf.extend_from_slice(&chunk[..n]);
                            if n < chunk.len() {
                                break;
                            }
                        }
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(_) => {
                            closed = true;
                            break;
                        }
                    }
                }
                // Split complete envelopes out of the buffer.
                let mut msgs = Vec::new();
                loop {
                    if c.buf.len() < 4 {
                        break;
                    }
                    let len = u32::from_le_bytes([c.buf[0], c.buf[1], c.buf[2], c.buf[3]]) as usize;
                    if len > MAX_PAYLOAD {
                        // Oversized envelope: protocol violation.
                        closed = true;
                        break;
                    }
                    if c.buf.len() < 4 + len {
                        break;
                    }
                    msgs.push(c.buf[4..4 + len].to_vec());
                    c.buf.drain(..4 + len);
                }
                (closed, msgs)
            };
            // Process complete messages even when the connection is being
            // closed: a burst hello+oversized-data must still register the
            // id so the leave is announced to the right peers.
            for msg in msgs {
                process_msg(i, &msg, &mut conns, &mut actions);
            }
            if closed {
                actions.push(Action::Close { idx: i });
            }
        }

        apply_actions(&mut actions, &mut conns);

        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Handle one envelope from the connection at `idx`.
fn process_msg(idx: usize, payload: &[u8], conns: &mut [Conn], actions: &mut Vec<Action>) {
    let (before, rest) = conns.split_at_mut(idx);
    let (me, after) = rest.split_at_mut(1);
    let me = &mut me[0];

    match me.id {
        None => {
            // Unregistered sockets may only hello.
            if payload.len() != 16 {
                actions.push(Action::Close { idx });
                return;
            }
            let id = id_of(payload);
            // A hello for an already-registered id replaces the old
            // connection (silently: peers never see a flap).
            let is_new = !before.iter().chain(after.iter()).any(|c| c.id == Some(id));
            if !is_new {
                for c in before.iter_mut().chain(after.iter_mut()) {
                    if c.id == Some(id) {
                        c.close_pending = true;
                    }
                }
            }
            me.id = Some(id);
            if is_new {
                let env = control(0, id);
                for (j, c) in before
                    .iter()
                    .enumerate()
                    .chain(after.iter().enumerate().map(|(k, c)| (k + idx + 1, c)))
                {
                    if c.id.is_some() {
                        actions.push(Action::Forward {
                            to: j,
                            env: env.clone(),
                        });
                    }
                }
                // ...and the newcomer learns everyone already here.
                for c in before.iter().chain(after.iter()) {
                    if let Some(existing) = c.id {
                        actions.push(Action::Forward {
                            to: idx,
                            env: control(0, existing),
                        });
                    }
                }
            }
        }
        Some(my_id) => {
            if payload.len() == 16 {
                // Repeat hello on an already-registered socket: no-op.
                return;
            }
            if payload.len() < 32 {
                actions.push(Action::Close { idx });
                return;
            }
            let from = id_of(&payload[0..16]);
            let to = id_of(&payload[16..32]);
            let frame_len = payload.len() - 32;
            if from != my_id {
                // The daemon certifies `from`: never route a forgery.
                eprintln!("tines-busd: dropped: from mismatch on registered socket");
                return;
            }
            if frame_len > MAX_FRAME {
                actions.push(Action::Close { idx });
                return;
            }
            // `position()` on the chain is the actual index only for peers
            // before the sender; after the sender it is one short.
            let to_idx = before.iter().position(|c| c.id == Some(to)).or_else(|| {
                after
                    .iter()
                    .position(|c| c.id == Some(to))
                    .map(|k| k + idx + 1)
            });
            match to_idx {
                Some(j) => {
                    let mut env = Vec::with_capacity(4 + payload.len());
                    env.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                    env.extend_from_slice(payload);
                    actions.push(Action::Forward { to: j, env });
                }
                None => eprintln!("tines-busd: dropped: unknown destination"),
            }
        }
    }
}

/// Run queued actions: forwards first, then closes (announcing leaves).
fn apply_actions(actions: &mut [Action], conns: &mut Vec<Conn>) {
    // Forwards (only to live, registered connections).
    for a in actions.iter() {
        if let Action::Forward { to, env } = a {
            if let Some(c) = conns.get_mut(*to) {
                if !c.close_pending && c.id.is_some() && c.stream.write_all(env).is_err() {
                    c.close_pending = true;
                }
            }
        }
    }

    // Closes. A registered connection that leaves (EOF, error, protocol
    // violation) is announced to everyone remaining; an evicted duplicate
    // already has `close_pending` set and is silent.
    let mut announced: Vec<NodeId> = Vec::new();
    for a in actions.iter() {
        if let Action::Close { idx } = a {
            if let Some(c) = conns.get_mut(*idx) {
                c.close_pending = true;
                if let Some(id) = c.id {
                    announced.push(id);
                }
            }
        }
    }
    if !announced.is_empty() {
        let envs: Vec<Vec<u8>> = announced.iter().map(|id| control(1, *id)).collect();
        for env in envs {
            for c in conns.iter_mut() {
                if c.close_pending || c.id.is_none() {
                    continue;
                }
                if c.stream.write_all(&env).is_err() {
                    c.close_pending = true;
                }
            }
        }
    }
    conns.retain(|c| !c.close_pending);
}
