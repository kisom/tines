//! The IPC client against a fake service speaking the real wire framing
//! (`[u32 LE len][JSON]`), from a test-local UnixListener.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use tines::{Request, Response};
use tines_gui::ipc_client::IpcClient;

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tines-gui-ipc-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn read_frame(stream: &mut UnixStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).unwrap();
    buf
}

fn write_frame(stream: &mut UnixStream, body: &[u8]) {
    let len = (body.len() as u32).to_le_bytes();
    stream.write_all(&len).unwrap();
    stream.write_all(body).unwrap();
    stream.flush().unwrap();
}

/// Answer one Observe request with an Observe response, then end the
/// connection (EOF).
fn answer_once(socket: &Path) {
    // A restarted server finds the previous server's socket path still on
    // disk (Linux leaves the file behind when a bound listener drops), so
    // it removes the stale path first — exactly what a real service does.
    let _ = std::fs::remove_file(socket);
    let (mut stream, _) = UnixListener::bind(socket)
        .unwrap()
        .accept()
        .expect("server accepts");
    let body = read_frame(&mut stream);
    // Request-agnostic: the client-side tests only assert the id pairing,
    // so any well-formed request gets an Observe-shaped response.
    let _: Request = serde_json::from_slice(&body).expect("well-formed request");
    write_frame(
        &mut stream,
        &serde_json::to_vec(&Response::Observe { id: "00".into() }).unwrap(),
    );
    // Dropping the stream here closes the connection: the client sees EOF.
}

#[test]
fn client_roundtrips_observe() {
    let dir = scratch_dir("rt");
    let socket = dir.join("s.sock");

    let server_socket = socket.clone();
    let server = thread::spawn(move || answer_once(&server_socket));

    let client = IpcClient::spawn(socket.clone()).expect("client spawns");
    let id = client
        .request(Request::Observe {
            topic: "ab".into(),
            payload: "cd".into(),
            to: None,
            lifetime_ms: None,
        })
        .expect("request queues");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some((got_id, resp)) = client.try_recv() {
            assert_eq!(got_id, id, "response must carry the request id");
            assert!(
                matches!(resp, Response::Observe { .. }),
                "expected Observe response, got {resp:?}"
            );
            break;
        }
        assert!(Instant::now() < deadline, "no response arrived");
        thread::sleep(Duration::from_millis(20));
    }
    server.join().unwrap();
}

#[test]
fn client_survives_server_restart() {
    let dir = scratch_dir("restart");
    let socket = dir.join("s.sock");

    // First server: one round trip, then it goes away.
    let server_socket = socket.clone();
    let first = thread::spawn(move || answer_once(&server_socket));

    let client = IpcClient::spawn(socket.clone()).expect("client spawns");
    let first_id = client
        .request(Request::Events)
        .expect("first request queues");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some((got, _)) = client.try_recv() {
            assert_eq!(got, first_id);
            break;
        }
        assert!(Instant::now() < deadline, "no response to first request");
        thread::sleep(Duration::from_millis(20));
    }
    first.join().unwrap();

    // Restart: a fresh listener on the same path answers the next request.
    client.reconnect();
    let server_socket = socket.clone();
    let second = thread::spawn(move || answer_once(&server_socket));

    let second_id = client
        .request(Request::Events)
        .expect("second request queues");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some((got, _)) = client.try_recv() {
            assert_eq!(got, second_id, "ids keep increasing across restarts");
            break;
        }
        assert!(Instant::now() < deadline, "no response after restart");
        thread::sleep(Duration::from_millis(20));
    }
    second.join().unwrap();
}
