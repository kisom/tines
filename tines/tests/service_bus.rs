//! End-to-end bus fleet test: two real `tines` service processes converge
//! over a live `tines-busd` — the sim-fleet deliverable without docker.
//!
//! Node A observes a broadcast; node B learns it over the bus (belief, store
//! list) and both see the contact change.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tines::{Request, Response};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "tines-service-bus-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn node_id_hex(n: u8) -> String {
    format!("{n:02x}").repeat(16)
}

fn spawn_tines(config_path: &Path, dir: &Path) -> Child {
    let stderr = std::fs::File::create(dir.join("stderr.log")).unwrap();
    Command::new(env!("CARGO_BIN_EXE_tines"))
        .arg(config_path)
        .stderr(Stdio::from(stderr))
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn tines service")
}

fn connect(path: &Path, deadline: Instant) -> UnixStream {
    loop {
        if let Ok(s) = UnixStream::connect(path) {
            return s;
        }
        assert!(
            Instant::now() < deadline,
            "failed to connect to {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn rpc(stream: &mut UnixStream, req: &Request) {
    let body = serde_json::to_vec(req).expect("serialize request");
    let len = (body.len() as u32).to_le_bytes();
    stream.write_all(&len).unwrap();
    stream.write_all(&body).unwrap();
    stream.flush().unwrap();
}

fn read_response(stream: &mut UnixStream) -> Response {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).unwrap();
    serde_json::from_slice(&buf).expect("valid response")
}

/// Poll `poll_once` until `pred` accepts a response, or fail after `deadline`.
fn poll_until(
    mut poll_once: impl FnMut() -> Response,
    pred: impl Fn(&Response) -> bool,
    deadline: Instant,
) -> Response {
    loop {
        let r = poll_once();
        if pred(&r) {
            return r;
        }
        assert!(Instant::now() < deadline, "timed out polling the service");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn two_services_converge_over_bus() {
    let dir = unique_dir("converge");
    let bus_socket = dir.join("bus.sock");

    // The bus daemon runs in-process on a thread (a thread in the test
    // process, not an external binary, so the test has no CARGO_BIN_EXE
    // dependency); poll until it has bound its socket.
    let daemon_socket = bus_socket.clone();
    std::thread::spawn(move || {
        let _ = tines_bus::daemon::serve(&daemon_socket);
    });
    {
        let mut bound = UnixStream::connect(&bus_socket);
        let start = Instant::now();
        while bound.is_err() && start.elapsed() < Duration::from_secs(5) {
            std::thread::sleep(Duration::from_millis(20));
            bound = UnixStream::connect(&bus_socket);
        }
        assert!(bound.is_ok(), "bus daemon did not bind its socket");
    }

    // Write both configs.
    let mk_config = |node: &str, log: &str, sock: &str| -> String {
        serde_json::json!({
            "node_id": node,
            "log_path": dir.join(log).to_str().unwrap(),
            "socket_path": dir.join(sock).to_str().unwrap(),
            "trusted_clock": "system",
            "transport": "bus",
            "bus_socket": bus_socket.to_str().unwrap(),
        })
        .to_string()
    };
    let cfg_a = dir.join("a.json");
    let cfg_b = dir.join("b.json");
    std::fs::write(&cfg_a, mk_config(&node_id_hex(1), "a.log", "a.sock")).unwrap();
    std::fs::write(&cfg_b, mk_config(&node_id_hex(2), "b.log", "b.sock")).unwrap();

    // The generated configs must parse as the lib-level Config type.
    let _: tines::config::Config =
        serde_json::from_str(&std::fs::read_to_string(&cfg_a).unwrap()).unwrap();

    let mut a_proc = spawn_tines(&cfg_a, &dir);
    let mut b_proc = spawn_tines(&cfg_b, &dir);

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut a = connect(&dir.join("a.sock"), deadline);
    let mut b = connect(&dir.join("b.sock"), deadline);

    let topic = hex("status");
    let payload = hex("ping");

    // A observes a broadcast.
    rpc(
        &mut a,
        &Request::Observe {
            topic: topic.clone(),
            payload: payload.clone(),
            to: None,
            lifetime_ms: None,
        },
    );
    match read_response(&mut a) {
        Response::Observe { .. } => {}
        other => panic!("expected Observe response, got {other:?}"),
    }

    // B's belief converges over the bus.
    poll_until(
        || {
            rpc(
                &mut b,
                &Request::Belief {
                    topic: topic.clone(),
                },
            );
            read_response(&mut b)
        },
        |r| matches!(r, Response::Belief { obs: Some(o) } if o.payload == payload),
        deadline,
    );

    // B's store listing contains it too.
    rpc(&mut b, &Request::List);
    match read_response(&mut b) {
        Response::List { observations, .. } => {
            assert!(
                observations
                    .iter()
                    .any(|o| o.payload == payload && o.topic == topic),
                "observation missing from B's store list: {observations:?}"
            );
        }
        other => panic!("expected List response, got {other:?}"),
    }

    // Both nodes saw the contact change.
    for (stream, expected_peer) in [(&mut a, node_id_hex(2)), (&mut b, node_id_hex(1))] {
        poll_until(
            || {
                rpc(stream, &Request::Events);
                read_response(stream)
            },
            |r| {
                matches!(r, Response::Events { contacts, .. }
                    if contacts.iter().any(|c| c.joined && c.peer == expected_peer))
            },
            deadline,
        );
    }

    drop(a);
    drop(b);
    let _ = a_proc.kill();
    let _ = b_proc.kill();
    let _ = a_proc.wait();
    let _ = b_proc.wait();
}

fn hex(bytes: &str) -> String {
    bytes
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
