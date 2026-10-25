//! Shared test setup for the integration suites. Compiled per test binary;
//! only declare `mod common;` in tests that use these helpers.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::OnceLock;

use tines::{Request, Response};

/// One shared scratch setup per test binary: a temp dir that becomes both
/// XDG_STATE_HOME (fleet dirs) and a PATH dir holding a fake `tines-busd`.
/// Env is process-global, so it is set exactly once before any test touches
/// `fleet_dir` — a per-test `set_var` from parallel test threads would race.
#[allow(dead_code)]
pub fn env_with_fake_busd() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("tines-gui-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Fake tines-busd: create the socket named by argv[1], then stay
        // alive so the spawning Fleet keeps a live child.
        let fake = dir.join("tines-busd");
        std::fs::write(&fake, "#!/bin/sh\ntouch \"$1\"\nsleep 60\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let path = format!(
            "{}:{}",
            dir.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        // SAFETY: set once, before any fleet_dir call; this process is a
        // test binary that exits afterwards, so nothing else observes it.
        unsafe {
            std::env::set_var("XDG_STATE_HOME", &dir);
            std::env::set_var("PATH", path);
        }
        dir
    })
}

/// A fake `tines` service speaking the real wire framing
/// (`[u32 LE len][JSON]`), answering every request through a handler
/// closure and logging every request it receives. Serving runs on a
/// thread; dropping the service shuts it down.
#[allow(dead_code)]
pub struct FakeService {
    /// The socket the service is bound to (what the client dials).
    pub socket: PathBuf,
    requests: std::sync::Arc<std::sync::Mutex<Vec<Request>>>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    listener: Option<std::os::unix::net::UnixListener>,
    handle: Option<std::thread::JoinHandle<()>>,
}

#[allow(dead_code)]
impl FakeService {
    /// Bind `socket` and serve requests forever (until dropped).
    pub fn start<F>(socket: PathBuf, handler: F) -> FakeService
    where
        F: Fn(&Request) -> Response + Send + Sync + 'static,
    {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixListener;

        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        use std::time::Duration;
        // A restarted service finds the previous one's socket path still on
        // disk (Linux leaves the file behind), so remove the stale path
        // first — exactly what a real service does.
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();

        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let serve_listener = listener.try_clone().unwrap();
        let serve_requests = std::sync::Arc::clone(&requests);
        let serve_shutdown = std::sync::Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || {
            while !serve_shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                match serve_listener.accept() {
                    Ok((mut stream, _)) => {
                        // Timeout idle reads so `Drop`'s join can always
                        // finish: the serve thread would otherwise block in
                        // `read_exact` on a connection the client keeps
                        // open with nothing in flight.
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(250)));
                        // Serve this connection until it closes.
                        loop {
                            let mut len_buf = [0u8; 4];
                            if stream.read_exact(&mut len_buf).is_err() {
                                break;
                            }
                            let len = u32::from_le_bytes(len_buf) as usize;
                            let mut buf = vec![0u8; len];
                            if stream.read_exact(&mut buf).is_err() {
                                break;
                            }
                            let req: Request = match serde_json::from_slice(&buf) {
                                Ok(req) => req,
                                Err(e) => {
                                    let body = serde_json::to_vec(&Response::Error {
                                        message: format!("unparseable: {e}"),
                                    })
                                    .unwrap();
                                    let len = (body.len() as u32).to_le_bytes();
                                    let _ = stream.write_all(&len);
                                    let _ = stream.write_all(&body);
                                    continue;
                                }
                            };
                            let body = serde_json::to_vec(&handler(&req)).unwrap();
                            serve_requests.lock().unwrap().push(req);
                            let len = (body.len() as u32).to_le_bytes();
                            if stream.write_all(&len).is_err() || stream.write_all(&body).is_err() {
                                break;
                            }
                            let _ = stream.flush();
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // Keep the poll tight: the harness can burn dozens
                        // of frames in the time one 10ms sleep takes, and
                        // the client's round trip must land within the
                        // step budget.
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
        });

        FakeService {
            socket,
            requests,
            shutdown,
            listener: Some(listener),
            handle: Some(handle),
        }
    }

    /// True when any received request satisfies the predicate.
    pub fn any_request(&self, pred: impl Fn(&Request) -> bool) -> bool {
        self.requests.lock().unwrap().iter().any(pred)
    }
}

impl Drop for FakeService {
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.listener.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
