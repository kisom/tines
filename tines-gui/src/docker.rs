//! Docker orchestration: build the runtime image and drive one container
//! per node. Every command runs through `std::process::Command` with
//! pinned argv vectors — nothing is ever shell-interpolated.

use std::io;
use std::path::PathBuf;
use std::process::{Command, Output};

use crate::config::fleet_dir;

/// A docker container id (the value `docker run -d` prints).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContainerId(pub String);

/// Failures the docker module can report.
#[derive(Debug)]
pub enum DockerError {
    /// The `docker` binary is not on PATH.
    NotInstalled,
    /// The docker process could not be spawned.
    Spawn(io::Error),
    /// Docker ran but exited nonzero.
    Exit {
        /// The exit code.
        code: i32,
        /// The process's captured stderr.
        stderr: String,
    },
}

impl std::fmt::Display for DockerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DockerError::NotInstalled => write!(f, "docker is not installed or not on PATH"),
            DockerError::Spawn(e) => write!(f, "failed to spawn docker: {e}"),
            DockerError::Exit { code, stderr } => {
                write!(f, "docker exited {code}: {}", stderr.trim())
            }
        }
    }
}

impl std::error::Error for DockerError {}

/// The docker frontend. A unit struct: docker state lives in the daemon,
/// not here.
pub struct Docker;

impl Docker {
    /// The pinned runtime image tag.
    pub fn image() -> &'static str {
        "tines-stack:latest"
    }

    /// Build the runtime image from the repo root — the crate's parent
    /// directory, baked in at compile time (`CARGO_MANIFEST_DIR` points at
    /// `tines-gui/`).
    pub fn build_image(&self) -> Result<(), DockerError> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("tines-gui lives one level below the repo root")
            .to_path_buf();
        let argv: Vec<String> = vec![
            "docker".into(),
            "build".into(),
            "-f".into(),
            "Dockerfile.tines".into(),
            "-t".into(),
            Self::image().into(),
            ".".into(),
        ];
        let mut cmd = spawn(&argv)?;
        cmd.current_dir(root);
        run(&mut cmd)?;
        Ok(())
    }

    /// The exact argv for `start_bt`. Exposed for tests and debugging;
    /// [`Self::start_bt`] runs this vector verbatim.
    pub fn start_bt_argv(&self, name: &str, fleet: &str) -> Vec<String> {
        vec![
            "docker".into(),
            "run".into(),
            "-d".into(),
            "--name".into(),
            format!("tines-bt-{name}"),
            "--privileged".into(),
            "--net=host".into(),
            "-v".into(),
            "/run/dbus/system_bus_socket:/run/dbus/system_bus_socket".into(),
            "-v".into(),
            format!("{}:/data", node_dir(fleet, name).display()),
            Self::image().into(),
            "tines".into(),
            "/data/config.json".into(),
        ]
    }

    /// The exact argv for `start_sim`. Exposed for tests and debugging;
    /// [`Self::start_sim`] runs this vector verbatim.
    pub fn start_sim_argv(&self, name: &str, fleet: &str) -> Vec<String> {
        vec![
            "docker".into(),
            "run".into(),
            "-d".into(),
            "--name".into(),
            format!("tines-sim-{name}"),
            "-v".into(),
            format!("{}:/data", node_dir(fleet, name).display()),
            Self::image().into(),
            "tines".into(),
            "/data/config.json".into(),
        ]
    }

    /// The exact argv for `stop`. Exposed for tests and debugging.
    pub fn stop_argv(&self, id: &ContainerId) -> Vec<String> {
        vec!["docker".into(), "stop".into(), id.0.clone()]
    }

    /// The exact argv for `remove`. Exposed for tests and debugging.
    pub fn remove_argv(&self, id: &ContainerId) -> Vec<String> {
        vec!["docker".into(), "rm".into(), id.0.clone()]
    }

    /// The exact argv for `logs`. Exposed for tests and debugging.
    pub fn logs_argv(&self, id: &ContainerId, tail: u32) -> Vec<String> {
        vec![
            "docker".into(),
            "logs".into(),
            "--tail".into(),
            tail.to_string(),
            id.0.clone(),
        ]
    }

    /// Start a bt-mode node container: privileged, host networking, the
    /// system D-Bus socket (for the host Bluetooth adapter), and the node's
    /// fleet dir mounted at `/data`. Returns the container id.
    pub fn start_bt(&self, name: &str, fleet: &str) -> Result<ContainerId, DockerError> {
        let out = run_output(&self.start_bt_argv(name, fleet))?;
        Ok(ContainerId(out.trim().to_string()))
    }

    /// Start a sim-mode node container: same shape, no privileges or host
    /// networking. Returns the container id.
    pub fn start_sim(&self, name: &str, fleet: &str) -> Result<ContainerId, DockerError> {
        let out = run_output(&self.start_sim_argv(name, fleet))?;
        Ok(ContainerId(out.trim().to_string()))
    }

    /// Stop a container.
    pub fn stop(&self, id: &ContainerId) -> Result<(), DockerError> {
        run(&mut spawn(&self.stop_argv(id))?)
    }

    /// Remove a (stopped) container.
    pub fn remove(&self, id: &ContainerId) -> Result<(), DockerError> {
        run(&mut spawn(&self.remove_argv(id))?)
    }

    /// The container's recent logs (stdout and stderr merged), newest
    /// `tail` lines last.
    pub fn logs(&self, id: &ContainerId, tail: u32) -> Result<String, DockerError> {
        run_output(&self.logs_argv(id, tail))
    }

    /// Whether `docker inspect` succeeds for the id — i.e. the container
    /// exists (the inspect exit status is the pin).
    pub fn is_running(&self, id: &ContainerId) -> bool {
        match Command::new("docker").arg("inspect").arg(&id.0).output() {
            Ok(Output { status, .. }) => status.success(),
            Err(_) => false,
        }
    }
}

/// `<fleet_dir>/<name>` — the dir mounted at `/data` inside the container.
fn node_dir(fleet: &str, name: &str) -> PathBuf {
    fleet_dir(fleet).join(name)
}

/// Run a command expecting success, capturing stdout.
fn run_output(argv: &[String]) -> Result<String, DockerError> {
    let out = spawn(argv)?.output().map_err(DockerError::Spawn)?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(DockerError::Exit {
            code: out.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

/// Run a command expecting success, output inherited.
fn run(cmd: &mut Command) -> Result<(), DockerError> {
    let status = cmd.status().map_err(DockerError::Spawn)?;
    if status.success() {
        Ok(())
    } else {
        Err(DockerError::Exit {
            code: status.code().unwrap_or(-1),
            stderr: String::new(),
        })
    }
}

/// The docker binary or a `NotInstalled` error.
fn spawn(argv: &[String]) -> Result<Command, DockerError> {
    if Command::new("docker").arg("--version").output().is_err() {
        return Err(DockerError::NotInstalled);
    }
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    Ok(cmd)
}
