//! Linux workspace mutation admission and kernel ownership of command descendants.
//!
//! This is opt-in for a managed workspace. A publication guard excludes new
//! command admission and native filesystem mutations, then checks the writer
//! cgroup rather than inferring quiescence from shell or output-stream exit.

use std::fs::File;
use std::future::Future;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::sync::RwLock;

const ENABLE_ENV: &str = "CODEX_WORKSPACE_SNAPSHOTS";

#[path = "workspace_publication.rs"]
mod publication;
pub use publication::PublicationAdmission;

pub enum Availability {
    Disabled,
    Ready(Arc<WorkspaceAdmission>),
    Unavailable(String),
}

/// One managed native process shares admission across its threads/sessions.
/// Failure to establish custody disables snapshot admission, not native tools.
pub fn availability() -> &'static Availability {
    static OWNER: OnceLock<Availability> = OnceLock::new();
    OWNER.get_or_init(|| {
        if std::env::var_os(ENABLE_ENV).is_none() {
            return Availability::Disabled;
        }
        match WorkspaceAdmission::create() {
            Ok(owner) => Availability::Ready(Arc::new(owner)),
            Err(error) => Availability::Unavailable(error.to_string()),
        }
    })
}

pub struct WorkspaceAdmission {
    gate: Arc<RwLock<()>>,
    scope: Arc<WriterScope>,
    publication: std::sync::Mutex<publication::Publication>,
    cwd: PathBuf,
    identity: ProcessIdentity,
    external_executor: OnceLock<OwnedRwLockReadGuard<()>>,
}

pub struct ProcessIdentity {
    pub start_ticks: u64,
    pub mount_namespace_inode: u64,
}

/// Held across an actual filesystem mutation, not an outer hosted tool call.
pub struct MutationGuard {
    _guard: OwnedRwLockReadGuard<()>,
}

/// Held until the mount publisher confirms a settled writable view. Losing a
/// control connection alone is not evidence that publication has stopped.
pub struct SnapshotGuard {
    _guard: OwnedRwLockWriteGuard<()>,
    _scope: Arc<WriterScope>,
}

pub enum SnapshotAdmission {
    Ready(SnapshotGuard),
    Busy,
    Unavailable(io::Error),
}

impl WorkspaceAdmission {
    fn create() -> io::Result<Self> {
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let start_ticks = stat
            .rsplit_once(')')
            .and_then(|(_, fields)| fields.split_whitespace().nth(19))
            .ok_or_else(|| io::Error::other("process start time unavailable"))?
            .parse()
            .map_err(io::Error::other)?;
        let identity = ProcessIdentity {
            start_ticks,
            mount_namespace_inode: std::fs::metadata("/proc/self/ns/mnt")?.ino(),
        };
        let membership = std::fs::read_to_string("/proc/self/cgroup")?;
        let relative = membership
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .ok_or_else(|| io::Error::other("unified cgroup membership unavailable"))?;
        let relative = Path::new(relative)
            .strip_prefix("/")
            .map_err(io::Error::other)?;
        if relative
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err(io::Error::other(
                "cgroup membership is outside the visible hierarchy",
            ));
        }
        let root = Path::new("/sys/fs/cgroup").join(relative);
        // Exclusive creation refuses a retained directory from a reused PID.
        // A pathname never substitutes for the opened cgroup control file.
        let path = root.join(format!("codex-writers-{}", std::process::id()));
        std::fs::create_dir(&path)?;
        let scope = WriterScope {
            join: match File::options().write(true).open(path.join("cgroup.procs")) {
                Ok(file) => file,
                Err(error) => {
                    let _ = std::fs::remove_dir(&path);
                    return Err(error);
                }
            },
            events: match File::open(path.join("cgroup.events")) {
                Ok(file) => file,
                Err(error) => {
                    let _ = std::fs::remove_dir(&path);
                    return Err(error);
                }
            },
            path,
        };
        // Reject unsupported or inaccessible accounting before admitting tools.
        scope.populated()?;
        Ok(Self {
            gate: Arc::new(RwLock::new(())),
            scope: Arc::new(scope),
            publication: std::sync::Mutex::new(publication::Publication::default()),
            cwd: std::env::current_dir()?,
            identity,
            external_executor: OnceLock::new(),
        })
    }

    pub async fn mutation(&self) -> MutationGuard {
        MutationGuard {
            _guard: self.gate.clone().read_owned().await,
        }
    }

    pub fn try_snapshot(&self) -> SnapshotAdmission {
        if self.external_executor.get().is_some() {
            return SnapshotAdmission::Unavailable(io::Error::other(
                "workspace publication is unavailable after an external executor connection",
            ));
        }
        let Ok(guard) = self.gate.clone().try_write_owned() else {
            return SnapshotAdmission::Busy;
        };
        match self.scope.populated() {
            Ok(false) => SnapshotAdmission::Ready(SnapshotGuard {
                _guard: guard,
                _scope: self.scope.clone(),
            }),
            Ok(true) => SnapshotAdmission::Busy,
            Err(error) => SnapshotAdmission::Unavailable(error),
        }
    }

    pub async fn track_process<F: Future>(&self, future: F) -> F::Output {
        let _admission = self.mutation().await;
        COMMAND_SCOPE.scope(self.scope.clone(), future).await
    }

    async fn disable_for_external_executor(&self) {
        if self.external_executor.get().is_none() {
            // Wait for an already-admitted mount transition before exposing the
            // executor. Remote descendants may outlive its client connection,
            // so disconnection cannot establish that local publication is safe.
            let guard = self.gate.clone().read_owned().await;
            let _ = self.external_executor.set(guard);
        }
    }

    async fn spawn_command(
        &self,
        mut command: tokio::process::Command,
    ) -> io::Result<tokio::process::Child> {
        let _admission = self.mutation().await;
        let scope = self.scope.clone();
        // SAFETY: the callback uses only the retained descriptor and
        // async-signal-safe syscalls. Admission spans attachment and exec.
        unsafe { command.pre_exec(move || scope.enter_child()) };
        command.spawn()
    }

    pub fn process_identity(&self) -> &ProcessIdentity {
        &self.identity
    }

    pub fn cgroup_path(&self) -> &Path {
        &self.scope.path
    }
}

/// External execution cannot be accounted for by this process's writer cgroup.
/// Disable publication for the rest of this native process while keeping local
/// tools and external execution available. Existing publication settles first.
pub async fn disable_publication_for_external_executor() {
    if let Availability::Ready(owner) = availability() {
        owner.disable_for_external_executor().await;
    }
}

/// Wrap only executor commands. Long-lived orchestration transports do not
/// participate in this task-local scope and cannot block their own publication.
pub async fn track_process<F: Future>(future: F) -> F::Output {
    match availability() {
        Availability::Ready(owner) => owner.track_process(future).await,
        Availability::Disabled | Availability::Unavailable(_) => future.await,
    }
}

/// Admit a direct authored command, including shell startup scripts and hooks.
/// Ownership transfers to kernel descendant accounting before this returns;
/// the caller keeps its existing waiting, output, and cancellation behavior.
pub async fn spawn_command(
    mut command: tokio::process::Command,
) -> io::Result<tokio::process::Child> {
    match availability() {
        Availability::Ready(owner) => owner.spawn_command(command).await,
        Availability::Disabled | Availability::Unavailable(_) => command.spawn(),
    }
}

/// Capture an admitted command with Tokio's `Command::output` semantics.
pub async fn command_output(
    mut command: tokio::process::Command,
) -> io::Result<std::process::Output> {
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    spawn_command(command).await?.wait_with_output().await
}

tokio::task_local! {
    static COMMAND_SCOPE: Arc<WriterScope>;
}

pub(crate) fn current_scope() -> Option<Arc<WriterScope>> {
    COMMAND_SCOPE.try_with(Arc::clone).ok()
}

pub(crate) struct WriterScope {
    path: PathBuf,
    join: File,
    events: File,
}

impl WriterScope {
    fn populated(&self) -> io::Result<bool> {
        let mut buffer = [0u8; 256];
        let count = self.events.read_at(&mut buffer, 0)?;
        if count == buffer.len() {
            return Err(io::Error::other("oversized cgroup event record"));
        }
        let events = std::str::from_utf8(&buffer[..count]).map_err(io::Error::other)?;
        match events
            .lines()
            .find_map(|line| line.strip_prefix("populated "))
        {
            Some("0") => Ok(false),
            Some("1") => Ok(true),
            _ => Err(io::Error::other("invalid cgroup populated state")),
        }
    }

    /// Called in the child before exec, before any authored command can fork.
    /// Uses only a pre-opened descriptor and async-signal-safe syscalls.
    pub(crate) fn enter_child(&self) -> io::Result<()> {
        loop {
            // SAFETY: the scope retains a valid writable descriptor and the
            // static one-byte buffer is valid for the duration of write.
            let written = unsafe { libc::write(self.join.as_raw_fd(), b"0".as_ptr().cast(), 1) };
            if written == 1 {
                return Ok(());
            }
            if written < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
    }
}

impl Drop for WriterScope {
    fn drop(&mut self) {
        // rmdir succeeds only for an empty cgroup; never kill remaining work.
        let _ = std::fs::remove_dir(&self.path);
    }
}

#[cfg(test)]
#[path = "workspace_admission_tests.rs"]
mod tests;
