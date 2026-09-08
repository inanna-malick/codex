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
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::sync::RwLock;

const ENABLE_ENV: &str = "CODEX_WORKSPACE_SNAPSHOTS";

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
}

/// Held across an actual filesystem mutation, not an outer hosted tool call.
pub struct MutationGuard {
    _guard: OwnedRwLockReadGuard<()>,
}

/// Held until the mount publisher finishes or its owning control connection ends.
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
        })
    }

    pub async fn mutation(&self) -> MutationGuard {
        MutationGuard {
            _guard: self.gate.clone().read_owned().await,
        }
    }

    pub fn try_snapshot(&self) -> SnapshotAdmission {
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

    pub fn cgroup_path(&self) -> &Path {
        &self.scope.path
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
