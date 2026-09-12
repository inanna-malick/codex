use codex_protocol::ThreadId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostToolCompletionKey {
    pub thread_id: ThreadId,
    pub context_call_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostToolCompletionState {
    Pending,
    ReconcilePending,
    Ready,
    Acknowledged,
    ReattachedWithoutCompletion,
    NotSubmitted,
}

impl HostToolCompletionState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::ReconcilePending => "reconcile_pending",
            Self::Ready => "ready",
            Self::Acknowledged => "acknowledged",
            Self::ReattachedWithoutCompletion => "reattached_without_completion",
            Self::NotSubmitted => "not_submitted",
        }
    }

    pub(crate) fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "reconcile_pending" => Ok(Self::ReconcilePending),
            "ready" => Ok(Self::Ready),
            "acknowledged" => Ok(Self::Acknowledged),
            "reattached_without_completion" => Ok(Self::ReattachedWithoutCompletion),
            "not_submitted" => Ok(Self::NotSubmitted),
            _ => anyhow::bail!("invalid host tool completion state: {value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostToolCompletionRegistration {
    New(HostToolCompletionRecord),
    Existing(HostToolCompletionRecord),
}

#[derive(Debug)]
pub enum HostToolCompletionError {
    Capacity,
    Conflict {
        current: HostToolCompletionState,
        requested: HostToolCompletionState,
    },
    Missing,
    Storage {
        retryable: bool,
        source: anyhow::Error,
    },
}

impl HostToolCompletionError {
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Storage {
                retryable: true,
                ..
            }
        )
    }

    pub(crate) fn storage(source: impl Into<anyhow::Error>) -> Self {
        let source = source.into();
        let retryable = source.chain().any(|cause| {
            let Some(error) = cause.downcast_ref::<sqlx::Error>() else {
                return false;
            };
            match error {
                sqlx::Error::Database(error) => error
                    .code()
                    .is_some_and(|code| sqlite_code_is_retryable(code.as_ref())),
                sqlx::Error::Io(error) => matches!(
                    error.kind(),
                    std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::WouldBlock
                ),
                sqlx::Error::PoolTimedOut | sqlx::Error::WorkerCrashed => true,
                _ => false,
            }
        });
        Self::Storage { retryable, source }
    }
}

fn sqlite_code_is_retryable(code: &str) -> bool {
    if let Ok(numeric) = code.parse::<i32>() {
        return matches!(numeric & 0xff, 5 | 6);
    }
    let code = code.to_ascii_lowercase();
    code == "sqlite_busy"
        || code.starts_with("sqlite_busy_")
        || code == "sqlite_locked"
        || code.starts_with("sqlite_locked_")
}

impl std::fmt::Display for HostToolCompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capacity => write!(f, "too many unresolved hosted tool completions"),
            Self::Conflict { current, requested } => write!(
                f,
                "host tool completion cannot transition from {} to {}",
                current.as_str(),
                requested.as_str()
            ),
            Self::Missing => write!(f, "host tool completion is not registered"),
            Self::Storage { source, .. } => {
                write!(f, "failed to access host tool completion ledger: {source}")
            }
        }
    }
}

impl std::error::Error for HostToolCompletionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage { source, .. } => Some(source.as_ref()),
            Self::Capacity | Self::Conflict { .. } | Self::Missing => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostToolCompletionRecord {
    pub key: HostToolCompletionKey,
    pub state: HostToolCompletionState,
}
