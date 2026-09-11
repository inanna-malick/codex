use codex_protocol::ThreadId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostToolCompletionKey {
    pub thread_id: ThreadId,
    pub context_call_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostToolCompletionState {
    Pending,
    Ready,
    Acknowledged,
    ReattachedWithoutCompletion,
}

impl HostToolCompletionState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Acknowledged => "acknowledged",
            Self::ReattachedWithoutCompletion => "reattached_without_completion",
        }
    }

    pub(crate) fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "ready" => Ok(Self::Ready),
            "acknowledged" => Ok(Self::Acknowledged),
            "reattached_without_completion" => Ok(Self::ReattachedWithoutCompletion),
            _ => anyhow::bail!("invalid host tool completion state: {value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostToolCompletionRecord {
    pub key: HostToolCompletionKey,
    pub state: HostToolCompletionState,
}
