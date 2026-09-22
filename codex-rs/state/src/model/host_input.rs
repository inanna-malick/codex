use codex_protocol::ThreadId;

/// Immutable host input identity. `producer_id` already binds run/inbox scope
/// and the exact actor incarnation; `sequence` is allocated by that inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostInputOperation {
    pub thread_id: ThreadId,
    pub producer_id: String,
    pub sequence: u64,
    pub purpose: String,
    pub mode: String,
    pub target_json: String,
    pub content_digest: String,
    pub payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostInputState {
    Ready,
    Dispatching,
    Presented,
    Withdrawn,
    Rejected,
    Unknown,
}

impl HostInputState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Dispatching => "dispatching",
            Self::Presented => "presented",
            Self::Withdrawn => "withdrawn",
            Self::Rejected => "rejected",
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "ready" => Ok(Self::Ready),
            "dispatching" => Ok(Self::Dispatching),
            "presented" => Ok(Self::Presented),
            "withdrawn" => Ok(Self::Withdrawn),
            "rejected" => Ok(Self::Rejected),
            "unknown" => Ok(Self::Unknown),
            _ => anyhow::bail!("invalid host input state: {value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostInputRecord {
    pub operation: HostInputOperation,
    pub state: HostInputState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostInputAdmission {
    Admitted(HostInputRecord),
    Existing(HostInputRecord),
    Conflict,
    ProducerSealed,
    Withdrawn,
    Compacted,
    AtCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostInputWithdrawal {
    Withdrawn(HostInputRecord),
    Existing(HostInputRecord),
    Unknown(HostInputRecord),
    Tombstoned,
}
