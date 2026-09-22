//! Private wire contract shared by the matched Codex and Tidepool revisions.
//!
//! This crate contains representation, version, path, and size policy only. It
//! deliberately has no dependency on either runtime, actor scheduling, or the
//! Haskell bridge. Each side converts these values into its own authority types.

use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::num::NonZeroU64;

pub const HOST_PROTOCOL_VERSION: u32 = 5;
pub const INPUT_CONTROL_PROTOCOL_VERSION: u32 = 5;

pub const REGISTRATION_PATH: &str = "/v1/dynamic-tools/registration";
pub const SESSION_PATH: &str = "/v1/dynamic-tools/session";
pub const CALL_PATH: &str = "/v1/dynamic-tools/call";
pub const CANCEL_PATH: &str = "/v1/dynamic-tools/cancel";
pub const INTERRUPTED_PATH: &str = "/v1/dynamic-tools/interrupted";
pub const COMPLETED_PATH: &str = "/v1/dynamic-tools/completed";
pub const INPUT_PATH: &str = "/v1/input";
pub const INPUT_CONTROL_PATH: &str = "/v1/input/control";
pub const WORKSPACE_PUBLICATION_PATH: &str = "/v1/workspace/publication";
pub const COMMAND_PATH: &str = "/v1/commands";

pub const MAX_REGISTRATION_RESPONSE_BYTES: usize = 1024 * 1024;
pub const MAX_CALL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_COMMAND_REPLY_BYTES: usize = 800 * 1024;
pub const MAX_WORKSPACE_REPLY_BYTES: usize = 16 * 1024;
pub const MAX_INPUT_CONTROL_REPLY_BYTES: usize = 16 * 1024;
pub const MAX_INPUT_BYTES: usize = 256 * 1024;
pub const MAX_CORRELATION_BYTES: usize = 256;
pub const MAX_PRODUCER_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Manifest {
    pub host_protocol_version: u32,
    pub input_control_protocol_version: u32,
    pub max_registration_response_bytes: usize,
    pub max_call_response_bytes: usize,
    pub max_command_reply_bytes: usize,
    pub max_workspace_reply_bytes: usize,
    pub max_input_control_reply_bytes: usize,
    pub max_input_bytes: usize,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HostedRegistrationScope {
    PrimaryThread,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostedRegistration<Tool, Path> {
    pub protocol_version: u32,
    pub dynamic_tools: Vec<Tool>,
    pub scope: HostedRegistrationScope,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub input_control_socket: Option<Path>,
    #[serde(default)]
    pub launch_id: String,
    #[serde(default)]
    pub input_control_nonce: String,
}

fn option_none<T>() -> Option<T> {
    None
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostedSessionRequest<Text, Path> {
    pub protocol_version: u32,
    pub thread_id: Text,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub input_control_socket: Option<Path>,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub launch_id: Option<Text>,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub application_instance_id: Option<Text>,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub session_generation: Option<u64>,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub input_control_nonce: Option<Text>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostedCallRequest<Text, Arguments> {
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub context_call_id: Option<Text>,
    pub protocol_version: u32,
    pub thread_id: Text,
    pub turn_id: Text,
    pub call_id: Text,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub namespace: Option<Text>,
    pub tool: Text,
    pub arguments: Arguments,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostedCompletionRequest<Text> {
    pub protocol_version: u32,
    pub thread_id: Text,
    pub context_call_id: Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostedCancellationRequest<Text> {
    pub protocol_version: u32,
    pub thread_id: Text,
    pub turn_id: Text,
    pub call_id: Text,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub context_call_id: Option<Text>,
    #[serde(default = "option_none", skip_serializing_if = "Option::is_none")]
    pub namespace: Option<Text>,
    pub launch_id: Text,
    pub application_instance_id: Text,
    pub session_generation: u64,
    pub input_control_nonce: Text,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            host_protocol_version: HOST_PROTOCOL_VERSION,
            input_control_protocol_version: INPUT_CONTROL_PROTOCOL_VERSION,
            max_registration_response_bytes: MAX_REGISTRATION_RESPONSE_BYTES,
            max_call_response_bytes: MAX_CALL_RESPONSE_BYTES,
            max_command_reply_bytes: MAX_COMMAND_REPLY_BYTES,
            max_workspace_reply_bytes: MAX_WORKSPACE_REPLY_BYTES,
            max_input_control_reply_bytes: MAX_INPUT_CONTROL_REPLY_BYTES,
            max_input_bytes: MAX_INPUT_BYTES,
            capabilities: vec![
                "hostedRegistration".into(),
                "boundInputControl".into(),
                "commandOperations".into(),
                "workspacePublication".into(),
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Binding {
    pub protocol_version: u32,
    pub launch_id: String,
    pub instance_id: String,
    pub generation: u64,
    pub nonce: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Purpose {
    Bootstrap,
    Assignment,
    RequestUpdate,
    Notification,
    OperatorInput,
}

impl Purpose {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Assignment => "assignment",
            Self::RequestUpdate => "requestUpdate",
            Self::Notification => "notification",
            Self::OperatorInput => "operatorInput",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Mode {
    QueueOnly,
    StartOrSteer,
}

impl Mode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueueOnly => "queueOnly",
            Self::StartOrSteer => "startOrSteer",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Target {
    pub conversation: String,
    pub actor: String,
    pub correlation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Envelope {
    pub producer_id: String,
    pub sequence: u64,
    pub purpose: Purpose,
    pub mode: Mode,
    pub target: Target,
    pub payload: Vec<u8>,
    pub content_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "camelCase", deny_unknown_fields)]
pub enum InputControlRequest {
    Bind {
        binding: Binding,
    },
    Submit {
        binding: Binding,
        envelope: Envelope,
    },
    Query {
        binding: Binding,
        producer_id: String,
        sequence: u64,
    },
    Withdraw {
        binding: Binding,
        producer_id: String,
        sequence: u64,
    },
    Seal {
        binding: Binding,
        producer_id: String,
    },
    Acknowledge {
        binding: Binding,
        producer_id: String,
        through_sequence: u64,
    },
}

impl InputControlRequest {
    #[must_use]
    pub fn binding(&self) -> &Binding {
        match self {
            Self::Bind { binding }
            | Self::Submit { binding, .. }
            | Self::Query { binding, .. }
            | Self::Withdraw { binding, .. }
            | Self::Seal { binding, .. }
            | Self::Acknowledge { binding, .. } => binding,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Outcome {
    Admitted,
    Dispatching,
    Presented,
    Withdrawn,
    Rejected,
    Unknown,
    Compacted,
    EvidenceUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputControlResponse {
    pub binding: Binding,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandRequest<Spec, Stream, Position> {
    pub binding: Binding,
    pub thread_id: String,
    pub id: String,
    #[serde(flatten)]
    pub operation: CommandOperation<Spec, Stream, Position>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandOperation<Spec, Stream, Position> {
    Start { spec: Spec },
    Wait,
    Output { bytes: usize },
    Read { stream: Stream, position: Position },
    Input { text: String },
    CloseInput,
    Resize { rows: u16, columns: u16 },
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandState {
    Starting,
    Finished { exit_code: i32, cancelled: bool },
    Failed { detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "result",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CommandResponse<Output, Page> {
    State(CommandState),
    Output(Output),
    Page(Page),
    Acknowledged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspacePublicationRequest {
    pub binding: Binding,
    pub thread_id: String,
    pub sequence: NonZeroU64,
    pub operation: WorkspacePublicationOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_identity: Option<WorkspaceProcessIdentity>,
}

/// An operation result tied to the exact challenged native generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BoundReply<T> {
    pub binding: Binding,
    pub payload: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkspacePublicationOperation {
    Begin,
    Finish,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceProcessIdentity {
    pub pid: u32,
    pub start_ticks: u64,
    pub mount_namespace_inode: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase", deny_unknown_fields)]
pub enum WorkspacePublicationReply<Path> {
    Ready {
        pid: u32,
        #[serde(rename = "startTicks")]
        start_ticks: u64,
        #[serde(rename = "mountNamespaceInode")]
        mount_namespace_inode: u64,
        #[serde(rename = "cgroupPath")]
        cgroup_path: Path,
    },
    Settled,
    Busy,
    Conflict,
    Unavailable {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedBinding {
    pub launch_id: String,
    pub instance_id: String,
    pub generation: u64,
    pub nonce: String,
}

impl ExpectedBinding {
    pub fn validate(&self, actual: &Binding) -> Result<(), BindingError> {
        if actual.protocol_version != INPUT_CONTROL_PROTOCOL_VERSION {
            return Err(BindingError::Version);
        }
        if actual.launch_id != self.launch_id || actual.instance_id != self.instance_id {
            return Err(BindingError::Instance);
        }
        if actual.generation != self.generation {
            return Err(BindingError::Generation);
        }
        if actual.nonce != self.nonce {
            return Err(BindingError::Nonce);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingError {
    Version,
    Instance,
    Generation,
    Nonce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeError {
    Producer,
    Sequence,
    Payload,
    Target,
    Digest,
}

impl Envelope {
    /// Validate representation invariants before a runtime converts this wire
    /// value into an admission request.
    pub fn validate(&self, expected_conversation: &str) -> Result<(), EnvelopeError> {
        if self.producer_id.is_empty() || self.producer_id.len() > MAX_PRODUCER_BYTES {
            return Err(EnvelopeError::Producer);
        }
        if self.sequence == 0 {
            return Err(EnvelopeError::Sequence);
        }
        if self.payload.len() > MAX_INPUT_BYTES {
            return Err(EnvelopeError::Payload);
        }
        if self.target.conversation != expected_conversation {
            return Err(EnvelopeError::Target);
        }
        if self.content_digest != canonical_digest(self.mode, &self.target, &self.payload) {
            return Err(EnvelopeError::Digest);
        }
        Ok(())
    }
}

#[must_use]
pub fn canonical_digest(mode: Mode, target: &Target, payload: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tidepool-interactive-input-v1\0");
    digest.update([match mode {
        Mode::QueueOnly => 0,
        Mode::StartOrSteer => 1,
    }]);
    digest_field(&mut digest, target.conversation.as_bytes());
    digest_field(&mut digest, target.actor.as_bytes());
    match &target.correlation {
        Some(value) => {
            digest.update([1]);
            digest_field(&mut digest, value.as_bytes());
        }
        None => digest.update([0]),
    }
    digest_field(&mut digest, payload);
    let bytes: [u8; 32] = digest.finalize().into();
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn digest_field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
