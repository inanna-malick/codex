//! Private, versioned protocol shared only with the actor host input adapter.

use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

pub(super) const INPUT_CONTROL_PROTOCOL_VERSION: u32 = 4;
pub(super) const MAX_INPUT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Binding {
    pub protocol_version: u32,
    pub launch_id: String,
    pub instance_id: String,
    pub generation: u64,
    pub nonce: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum Purpose {
    Bootstrap,
    Assignment,
    RequestUpdate,
    Notification,
    OperatorInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum Mode {
    QueueOnly,
    StartOrSteer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Target {
    pub conversation: String,
    pub actor: String,
    pub correlation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Envelope {
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
pub(super) enum Request {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum Outcome {
    Admitted,
    Dispatching,
    Presented,
    Withdrawn,
    Rejected,
    Unknown,
    EvidenceUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Response {
    pub binding: Binding,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExpectedBinding {
    pub launch_id: String,
    pub instance_id: String,
    pub generation: u64,
    pub nonce: String,
}

impl ExpectedBinding {
    pub(super) fn validate(&self, actual: &Binding) -> Result<(), BindingError> {
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
pub(super) enum BindingError {
    Version,
    Instance,
    Generation,
    Nonce,
}

pub(super) struct ValidatedEnvelope {
    pub producer_id: String,
    pub sequence: u64,
    pub mode: Mode,
    pub target: Target,
    pub payload: Vec<u8>,
    pub content_digest: String,
}

impl Envelope {
    /// This is the only conversion toward queue admission. It verifies the
    /// bounded bytes, target thread, and exact host-compatible digest first.
    pub(super) fn validate(self, thread: ThreadId) -> Result<ValidatedEnvelope, ValidationError> {
        if self.producer_id.is_empty() || self.producer_id.len() > 512 {
            return Err(ValidationError::Producer);
        }
        if self.sequence == 0 {
            return Err(ValidationError::Sequence);
        }
        if self.payload.len() > MAX_INPUT_BYTES {
            return Err(ValidationError::Payload);
        }
        if self.target.conversation != thread.to_string() {
            return Err(ValidationError::Target);
        }
        let expected = canonical_digest(self.mode, &self.target, &self.payload);
        if self.content_digest != expected {
            return Err(ValidationError::Digest);
        }
        Ok(ValidatedEnvelope {
            producer_id: self.producer_id,
            sequence: self.sequence,
            mode: self.mode,
            target: self.target,
            payload: self.payload,
            content_digest: expected,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ValidationError {
    Producer,
    Sequence,
    Payload,
    Target,
    Digest,
}

fn canonical_digest(mode: Mode, target: &Target, payload: &[u8]) -> String {
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
mod tests {
    use super::*;

    const GOLDEN_JSON: &str = "{\"producerId\":\"run-7/inbox-2/actor-3.1\",\"sequence\":9,\"purpose\":\"assignment\",\"mode\":\"queueOnly\",\"target\":{\"conversation\":\"00000000-0000-0000-0000-000000000004\",\"actor\":\"actor-3.1\",\"correlation\":\"request-5\"},\"payload\":[104,101,108,108,111],\"contentDigest\":\"282cff748dac084436730b60201fc26b7b9b4f2ccfbbbf4cbd6966e7fc4d5cd9\"}";

    #[test]
    fn golden_vector_rejects_changed_canonical_mode_before_admission() {
        let mut envelope: Envelope = serde_json::from_str(GOLDEN_JSON).unwrap();
        assert_eq!(serde_json::to_string(&envelope).unwrap(), GOLDEN_JSON);
        let thread = ThreadId::from_string("00000000-0000-0000-0000-000000000004").unwrap();
        assert!(envelope.clone().validate(thread).is_ok());
        envelope.mode = Mode::StartOrSteer;
        assert_eq!(
            envelope.validate(thread).err(),
            Some(ValidationError::Digest)
        );
    }

    #[test]
    fn binding_rejects_stale_generation_and_wrong_nonce() {
        let expected = ExpectedBinding {
            launch_id: "launch-1".into(),
            instance_id: "instance-2".into(),
            generation: 7,
            nonce: "nonce-3".into(),
        };
        let mut actual = Binding {
            protocol_version: INPUT_CONTROL_PROTOCOL_VERSION,
            launch_id: expected.launch_id.clone(),
            instance_id: expected.instance_id.clone(),
            generation: expected.generation,
            nonce: expected.nonce.clone(),
        };
        assert_eq!(expected.validate(&actual), Ok(()));
        actual.generation -= 1;
        assert_eq!(expected.validate(&actual), Err(BindingError::Generation));
        actual.generation = expected.generation;
        actual.nonce = "wrong".into();
        assert_eq!(expected.validate(&actual), Err(BindingError::Nonce));
    }
}
