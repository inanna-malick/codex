use super::*;

const GOLDEN_JSON: &str = "{\"producerId\":\"run-7/inbox-2/actor-3.1\",\"sequence\":9,\"purpose\":\"assignment\",\"mode\":\"queueOnly\",\"target\":{\"conversation\":\"00000000-0000-0000-0000-000000000004\",\"actor\":\"actor-3.1\",\"correlation\":\"request-5\"},\"payload\":[104,101,108,108,111],\"contentDigest\":\"282cff748dac084436730b60201fc26b7b9b4f2ccfbbbf4cbd6966e7fc4d5cd9\"}";

#[test]
fn golden_input_envelope_round_trips_and_rejects_changed_mode() {
    let mut envelope: Envelope = serde_json::from_str(GOLDEN_JSON).unwrap();
    assert_eq!(serde_json::to_string(&envelope).unwrap(), GOLDEN_JSON);
    assert_eq!(
        envelope.validate("00000000-0000-0000-0000-000000000004"),
        Ok(())
    );
    envelope.mode = Mode::StartOrSteer;
    assert_eq!(
        envelope.validate("00000000-0000-0000-0000-000000000004"),
        Err(EnvelopeError::Digest)
    );
}

#[test]
fn binding_rejects_version_identity_generation_and_nonce_independently() {
    let expected = ExpectedBinding {
        launch_id: "launch-1".into(),
        instance_id: "instance-2".into(),
        generation: 7,
        nonce: "nonce-3".into(),
    };
    let valid = Binding {
        protocol_version: INPUT_CONTROL_PROTOCOL_VERSION,
        launch_id: expected.launch_id.clone(),
        instance_id: expected.instance_id.clone(),
        generation: expected.generation,
        nonce: expected.nonce.clone(),
    };
    assert_eq!(expected.validate(&valid), Ok(()));
    for (mut binding, error) in [
        (
            {
                let mut value = valid.clone();
                value.protocol_version += 1;
                value
            },
            BindingError::Version,
        ),
        (
            {
                let mut value = valid.clone();
                value.instance_id.push('x');
                value
            },
            BindingError::Instance,
        ),
        (
            {
                let mut value = valid.clone();
                value.generation += 1;
                value
            },
            BindingError::Generation,
        ),
        (
            {
                let mut value = valid.clone();
                value.nonce.push('x');
                value
            },
            BindingError::Nonce,
        ),
    ] {
        assert_eq!(expected.validate(&binding), Err(error));
        binding = valid.clone();
        assert_eq!(expected.validate(&binding), Ok(()));
    }
}

#[test]
fn manifest_is_stable_machine_readable_compatibility_evidence() {
    let encoded = serde_json::to_value(Manifest::default()).unwrap();
    assert_eq!(encoded["hostProtocolVersion"], HOST_PROTOCOL_VERSION);
    assert_eq!(
        encoded["inputControlProtocolVersion"],
        INPUT_CONTROL_PROTOCOL_VERSION
    );
    assert_eq!(encoded["capabilities"].as_array().unwrap().len(), 4);
}

#[test]
fn command_and_workspace_boundaries_reject_unknown_wire_fields() {
    type Command = CommandRequest<serde_json::Value, String, serde_json::Value>;
    let command = serde_json::from_str::<Command>(
        r#"{"threadId":"thread-1","id":"job-2","operation":"cancel","extra":true}"#,
    );
    assert!(command.is_err());

    let workspace = serde_json::from_str::<WorkspacePublicationRequest>(
        r#"{"threadId":"thread-1","sequence":1,"operation":"begin","expectedIdentity":null}"#,
    )
    .unwrap();
    assert_eq!(workspace.sequence.get(), 1);
    assert_eq!(
        serde_json::to_string(&WorkspacePublicationReply::<String>::Busy).unwrap(),
        r#"{"status":"busy"}"#
    );
}
