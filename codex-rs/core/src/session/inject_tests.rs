use crate::session::tests::make_session_and_context_with_rx;
use codex_features::Feature;
use codex_history::CodexHarnessMetadata;
use codex_history::ResponseItemEnvelope;
use codex_history::RolloutItem;
use codex_protocol::models::ConfigurationReasoning;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::EventMsg;
use pretty_assertions::assert_eq;

fn function_output(call_id: &str, text: &str) -> ResponseItem {
    ResponseItem::FunctionCallOutput {
        id: None,
        call_id: Some(call_id.to_string()),
        name: None,
        namespace: None,
        output: FunctionCallOutputPayload::from_text(text.to_string()),
        internal_chat_message_metadata_passthrough: None,
    }
}

#[tokio::test]
async fn recovery_injection_racing_late_result_records_one_terminal_output_per_call() {
    let (session, turn_context, _rx_event) = make_session_and_context_with_rx().await;
    let recovered_items = vec![function_output("recovered-call", "recovered")];
    let late_items = vec![function_output("recovered-call", "late")];
    let first = session.inject_client_response_items(recovered_items, turn_context.as_ref());
    let late = session.record_conversation_items(turn_context.as_ref(), &late_items);

    tokio::join!(first, late);

    assert_eq!(
        session
            .clone_history()
            .await
            .raw_items()
            .filter_map(super::super::terminal_tool_call_id)
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        vec!["recovered-call".to_string()]
    );

    let (session, turn_context, _rx_event) = make_session_and_context_with_rx().await;
    session
        .record_conversation_items(
            turn_context.as_ref(),
            &[function_output("late-first", "late")],
        )
        .await;
    session
        .inject_client_response_items(
            vec![function_output("late-first", "recovered")],
            turn_context.as_ref(),
        )
        .await;
    assert_eq!(
        session
            .clone_history()
            .await
            .raw_items()
            .filter_map(super::super::terminal_tool_call_id)
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        vec!["late-first".to_string()]
    );
}

#[tokio::test]
async fn harness_authored_configuration_updates_preserve_metadata_and_resume() {
    let (session, turn_context, rx_event) = make_session_and_context_with_rx().await;
    assert!(!session.enabled(Feature::RetainClientDeveloperMessages));

    let expected = ResponseItemEnvelope {
        item: ResponseItem::ConfigurationUpdate {
            reasoning: ConfigurationReasoning {
                effort: ReasoningEffort::High,
            },
        },
        metadata: Some(CodexHarnessMetadata {
            harness_authored_configuration: true,
            ..Default::default()
        }),
    };
    session
        .record_annotated_conversation_items(&turn_context, vec![expected.clone()])
        .await;

    let recorded = session.clone_history().await.into_annotated_items();
    assert_eq!(recorded, vec![expected.clone()]);
    let mut raw_items = Vec::new();
    while let Ok(event) = rx_event.try_recv() {
        if let EventMsg::RawResponseItem(event) = event.msg {
            raw_items.push(event.item);
        }
    }
    assert_eq!(raw_items, vec![expected.item]);

    let rollout_items = recorded
        .iter()
        .cloned()
        .map(RolloutItem::ResponseItem)
        .collect::<Vec<_>>();
    let reconstructed = session
        .reconstruct_history_from_rollout(&turn_context, &rollout_items)
        .await;
    assert_eq!(reconstructed.history, recorded);
}
