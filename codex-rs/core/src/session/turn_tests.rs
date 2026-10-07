use super::*;
use codex_extension_api::ExtensionData;
use codex_extension_api::TurnItemContributor;
use codex_protocol::ResponseItemId;
use codex_protocol::items::AgentMessageContent;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tracing_subscriber::prelude::*;

struct RewriteAgentMessageContributor;

impl TurnItemContributor for RewriteAgentMessageContributor {
    fn contribute<'a>(
        &'a self,
        _thread_store: &'a ExtensionData,
        _turn_store: &'a ExtensionData,
        item: &'a mut TurnItem,
    ) -> codex_extension_api::ExtensionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if let TurnItem::AgentMessage(agent_message) = item {
                agent_message.content = vec![AgentMessageContent::Text {
                    text: "plan contributed assistant text".to_string(),
                }];
            }
            Ok(())
        })
    }
}

fn assistant_output_text(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: Some(ResponseItemId::with_suffix("msg", "1")),
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

#[test]
fn post_sampling_token_estimate_is_disabled_by_always_on_sinks() {
    let feedback = codex_feedback::CodexFeedback::new();
    let subscriber = tracing_subscriber::registry()
        .with(feedback.logger_layer())
        .with(tracing_subscriber::fmt::layer().with_filter(codex_state::log_db::default_filter()));

    static METADATA: tracing::Metadata<'static> = tracing::metadata! {
        name: "post sampling token estimate filter probe",
        target: POST_SAMPLING_TOKEN_ESTIMATE_TARGET,
        level: tracing::Level::TRACE,
        fields: &["turn_id", "estimated_token_count", "message"],
        callsite: &CALLSITE,
        kind: tracing::metadata::Kind::EVENT.hint(),
    };
    static CALLSITE: tracing::callsite::DefaultCallsite =
        tracing::callsite::DefaultCallsite::new(&METADATA);

    assert!(tracing::Subscriber::register_callsite(&subscriber, &METADATA).is_never());
}

#[tokio::test]
async fn plan_mode_uses_contributed_turn_item_for_last_agent_message() {
    let (mut session, turn_context) = crate::session::tests::make_session_and_context().await;
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_item_contributor(Arc::new(RewriteAgentMessageContributor));
    session.services.extensions = Arc::new(builder.build());
    let turn_store = ExtensionData::new(turn_context.sub_id.clone());
    let mut state = PlanModeStreamState::new(&turn_context.sub_id);
    let mut last_agent_message = None;
    let item = assistant_output_text("original assistant text");

    let step_context = StepContext::for_test(Arc::new(turn_context));
    let handled = handle_assistant_item_done_in_plan_mode(
        &session,
        &step_context,
        &turn_store,
        &item,
        &mut state,
        /*previously_active_item*/ None,
        &mut last_agent_message,
    )
    .await;

    assert!(handled);
    assert_eq!(
        last_agent_message.as_deref(),
        Some("plan contributed assistant text")
    );
}

#[test]
fn realtime_user_verification_notice_excludes_request_payload() {
    let event = EventMsg::ElicitationRequest(codex_protocol::approvals::ElicitationRequestEvent {
        turn_id: None,
        server_name: "private-server-name".to_string(),
        id: codex_protocol::mcp::RequestId::String("private-request-id".to_string()),
        request: codex_protocol::approvals::ElicitationRequest::UserVerification {
            meta: None,
            title: "private-title".to_string(),
            description: "private-description".to_string(),
            challenge: "private-challenge".to_string(),
        },
    });
    assert_eq!(
        realtime_text_for_event(&event),
        Some(RealtimeEventText::Handoff(
            "<user_verification_notice>User verification is required. Please respond in the app.</user_verification_notice>".to_string(),
            None,
        )),
    );
}

#[tokio::test]
async fn citation_contained_plan_does_not_replace_the_completed_visible_plan() {
    let text = "<proposed_plan>\nvisible step\n</proposed_plan>\n<oai-mem-citation>\n<proposed_plan>\ncitation example\n</proposed_plan>\n</oai-mem-citation>";
    let chunks = text.chars().map(|ch| ch.to_string()).collect::<Vec<_>>();
    let (session, turn, rx) = crate::session::tests::make_session_and_context_with_rx().await;
    let mut parsers = AssistantMessageStreamParsers::new(true);
    let mut state = PlanModeStreamState::new(&turn.sub_id);
    let item_id = "plan-source";
    for (index, chunk) in chunks.iter().enumerate() {
        let parsed = if index == 0 {
            parsers.seed_item_text(item_id, chunk)
        } else {
            parsers.parse_delta(item_id, chunk)
        };
        emit_streamed_assistant_text_delta(&session, &turn, Some(&mut state), item_id, parsed)
            .await;
    }
    let tail = parsers.finish_item(item_id);
    emit_streamed_assistant_text_delta(&session, &turn, Some(&mut state), item_id, tail).await;
    assert!(parsers.finish_item(item_id).is_empty());
    let item = ResponseItem::Message {
        id: Some(ResponseItemId::from_server(item_id.to_owned())),
        role: "assistant".to_owned(),
        content: vec![ContentItem::OutputText {
            text: text.to_owned(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };
    maybe_complete_plan_item_from_message(&session, &turn, &mut state, &item).await;
    let mut streamed = String::new();
    let mut completed = Vec::new();
    while let Ok(event) = rx.try_recv() {
        match event.msg {
            EventMsg::PlanDelta(delta) => streamed.push_str(&delta.delta),
            EventMsg::ItemCompleted(done) => {
                if let TurnItem::Plan(plan) = done.item {
                    completed.push(plan.text);
                }
            }
            _ => {}
        }
    }
    assert_eq!(streamed, "visible step\n");
    assert_eq!(completed, vec!["visible step\n"]);
}
