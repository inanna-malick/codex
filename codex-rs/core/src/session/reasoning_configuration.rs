//! Durable reasoning configuration for Responses Lite. The first update fixes
//! the request-level baseline; later updates change effort without rewriting it.

use super::Session;
use super::StepContext;
use crate::client::reasoning_effort_for_request;
use codex_history::CodexHarnessMetadata;
use codex_history::ResponseItemEnvelope;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use codex_protocol::models::ConfigurationReasoning;
use codex_protocol::models::ResponseItem;

pub(super) async fn record_for_step(session: &Session, step: &StepContext) -> Result<()> {
    if !step.settings.model_info.use_responses_lite {
        return Ok(());
    }
    let Some(effort) = step.settings.effective_reasoning_effort() else {
        return Ok(());
    };
    let effort = reasoning_effort_for_request(&step.settings.model_info, effort);
    // Custom model-defined values are supported, but must not turn a small
    // trusted control into an unbounded model-visible history item.
    if effort.as_str().len() > 128 {
        return Err(CodexErr::InvalidRequest(
            "reasoning effort exceeds the 128-byte configuration-update limit".to_string(),
        ));
    }
    let history = session.clone_history().await;
    let previous = history.annotated_items().iter().rev().find_map(|envelope| {
        if let ResponseItem::ConfigurationUpdate { reasoning } = &envelope.item
            && envelope
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.harness_authored_configuration)
        {
            Some(&reasoning.effort)
        } else {
            None
        }
    });
    if previous == Some(&effort) {
        return Ok(());
    }
    session
        .record_annotated_conversation_items(
            &step.turn,
            vec![ResponseItemEnvelope {
                item: ResponseItem::ConfigurationUpdate {
                    reasoning: ConfigurationReasoning { effort },
                },
                metadata: Some(CodexHarnessMetadata {
                    harness_authored_configuration: true,
                    ..Default::default()
                }),
            }],
        )
        .await;
    Ok(())
}
