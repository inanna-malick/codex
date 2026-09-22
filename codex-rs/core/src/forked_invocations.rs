use std::collections::HashSet;
use std::sync::Arc;

use codex_history::InitialHistory;
use codex_history::RolloutItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseItem;

use crate::context::ContextualUserFragment;
use crate::context::ForkedInvocation;

/// Close inherited invocations only after the entire frozen prefix. No source operation is
/// interrupted, completed, or dispatched by this transformation.
pub(crate) fn close(history: InitialHistory) -> InitialHistory {
    let mut items = match history {
        InitialHistory::New | InitialHistory::Cleared => return history,
        InitialHistory::Forked(items) => items,
        InitialHistory::Resumed(history) => Arc::unwrap_or_clone(history.history),
    };
    let completed: HashSet<_> = items
        .iter()
        .filter_map(|item| match item {
            RolloutItem::ResponseItem(envelope) => match &envelope.item {
                ResponseItem::FunctionCallOutput {
                    call_id: Some(call_id),
                    ..
                }
                | ResponseItem::ToolSearchOutput {
                    call_id: Some(call_id),
                    ..
                }
                | ResponseItem::CustomToolCallOutput { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    let closures: Vec<_> = items
        .iter()
        .filter_map(|item| {
            let RolloutItem::ResponseItem(envelope) = item else {
                return None;
            };
            let output = match &envelope.item {
                ResponseItem::ToolSearchCall {
                    call_id: Some(call_id),
                    ..
                } if !completed.contains(call_id.as_str()) => ResponseItem::ToolSearchOutput {
                    id: None,
                    call_id: Some(call_id.clone()),
                    status: "completed".to_string(),
                    execution: "client".to_string(),
                    tools: Vec::new(),
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::FunctionCall { call_id, .. }
                | ResponseItem::LocalShellCall {
                    call_id: Some(call_id),
                    ..
                } if !completed.contains(call_id.as_str()) => ResponseItem::FunctionCallOutput {
                    id: None,
                    call_id: Some(call_id.clone()),
                    name: None,
                    namespace: None,
                    output: FunctionCallOutputPayload::from_text(ForkedInvocation.render()),
                    internal_chat_message_metadata_passthrough: None,
                },
                ResponseItem::CustomToolCall { call_id, .. }
                    if !completed.contains(call_id.as_str()) =>
                {
                    ResponseItem::CustomToolCallOutput {
                        id: None,
                        call_id: call_id.clone(),
                        name: None,
                        output: FunctionCallOutputPayload::from_text(ForkedInvocation.render()),
                        internal_chat_message_metadata_passthrough: None,
                    }
                }
                _ => return None,
            };
            Some(RolloutItem::ResponseItem(output.into()))
        })
        .collect();
    items.extend(closures);
    InitialHistory::Forked(items)
}
