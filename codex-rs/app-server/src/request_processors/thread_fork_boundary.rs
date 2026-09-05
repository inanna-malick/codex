use codex_app_server_protocol::JSONRPCErrorError;
use codex_protocol::models::ResponseItem;
use codex_rollout::RolloutItem;

pub(super) fn through_call(
    mut items: Vec<RolloutItem>,
    call_id: &str,
) -> Result<Vec<RolloutItem>, JSONRPCErrorError> {
    let mut matches = items.iter().enumerate().filter_map(|(index, item)| {
        let RolloutItem::ResponseItem(envelope) = item else {
            return None;
        };
        matches!(&envelope.item,
            ResponseItem::FunctionCall { call_id: id, .. }
            | ResponseItem::CustomToolCall { call_id: id, .. } if id == call_id
        )
        .then_some(index)
    });
    let Some(index) = matches.next() else {
        return Err(super::invalid_request(format!(
            "no durable invocation found for call id '{call_id}'"
        )));
    };
    if matches.next().is_some() {
        return Err(super::invalid_request(format!(
            "call id '{call_id}' is ambiguous in source history"
        )));
    }
    items.truncate(index + 1);
    Ok(items)
}
