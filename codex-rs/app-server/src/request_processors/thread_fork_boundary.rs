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

/// Preserve the first closed prefix after the real result, without synthetic items.
pub(super) fn after_call(
    mut items: Vec<RolloutItem>,
    call_id: &str,
) -> Result<Vec<RolloutItem>, JSONRPCErrorError> {
    let mut boundary = codex_rollout::CompletedCallBoundary::new(call_id);
    let mut end = None;
    for (index, item) in items.iter().enumerate() {
        if let RolloutItem::ResponseItem(item) = item
            && boundary
                .observe(&item.item)
                .map_err(super::invalid_request)?
        {
            end.get_or_insert(index + 1);
        }
    }
    let end = end.ok_or_else(|| {
        super::invalid_request(format!(
            "no completed tool boundary for call id '{call_id}'"
        ))
    })?;
    items.truncate(end);
    Ok(items)
}
