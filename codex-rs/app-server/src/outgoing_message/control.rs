use super::*;
use codex_app_server_protocol::ControlPendingEntry;
use codex_app_server_protocol::ControlPendingListParams;
use codex_app_server_protocol::ControlPendingListResponse;
use codex_app_server_protocol::ControlState;

const MAX_RECOVERY_REQUESTS: usize = 1024;

impl OutgoingMessageSender {
    pub(crate) fn with_control(mut self, control: Arc<crate::control::Control>) -> Self {
        self.control = control;
        self
    }

    pub(crate) async fn fence_pending_requests(&self) {
        let (mut pending, truncated) = {
            let mut callbacks = self.request_id_to_callback.lock().await;
            let truncated = callbacks.len() > MAX_RECOVERY_REQUESTS;
            let mut pending = Vec::new();
            for (id, entry) in callbacks.drain() {
                if pending.len() < MAX_RECOVERY_REQUESTS {
                    pending.push(describe_request(&id, &entry));
                }
                let _ = entry.callback.send(Err(crate::control::control_error(
                    "controller disconnected; external effect outcome is uncertain",
                )));
            }
            (pending, truncated)
        };
        pending.sort_by(|left, right| left.request_id.cmp(&right.request_id));
        *self.fenced_pending.lock().await = (pending, truncated);
    }

    pub(crate) async fn control_pending_list(
        &self,
        params: ControlPendingListParams,
    ) -> std::result::Result<ControlPendingListResponse, JSONRPCErrorError> {
        let state = self.control.status()?.state;
        let offset = params
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| crate::control::control_error("invalid pending cursor"))?;
        let limit = params.limit.unwrap_or(100).clamp(1, 100) as usize;
        let (entries, truncated) = if state == ControlState::Fenced {
            self.fenced_pending.lock().await.clone()
        } else {
            let callbacks = self.request_id_to_callback.lock().await;
            let mut entries = callbacks
                .iter()
                .map(|(id, entry)| describe_request(id, entry))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.request_id.cmp(&right.request_id));
            (entries, false)
        };
        let end = offset.saturating_add(limit).min(entries.len());
        let next_cursor = (end < entries.len()).then(|| end.to_string());
        Ok(ControlPendingListResponse {
            data: entries.into_iter().skip(offset).take(limit).collect(),
            next_cursor,
            state,
            truncated,
        })
    }
}

fn describe_request(id: &RequestId, entry: &PendingCallbackEntry) -> ControlPendingEntry {
    let value = serde_json::to_value(&entry.request).unwrap_or_default();
    let string = |value: &serde_json::Value| value.as_str().map(str::to_string);
    ControlPendingEntry {
        request_id: id.clone(),
        thread_id: entry.thread_id.map(|id| id.to_string()),
        method: string(&value["method"]).unwrap_or_default(),
        turn_id: string(&value["params"]["turnId"]),
        call_id: string(&value["params"]["callId"]),
    }
}
