use std::collections::BTreeSet;

use codex_app_server_protocol::RawResponseItemCompletedNotification;
use codex_rollout::CompletedCallBoundary;
use serde::Serialize;

use super::HostDynamicTools;

#[derive(Debug, Default)]
pub(super) struct HostToolCompletions {
    batch: Option<CompletedCallBoundary>,
    pending: BTreeSet<String>,
    ready: BTreeSet<String>,
}

impl HostToolCompletions {
    pub(super) fn register(&mut self, call_id: String) -> color_eyre::Result<()> {
        if self.pending.len() + self.ready.len() >= 256 {
            color_eyre::eyre::bail!("too many unacknowledged hosted tool completions");
        }
        self.pending.insert(call_id);
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompletionRequest<'a> {
    protocol_version: u32,
    thread_id: &'a str,
    context_call_id: &'a str,
}

impl HostDynamicTools {
    pub(crate) async fn settle_turn(&self, thread_id: &str) -> color_eyre::Result<()> {
        let Some(primary) = self
            .primary_thread_id()
            .filter(|id| id.to_string() == thread_id)
        else {
            return Ok(());
        };
        let unacknowledged = {
            let mut state = self
                .completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.batch = None;
            !state.pending.is_empty() || !state.ready.is_empty()
        };
        if unacknowledged {
            // An interrupted or failed turn may have no durable tool result.
            // Reattachment settles its queued host effects instead of inventing one.
            self.attach_primary(primary).await?;
            *self
                .completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                HostToolCompletions::default();
        }
        Ok(())
    }

    pub(crate) async fn observe_completion(
        &self,
        notification: &RawResponseItemCompletedNotification,
    ) -> color_eyre::Result<()> {
        if self
            .primary_thread_id()
            .is_none_or(|id| id.to_string() != notification.thread_id)
        {
            return Ok(());
        }
        let ready = {
            let mut state = self
                .completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.batch.is_none()
                && let Some(id) = CompletedCallBoundary::invocation_id(&notification.item)
            {
                state.batch = Some(CompletedCallBoundary::new(id));
            }
            let closed = state
                .batch
                .as_mut()
                .map(|batch| batch.observe(&notification.item))
                .transpose()
                .map_err(|error| color_eyre::eyre::eyre!(error))?
                .unwrap_or(false);
            if closed {
                state.batch = None;
                let pending = std::mem::take(&mut state.pending);
                state.ready.extend(pending);
            }
            state.ready.iter().cloned().collect::<Vec<_>>()
        };
        for call_id in ready {
            #[cfg(unix)]
            for attempt in 0..3 {
                let result = self
                    .client
                    .post("http://localhost/v1/dynamic-tools/completed")
                    .timeout(super::CONTROL_REQUEST_TIMEOUT)
                    .json(&CompletionRequest {
                        protocol_version: super::PROTOCOL_VERSION,
                        thread_id: &notification.thread_id,
                        context_call_id: &call_id,
                    })
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status);
                match result {
                    Ok(_) => break,
                    Err(error) if attempt == 2 => return Err(error.into()),
                    Err(error) => {
                        tracing::warn!(%error, attempt, "retrying hosted tool completion acknowledgement");
                        tokio::time::sleep(std::time::Duration::from_millis(100 << attempt)).await;
                    }
                }
            }
            self.completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .ready
                .remove(&call_id);
        }
        Ok(())
    }
}
