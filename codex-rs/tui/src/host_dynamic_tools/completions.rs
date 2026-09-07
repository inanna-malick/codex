use std::collections::BTreeSet;
use std::sync::Arc;

use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;
use codex_app_server_protocol::ServerNotification;

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
    // Classify boundaries in event order, before a subsequent hosted call can
    // register. Only network settlement runs off the UI loop.
    pub(crate) fn enqueue_settlement(
        self: &Arc<Self>,
        notification: &ServerNotification,
        events: &AppEventSender,
    ) {
        if self.is_disabled() {
            return;
        }
        let work = match notification {
            ServerNotification::RawResponseItemCompleted(item) => self.prepare_completion(item),
            ServerNotification::TurnCompleted(turn) => Ok(self.prepare_turn(&turn.thread_id)),
            _ => return,
        };
        let work = match work {
            Ok(Some(work)) => work,
            Ok(None) => return,
            Err(error) => {
                self.settlement_failed(error, events);
                return;
            }
        };
        let mut sender = self
            .settlement_sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sender = sender.get_or_insert_with(|| {
            let (sender, mut receiver) = tokio::sync::mpsc::channel::<SettlementWork>(256);
            let host = Arc::downgrade(self);
            let events = events.clone();
            tokio::spawn(async move {
                while let Some(work) = receiver.recv().await {
                    let Some(host) = host.upgrade() else { break };
                    if host.is_disabled() {
                        break;
                    }
                    if let Err(error) = host.execute_settlement(work).await {
                        host.settlement_failed(error, &events);
                        break;
                    }
                }
            });
            sender
        });
        if let Err(error) = sender.try_send(work) {
            self.settlement_failed(
                color_eyre::eyre::eyre!("host settlement queue unavailable: {error}"),
                events,
            );
        }
    }

    fn settlement_failed(&self, error: color_eyre::Report, events: &AppEventSender) {
        if self
            .disabled
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        tracing::warn!(error = ?error, "host dynamic tools disabled after settlement failure");
        events.send(AppEvent::InsertHistoryCell(Box::new(
            crate::history_cell::new_error_event(format!(
                "{} Completion error: {error:#}",
                super::DISABLED_MESSAGE
            )),
        )));
    }

    fn prepare_turn(&self, thread_id: &str) -> Option<SettlementWork> {
        if self.is_disabled()
            || self
                .primary_thread_id()
                .is_none_or(|id| id.to_string() != thread_id)
        {
            return None;
        }
        let mut state = self
            .completions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.batch = None;
        let calls = state
            .pending
            .union(&state.ready)
            .cloned()
            .collect::<BTreeSet<_>>();
        (!calls.is_empty()).then(|| SettlementWork::Reattach {
            thread_id: thread_id.to_owned(),
            calls,
        })
    }

    #[cfg(test)]
    pub(crate) async fn settle_turn(&self, thread_id: &str) -> color_eyre::Result<()> {
        if let Some(work) = self.prepare_turn(thread_id) {
            self.execute_settlement(work).await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn observe_completion(
        &self,
        notification: &RawResponseItemCompletedNotification,
    ) -> color_eyre::Result<()> {
        if let Some(work) = self.prepare_completion(notification)? {
            self.execute_settlement(work).await?;
        }
        Ok(())
    }

    fn prepare_completion(
        &self,
        notification: &RawResponseItemCompletedNotification,
    ) -> color_eyre::Result<Option<SettlementWork>> {
        if self.is_disabled()
            || self
                .primary_thread_id()
                .is_none_or(|id| id.to_string() != notification.thread_id)
        {
            return Ok(None);
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
                state.ready.extend(pending.iter().cloned());
                pending.into_iter().collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        };
        Ok((!ready.is_empty()).then(|| SettlementWork::Acknowledge {
            thread_id: notification.thread_id.clone(),
            calls: ready,
        }))
    }

    async fn execute_settlement(&self, work: SettlementWork) -> color_eyre::Result<()> {
        let (thread_id, ready) = match work {
            SettlementWork::Acknowledge { thread_id, calls } => (thread_id, calls),
            SettlementWork::Reattach { thread_id, calls } => {
                let outstanding = {
                    let state = self
                        .completions
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    calls
                        .iter()
                        .any(|id| state.pending.contains(id) || state.ready.contains(id))
                };
                if outstanding {
                    // Interrupted calls have no durable result boundary. Reattachment
                    // settles these effects without inventing a completion or replaying them.
                    let primary = codex_protocol::ThreadId::from_string(&thread_id)?;
                    self.attach_primary(primary).await?;
                    let mut state = self
                        .completions
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.pending.retain(|id| !calls.contains(id));
                    state.ready.retain(|id| !calls.contains(id));
                }
                return Ok(());
            }
        };
        for call_id in ready {
            if !self
                .completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .ready
                .contains(&call_id)
            {
                continue;
            }
            #[cfg(unix)]
            for attempt in 0..3 {
                let result = self
                    .client
                    .post("http://localhost/v1/dynamic-tools/completed")
                    .timeout(super::SETTLEMENT_REQUEST_TIMEOUT)
                    .json(&CompletionRequest {
                        protocol_version: super::PROTOCOL_VERSION,
                        thread_id: &thread_id,
                        context_call_id: &call_id,
                    })
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status);
                match result {
                    Ok(_) => break,
                    Err(error) if attempt == 2 => return Err(error.into()),
                    Err(error) => {
                        tracing::warn!(error = ?error, attempt, "retrying hosted tool completion acknowledgement");
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

#[derive(Debug)]
pub(super) enum SettlementWork {
    Acknowledge {
        thread_id: String,
        calls: Vec<String>,
    },
    Reattach {
        thread_id: String,
        calls: BTreeSet<String>,
    },
}
