use std::collections::BTreeSet;
use std::sync::Arc;

use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;
use codex_app_server_protocol::ServerNotification;

use codex_app_server_protocol::RawResponseItemCompletedNotification;
use codex_rollout::CompletedCallBoundary;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;
use serde::Serialize;

use super::HostDynamicTools;

#[derive(Debug, Default)]
pub(super) struct HostToolCompletions {
    batch: Option<CompletedCallBoundary>,
    batch_calls: BTreeSet<String>,
    pending: BTreeSet<String>,
    ready: BTreeSet<String>,
}

impl HostToolCompletions {
    pub(super) fn register(&mut self, call_id: String) -> color_eyre::Result<()> {
        if self.pending.len() + self.ready.len() >= 256 {
            color_eyre::eyre::bail!("too many unacknowledged hosted tool completions");
        }
        self.pending.insert(call_id.clone());
        if self.batch.is_some() {
            self.batch_calls.insert(call_id);
        }
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
        state.batch_calls.clear();
        let calls = state.pending.clone();
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

    #[cfg(test)]
    pub(crate) async fn acknowledge_then_execute_stale_reattach(
        &self,
        notification: &RawResponseItemCompletedNotification,
        call_id: &str,
    ) -> color_eyre::Result<bool> {
        let work = self
            .prepare_completion(notification)?
            .ok_or_else(|| color_eyre::eyre::eyre!("completion did not close its call boundary"))?;
        let turn_prepared_reattach = self.prepare_turn(&notification.thread_id).is_some();
        self.execute_settlement(work).await?;
        self.execute_settlement(SettlementWork::Reattach {
            thread_id: notification.thread_id.clone(),
            calls: BTreeSet::from([call_id.to_owned()]),
        })
        .await?;
        Ok(turn_prepared_reattach)
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
                if state.pending.contains(id) {
                    state.batch_calls.insert(id.to_owned());
                }
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
                let batch_calls = std::mem::take(&mut state.batch_calls);
                let ready = batch_calls
                    .into_iter()
                    .filter(|call| state.pending.remove(call))
                    .collect::<Vec<_>>();
                state.ready.extend(ready.iter().cloned());
                ready
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
                self.reattach_pending(&thread_id, &calls).await?;
                return Ok(());
            }
        };
        for call_id in ready {
            let key = completion_key(&thread_id, &call_id)?;
            self.state_db
                .thread_queue()
                .mark_host_tool_completion_ready(&key)
                .await
                .map_err(store_error)?;
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
            self.state_db
                .thread_queue()
                .acknowledge_host_tool_completion(&key)
                .await
                .map_err(store_error)?;
            self.completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .ready
                .remove(&call_id);
        }
        Ok(())
    }

    pub(super) async fn recover_completions_before_reattach(
        &self,
        thread_id: codex_protocol::ThreadId,
    ) -> color_eyre::Result<BTreeSet<String>> {
        let unresolved = self
            .state_db
            .thread_queue()
            .list_unresolved_host_tool_completions(thread_id)
            .await
            .map_err(store_error)?;
        if unresolved.is_empty() {
            return Ok(BTreeSet::new());
        }
        let metadata = self
            .state_db
            .get_thread(thread_id)
            .await
            .map_err(store_error)?
            .ok_or_else(|| color_eyre::eyre::eyre!("hosted completion thread is not persisted"))?;
        let (items, history_thread, parse_errors) =
            RolloutRecorder::load_rollout_items(&metadata.rollout_path).await?;
        if history_thread != Some(thread_id) || parse_errors != 0 {
            color_eyre::eyre::bail!(
                "hosted completion history identity or integrity is unconfirmed"
            );
        }
        let thread = thread_id.to_string();
        let mut pending = BTreeSet::new();
        let mut ready = Vec::new();
        for record in unresolved {
            match record.state {
                codex_state::HostToolCompletionState::Ready => {
                    ready.push(record.key.context_call_id);
                }
                codex_state::HostToolCompletionState::Pending => {
                    if completion_is_closed(&items, &record.key.context_call_id)? {
                        ready.push(record.key.context_call_id);
                    } else {
                        pending.insert(record.key.context_call_id);
                    }
                }
                codex_state::HostToolCompletionState::Acknowledged
                | codex_state::HostToolCompletionState::ReattachedWithoutCompletion => {}
            }
        }
        if !ready.is_empty() {
            self.execute_settlement(SettlementWork::Acknowledge {
                thread_id: thread.clone(),
                calls: ready,
            })
            .await?;
        }
        Ok(pending)
    }

    async fn reattach_pending(
        &self,
        thread_id: &str,
        calls: &BTreeSet<String>,
    ) -> color_eyre::Result<()> {
        if calls.is_empty() {
            return Ok(());
        }
        let primary = codex_protocol::ThreadId::from_string(thread_id)?;
        let durable_pending = self
            .state_db
            .thread_queue()
            .list_unresolved_host_tool_completions(primary)
            .await
            .map_err(store_error)?
            .into_iter()
            .filter_map(|record| {
                (record.state == codex_state::HostToolCompletionState::Pending)
                    .then_some(record.key.context_call_id)
            })
            .collect::<BTreeSet<_>>();
        // Settlement work is queued after classification. Reconcile it before
        // the host-side effect so a completion now owned by acknowledgment is
        // never abandoned by a stale reattach.
        let calls = {
            let state = self
                .completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            calls
                .intersection(&state.pending)
                .filter(|call| durable_pending.contains(*call))
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        if calls.is_empty() {
            return Ok(());
        }
        let (input_socket, binding_state) = {
            let control = self.input_control.lock().await;
            match control.as_ref() {
                Some(control) => (Some(control.socket.clone()), Some(control.binding_state())),
                None => (None, None),
            }
        };
        let binding = match binding_state {
            Some(binding) => Some(binding.lock().await.clone()),
            None => None,
        };
        super::send_session(
            &self.client,
            primary,
            input_socket.as_ref(),
            binding.as_ref(),
        )
        .await?;
        self.settle_reattached_pending(thread_id, &calls).await
    }

    pub(super) async fn settle_reattached_pending(
        &self,
        thread_id: &str,
        calls: &BTreeSet<String>,
    ) -> color_eyre::Result<()> {
        for call_id in calls {
            self.state_db
                .thread_queue()
                .mark_host_tool_completion_reattached(&completion_key(thread_id, call_id)?)
                .await
                .map_err(store_error)?;
        }
        let mut state = self
            .completions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending.retain(|id| !calls.contains(id));
        state.ready.retain(|id| !calls.contains(id));
        Ok(())
    }
}

fn completion_key(
    thread_id: &str,
    context_call_id: &str,
) -> color_eyre::Result<codex_state::HostToolCompletionKey> {
    Ok(codex_state::HostToolCompletionKey {
        thread_id: codex_protocol::ThreadId::from_string(thread_id)?,
        context_call_id: context_call_id.to_owned(),
    })
}

fn completion_is_closed(items: &[RolloutItem], call_id: &str) -> color_eyre::Result<bool> {
    let mut boundary = CompletedCallBoundary::new(call_id);
    let mut closed = false;
    for item in items {
        if let RolloutItem::ResponseItem(item) = item {
            closed |= boundary
                .observe(&item.item)
                .map_err(|error| color_eyre::eyre::eyre!(error))?;
        }
    }
    Ok(closed)
}

fn store_error(error: anyhow::Error) -> color_eyre::Report {
    color_eyre::eyre::eyre!("{error:#}")
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
