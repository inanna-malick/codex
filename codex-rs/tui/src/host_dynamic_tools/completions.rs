use std::collections::BTreeSet;
use std::sync::Arc;

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
    reconcile: BTreeSet<String>,
    pub(super) not_submitted: BTreeSet<String>,
}

impl HostToolCompletions {
    pub(super) fn forget(&mut self, call_id: &str) {
        self.pending.remove(call_id);
        self.ready.remove(call_id);
        self.reconcile.remove(call_id);
        self.not_submitted.remove(call_id);
    }
    pub(super) fn register(&mut self, call_id: String) {
        self.pending.insert(call_id.clone());
        if self.batch.is_some() {
            self.batch_calls.insert(call_id);
        }
    }

    pub(super) fn reconcile_call(&mut self, call_id: String) {
        self.pending.insert(call_id.clone());
        self.reconcile.insert(call_id);
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
    pub(super) async fn reconcile_completions(&self) -> color_eyre::Result<()> {
        if let Some(reason) = self
            .recovery
            .classification_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Err(super::recovery::Intervention(reason).into());
        }
        let Some(thread) = self.primary_thread_id() else {
            return Ok(());
        };
        let thread_id = thread.to_string();
        if self
            .recovery
            .history_pending
            .load(std::sync::atomic::Ordering::Acquire)
        {
            let pending = self.recover_completions_before_reattach(thread).await?;
            let mut state = self
                .completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.pending.extend(pending.iter().cloned());
            state.reconcile.extend(pending);
            self.recovery
                .history_pending
                .store(false, std::sync::atomic::Ordering::Release);
        }
        let (ready, reconcile, not_submitted) = {
            let state = self
                .completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.ready.clone(),
                state.reconcile.clone(),
                state.not_submitted.clone(),
            )
        };
        for call_id in not_submitted {
            self.state_db
                .thread_queue()
                .mark_host_tool_completion_not_submitted(&completion_key(&thread_id, &call_id)?)
                .await
                .map_err(store_error)?;
            self.completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .forget(&call_id);
        }
        // Classification is synchronous with native history events. Flush its
        // intent before transport; restart can also reconstruct it from rollout.
        for call_id in ready {
            let record = self
                .state_db
                .thread_queue()
                .mark_host_tool_completion_ready(&completion_key(&thread_id, &call_id)?)
                .await
                .map_err(store_error)?;
            if matches!(
                record.state,
                codex_state::HostToolCompletionState::Acknowledged
                    | codex_state::HostToolCompletionState::ReattachedWithoutCompletion
                    | codex_state::HostToolCompletionState::NotSubmitted
            ) {
                self.completions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .forget(&call_id);
            }
        }
        for call_id in reconcile {
            let record = self
                .state_db
                .thread_queue()
                .mark_host_tool_completion_reconcile(&completion_key(&thread_id, &call_id)?)
                .await
                .map_err(store_error)?;
            if matches!(
                record.state,
                codex_state::HostToolCompletionState::Acknowledged
                    | codex_state::HostToolCompletionState::ReattachedWithoutCompletion
                    | codex_state::HostToolCompletionState::NotSubmitted
            ) {
                self.completions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .forget(&call_id);
            }
        }
        let records = self
            .state_db
            .thread_queue()
            .list_unresolved_host_tool_completions(thread)
            .await
            .map_err(store_error)?;
        let mut deferred: Option<color_eyre::Report> = None;
        for record in records {
            let result = match record.state {
                codex_state::HostToolCompletionState::Ready => {
                    self.execute_settlement(SettlementWork::Acknowledge {
                        thread_id: thread_id.clone(),
                        calls: vec![record.key.context_call_id],
                    })
                    .await
                }
                codex_state::HostToolCompletionState::ReconcilePending => {
                    self.reconcile_interrupted(&thread_id, &record.key.context_call_id)
                        .await
                }
                codex_state::HostToolCompletionState::Pending
                | codex_state::HostToolCompletionState::Acknowledged
                | codex_state::HostToolCompletionState::NotSubmitted
                | codex_state::HostToolCompletionState::ReattachedWithoutCompletion => Ok(()),
            };
            if let Err(error) = result {
                let new_is_intervention = error
                    .downcast_ref::<super::recovery::Intervention>()
                    .is_some();
                let existing_is_intervention = deferred.as_ref().is_some_and(|error| {
                    error
                        .downcast_ref::<super::recovery::Intervention>()
                        .is_some()
                });
                if new_is_intervention || !existing_is_intervention {
                    deferred = Some(error);
                }
            }
        }
        match deferred {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn reconcile_interrupted(
        &self,
        thread_id: &str,
        call_id: &str,
    ) -> color_eyre::Result<()> {
        #[derive(serde::Deserialize)]
        #[serde(tag = "status", rename_all = "camelCase")]
        enum Receipt {
            Settled,
            Pending,
            Recovered {
                reply: codex_app_server_protocol::DynamicToolCallResponse,
            },
        }
        #[cfg(unix)]
        {
            let response = self
                .client
                .post("http://localhost/v1/dynamic-tools/interrupted")
                .timeout(super::SETTLEMENT_REQUEST_TIMEOUT)
                .json(&CompletionRequest {
                    protocol_version: super::PROTOCOL_VERSION,
                    thread_id,
                    context_call_id: call_id,
                })
                .send()
                .await
                .map_err(transport_error)?;
            let response = response.error_for_status().map_err(transport_error)?;
            let bytes = super::read_bounded(response, super::MAX_CALL_RESPONSE_BYTES).await?;
            match serde_json::from_slice::<Receipt>(&bytes).map_err(|error| {
                super::recovery::Intervention(format!("invalid exact settlement receipt: {error}"))
            })? {
                Receipt::Settled => {
                    self.settle_reattached_pending(thread_id, &BTreeSet::from([call_id.to_owned()]))
                        .await
                }
                Receipt::Recovered { reply } => {
                    self.restore_completion(thread_id, call_id, reply).await?;
                    Box::pin(self.execute_settlement(SettlementWork::Acknowledge {
                        thread_id: thread_id.to_owned(),
                        calls: vec![call_id.to_owned()],
                    }))
                    .await
                }
                Receipt::Pending => color_eyre::eyre::bail!(
                    "call {call_id} is still reconciling; retained work was not replayed"
                ),
            }
        }
        #[cfg(not(unix))]
        Err(super::recovery::Intervention(
            "host reconciliation is unavailable on this platform".into(),
        )
        .into())
    }

    // Classify boundaries in event order, before a subsequent hosted call can
    // register. Only network settlement runs off the UI loop.
    pub(crate) fn enqueue_settlement(
        self: &Arc<Self>,
        notification: &ServerNotification,
        events: &AppEventSender,
    ) {
        let work = match notification {
            ServerNotification::RawResponseItemCompleted(item) => self.prepare_completion(item),
            ServerNotification::TurnCompleted(turn) => Ok(self.prepare_turn(&turn.thread_id)),
            _ => return,
        };
        if work.as_ref().is_ok_and(Option::is_some) {
            self.recovery.start_recovering();
        }
        if let Err(error) = work {
            let reason = format!("native completion boundary could not be classified: {error}");
            *self
                .recovery
                .classification_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(reason.clone());
            self.recovery
                .failed(&super::recovery::Intervention(reason).into());
        }
        self.wake_recovery(Some(events));
    }

    fn prepare_turn(&self, thread_id: &str) -> Option<SettlementWork> {
        if self
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
        state.reconcile.extend(calls.iter().cloned());
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
        if self
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
            let record = self
                .state_db
                .thread_queue()
                .mark_host_tool_completion_ready(&key)
                .await
                .map_err(store_error)?;
            if matches!(
                record.state,
                codex_state::HostToolCompletionState::Acknowledged
                    | codex_state::HostToolCompletionState::NotSubmitted
                    | codex_state::HostToolCompletionState::ReattachedWithoutCompletion
            ) {
                let mut state = self
                    .completions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.ready.remove(&call_id);
                state.reconcile.remove(&call_id);
                state.pending.remove(&call_id);
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
                    Err(error) if attempt == 2 => return Err(transport_error(error)),
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
            self.completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .reconcile
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
            .ok_or_else(|| {
                super::recovery::Intervention("hosted completion thread is not persisted".into())
            })?;
        let (items, history_thread, parse_errors) =
            RolloutRecorder::load_rollout_items(&metadata.rollout_path).await?;
        if history_thread != Some(thread_id) || parse_errors != 0 {
            return Err(super::recovery::Intervention(
                "hosted completion history identity or integrity is unconfirmed".into(),
            )
            .into());
        }
        let thread = thread_id.to_string();
        let mut pending = BTreeSet::new();
        let mut ready = Vec::new();
        for record in unresolved {
            match record.state {
                codex_state::HostToolCompletionState::Ready => {
                    ready.push(record.key.context_call_id);
                }
                codex_state::HostToolCompletionState::Pending
                | codex_state::HostToolCompletionState::ReconcilePending => {
                    if completion_is_closed(&items, &record.key.context_call_id)? {
                        ready.push(record.key.context_call_id);
                    } else {
                        pending.insert(record.key.context_call_id);
                    }
                }
                codex_state::HostToolCompletionState::Acknowledged
                | codex_state::HostToolCompletionState::NotSubmitted
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
        for call_id in &calls {
            self.state_db
                .thread_queue()
                .mark_host_tool_completion_reconcile(&completion_key(thread_id, call_id)?)
                .await
                .map_err(store_error)?;
            self.reconcile_interrupted(thread_id, call_id).await?;
        }
        Ok(())
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
        state.reconcile.retain(|id| !calls.contains(id));
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

pub(super) fn completion_is_closed(
    items: &[RolloutItem],
    call_id: &str,
) -> color_eyre::Result<bool> {
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

pub(super) fn store_error(error: anyhow::Error) -> color_eyre::Report {
    if error
        .downcast_ref::<codex_state::HostToolCompletionError>()
        .is_some_and(|error| !error.retryable())
    {
        super::recovery::Intervention(format!("{error:#}")).into()
    } else {
        color_eyre::eyre::eyre!("{error:#}")
    }
}

fn transport_error(error: reqwest::Error) -> color_eyre::Report {
    if error.status().is_some_and(|status| {
        status.is_client_error()
            && status != reqwest::StatusCode::REQUEST_TIMEOUT
            && status != reqwest::StatusCode::TOO_MANY_REQUESTS
    }) {
        super::recovery::Intervention(format!("host rejected settlement: {error}")).into()
    } else {
        error.into()
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
