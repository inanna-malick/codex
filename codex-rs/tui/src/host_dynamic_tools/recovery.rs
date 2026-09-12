use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;

use super::HostDynamicTools;
use super::cancellation::HostedCallAdmission;

pub(super) const ADMISSION_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) enum Health {
    #[default]
    Healthy,
    Recovering,
    NeedsIntervention(String),
}

/// Recovery wakes are hints; the completion ledger and retained classifications
/// own the work. Dropping a wake cannot drop a settlement obligation.
pub(super) struct Recovery {
    pub(super) health: tokio::sync::watch::Sender<Health>,
    wake: Arc<tokio::sync::Notify>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    events: Mutex<Option<AppEventSender>>,
    warning: Mutex<Option<Health>>,
    reported: std::sync::atomic::AtomicBool,
    generation: std::sync::atomic::AtomicU64,
    transition: Mutex<()>,
    pub(super) history_pending: std::sync::atomic::AtomicBool,
    pub(super) classification_failure: Mutex<Option<String>>,
    pub(super) handle: Mutex<Option<codex_app_server_client::AppServerRequestHandle>>,
    pub(super) serial: tokio::sync::Mutex<()>,
}

impl Default for Recovery {
    fn default() -> Self {
        Self {
            health: tokio::sync::watch::channel(Health::Healthy).0,
            wake: Arc::new(tokio::sync::Notify::new()),
            worker: Mutex::new(None),
            events: Mutex::new(None),
            warning: Mutex::new(None),
            reported: std::sync::atomic::AtomicBool::new(false),
            generation: std::sync::atomic::AtomicU64::new(0),
            transition: Mutex::new(()),
            history_pending: std::sync::atomic::AtomicBool::new(false),
            classification_failure: Mutex::new(None),
            handle: Mutex::new(None),
            serial: tokio::sync::Mutex::new(()),
        }
    }
}

impl Drop for Recovery {
    fn drop(&mut self) {
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            worker.abort();
        }
    }
}

impl Recovery {
    #[cfg(test)]
    pub(super) async fn admit(&self) -> Result<(), String> {
        self.admit_until(tokio::time::Instant::now() + ADMISSION_GRACE)
            .await
    }

    pub(super) async fn admit_until(&self, deadline: tokio::time::Instant) -> Result<(), String> {
        let mut health = self.health.subscribe();
        let waiting = async {
            loop {
                match health.borrow_and_update().clone() {
                    Health::Healthy => return Ok(()),
                    Health::NeedsIntervention(reason) => {
                        self.reported
                            .store(true, std::sync::atomic::Ordering::Release);
                        return Err(format!(
                            "Not submitted: hosted-call recovery needs intervention: {reason}. Existing calls remain retained."
                        ));
                    }
                    Health::Recovering => {}
                }
                if health.changed().await.is_err() {
                    return Err("Not submitted: hosted-call recovery owner stopped.".into());
                }
            }
        };
        match tokio::time::timeout_at(deadline, waiting).await {
            Ok(result) => result,
            Err(_) => {
                self.reported
                    .store(true, std::sync::atomic::Ordering::Release);
                Err("Not submitted: hosted-call recovery is still running after 30 seconds. This call will not execute later. Recovery is automatic; no polling or replay of earlier calls is needed.".into())
            }
        }
    }

    pub(super) fn failed(&self, error: &color_eyre::Report) {
        tracing::warn!(error = ?error, "hosted completion reconciliation deferred");
        let _transition = self
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let requested = if let Some(error) = error.downcast_ref::<Intervention>() {
            Health::NeedsIntervention(error.0.clone())
        } else {
            Health::Recovering
        };
        let health = match (&*self.health.borrow(), requested) {
            (Health::NeedsIntervention(reason), Health::Recovering) => {
                Health::NeedsIntervention(reason.clone())
            }
            (_, requested) => requested,
        };
        self.health.send_replace(health.clone());
        let mut warning = self
            .warning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if warning.as_ref() != Some(&health)
            && let Some(events) = self
                .events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
        {
            let message = match &health {
                Health::NeedsIntervention(reason) => format!(
                    "Hosted calls are retained; recovery needs intervention. New executions are not submitted. {reason}"
                ),
                Health::Recovering => format!(
                    "Hosted calls are retained; settlement is recovering. New executions wait up to 30 seconds, then return not submitted. {error}"
                ),
                Health::Healthy => unreachable!(),
            };
            events.send(AppEvent::InsertHistoryCell(Box::new(
                crate::history_cell::new_warning_event(message),
            )));
            *warning = Some(health);
        }
    }

    pub(super) fn start_recovering(&self) {
        let _transition = self
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if !matches!(&*self.health.borrow(), Health::NeedsIntervention(_)) {
            self.health.send_replace(Health::Recovering);
        }
    }

    pub(super) fn try_dispatch(&self, admission: &HostedCallAdmission) -> DispatchOutcome {
        let _transition = self
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(&*self.health.borrow(), Health::Healthy) {
            return DispatchOutcome::Recovering;
        }
        if admission.try_dispatch() {
            DispatchOutcome::Dispatched
        } else {
            DispatchOutcome::Cancelled
        }
    }

    fn generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::Acquire)
    }

    fn recovered(&self, generation: u64) -> RecoveryPassCompletion {
        let _transition = self
            .transition
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.generation() != generation {
            return RecoveryPassCompletion::Stale;
        }
        self.health.send_replace(Health::Healthy);
        let reported = self
            .reported
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        let warned = self
            .warning
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .is_some();
        if (reported || warned)
            && let Some(events) = self
                .events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
        {
            events.send(AppEvent::InsertHistoryCell(Box::new(crate::history_cell::new_info_event(
                "Hosted tools recovered. Blocked calls were not submitted; existing calls were reconciled without reexecution.".into(),
                None,
            ))));
        }
        RecoveryPassCompletion::Recovered { reported }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DispatchOutcome {
    Dispatched,
    Cancelled,
    Recovering,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryPassCompletion {
    Recovered { reported: bool },
    Stale,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct Intervention(pub(super) String);

impl HostDynamicTools {
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "one recovery pass must retain serial ownership across reconciliation"
    )]
    pub(super) fn wake_recovery(self: &Arc<Self>, events: Option<&AppEventSender>) {
        if let Some(events) = events {
            *self
                .recovery
                .events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(events.clone());
        }
        let mut worker = self
            .recovery
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if worker
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
        {
            let weak = Arc::downgrade(self);
            let wake = Arc::clone(&self.recovery.wake);
            *worker = Some(tokio::spawn(async move {
                let mut delay = Duration::from_millis(100);
                loop {
                    let Some(host) = weak.upgrade() else { return };
                    let generation = host.recovery.generation();
                    // A child task contains panics without losing the supervisor
                    // or the durable settlement intent.
                    let attempt_host = Arc::clone(&host);
                    let result = tokio::spawn(async move {
                        let _serial = attempt_host.recovery.serial.lock().await;
                        attempt_host.reconcile_completions().await
                    })
                    .await;
                    let wait = match result {
                        Ok(Ok(())) => {
                            match host.recovery.recovered(generation) {
                                RecoveryPassCompletion::Recovered { reported } => {
                                    if reported {
                                        host.notify_recovery();
                                    }
                                }
                                RecoveryPassCompletion::Stale => continue,
                            }
                            delay = Duration::from_millis(100);
                            None
                        }
                        result => {
                            let error = match result {
                                Ok(Err(error)) => error,
                                Err(error) => color_eyre::eyre::eyre!(
                                    "reconciliation worker interrupted: {error}"
                                ),
                                Ok(Ok(())) => unreachable!(),
                            };
                            host.recovery.failed(&error);
                            let wait = delay;
                            delay = (delay * 2).min(Duration::from_secs(5));
                            Some(wait)
                        }
                    };
                    drop(host);
                    if let Some(wait) = wait {
                        tokio::select! {
                            () = wake.notified() => {}
                            () = tokio::time::sleep(wait) => {}
                        }
                    } else {
                        wake.notified().await;
                    }
                }
            }));
        }
        self.recovery.wake.notify_one();
    }

    fn notify_recovery(&self) {
        let handle = self
            .recovery
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let (Some(handle), Some(thread)) = (handle, self.primary_thread_id()) else {
            return;
        };
        tokio::spawn(async move {
            let id = format!("host-recovered-{}", uuid::Uuid::new_v4());
            let result = handle.request_typed::<codex_app_server_protocol::TurnStartResponse>(
                codex_app_server_protocol::ClientRequest::TurnStart {
                    request_id: codex_app_server_protocol::RequestId::String(id.clone()),
                    params: codex_app_server_protocol::TurnStartParams {
                        thread_id: thread.to_string(),
                        client_user_message_id: Some(id),
                        input: vec![codex_app_server_protocol::UserInput::Text {
                            text: "Hosted tools recovered. Calls reported not submitted did not execute. Earlier submitted calls were reconciled without rerunning them; continue from retained results and bindings.".into(),
                            text_elements: Vec::new(),
                        }],
                        ..Default::default()
                    },
                }
            ).await;
            if let Err(error) = result {
                tracing::warn!(%error, "could not deliver hosted-tool recovery notice; recovery remains visible in the TUI");
            }
        });
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
