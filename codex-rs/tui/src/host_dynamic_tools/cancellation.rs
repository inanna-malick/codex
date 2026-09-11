use std::sync::Arc;

use codex_app_server_protocol::DynamicToolCallParams;
use codex_app_server_protocol::DynamicToolCallResponse;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Notify;

use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;

use super::CANCEL_PATH;
#[cfg(unix)]
use super::MAX_CALL_RESPONSE_BYTES;
use super::PROTOCOL_VERSION;
#[cfg(unix)]
use super::endpoint;
#[cfg(unix)]
use super::read_bounded;
#[cfg(unix)]
use super::require_status;
#[cfg(unix)]
use super::send_request;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CancellationRequest {
    protocol_version: u32,
    thread_id: String,
    turn_id: String,
    call_id: String,
    context_call_id: Option<String>,
    namespace: Option<String>,
    launch_id: String,
    application_instance_id: String,
    session_generation: u64,
    input_control_nonce: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum CancellationResponse {
    Cancelled {
        execution: String,
        reply: DynamicToolCallResponse,
    },
    Expired {
        execution: String,
        reply: DynamicToolCallResponse,
    },
    Unconfirmed {
        execution: String,
    },
    NotSleeping {
        execution: String,
    },
    UnknownEvaluation {
        execution: String,
    },
}

#[derive(Debug)]
enum SettlementPhase {
    Running,
    Cancelling,
    AwaitingTerminal(String),
    Uncertain(String),
    Terminal(DynamicToolCallResponse),
    Resolved,
}

pub(crate) struct ActiveHostedCall {
    client: reqwest::Client,
    request: CancellationRequest,
    request_id: codex_app_server_protocol::RequestId,
    events: AppEventSender,
    phase: tokio::sync::Mutex<SettlementPhase>,
    changed: Notify,
}

impl ActiveHostedCall {
    pub(super) fn new(
        client: reqwest::Client,
        params: &DynamicToolCallParams,
        request_id: codex_app_server_protocol::RequestId,
        events: AppEventSender,
        launch_id: String,
        application_instance_id: String,
        session_generation: u64,
        input_control_nonce: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            request: CancellationRequest {
                protocol_version: PROTOCOL_VERSION,
                thread_id: params.thread_id.clone(),
                turn_id: params.turn_id.clone(),
                call_id: params.call_id.clone(),
                context_call_id: params.context_call_id.clone(),
                namespace: params.namespace.clone(),
                launch_id,
                application_instance_id,
                session_generation,
                input_control_nonce,
            },
            request_id,
            events,
            phase: tokio::sync::Mutex::new(SettlementPhase::Running),
            changed: Notify::new(),
        })
    }

    pub(crate) async fn complete_from_call(&self, response: DynamicToolCallResponse) {
        self.record_terminal(response).await;
    }

    pub(crate) async fn transport_uncertain(&self, detail: String) {
        let _ = self
            .record_uncertain(format!(
                "hosted workbench result transport is unconfirmed: {detail}"
            ))
            .await;
    }

    pub(crate) async fn cancel_before_input(&self) -> Result<(), String> {
        loop {
            let should_cancel = {
                let mut phase = self.phase.lock().await;
                match &*phase {
                    SettlementPhase::Running | SettlementPhase::Uncertain(_) => {
                        *phase = SettlementPhase::Cancelling;
                        true
                    }
                    SettlementPhase::Cancelling | SettlementPhase::AwaitingTerminal(_) => false,
                    SettlementPhase::Terminal(_) => false,
                    SettlementPhase::Resolved => return Ok(()),
                }
            };
            if should_cancel {
                self.cancel_once().await?;
            }
            let notified = self.changed.notified();
            match &*self.phase.lock().await {
                SettlementPhase::Resolved => return Ok(()),
                SettlementPhase::Uncertain(detail) => return Err(detail.clone()),
                SettlementPhase::Running
                | SettlementPhase::Cancelling
                | SettlementPhase::AwaitingTerminal(_)
                | SettlementPhase::Terminal(_) => {}
            }
            notified.await;
        }
    }

    #[cfg(unix)]
    async fn cancel_once(&self) -> Result<(), String> {
        loop {
            let outcome = async {
                let response =
                    send_request(self.client.post(endpoint(CANCEL_PATH)).json(&self.request))
                        .await?;
                require_status(response.status(), reqwest::StatusCode::OK)?;
                let body = read_bounded(response, MAX_CALL_RESPONSE_BYTES).await?;
                serde_json::from_slice::<CancellationResponse>(&body).map_err(|error| {
                    color_eyre::eyre::eyre!(
                        "invalid exact hosted workbench cancellation response: {error}"
                    )
                })
            }
            .await
            .map_err(|error| {
                format!("exact hosted workbench cancellation is unconfirmed: {error:#}")
            });
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => return self.record_uncertain(error).await,
            };
            let pending = matches!(
                &outcome,
                CancellationResponse::Unconfirmed { .. }
                    | CancellationResponse::NotSleeping { .. }
                    | CancellationResponse::UnknownEvaluation { .. }
            );
            self.apply_outcome(outcome).await?;
            if !pending {
                return Ok(());
            }

            let notified = self.changed.notified();
            match &*self.phase.lock().await {
                SettlementPhase::AwaitingTerminal(_) => {}
                SettlementPhase::Terminal(_) | SettlementPhase::Resolved => return Ok(()),
                SettlementPhase::Uncertain(detail) => return Err(detail.clone()),
                SettlementPhase::Running | SettlementPhase::Cancelling => continue,
            }
            tokio::select! {
                () = notified => {}
                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }
        }
    }

    #[cfg(not(unix))]
    async fn cancel_once(&self) -> Result<(), String> {
        self.record_uncertain(
            "exact hosted workbench cancellation is unavailable on this platform".to_string(),
        )
        .await
    }

    async fn apply_outcome(&self, outcome: CancellationResponse) -> Result<(), String> {
        match outcome {
            CancellationResponse::Cancelled { execution, reply }
            | CancellationResponse::Expired { execution, reply } => {
                tracing::debug!(%execution, "hosted workbench reached an exact terminal outcome");
                self.record_terminal(reply).await;
                Ok(())
            }
            CancellationResponse::Unconfirmed { execution } => {
                self.await_terminal(format!(
                    "host reported cancellation for {execution} as unconfirmed; terminal settlement is pending"
                ))
                .await;
                Ok(())
            }
            CancellationResponse::NotSleeping { execution } => {
                self.await_terminal(format!(
                    "host observed {execution} before it reached cancellable sleep; terminal settlement is pending"
                ))
                .await;
                Ok(())
            }
            CancellationResponse::UnknownEvaluation { execution } => {
                self.await_terminal(format!(
                    "host has not yet observed exact workbench evaluation {execution}; terminal settlement is pending"
                ))
                .await;
                Ok(())
            }
        }
    }

    async fn record_terminal(&self, response: DynamicToolCallResponse) {
        let mut phase = self.phase.lock().await;
        if matches!(
            &*phase,
            SettlementPhase::Terminal(_) | SettlementPhase::Resolved
        ) {
            return;
        }
        if let SettlementPhase::AwaitingTerminal(detail) = &*phase {
            tracing::debug!(%detail, "hosted workbench uncertainty reached terminal settlement");
        }
        *phase = SettlementPhase::Terminal(response.clone());
        self.events.send(AppEvent::DynamicToolCallCompleted {
            request_id: self.request_id.clone(),
            response,
        });
        self.changed.notify_waiters();
    }

    async fn record_uncertain(&self, detail: String) -> Result<(), String> {
        let mut phase = self.phase.lock().await;
        if let SettlementPhase::Terminal(_) | SettlementPhase::Resolved = &*phase {
            return Ok(());
        }
        *phase = SettlementPhase::Uncertain(detail.clone());
        self.changed.notify_waiters();
        Err(detail)
    }

    async fn await_terminal(&self, detail: String) {
        let mut phase = self.phase.lock().await;
        if matches!(
            &*phase,
            SettlementPhase::Terminal(_) | SettlementPhase::Resolved
        ) {
            return;
        }
        *phase = SettlementPhase::AwaitingTerminal(detail);
        self.changed.notify_waiters();
    }

    pub(crate) async fn mark_original_resolved(&self) {
        let mut phase = self.phase.lock().await;
        if matches!(&*phase, SettlementPhase::Terminal(_)) {
            *phase = SettlementPhase::Resolved;
            self.changed.notify_waiters();
        }
    }

    pub(crate) async fn terminal_response(&self) -> Option<DynamicToolCallResponse> {
        match &*self.phase.lock().await {
            SettlementPhase::Terminal(response) => Some(response.clone()),
            SettlementPhase::Running
            | SettlementPhase::Cancelling
            | SettlementPhase::AwaitingTerminal(_)
            | SettlementPhase::Uncertain(_)
            | SettlementPhase::Resolved => None,
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct InputSettlementGate {
    active: Arc<tokio::sync::RwLock<Option<std::sync::Weak<ActiveHostedCall>>>>,
}

impl InputSettlementGate {
    pub(super) async fn activate(&self, call: &Arc<ActiveHostedCall>) -> Result<(), String> {
        let mut active = self.active.write().await;
        if active.as_ref().and_then(std::sync::Weak::upgrade).is_some() {
            return Err("another hosted workbench evaluation is still unsettled".to_string());
        }
        *active = Some(Arc::downgrade(call));
        Ok(())
    }

    pub(super) async fn before_input(&self) -> Result<(), String> {
        let call = self
            .active
            .read()
            .await
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        match call {
            Some(call) => call.cancel_before_input().await,
            None => Ok(()),
        }
    }

    pub(super) async fn clear(&self, call: &Arc<ActiveHostedCall>) {
        let mut active = self.active.write().await;
        if active
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, call))
        {
            *active = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_app_server_protocol::DynamicToolCallOutputContentItem;
    use codex_app_server_protocol::RequestId;
    use tokio::sync::mpsc::unbounded_channel;

    fn response(text: &str) -> DynamicToolCallResponse {
        DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: text.to_string(),
            }],
            success: false,
        }
    }

    fn call() -> (
        Arc<ActiveHostedCall>,
        tokio::sync::mpsc::UnboundedReceiver<AppEvent>,
    ) {
        let params = DynamicToolCallParams {
            context_call_id: Some("context-call".to_string()),
            thread_id: "thread".to_string(),
            turn_id: "turn".to_string(),
            call_id: "call".to_string(),
            namespace: None,
            tool: "haskell".to_string(),
            arguments: serde_json::json!({"source": "watch"}),
        };
        let (events, receiver) = unbounded_channel();
        (
            ActiveHostedCall::new(
                reqwest::Client::new(),
                &params,
                RequestId::String("request".to_string()),
                AppEventSender::new(events),
                "launch".to_string(),
                "application".to_string(),
                7,
                "nonce".to_string(),
            ),
            receiver,
        )
    }

    #[test]
    fn exact_cancel_packet_preserves_all_correlation() {
        let (call, _) = call();
        assert_eq!(
            serde_json::to_value(&call.request).expect("serialize exact cancel request"),
            serde_json::json!({
                "protocolVersion": 3,
                "threadId": "thread",
                "turnId": "turn",
                "callId": "call",
                "contextCallId": "context-call",
                "namespace": null,
                "launchId": "launch",
                "applicationInstanceId": "application",
                "sessionGeneration": 7,
                "inputControlNonce": "nonce"
            })
        );
    }

    #[test]
    fn unconfirmed_packet_is_execution_identity_without_a_reply() {
        let outcome: CancellationResponse = serde_json::from_value(serde_json::json!({
            "status": "unconfirmed",
            "execution": "execution"
        }))
        .expect("decode coordinator unconfirmed packet");
        assert!(matches!(
            outcome,
            CancellationResponse::Unconfirmed { execution } if execution == "execution"
        ));
    }

    #[tokio::test]
    async fn uncertainty_fences_until_retained_terminal_is_resolved() {
        let (call, mut events) = call();
        call.apply_outcome(CancellationResponse::Unconfirmed {
            execution: "execution".to_string(),
        })
        .await
        .expect("unconfirmed cancellation keeps waiting for terminal settlement");
        assert!(call.terminal_response().await.is_none());
        let waiting_call = call.clone();
        let mut waiter = tokio::spawn(async move { waiting_call.cancel_before_input().await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiter)
                .await
                .is_err(),
            "unconfirmed cancellation must keep input fenced instead of returning 503"
        );

        let terminal = response("cancelled");
        call.apply_outcome(CancellationResponse::Cancelled {
            execution: "execution".to_string(),
            reply: terminal.clone(),
        })
        .await
        .expect("retained terminal packet settles uncertainty");
        match events.recv().await.expect("original request completion") {
            AppEvent::DynamicToolCallCompleted { response, .. } => {
                assert_eq!(response, terminal);
            }
            event => panic!("unexpected event: {event:?}"),
        }

        call.apply_outcome(CancellationResponse::UnknownEvaluation {
            execution: "execution".to_string(),
        })
        .await
        .expect("later loss cannot erase terminal proof");
        call.transport_uncertain("socket closed after response".to_string())
            .await;
        assert_eq!(call.terminal_response().await, Some(terminal));

        call.mark_original_resolved().await;
        waiter
            .await
            .expect("input waiter task")
            .expect("input releases only after original resolution");
    }

    #[tokio::test]
    async fn input_gate_waits_for_original_request_resolution_after_terminal_proof() {
        let (call, _events) = call();
        let gate = InputSettlementGate::default();
        gate.activate(&call).await.expect("activate exact call");
        call.complete_from_call(response("terminal")).await;

        let waiting_gate = gate.clone();
        let mut waiter = tokio::spawn(async move { waiting_gate.before_input().await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiter)
                .await
                .is_err(),
            "terminal proof alone must not release input before original request resolution"
        );

        call.mark_original_resolved().await;
        waiter
            .await
            .expect("input gate task")
            .expect("resolved original request releases input");
    }
}
