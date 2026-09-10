//! Hosted input reaches this TUI's existing app-server connection. The host owns
//! the private socket directory; this listener never resumes or creates a thread.

#[allow(dead_code)]
#[path = "input_control_protocol.rs"]
pub(super) mod protocol;
use axum::Router;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_protocol::ThreadId;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(super) struct InputControl {
    pub(super) socket: AbsolutePathBuf,
    shutdown: CancellationToken,
    handle: watch::Sender<AppServerRequestHandle>,
    binding: std::sync::Arc<tokio::sync::Mutex<protocol::ExpectedBinding>>,
}

impl std::fmt::Debug for InputControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InputControl")
            .field("socket", &self.socket)
            .finish_non_exhaustive()
    }
}

impl Drop for InputControl {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Clone)]
pub(super) struct InputTarget {
    pub(super) thread: ThreadId,
    pub(super) handle: watch::Receiver<AppServerRequestHandle>,
    binding: std::sync::Arc<tokio::sync::Mutex<protocol::ExpectedBinding>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InputRequest {
    thread_id: String,
    client_user_message_id: String,
    message: String,
}

fn turn_request(target: ThreadId, input: InputRequest) -> Result<ClientRequest, StatusCode> {
    if input.thread_id != target.to_string() {
        return Err(StatusCode::CONFLICT);
    }
    if input.client_user_message_id.is_empty()
        || input.client_user_message_id.len() > 256
        || input.message.is_empty()
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(ClientRequest::TurnStart {
        // The existing TUI connection has its own integer request IDs.
        request_id: RequestId::String(format!("host-input-{}", Uuid::new_v4())),
        params: TurnStartParams {
            thread_id: input.thread_id,
            client_user_message_id: Some(input.client_user_message_id),
            input: vec![UserInput::Text {
                text: input.message,
                text_elements: Vec::new(),
            }],
            ..Default::default()
        },
    })
}

async fn present(
    State(target): State<InputTarget>,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    let input = serde_json::from_slice(&body).map_err(|_| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid hosted input payload".to_string(),
        )
    })?;
    let request = turn_request(target.thread, input).map_err(|status| {
        (
            status,
            "invalid hosted input identity or payload".to_string(),
        )
    })?;
    let handle = target.handle.borrow().clone();
    match tokio::time::timeout(Duration::from_secs(/*secs*/ 60), handle.request(request)).await {
        Ok(Ok(Ok(_))) => Ok(StatusCode::ACCEPTED),
        Ok(Ok(Err(error))) => Err((StatusCode::CONFLICT, error.message)),
        Ok(Err(error)) => Err((StatusCode::SERVICE_UNAVAILABLE, error.to_string())),
        Err(_) => Err((
            StatusCode::GATEWAY_TIMEOUT,
            "native input acceptance is unconfirmed".to_string(),
        )),
    }
}

fn protocol_outcome(
    outcome: codex_app_server_client::InProcessHostInputOutcome,
) -> protocol::Outcome {
    use codex_app_server_client::InProcessHostInputOutcome as Native;
    match outcome {
        Native::Admitted => protocol::Outcome::Admitted,
        Native::Dispatching => protocol::Outcome::Dispatching,
        Native::Presented => protocol::Outcome::Presented,
        Native::Withdrawn => protocol::Outcome::Withdrawn,
        Native::Rejected | Native::Conflict | Native::ProducerSealed | Native::AtCapacity => {
            protocol::Outcome::Rejected
        }
        Native::Unknown => protocol::Outcome::Unknown,
        Native::Compacted => protocol::Outcome::Compacted,
        Native::EvidenceUnavailable => protocol::Outcome::EvidenceUnavailable,
    }
}

async fn control(
    State(target): State<InputTarget>,
    body: Bytes,
) -> Result<axum::Json<protocol::Response>, (StatusCode, String)> {
    let request: protocol::Request = serde_json::from_slice(&body).map_err(|_| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid hosted input control payload".to_string(),
        )
    })?;
    let binding = match &request {
        protocol::Request::Submit { binding, .. }
        | protocol::Request::Query { binding, .. }
        | protocol::Request::Withdraw { binding, .. }
        | protocol::Request::Seal { binding, .. }
        | protocol::Request::Acknowledge { binding, .. } => binding.clone(),
    };
    target
        .binding
        .lock()
        .await
        .validate(&binding)
        .map_err(|_| {
            (
                StatusCode::CONFLICT,
                "stale or foreign native binding".to_string(),
            )
        })?;
    let handle = target.handle.borrow().clone();
    let native = match handle {
        AppServerRequestHandle::InProcess(handle) => handle.host_input_control(),
        AppServerRequestHandle::Remote(_) => None,
    };
    let Some(native) = native else {
        return Ok(axum::Json(protocol::Response {
            binding,
            outcome: protocol::Outcome::EvidenceUnavailable,
        }));
    };
    let outcome = match request {
        protocol::Request::Submit { envelope, .. } => {
            let envelope = envelope.validate(target.thread).map_err(|_| {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "noncanonical hosted input envelope".to_string(),
                )
            })?;
            let target_json = serde_json::to_string(&envelope.target)
                .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
            let payload = String::from_utf8(envelope.payload).map_err(|_| {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "hosted input payload is not utf-8".to_string(),
                )
            })?;
            let purpose = envelope.purpose.as_str().to_string();
            let mode = envelope.mode.as_str().to_string();
            protocol_outcome(
                native
                    .submit(codex_app_server_client::InProcessHostInputSubmission {
                        thread_id: target.thread,
                        producer_id: envelope.producer_id,
                        sequence: envelope.sequence,
                        purpose,
                        mode,
                        target_json,
                        content_digest: envelope.content_digest,
                        payload,
                    })
                    .await
                    .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?,
            )
        }
        protocol::Request::Query {
            producer_id,
            sequence,
            ..
        } => protocol_outcome(
            native
                .query(&producer_id, sequence)
                .await
                .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?,
        ),
        protocol::Request::Withdraw {
            producer_id,
            sequence,
            ..
        } => protocol_outcome(
            native
                .withdraw(target.thread, &producer_id, sequence)
                .await
                .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?,
        ),
        protocol::Request::Seal { producer_id, .. } => {
            native
                .seal(target.thread, &producer_id)
                .await
                .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?;
            protocol::Outcome::Withdrawn
        }
        protocol::Request::Acknowledge {
            producer_id,
            through_sequence,
            ..
        } => protocol_outcome(
            native
                .acknowledge(target.thread, &producer_id, through_sequence)
                .await
                .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?,
        ),
    };
    Ok(axum::Json(protocol::Response { binding, outcome }))
}

impl InputControl {
    pub(super) fn update_handle(&self, handle: AppServerRequestHandle) {
        self.handle.send_replace(handle);
    }

    pub(super) async fn update_binding(&self, binding: protocol::ExpectedBinding) {
        *self.binding.lock().await = binding;
    }

    pub(super) async fn binding(&self) -> protocol::ExpectedBinding {
        self.binding.lock().await.clone()
    }

    pub(super) fn start(
        socket: AbsolutePathBuf,
        thread: ThreadId,
        handle: AppServerRequestHandle,
        binding: protocol::ExpectedBinding,
    ) -> std::io::Result<Self> {
        // Bind only a fresh, host-selected path. Never unlink a competing owner.
        let listener = UnixListener::bind(socket.as_path())?;
        let (handle, receiver) = watch::channel(handle);
        let binding = std::sync::Arc::new(tokio::sync::Mutex::new(binding));
        let router = Router::new()
            .route("/v1/input", post(present))
            .route("/v1/input/control", post(control));
        #[cfg(target_os = "linux")]
        let router = router.route(
            "/v1/workspace/publication",
            post(super::workspace_control::publication),
        );
        let router = router
            .layer(DefaultBodyLimit::max(1024 * 1024))
            .with_state(InputTarget {
                thread,
                handle: receiver,
                binding: binding.clone(),
            });
        let shutdown = CancellationToken::new();
        let stopped = shutdown.clone();
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, router)
                .with_graceful_shutdown(stopped.cancelled_owned())
                .await
            {
                tracing::warn!(%error, "hosted input listener stopped");
            }
        });
        Ok(Self {
            socket,
            shutdown,
            handle,
            binding,
        })
    }
}
