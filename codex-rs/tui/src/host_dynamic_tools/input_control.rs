//! Hosted input reaches this TUI's existing app-server connection. The host owns
//! the private socket directory; this listener never resumes or creates a thread.

#[allow(dead_code)]
#[path = "input_control_protocol.rs"]
mod protocol;
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

impl InputControl {
    pub(super) fn update_handle(&self, handle: AppServerRequestHandle) {
        self.handle.send_replace(handle);
    }

    pub(super) fn start(
        socket: AbsolutePathBuf,
        thread: ThreadId,
        handle: AppServerRequestHandle,
    ) -> std::io::Result<Self> {
        // Bind only a fresh, host-selected path. Never unlink a competing owner.
        let listener = UnixListener::bind(socket.as_path())?;
        let (handle, receiver) = watch::channel(handle);
        let router = Router::new().route("/v1/input", post(present));
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
        })
    }
}
