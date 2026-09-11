mod cancellation;
#[cfg(target_os = "linux")]
mod command_output;
mod commands;
mod completions;
#[cfg(unix)]
mod input_control;
#[cfg(target_os = "linux")]
mod workspace_control;

pub(crate) use cancellation::ActiveHostedCall;

use codex_app_server_protocol::DynamicToolCallParams;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartPersistence;
use codex_protocol::ThreadId;
use codex_protocol::dynamic_tools::DynamicToolNamespaceTool;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_rollout::StateDbHandle;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

const PROTOCOL_VERSION: u32 = 3;
const REGISTRATION_PATH: &str = "/v1/dynamic-tools/registration";
const SESSION_PATH: &str = "/v1/dynamic-tools/session";
const CALL_PATH: &str = "/v1/dynamic-tools/call";
const CANCEL_PATH: &str = "/v1/dynamic-tools/cancel";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
// Settlement can wait for the host actor and its shared machine checkout.
const SETTLEMENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const DISABLED_MESSAGE: &str = "Host dynamic tools are disabled for this session because completion could not be confirmed. Previous host operations may have taken effect; do not repeat them without checking. Other Codex tools remain available.";
const MAX_REGISTRATION_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_CALL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DynamicToolKind {
    Function,
    Custom,
}

impl DynamicToolKind {
    fn accepts(self, arguments: &Value) -> bool {
        match self {
            Self::Function => arguments.is_object(),
            Self::Custom => arguments.is_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
enum HostDynamicToolScope {
    PrimaryThread,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct HostDynamicToolRegistration {
    protocol_version: u32,
    dynamic_tools: Vec<DynamicToolSpec>,
    scope: HostDynamicToolScope,
    #[serde(default)]
    input_control_socket: Option<AbsolutePathBuf>,
    #[serde(default)]
    launch_id: String,
    #[serde(default)]
    input_control_nonce: String,
}

pub(crate) struct HostDynamicTools {
    registration: HostDynamicToolRegistration,
    #[cfg(unix)]
    input_control: tokio::sync::Mutex<Option<input_control::InputControl>>,
    identities: HashMap<(Option<String>, String), DynamicToolKind>,
    primary_thread_id: Mutex<Option<ThreadId>>,
    state_db: StateDbHandle,
    completions: Mutex<completions::HostToolCompletions>,
    settlement_sender: Mutex<Option<tokio::sync::mpsc::Sender<completions::SettlementWork>>>,
    input_settlement: cancellation::InputSettlementGate,
    disabled: AtomicBool,
    application_instance_id: String,
    session_generation: AtomicU64,
    #[cfg(unix)]
    client: reqwest::Client,
}

impl std::fmt::Debug for HostDynamicTools {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostDynamicTools")
            .field("registration", &self.registration)
            .field("primary_thread_id", &self.primary_thread_id())
            .field("disabled", &self.is_disabled())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostDynamicToolRouting {
    Unregistered,
    Forward,
    Reject,
    Disabled,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionRequest<'a> {
    protocol_version: u32,
    thread_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_control_socket: Option<&'a AbsolutePathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    launch_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    application_instance_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_control_nonce: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CallRequest<'a> {
    protocol_version: u32,
    #[serde(flatten)]
    params: &'a DynamicToolCallParams,
}

impl HostDynamicTools {
    #[cfg(target_os = "linux")]
    pub(crate) async fn command_output(
        &self,
        notification: &codex_app_server_protocol::ServerNotification,
    ) -> bool {
        if let codex_app_server_protocol::ServerNotification::CommandExecOutputDelta(output) =
            notification
            && let Some(control) = self.input_control.lock().await.as_ref()
        {
            return control.commands.output(output);
        }
        false
    }

    fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Acquire)
    }

    pub(crate) fn configure_fork(&self, params: &mut codex_app_server_protocol::ThreadForkParams) {
        params.experimental_raw_events = true;
        params.expected_dynamic_tools = Some(self.registration.dynamic_tools.clone());
    }

    #[cfg(test)]
    pub(crate) async fn connect(
        socket_path: Option<AbsolutePathBuf>,
    ) -> color_eyre::Result<Option<Arc<Self>>> {
        let home = tempfile::tempdir()?.keep();
        let state_db = codex_state::StateRuntime::init(
            codex_state::SqliteConfig::new_for_testing(AbsolutePathBuf::from_absolute_path(home)?),
            "test-provider".to_string(),
        )
        .await
        .map_err(|error| color_eyre::eyre::eyre!("{error:#}"))?;
        Self::connect_with_state(socket_path, Some(state_db)).await
    }

    pub(crate) async fn connect_with_state(
        socket_path: Option<AbsolutePathBuf>,
        state_db: Option<StateDbHandle>,
    ) -> color_eyre::Result<Option<Arc<Self>>> {
        let Some(socket_path) = socket_path else {
            return Ok(None);
        };
        let state_db = state_db.ok_or_else(|| {
            color_eyre::eyre::eyre!("host dynamic tools require durable completion storage")
        })?;
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = socket_path;
            color_eyre::eyre::bail!(
                "--host-dynamic-tools-socket is supported only on Linux and macOS"
            );
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            validate_socket_path(&socket_path).await?;
            let client = reqwest::Client::builder()
                .unix_socket(socket_path.as_path())
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .http1_only()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .map_err(|err| {
                    color_eyre::eyre::eyre!("failed to create host dynamic-tools client: {err}")
                })?;
            let registration =
                tokio::time::timeout(CONTROL_REQUEST_TIMEOUT, fetch_registration(&client))
                    .await
                    .map_err(|_| {
                        color_eyre::eyre::eyre!("host dynamic-tools registration timed out")
                    })??;
            let identities = validate_registration(&registration)?;
            if let Some(input_socket) = &registration.input_control_socket
                && (input_socket.as_path().parent() != socket_path.as_path().parent()
                    || input_socket == &socket_path)
            {
                color_eyre::eyre::bail!(
                    "host input socket must be a distinct path in the private host socket directory"
                );
            }
            Ok(Some(Arc::new(Self {
                registration,
                input_control: tokio::sync::Mutex::new(None),
                identities,
                primary_thread_id: Mutex::new(None),
                state_db,
                completions: Mutex::new(completions::HostToolCompletions::default()),
                settlement_sender: Mutex::new(None),
                input_settlement: cancellation::InputSettlementGate::default(),
                disabled: AtomicBool::new(false),
                application_instance_id: uuid::Uuid::new_v4().to_string(),
                session_generation: AtomicU64::new(0),
                client,
            })))
        }
    }

    pub(crate) fn configure_primary_start(&self, params: &mut ThreadStartParams) -> bool {
        if self.primary_thread_id().is_some() {
            return false;
        }
        params
            .dynamic_tools
            .get_or_insert_default()
            .extend(self.registration.dynamic_tools.clone());
        params.ephemeral = Some(false);
        params.persistence = Some(ThreadStartPersistence::Immediate);
        params.experimental_raw_events = true;
        true
    }

    pub(crate) fn routing(&self, params: &DynamicToolCallParams) -> HostDynamicToolRouting {
        let key = (params.namespace.clone(), params.tool.clone());
        let Some(kind) = self.identities.get(&key).copied() else {
            return HostDynamicToolRouting::Unregistered;
        };
        if self.is_disabled() {
            return HostDynamicToolRouting::Disabled;
        }
        let authorized = ThreadId::from_string(&params.thread_id)
            .ok()
            .is_some_and(|thread_id| self.primary_thread_id() == Some(thread_id));
        if authorized && kind.accepts(&params.arguments) {
            HostDynamicToolRouting::Forward
        } else {
            HostDynamicToolRouting::Reject
        }
    }

    pub(crate) fn should_attach(&self, thread_id: ThreadId) -> bool {
        self.primary_thread_id()
            .is_none_or(|primary_thread_id| primary_thread_id == thread_id)
    }

    pub(crate) async fn attach_primary_with_input(
        &self,
        thread_id: ThreadId,
        handle: codex_app_server_client::AppServerRequestHandle,
    ) -> color_eyre::Result<()> {
        #[cfg(not(unix))]
        let _ = handle;
        if self.is_disabled() || !self.should_attach(thread_id) {
            return Ok(());
        }
        #[cfg(unix)]
        if let Some(socket) = &self.registration.input_control_socket {
            let mut control = self.input_control.lock().await;
            if let Some(control) = control.as_ref() {
                // Reconnecting the same application and primary thread preserves
                // the binding held by outstanding exact cancellation requests.
                control.update_handle(thread_id, handle)?;
            } else {
                let binding = input_control::protocol::ExpectedBinding {
                    launch_id: self.registration.launch_id.clone(),
                    instance_id: self.application_instance_id.clone(),
                    generation: self.session_generation.fetch_add(1, Ordering::AcqRel) + 1,
                    nonce: self.registration.input_control_nonce.clone(),
                };
                match input_control::InputControl::start(
                    socket.clone(),
                    thread_id,
                    handle,
                    binding,
                    self.input_settlement.clone(),
                ) {
                    Ok(listener) => *control = Some(listener),
                    Err(error) => {
                        tracing::warn!(%error, "hosted input is unavailable; continuing without active steering")
                    }
                }
            }
        }
        self.attach_primary(thread_id).await
    }

    pub(crate) async fn attach_primary(&self, thread_id: ThreadId) -> color_eyre::Result<()> {
        if self.is_disabled() || !self.should_attach(thread_id) {
            return Ok(());
        }
        let pending = self.recover_completions_before_reattach(thread_id).await?;
        #[cfg(unix)]
        let (input_socket, binding_state) = {
            let control = self.input_control.lock().await;
            match control.as_ref() {
                Some(control) => (Some(control.socket.clone()), Some(control.binding_state())),
                None => (None, None),
            }
        };
        #[cfg(unix)]
        let binding = match binding_state {
            Some(binding) => Some(binding.lock().await.clone()),
            None => None,
        };
        #[cfg(unix)]
        tokio::time::timeout(
            SETTLEMENT_REQUEST_TIMEOUT,
            send_session(
                &self.client,
                thread_id,
                input_socket.as_ref(),
                binding.as_ref(),
            ),
        )
        .await
        .map_err(|_| {
            color_eyre::eyre::eyre!("host dynamic-tools session attachment timed out")
        })??;
        self.settle_reattached_pending(&thread_id.to_string(), &pending)
            .await?;
        *self
            .primary_thread_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(thread_id);
        Ok(())
    }

    pub(crate) async fn revalidate_registration(&self) -> color_eyre::Result<()> {
        if self.is_disabled() {
            return Ok(());
        }
        #[cfg(unix)]
        let registration =
            tokio::time::timeout(CONTROL_REQUEST_TIMEOUT, fetch_registration(&self.client))
                .await
                .map_err(|_| {
                    color_eyre::eyre::eyre!("host dynamic-tools registration timed out")
                })??;
        #[cfg(unix)]
        if registration != self.registration {
            color_eyre::eyre::bail!("host dynamic-tools registration changed during reconnect");
        }
        Ok(())
    }

    pub(crate) async fn call(
        &self,
        params: &DynamicToolCallParams,
    ) -> color_eyre::Result<DynamicToolCallResponse> {
        if self.is_disabled() {
            return Ok(crate::dynamic_tools::failure_response(DISABLED_MESSAGE));
        }
        if let Some(call_id) = &params.context_call_id {
            self.state_db
                .thread_queue()
                .register_host_tool_completion(&codex_state::HostToolCompletionKey {
                    thread_id: ThreadId::from_string(&params.thread_id)?,
                    context_call_id: call_id.clone(),
                })
                .await
                .map_err(|error| color_eyre::eyre::eyre!("{error:#}"))?;
            self.completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .register(call_id.clone())?;
        }
        #[cfg(unix)]
        return send_call(&self.client, params).await;
        #[cfg(not(unix))]
        color_eyre::eyre::bail!("host dynamic tools are unavailable on this platform")
    }

    pub(crate) async fn begin_cancellable_call(
        &self,
        request_id: codex_app_server_protocol::RequestId,
        params: &DynamicToolCallParams,
        events: crate::app_event_sender::AppEventSender,
    ) -> Result<Option<Arc<ActiveHostedCall>>, String> {
        if params.namespace.is_some() || params.tool != "haskell" {
            return Ok(None);
        }
        #[cfg(not(unix))]
        {
            let _ = (request_id, events);
            return Ok(None);
        }
        #[cfg(unix)]
        {
            let call = ActiveHostedCall::new(
                self.client.clone(),
                params,
                request_id,
                events,
                self.registration.launch_id.clone(),
                self.application_instance_id.clone(),
                self.session_generation.load(Ordering::Acquire),
                self.registration.input_control_nonce.clone(),
            );
            self.input_settlement.activate(&call).await?;
            Ok(Some(call))
        }
    }

    pub(crate) async fn finish_cancellable_call(&self, call: &Arc<ActiveHostedCall>) {
        call.mark_original_resolved().await;
        self.input_settlement.clear(call).await;
    }

    fn primary_thread_id(&self) -> Option<ThreadId> {
        *self
            .primary_thread_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn validate_registration(
    registration: &HostDynamicToolRegistration,
) -> color_eyre::Result<HashMap<(Option<String>, String), DynamicToolKind>> {
    if registration.protocol_version != PROTOCOL_VERSION {
        color_eyre::eyre::bail!(
            "unsupported host dynamic-tools protocol version {}",
            registration.protocol_version
        );
    }
    if registration.dynamic_tools.is_empty() {
        color_eyre::eyre::bail!("host dynamic-tools registration is empty");
    }
    if registration.input_control_socket.is_some()
        && (registration.launch_id.is_empty() || registration.input_control_nonce.is_empty())
    {
        color_eyre::eyre::bail!("host input control challenge is incomplete");
    }
    let mut identities = HashMap::new();
    for spec in &registration.dynamic_tools {
        match spec {
            DynamicToolSpec::Function(function) => {
                insert_identity(
                    &mut identities,
                    None,
                    &function.name,
                    DynamicToolKind::Function,
                )?;
            }
            DynamicToolSpec::Custom(custom) => {
                insert_identity(&mut identities, None, &custom.name, DynamicToolKind::Custom)?;
            }
            DynamicToolSpec::Namespace(namespace) => {
                if namespace.name == crate::dynamic_tools::NAMESPACE {
                    color_eyre::eyre::bail!(
                        "host dynamic tools may not use the codex_tui namespace"
                    );
                }
                for tool in &namespace.tools {
                    let (name, kind) = match tool {
                        DynamicToolNamespaceTool::Function(function) => {
                            (&function.name, DynamicToolKind::Function)
                        }
                        DynamicToolNamespaceTool::Custom(custom) => {
                            (&custom.name, DynamicToolKind::Custom)
                        }
                    };
                    insert_identity(&mut identities, Some(namespace.name.clone()), name, kind)?;
                }
            }
        }
    }
    if identities.is_empty() {
        color_eyre::eyre::bail!("host dynamic-tools registration contains no callable tools");
    }
    Ok(identities)
}

fn insert_identity(
    identities: &mut HashMap<(Option<String>, String), DynamicToolKind>,
    namespace: Option<String>,
    name: &str,
    kind: DynamicToolKind,
) -> color_eyre::Result<()> {
    if identities
        .insert((namespace.clone(), name.to_string()), kind)
        .is_some()
    {
        let identity = namespace.map_or_else(
            || name.to_string(),
            |namespace| format!("{namespace}.{name}"),
        );
        color_eyre::eyre::bail!("duplicate host dynamic-tool identity `{identity}`");
    }
    Ok(())
}

#[cfg(unix)]
async fn fetch_registration(
    client: &reqwest::Client,
) -> color_eyre::Result<HostDynamicToolRegistration> {
    let response = send_request(client.get(endpoint(REGISTRATION_PATH))).await?;
    require_status(response.status(), reqwest::StatusCode::OK)?;
    let body = read_bounded(response, MAX_REGISTRATION_RESPONSE_BYTES).await?;
    serde_json::from_slice(&body)
        .map_err(|err| color_eyre::eyre::eyre!("invalid host dynamic-tools registration: {err}"))
}

#[cfg(unix)]
async fn send_session(
    client: &reqwest::Client,
    thread_id: ThreadId,
    input_control_socket: Option<&AbsolutePathBuf>,
    binding: Option<&input_control::protocol::ExpectedBinding>,
) -> color_eyre::Result<()> {
    let thread_id = thread_id.to_string();
    let response = send_request(client.post(endpoint(SESSION_PATH)).json(&SessionRequest {
        protocol_version: PROTOCOL_VERSION,
        thread_id: &thread_id,
        input_control_socket,
        launch_id: binding.map(|binding| binding.launch_id.as_str()),
        application_instance_id: binding.map(|binding| binding.instance_id.as_str()),
        session_generation: binding.map(|binding| binding.generation),
        input_control_nonce: binding.map(|binding| binding.nonce.as_str()),
    }))
    .await?;
    require_status(response.status(), reqwest::StatusCode::NO_CONTENT)
}

#[cfg(unix)]
async fn send_call(
    client: &reqwest::Client,
    params: &DynamicToolCallParams,
) -> color_eyre::Result<DynamicToolCallResponse> {
    let response = send_request(client.post(endpoint(CALL_PATH)).json(&CallRequest {
        protocol_version: PROTOCOL_VERSION,
        params,
    }))
    .await?;
    require_status(response.status(), reqwest::StatusCode::OK)?;
    let body = read_bounded(response, MAX_CALL_RESPONSE_BYTES).await?;
    serde_json::from_slice(&body)
        .map_err(|err| color_eyre::eyre::eyre!("invalid host dynamic-tool response: {err}"))
}

#[cfg(unix)]
async fn send_request(builder: reqwest::RequestBuilder) -> color_eyre::Result<reqwest::Response> {
    builder
        .header(reqwest::header::CONNECTION, "close")
        .send()
        .await
        .map_err(|err| color_eyre::eyre::eyre!("host dynamic-tools endpoint is unavailable: {err}"))
}

#[cfg(unix)]
fn endpoint(path: &str) -> String {
    format!("http://localhost{path}")
}

#[cfg(unix)]
fn require_status(
    actual: reqwest::StatusCode,
    expected: reqwest::StatusCode,
) -> color_eyre::Result<()> {
    if actual != expected {
        color_eyre::eyre::bail!("host dynamic-tools endpoint returned HTTP {actual}");
    }
    Ok(())
}

#[cfg(unix)]
async fn read_bounded(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> color_eyre::Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|content_length| content_length > max_bytes as u64)
    {
        color_eyre::eyre::bail!("host dynamic-tools response exceeded {max_bytes} bytes");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|err| {
        color_eyre::eyre::eyre!("failed reading host dynamic-tools response: {err}")
    })? {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            color_eyre::eyre::bail!("host dynamic-tools response exceeded {max_bytes} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn validate_socket_path(socket_path: &AbsolutePathBuf) -> color_eyre::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::fs::PermissionsExt;

    #[cfg(target_os = "linux")]
    const MAX_SOCKET_PATH_BYTES: usize = 107;
    #[cfg(target_os = "macos")]
    const MAX_SOCKET_PATH_BYTES: usize = 103;

    if socket_path.as_path().as_os_str().as_bytes().len() > MAX_SOCKET_PATH_BYTES {
        color_eyre::eyre::bail!("host dynamic-tools socket path is too long");
    }
    let metadata = tokio::fs::symlink_metadata(socket_path.as_path())
        .await
        .map_err(|err| color_eyre::eyre::eyre!("cannot access host dynamic-tools socket: {err}"))?;
    if !metadata.file_type().is_socket() {
        color_eyre::eyre::bail!("host dynamic-tools path is not a Unix socket");
    }
    let parent = socket_path.as_path().parent().ok_or_else(|| {
        color_eyre::eyre::eyre!("host dynamic-tools socket has no parent directory")
    })?;
    let parent_metadata = tokio::fs::metadata(parent).await.map_err(|err| {
        color_eyre::eyre::eyre!("cannot inspect host dynamic-tools socket directory: {err}")
    })?;
    if parent_metadata.permissions().mode() & 0o077 != 0 {
        color_eyre::eyre::bail!("host dynamic-tools socket directory must be owner-only");
    }
    Ok(())
}

pub(crate) fn infrastructure_failure() -> DynamicToolCallResponse {
    crate::dynamic_tools::failure_response("host dynamic-tool infrastructure failure")
}

#[cfg(test)]
#[path = "host_dynamic_tools_tests.rs"]
mod tests;

#[cfg(all(test, unix))]
pub(crate) use tests::spawn_cancellable_host;
#[cfg(all(test, unix))]
pub(crate) use tests::spawn_cancellable_host_with_input;
#[cfg(test)]
pub(crate) use tests::spawn_host;
#[cfg(all(test, unix))]
pub(crate) use tests::spawn_host_with_input;
