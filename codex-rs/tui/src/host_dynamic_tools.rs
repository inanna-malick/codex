use codex_app_server_protocol::DynamicToolCallParams;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_protocol::ThreadId;
use codex_protocol::dynamic_tools::DynamicToolNamespaceTool;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

const PROTOCOL_VERSION: u32 = 1;
const REGISTRATION_PATH: &str = "/v1/dynamic-tools/registration";
const SESSION_PATH: &str = "/v1/dynamic-tools/session";
const CALL_PATH: &str = "/v1/dynamic-tools/call";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
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
}

#[derive(Debug)]
pub(crate) struct HostDynamicTools {
    registration: HostDynamicToolRegistration,
    identities: HashMap<(Option<String>, String), DynamicToolKind>,
    primary_thread_id: Mutex<Option<ThreadId>>,
    #[cfg(unix)]
    client: reqwest::Client,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostDynamicToolRouting {
    Unregistered,
    Forward,
    Reject,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionRequest<'a> {
    protocol_version: u32,
    thread_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CallRequest<'a> {
    protocol_version: u32,
    #[serde(flatten)]
    params: &'a DynamicToolCallParams,
}

impl HostDynamicTools {
    pub(crate) async fn connect(
        socket_path: Option<AbsolutePathBuf>,
    ) -> color_eyre::Result<Option<Arc<Self>>> {
        let Some(socket_path) = socket_path else {
            return Ok(None);
        };
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
            Ok(Some(Arc::new(Self {
                registration,
                identities,
                primary_thread_id: Mutex::new(None),
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
        true
    }

    pub(crate) fn routing(&self, params: &DynamicToolCallParams) -> HostDynamicToolRouting {
        let key = (params.namespace.clone(), params.tool.clone());
        let Some(kind) = self.identities.get(&key).copied() else {
            return HostDynamicToolRouting::Unregistered;
        };
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

    pub(crate) async fn attach_primary(&self, thread_id: ThreadId) -> color_eyre::Result<()> {
        if !self.should_attach(thread_id) {
            return Ok(());
        }
        #[cfg(unix)]
        tokio::time::timeout(
            CONTROL_REQUEST_TIMEOUT,
            send_session(&self.client, thread_id),
        )
        .await
        .map_err(|_| {
            color_eyre::eyre::eyre!("host dynamic-tools session attachment timed out")
        })??;
        *self
            .primary_thread_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(thread_id);
        Ok(())
    }

    pub(crate) async fn revalidate_registration(&self) -> color_eyre::Result<()> {
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
        #[cfg(unix)]
        return send_call(&self.client, params).await;
        #[cfg(not(unix))]
        color_eyre::eyre::bail!("host dynamic tools are unavailable on this platform")
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
async fn send_session(client: &reqwest::Client, thread_id: ThreadId) -> color_eyre::Result<()> {
    let thread_id = thread_id.to_string();
    let response = send_request(client.post(endpoint(SESSION_PATH)).json(&SessionRequest {
        protocol_version: PROTOCOL_VERSION,
        thread_id: &thread_id,
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

#[cfg(test)]
pub(crate) use tests::spawn_host;
