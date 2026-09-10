//! Private command control through the owning TUI's existing app-server connection.
use super::input_control::InputTarget;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::CommandExecOutputDeltaNotification;
use codex_app_server_protocol::CommandExecOutputStream;
use codex_app_server_protocol::CommandExecParams;
use codex_app_server_protocol::CommandExecResizeParams;
use codex_app_server_protocol::CommandExecResponse;
use codex_app_server_protocol::CommandExecTerminalSize;
use codex_app_server_protocol::CommandExecTerminateParams;
use codex_app_server_protocol::CommandExecWriteParams;
use codex_app_server_protocol::RequestId;
use codex_utils_pty::OutputTail;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

const STREAM_CAP: usize = OutputTail::CAPACITY;
const RETAINED_JOBS: usize = 32;

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Spec {
    argv: Vec<String>,
    directory: Option<String>,
    environment: Vec<(String, String)>,
    memory: i64,
    input: Input,
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
enum Input {
    #[serde(rename = "ClosedInput")]
    Closed,
    #[serde(rename = "PipeInput")]
    Pipe,
    #[serde(rename = "TerminalInput")]
    Terminal,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Request {
    thread_id: String,
    id: String,
    #[serde(flatten)]
    operation: Operation,
}
#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum Operation {
    Start { spec: Spec },
    Wait,
    Output { bytes: usize },
    Input { text: String },
    CloseInput,
    Resize { rows: u16, columns: u16 },
    Cancel,
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum JobState {
    Starting,
    Finished { exit_code: i32, cancelled: bool },
    Failed { detail: String },
}
#[derive(Serialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub(super) enum Response {
    State(JobState),
    Output {
        stdout: String,
        stderr: String,
        truncated: bool,
    },
    Acknowledged,
}
struct Job {
    spec: Spec,
    phase: watch::Sender<JobState>,
    stdout: OutputTail,
    stderr: OutputTail,
    expired: bool,
    truncated: bool,
    cancelled: bool,
}
#[derive(Default)]
struct JobsState {
    jobs: HashMap<String, Job>,
    completed: VecDeque<String>,
}
#[derive(Clone, Default)]
pub(super) struct Jobs(Arc<Mutex<JobsState>>);

impl Jobs {
    pub(super) fn output(&self, notification: &CommandExecOutputDeltaNotification) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(job) = state.jobs.get_mut(&notification.process_id) else {
            return false;
        };
        if !matches!(*job.phase.borrow(), JobState::Starting) {
            return true;
        }
        let Ok(bytes) = STANDARD.decode(&notification.delta_base64) else {
            return true;
        };
        let buffer = match notification.stream {
            CommandExecOutputStream::Stdout => &mut job.stdout,
            CommandExecOutputStream::Stderr => &mut job.stderr,
        };
        buffer.push(&bytes);
        job.truncated |= notification.cap_reached;
        true
    }

    fn finish(&self, id: &str, response: Result<CommandExecResponse, String>) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(job) = state.jobs.get_mut(id) {
            let phase = match response {
                Ok(response) => {
                    job.stdout = OutputTail::default();
                    job.stderr = OutputTail::default();
                    job.stdout.push(response.stdout.as_bytes());
                    job.stderr.push(response.stderr.as_bytes());
                    job.truncated |=
                        response.stdout.len() >= STREAM_CAP || response.stderr.len() >= STREAM_CAP;
                    JobState::Finished {
                        exit_code: response.exit_code,
                        cancelled: job.cancelled,
                    }
                }
                Err(detail) => JobState::Failed { detail },
            };
            job.phase.send_replace(phase);
            state.completed.push_back(id.to_owned());
            while state.completed.len() > RETAINED_JOBS {
                if let Some(old) = state.completed.pop_front()
                    && let Some(job) = state.jobs.get_mut(&old)
                {
                    job.stdout = OutputTail::default();
                    job.stderr = OutputTail::default();
                    job.expired = true;
                }
            }
        }
    }
}

fn request_id() -> RequestId {
    RequestId::String(format!("host-command-{}", uuid::Uuid::new_v4()))
}
fn failure(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::CONFLICT, error.to_string())
}

pub(super) async fn dispatch(
    State(target): State<InputTarget>,
    Json(request): Json<Request>,
) -> Result<Json<Response>, (StatusCode, String)> {
    if request.thread_id != target.thread.to_string() || uuid::Uuid::parse_str(&request.id).is_err()
    {
        return Err(failure("invalid hosted command identity"));
    }
    let id = request.id;
    let handle = target.handle.borrow().clone();
    if let Operation::Start { spec } = request.operation {
        if !codex_utils_pty::managed_commands() {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "hosted commands require managed command resources".into(),
            ));
        }
        if spec.argv.is_empty() || spec.memory <= 0 {
            return Err(failure("invalid command description"));
        }
        let size = {
            let mut state = target
                .commands
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(job) = state.jobs.get(&id) {
                if job.spec != spec {
                    return Err(failure(
                        "command identity already binds another description",
                    ));
                }
                return Ok(Json(Response::State(job.phase.borrow().clone())));
            }
            let size = match spec.input {
                Input::Terminal => {
                    let (cols, rows) = crossterm::terminal::size().map_err(failure)?;
                    Some(CommandExecTerminalSize { rows, cols })
                }
                Input::Closed | Input::Pipe => None,
            };
            state.jobs.insert(
                id.clone(),
                Job {
                    spec: spec.clone(),
                    phase: watch::channel(JobState::Starting).0,
                    stdout: OutputTail::default(),
                    stderr: OutputTail::default(),
                    expired: false,
                    truncated: false,
                    cancelled: false,
                },
            );
            size
        };
        let params = CommandExecParams {
            command: spec.argv,
            process_id: Some(id.clone()),
            tty: size.is_some(),
            stream_stdin: !matches!(spec.input, Input::Closed),
            stream_stdout_stderr: true,
            output_bytes_cap: None,
            disable_output_cap: true,
            disable_timeout: true,
            timeout_ms: None,
            cwd: spec.directory.map(Into::into),
            env: Some(
                spec.environment
                    .into_iter()
                    .map(|(k, v)| (k, Some(v)))
                    .collect(),
            ),
            size,
            sandbox_policy: None,
            permission_profile: None,
        };
        let jobs = target.commands.clone();
        tokio::spawn(async move {
            let result = handle
                .request_typed::<CommandExecResponse>(ClientRequest::OneOffCommandExec {
                    request_id: request_id(),
                    params,
                })
                .await
                .map_err(|error| error.to_string());
            jobs.finish(&id, result);
        });
        return Ok(Json(Response::State(JobState::Starting)));
    }
    let mut phase = {
        let state = target
            .commands
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let job = state.jobs.get(&id).ok_or_else(|| {
            failure("native command is unavailable or its retained output expired")
        })?;
        job.phase.subscribe()
    };
    let call = match request.operation {
        Operation::Start { .. } => unreachable!("start handled above"),
        Operation::Wait => {
            if matches!(*phase.borrow_and_update(), JobState::Starting) {
                let _ = tokio::time::timeout(Duration::from_secs(20), phase.changed()).await;
            }
            return Ok(Json(Response::State(phase.borrow().clone())));
        }
        Operation::Output { bytes } => {
            if bytes > 65536 {
                return Err(failure("output reads are bounded to 65536 bytes"));
            }
            let state = target
                .commands
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let job = state
                .jobs
                .get(&id)
                .ok_or_else(|| failure("output expired"))?;
            if job.expired {
                return Err(failure("retained command output expired"));
            }
            return Ok(Json(Response::Output {
                stdout: String::from_utf8_lossy(&job.stdout.read(bytes / 2)).into_owned(),
                stderr: String::from_utf8_lossy(&job.stderr.read(bytes / 2)).into_owned(),
                truncated: job.truncated
                    || job.stdout.truncated(bytes / 2)
                    || job.stderr.truncated(bytes / 2),
            }));
        }
        Operation::Input { text } => ClientRequest::CommandExecWrite {
            request_id: request_id(),
            params: CommandExecWriteParams {
                process_id: id,
                delta_base64: Some(STANDARD.encode(text.as_bytes())),
                close_stdin: false,
            },
        },
        Operation::CloseInput => {
            let state = target
                .commands
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state
                .jobs
                .get(&id)
                .is_some_and(|job| matches!(job.spec.input, Input::Terminal))
            {
                return Err(failure(
                    "PTY input has terminal semantics; send the terminal EOF key explicitly",
                ));
            }
            ClientRequest::CommandExecWrite {
                request_id: request_id(),
                params: CommandExecWriteParams {
                    process_id: id,
                    delta_base64: None,
                    close_stdin: true,
                },
            }
        }
        Operation::Resize { rows, columns } => ClientRequest::CommandExecResize {
            request_id: request_id(),
            params: CommandExecResizeParams {
                process_id: id,
                size: CommandExecTerminalSize {
                    rows,
                    cols: columns,
                },
            },
        },
        Operation::Cancel => {
            let mut state = target
                .commands
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(job) = state.jobs.get_mut(&id) {
                job.cancelled = true;
            }
            if !matches!(*phase.borrow(), JobState::Starting) {
                return Ok(Json(Response::Acknowledged));
            }
            let request = ClientRequest::CommandExecTerminate {
                request_id: request_id(),
                params: CommandExecTerminateParams { process_id: id },
            };
            // This is an intent receipt. The retained completion proves termination;
            // the resource authority separately fences joins and kills descendants.
            tokio::spawn(async move {
                if let Err(error) =
                    tokio::time::timeout(Duration::from_secs(30), handle.request(request)).await
                {
                    tracing::warn!(%error, "native command cancellation response unavailable");
                }
            });
            return Ok(Json(Response::Acknowledged));
        }
    };
    tokio::time::timeout(Duration::from_secs(30), handle.request(call))
        .await
        .map_err(failure)?
        .map_err(failure)?
        .map_err(|error| failure(error.message))?;
    Ok(Json(Response::Acknowledged))
}
