//! Private command control through the owning TUI's existing app-server connection.
use super::command_output::RetainedOutput;
use super::input_control::InputTarget;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::CommandExecOutputDeltaNotification;
use codex_app_server_protocol::CommandExecOutputEnd;
use codex_app_server_protocol::CommandExecOutputStream;
use codex_app_server_protocol::CommandExecParams;
use codex_app_server_protocol::CommandExecResizeParams;
use codex_app_server_protocol::CommandExecResponse;
use codex_app_server_protocol::CommandExecTerminalSize;
use codex_app_server_protocol::CommandExecTerminateParams;
use codex_app_server_protocol::CommandExecWriteParams;
use codex_app_server_protocol::RequestId;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

const RETAINED_JOBS: usize = 32;
const RETAINED_BYTES: usize = 128 * 1024 * 1024;

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
    Read { stream: Stream, position: Position },
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
    Output { stdout: Page, stderr: Page },
    Page(Page),
    Acknowledged,
}
#[derive(Deserialize)]
enum Stream {
    Stdout,
    Stderr,
}
#[derive(Deserialize)]
#[expect(
    clippy::enum_variant_names,
    reason = "Matches Haskell constructors on the private command relay"
)]
enum Position {
    OutputBeginning,
    OutputTail,
    OutputOffset(i64),
    OutputSlice(i64, i64),
}
#[derive(Serialize)]
pub(super) struct Page {
    text: String,
    start: i64,
    end: i64,
    available_end: i64,
    retained_start: i64,
    lost_bytes: i64,
    finished: bool,
    lossy: bool,
    leading_fragment: bool,
    trailing_fragment: bool,
}
impl Page {
    fn read(
        buffer: &RetainedOutput,
        position: Position,
        bytes: usize,
        finished: bool,
    ) -> Result<Self, (StatusCode, String)> {
        let bytes = match &position {
            Position::OutputSlice(_, requested) => usize::try_from(*requested)
                .ok()
                .filter(|bytes| (1..=65536).contains(bytes))
                .ok_or_else(|| failure("output slice size must be 1..65536 bytes"))?,
            _ => bytes,
        };
        let offset = match position {
            Position::OutputBeginning => Some(0),
            Position::OutputTail => None,
            Position::OutputOffset(n) | Position::OutputSlice(n, _) => {
                Some(u64::try_from(n).map_err(|_| failure("negative output position"))?)
            }
        };
        let page = buffer.page(offset, bytes).map_err(failure)?;
        let text = String::from_utf8_lossy(&page.bytes);
        Ok(Self {
            lossy: matches!(text, std::borrow::Cow::Owned(_)),
            text: text.into_owned(),
            start: i64::try_from(page.start).map_err(failure)?,
            end: i64::try_from(page.end).map_err(failure)?,
            available_end: i64::try_from(page.available_end).map_err(failure)?,
            retained_start: i64::try_from(page.retained_start).map_err(failure)?,
            lost_bytes: i64::try_from(page.lost_bytes).map_err(failure)?,
            finished,
            leading_fragment: page.leading_fragment,
            trailing_fragment: page.trailing_fragment,
        })
    }
}
struct Job {
    spec: Spec,
    phase: watch::Sender<JobState>,
    stdout: RetainedOutput,
    stderr: RetainedOutput,
    expired: bool,
    cancelled: bool,
    stdout_closed: bool,
    stderr_closed: bool,
    pending_finish: Option<JobState>,
}
#[derive(Default)]
struct JobsState {
    jobs: HashMap<String, Job>,
    completed: VecDeque<String>,
    retained_bytes: usize,
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
            job.phase.send_replace(JobState::Failed {
                detail: "invalid command output encoding".into(),
            });
            Self::retain_completed(&mut state, &notification.process_id);
            return true;
        };
        if match notification.stream {
            CommandExecOutputStream::Stdout => job.stdout_closed,
            CommandExecOutputStream::Stderr => job.stderr_closed,
        } {
            return true;
        }
        let growth = match notification.stream {
            CommandExecOutputStream::Stdout => job.stdout.growth(bytes.len()),
            CommandExecOutputStream::Stderr => job.stderr.growth(bytes.len()),
        };
        Self::make_room(&mut state, growth);
        let available = RETAINED_BYTES.saturating_sub(state.retained_bytes);
        let Some(job) = state.jobs.get_mut(&notification.process_id) else {
            return false;
        };
        let (buffer, closed) = match notification.stream {
            CommandExecOutputStream::Stdout => (&mut job.stdout, &mut job.stdout_closed),
            CommandExecOutputStream::Stderr => (&mut job.stderr, &mut job.stderr_closed),
        };
        let before = buffer.retained_len();
        buffer.push(&bytes, before.saturating_add(available));
        let after = buffer.retained_len();
        *closed = notification.end_of_stream.is_some();
        // Hosted commands disable the app-server cap. Missing chunks must not
        // silently acquire authoritative positions.
        if notification.cap_reached
            || matches!(
                notification.end_of_stream,
                Some(CommandExecOutputEnd::Capped | CommandExecOutputEnd::DrainTimeout)
            )
        {
            job.pending_finish = Some(JobState::Failed {
                detail: "command output stream ended without complete capture".into(),
            });
        }
        let finished = job.stdout_closed && job.stderr_closed;
        let phase = if finished {
            job.pending_finish.take()
        } else {
            None
        };
        let settled = phase.is_some();
        if let Some(phase) = phase {
            job.phase.send_replace(phase);
        }
        state.retained_bytes = state
            .retained_bytes
            .saturating_sub(before)
            .saturating_add(after);
        if settled {
            Self::retain_completed(&mut state, &notification.process_id);
        }
        true
    }

    fn finish(&self, id: &str, response: Result<CommandExecResponse, String>) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(job) = state.jobs.get_mut(id) {
            if !matches!(*job.phase.borrow(), JobState::Starting) {
                return;
            }
            let phase = match (job.pending_finish.take(), response) {
                (Some(failure @ JobState::Failed { .. }), _) => failure,
                (_, Ok(response)) => {
                    // Stream notifications precede completion on the owning connection.
                    // Retain their byte positions rather than replacing them with the
                    // response's bounded, decoded tails.
                    JobState::Finished {
                        exit_code: response.exit_code,
                        cancelled: job.cancelled,
                    }
                }
                (_, Err(detail)) => JobState::Failed { detail },
            };
            if job.stdout_closed && job.stderr_closed || matches!(phase, JobState::Failed { .. }) {
                job.phase.send_replace(phase);
                Self::retain_completed(&mut state, id);
            } else {
                job.pending_finish = Some(phase);
            }
        }
    }

    fn make_room(state: &mut JobsState, additional: usize) {
        while state.retained_bytes.saturating_add(additional) > RETAINED_BYTES {
            let Some(old) = state.completed.pop_front() else {
                break;
            };
            Self::expire_output(state, &old);
        }
    }

    fn expire_output(state: &mut JobsState, id: &str) {
        if let Some(job) = state.jobs.get_mut(id) {
            state.retained_bytes = state
                .retained_bytes
                .saturating_sub(job.stdout.retained_len() + job.stderr.retained_len());
            job.stdout = RetainedOutput::default();
            job.stderr = RetainedOutput::default();
            job.expired = true;
        }
    }

    fn retain_completed(state: &mut JobsState, id: &str) {
        state.completed.push_back(id.to_owned());
        while state.completed.len() > RETAINED_JOBS {
            if let Some(old) = state.completed.pop_front() {
                Self::expire_output(state, &old);
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
                    stdout: RetainedOutput::default(),
                    stderr: RetainedOutput::default(),
                    expired: false,
                    cancelled: false,
                    stdout_closed: false,
                    stderr_closed: false,
                    pending_finish: None,
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
            if bytes > 1024 * 1024 {
                return Err(failure(
                    "output reads are bounded to 1048576 bytes per stream",
                ));
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
            let finished = matches!(*job.phase.borrow(), JobState::Finished { .. });
            return Ok(Json(Response::Output {
                stdout: Page::read(&job.stdout, Position::OutputBeginning, bytes, finished)?,
                stderr: Page::read(&job.stderr, Position::OutputBeginning, bytes, finished)?,
            }));
        }
        Operation::Read { stream, position } => {
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
            let buffer = match stream {
                Stream::Stdout => &job.stdout,
                Stream::Stderr => &job.stderr,
            };
            return Ok(Json(Response::Page(Page::read(
                buffer,
                position,
                64 * 1024,
                matches!(*job.phase.borrow(), JobState::Finished { .. }),
            )?)));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn jobs() -> Jobs {
        let jobs = Jobs::default();
        jobs.0.lock().unwrap().jobs.insert(
            "test".into(),
            Job {
                spec: Spec {
                    argv: vec!["true".into()],
                    directory: None,
                    environment: vec![],
                    memory: 256,
                    input: Input::Closed,
                },
                phase: watch::channel(JobState::Starting).0,
                stdout: RetainedOutput::default(),
                stderr: RetainedOutput::default(),
                expired: false,
                cancelled: false,
                stdout_closed: false,
                stderr_closed: false,
                pending_finish: None,
            },
        );
        jobs
    }

    fn output(jobs: &Jobs, stream: CommandExecOutputStream, bytes: &[u8], end_of_stream: bool) {
        assert!(jobs.output(&CommandExecOutputDeltaNotification {
            process_id: "test".into(),
            stream,
            delta_base64: STANDARD.encode(bytes),
            cap_reached: false,
            end_of_stream: end_of_stream.then_some(CommandExecOutputEnd::Complete),
        }));
    }

    #[test]
    fn completion_waits_for_both_streams_in_either_delivery_order() {
        for response_first in [false, true] {
            let jobs = jobs();
            output(&jobs, CommandExecOutputStream::Stdout, b"first\n", false);
            let before = jobs.0.lock().unwrap().jobs["test"]
                .stdout
                .page(Some(0), 8192)
                .unwrap();
            let finish = || {
                jobs.finish(
                    "test",
                    Ok(CommandExecResponse {
                        exit_code: 0,
                        stdout: "ignored-response-tail".into(),
                        stderr: String::new(),
                    }),
                )
            };
            if response_first {
                finish();
            }
            assert!(matches!(
                *jobs.0.lock().unwrap().jobs["test"].phase.borrow(),
                JobState::Starting
            ));
            output(&jobs, CommandExecOutputStream::Stdout, b"last\n", true);
            assert!(matches!(
                *jobs.0.lock().unwrap().jobs["test"].phase.borrow(),
                JobState::Starting
            ));
            output(&jobs, CommandExecOutputStream::Stderr, b"", true);
            if !response_first {
                finish();
            }
            let state = jobs.0.lock().unwrap();
            let job = &state.jobs["test"];
            assert!(matches!(
                *job.phase.borrow(),
                JobState::Finished { exit_code: 0, .. }
            ));
            assert_eq!(
                job.stdout.page(Some(before.end), 8192).unwrap().bytes,
                b"last\n"
            );
            assert_eq!(state.completed.len(), 1);
        }
    }

    #[test]
    fn incomplete_stream_end_cannot_become_successful_output() {
        for end in [
            CommandExecOutputEnd::Capped,
            CommandExecOutputEnd::DrainTimeout,
        ] {
            let jobs = jobs();
            jobs.finish(
                "test",
                Ok(CommandExecResponse {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                }),
            );
            assert!(jobs.output(&CommandExecOutputDeltaNotification {
                process_id: "test".into(),
                stream: CommandExecOutputStream::Stdout,
                delta_base64: STANDARD.encode(b"partial"),
                cap_reached: false,
                end_of_stream: Some(end),
            }));
            output(&jobs, CommandExecOutputStream::Stderr, b"", true);
            let state = jobs.0.lock().unwrap();
            assert!(matches!(
                *state.jobs["test"].phase.borrow(),
                JobState::Failed { .. }
            ));
            assert_eq!(
                state.jobs["test"].stdout.page(Some(0), 8192).unwrap().bytes,
                b"partial"
            );
            assert_eq!(state.completed.len(), 1);
        }
    }

    #[test]
    fn quota_evicts_completed_output_before_limiting_active_jobs() {
        let jobs = jobs();
        output(&jobs, CommandExecOutputStream::Stdout, b"live", false);
        let mut state = jobs.0.lock().unwrap();
        let mut completed = Job {
            spec: state.jobs["test"].spec.clone(),
            phase: watch::channel(JobState::Finished {
                exit_code: 0,
                cancelled: false,
            })
            .0,
            stdout: RetainedOutput::default(),
            stderr: RetainedOutput::default(),
            expired: false,
            cancelled: false,
            stdout_closed: true,
            stderr_closed: true,
            pending_finish: None,
        };
        completed.stdout.push(b"old log", RETAINED_BYTES);
        state.retained_bytes += completed.stdout.retained_len();
        state.jobs.insert("done".into(), completed);
        state.completed.push_back("done".into());
        let live_bytes = state.jobs["test"].stdout.retained_len();
        Jobs::make_room(&mut state, RETAINED_BYTES - live_bytes);
        assert!(state.jobs["done"].expired);
        assert!(!state.jobs["test"].expired);
        assert_eq!(state.retained_bytes, live_bytes);
        assert_eq!(state.jobs["done"].stdout.retained_len(), 0);
        assert_eq!(
            state.jobs["test"].stdout.page(Some(0), 64).unwrap().bytes,
            b"live"
        );
    }

    #[test]
    fn bounded_read_preserves_byte_positions_for_unicode_and_invalid_utf8() {
        let mut output = RetainedOutput::default();
        output.push("αβγ".as_bytes(), RETAINED_BYTES);
        let position: Position = serde_json::from_str(r#"{"OutputSlice":[0,3]}"#).unwrap();
        let first = Page::read(&output, position, 65536, false).unwrap();
        assert_eq!(
            (
                first.text.as_str(),
                first.start,
                first.end,
                first.available_end,
                first.finished
            ),
            ("α", 0, 2, 6, false)
        );
        let next = Page::read(&output, Position::OutputSlice(first.end, 4), 65536, true).unwrap();
        assert_eq!(
            (next.text.as_str(), next.start, next.end, next.lossy),
            ("βγ", 2, 6, false)
        );
        output.push(&[0xff, 0xff, 0xff], RETAINED_BYTES);
        let invalid = Page::read(&output, Position::OutputSlice(6, 2), 65536, true).unwrap();
        assert_eq!(
            (
                invalid.text.as_str(),
                invalid.start,
                invalid.end,
                invalid.lossy
            ),
            ("��", 6, 8, true)
        );
        for position in [
            Position::OutputSlice(-1, 8),
            Position::OutputSlice(0, 0),
            Position::OutputSlice(0, 65537),
        ] {
            assert!(Page::read(&output, position, 65536, true).is_err());
        }
    }

    #[test]
    fn page_rendering_reports_loss_and_preserves_valid_unicode() {
        let mut tail = RetainedOutput::default();
        tail.push("αβ\n".as_bytes(), RETAINED_BYTES);
        let page = Page::read(&tail, Position::OutputBeginning, 8192, true).unwrap();
        assert_eq!(page.text, "αβ\n");
        assert!(!page.lossy);
        tail.push(
            &vec![b'x'; codex_utils_pty::OutputTail::CAPACITY],
            codex_utils_pty::OutputTail::CAPACITY,
        );
        let page = Page::read(&tail, Position::OutputBeginning, 8192, true).unwrap();
        assert_eq!(page.lost_bytes, 0);
        assert_eq!(page.retained_start, 0);
        assert_eq!(page.text, "αβ\n");
        assert!(Page::read(&tail, Position::OutputOffset(-1), 8192, true).is_err());
    }
}
