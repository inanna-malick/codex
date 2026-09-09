//! Private Shoal command admission; the host retains process-tree resource custody.
use super::workspace_admission::WriterScope;
use serde::Deserialize;
use serde::Serialize;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Acquire,
    Started,
    Finished,
    Cancel,
}
#[derive(Serialize)]
struct Request<'a> {
    id: &'a str,
    operation: Operation,
}
#[derive(Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Status {
    Queued,
    Admitted { cgroup: PathBuf },
    Running,
    Completed,
    ResourceExhausted,
    AdmissionTimedOut,
    CancelledBeforeStart,
    CleanupUnconfirmed { detail: String },
}
pub(super) struct Grant {
    client: reqwest::Client,
    id: String,
    scope: Arc<WriterScope>,
    submitted: bool,
}
impl Grant {
    pub(super) async fn acquire() -> io::Result<Option<Self>> {
        let Some(socket) = std::env::var_os("CODEX_COMMAND_RESOURCE_SOCKET") else {
            return Ok(None);
        };
        let client = reqwest::Client::builder()
            .unix_socket(PathBuf::from(socket))
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(std::time::Duration::from_secs(310))
            .build()
            .map_err(io::Error::other)?;
        let id = uuid::Uuid::new_v4().to_string();
        // Cancellation during admission must cancel the queued identity too.
        let mut pending = Pending {
            client: client.clone(),
            id: id.clone(),
            active: true,
        };
        let response = client
            .post("http://localhost/v1/commands/resources")
            .json(&Request {
                id: &id,
                operation: Operation::Acquire,
            })
            .send()
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "command resource admission failed; command not started: {error}"
                ))
            })?
            .error_for_status()
            .map_err(|error| {
                io::Error::other(format!(
                    "command resource admission rejected; command not started: {error}"
                ))
            })?;
        let status: Status = response.json().await.map_err(io::Error::other)?;
        match status {
            Status::Admitted { cgroup } => {
                let scope = Arc::new(WriterScope::command(
                    cgroup,
                    Receipt {
                        client: client.clone(),
                        id: id.clone(),
                    },
                )?);
                pending.active = false;
                Ok(Some(Self {
                    client,
                    id,
                    scope,
                    submitted: false,
                }))
            }
            Status::AdmissionTimedOut => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "command resource admission timed out; command not started",
            )),
            Status::CancelledBeforeStart => Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "command resource admission cancelled; command not started",
            )),
            Status::CleanupUnconfirmed { detail } => Err(io::Error::other(format!(
                "command resource custody unconfirmed: {detail}"
            ))),
            Status::Queued | Status::Running | Status::Completed | Status::ResourceExhausted => {
                Err(io::Error::other(
                    "unexpected command resource admission state; command not started",
                ))
            }
        }
    }
    pub(super) fn scope(&self) -> Arc<WriterScope> {
        self.scope.clone()
    }
    pub(super) fn started(&mut self) {
        self.submitted = true;
        send(&self.client, &self.id, Operation::Started);
    }
}
struct Pending {
    client: reqwest::Client,
    id: String,
    active: bool,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if self.active {
            send(&self.client, &self.id, Operation::Cancel);
        }
    }
}
impl Drop for Grant {
    fn drop(&mut self) {
        if !self.submitted {
            send(&self.client, &self.id, Operation::Cancel);
        }
    }
}
fn send(client: &reqwest::Client, id: &str, operation: Operation) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let client = client.clone();
        let id = id.to_owned();
        runtime.spawn(async move {
            let _ = client
                .post("http://localhost/v1/commands/resources")
                .timeout(std::time::Duration::from_secs(10))
                .json(&Request { id: &id, operation })
                .send()
                .await;
        });
    }
}

#[derive(Clone)]
pub(super) struct Receipt {
    client: reqwest::Client,
    id: String,
}
impl Receipt {
    pub(super) async fn resource_exhausted(&self) -> io::Result<bool> {
        let response = self
            .client
            .post("http://localhost/v1/commands/resources")
            .timeout(std::time::Duration::from_secs(10))
            .json(&Request {
                id: &self.id,
                operation: Operation::Finished,
            })
            .send()
            .await
            .map_err(io::Error::other)?
            .error_for_status()
            .map_err(io::Error::other)?;
        match response.json::<Status>().await.map_err(io::Error::other)? {
            Status::ResourceExhausted => Ok(true),
            Status::Completed | Status::Running => Ok(false),
            Status::CleanupUnconfirmed { detail } => Err(io::Error::other(detail)),
            Status::Queued
            | Status::Admitted { .. }
            | Status::AdmissionTimedOut
            | Status::CancelledBeforeStart => {
                Err(io::Error::other("command result is unconfirmed"))
            }
        }
    }
}
