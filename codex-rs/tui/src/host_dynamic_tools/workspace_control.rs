//! Private hosted publication control for the owning in-process native runtime.

use super::input_control::InputTarget;
use axum::Json;
use axum::extract::State;
use codex_app_server_client::AppServerRequestHandle;
use codex_utils_pty::workspace_admission::Availability;
use codex_utils_pty::workspace_admission::PublicationAdmission;
use codex_utils_pty::workspace_admission::{self};
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU64;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Request {
    thread_id: String,
    sequence: NonZeroU64,
    operation: Operation,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum Operation {
    Begin,
    Finish,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub(super) enum Response {
    Ready {
        pid: u32,
        #[serde(rename = "cgroupPath")]
        cgroup_path: String,
    },
    Settled,
    Busy,
    Conflict,
    Unavailable {
        reason: String,
    },
}

pub(super) async fn publication(
    State(target): State<InputTarget>,
    Json(request): Json<Request>,
) -> Json<Response> {
    if request.thread_id != target.thread.to_string() {
        return Json(Response::Conflict);
    }
    if !matches!(
        &*target.handle.borrow(),
        AppServerRequestHandle::InProcess(_)
    ) {
        return Json(Response::Unavailable {
            reason: "publication requires the owning in-process runtime".into(),
        });
    }
    let owner = match workspace_admission::availability() {
        Availability::Ready(owner) => owner,
        Availability::Disabled => {
            return Json(Response::Unavailable {
                reason: "workspace snapshots are disabled".into(),
            });
        }
        Availability::Unavailable(reason) => {
            return Json(Response::Unavailable {
                reason: reason.clone(),
            });
        }
    };
    let outcome = match request.operation {
        Operation::Begin => owner.begin_publication(request.sequence),
        Operation::Finish => owner.finish_publication(request.sequence),
    };
    Json(match outcome {
        PublicationAdmission::Ready => Response::Ready {
            pid: std::process::id(),
            cgroup_path: owner.cgroup_path().to_string_lossy().into_owned(),
        },
        PublicationAdmission::Settled => Response::Settled,
        PublicationAdmission::Busy => Response::Busy,
        PublicationAdmission::Conflict => Response::Conflict,
        PublicationAdmission::Unavailable(error) => Response::Unavailable {
            reason: error.to_string(),
        },
    })
}
