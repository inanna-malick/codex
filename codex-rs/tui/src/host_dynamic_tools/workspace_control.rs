//! Private hosted publication control for the owning in-process native runtime.

use super::input_control::InputTarget;
use axum::Json;
use axum::extract::State;
use codex_app_server_client::AppServerRequestHandle;
use codex_shoal_protocol::WorkspacePublicationOperation as Operation;
use codex_shoal_protocol::WorkspacePublicationReply as Response;
use codex_shoal_protocol::WorkspacePublicationRequest as Request;
use codex_utils_pty::workspace_admission::Availability;
use codex_utils_pty::workspace_admission::PublicationAdmission;
use codex_utils_pty::workspace_admission::{self};

pub(super) async fn publication(
    State(target): State<InputTarget>,
    Json(request): Json<Request>,
) -> Result<
    Json<codex_shoal_protocol::BoundReply<Response<String>>>,
    (axum::http::StatusCode, String),
> {
    let binding = request.binding.clone();
    let response = |payload| {
        Json(codex_shoal_protocol::BoundReply {
            binding: binding.clone(),
            payload,
        })
    };
    if !target.binding_matches(&binding).await {
        return Err((
            axum::http::StatusCode::CONFLICT,
            "stale or foreign native binding".into(),
        ));
    }
    if request.thread_id != target.thread.to_string() {
        return Ok(response(Response::Conflict));
    }
    if !matches!(
        &*target.handle.borrow(),
        AppServerRequestHandle::InProcess(_)
    ) {
        return Ok(response(Response::Unavailable {
            reason: "publication requires the owning in-process runtime".into(),
        }));
    }
    let owner = match workspace_admission::availability() {
        Availability::Ready(owner) => owner,
        Availability::Disabled => {
            return Ok(response(Response::Unavailable {
                reason: "workspace snapshots are disabled".into(),
            }));
        }
        Availability::Unavailable(reason) => {
            return Ok(response(Response::Unavailable {
                reason: reason.clone(),
            }));
        }
    };
    if request.expected_identity.as_ref().is_some_and(|expected| {
        expected.pid != std::process::id()
            || expected.start_ticks != owner.process_identity().start_ticks
            || expected.mount_namespace_inode != owner.process_identity().mount_namespace_inode
    }) || (matches!(request.operation, Operation::Finish) && request.expected_identity.is_none())
    {
        return Ok(response(Response::Conflict));
    }
    let outcome = match request.operation {
        Operation::Begin => owner.begin_publication(request.sequence),
        Operation::Finish => owner.finish_publication(request.sequence),
    };
    Ok(response(match outcome {
        PublicationAdmission::Ready => Response::Ready {
            pid: std::process::id(),
            start_ticks: owner.process_identity().start_ticks,
            mount_namespace_inode: owner.process_identity().mount_namespace_inode,
            cgroup_path: owner.cgroup_path().to_string_lossy().into_owned(),
        },
        PublicationAdmission::Settled => Response::Settled,
        PublicationAdmission::Busy => Response::Busy,
        PublicationAdmission::Conflict => Response::Conflict,
        PublicationAdmission::Unavailable(error) => Response::Unavailable {
            reason: error.to_string(),
        },
    }))
}
