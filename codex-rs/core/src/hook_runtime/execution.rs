//! Hook admission shares the owning execution's irreversible cancellation boundary.
use crate::session::Session;
use std::future::Future;

pub(super) async fn execute_hook<T>(session: &Session, hook: impl Future<Output = T>) -> Option<T> {
    let fence = session
        .services
        .agent_control
        .execution_cancellation_token();
    tokio::select! {
        biased;
        _ = fence.cancelled() => None,
        outcome = hook => Some(outcome),
    }
}
