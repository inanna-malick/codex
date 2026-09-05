use std::sync::atomic::Ordering;

use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;

use crate::CodexThread;
use crate::session::Session;

impl Session {
    pub(crate) fn ensure_client_ready(&self) -> Result<()> {
        if !self.client_ready.load(Ordering::Acquire) {
            return Err(CodexErr::InvalidRequest(
                "thread awaits trusted client readiness; acknowledge with thread/ready before inference".to_string(),
            ));
        }
        Ok(())
    }
}

impl CodexThread {
    /// Acknowledge this runtime's external setup. Idempotent and never starts inference itself.
    /// Reattachment requires another acknowledgment when the durable thread policy requires it.
    pub async fn acknowledge_client_readiness(&self) {
        if !self.session.client_ready.swap(true, Ordering::AcqRel) {
            // Reconsider durable input the queue watcher may have observed while admission was
            // closed. The lifecycle does not manufacture input or continue an inherited goal.
            for contributor in self
                .session
                .services
                .extensions
                .thread_lifecycle_contributors()
            {
                contributor
                    .on_thread_input_ready(codex_extension_api::ThreadInputReadyInput {
                        thread_store: &self.session.services.thread_extension_data,
                    })
                    .await;
            }
        }
    }
}
