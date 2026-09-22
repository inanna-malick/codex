//! Irreversible process-owner cancellation, independent of per-thread readiness.

use std::sync::atomic::Ordering;

use crate::ThreadManager;
use crate::thread_manager::ThreadManagerState;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;

impl ThreadManager {
    /// Require explicit client readiness for roots created or resumed by this manager.
    /// Configure this before creating threads. It is persisted with new thread metadata.
    pub fn require_client_readiness(&self) {
        self.state
            .require_client_readiness
            .store(true, Ordering::Release);
    }

    /// Permanently prevent further execution and cancel tasks across this manager's tree.
    /// Already dispatched external effects may still complete; this does not undo them.
    pub fn fence_execution(&self) {
        self.state.execution_fence.cancel();
    }
}

impl ThreadManagerState {
    pub(crate) fn ensure_execution_active(&self) -> Result<()> {
        if self.execution_fence.is_cancelled() {
            return Err(CodexErr::InvalidRequest(
                "execution owner disconnected; service restart and reconciliation required"
                    .to_string(),
            ));
        }
        Ok(())
    }
}
