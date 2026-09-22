//! Revoke execution custody before awaiting connection and session cleanup.
use super::ConnectionSessionState;
use super::MessageProcessor;
use crate::outgoing_message::ConnectionId;
use codex_app_server_protocol::ServerNotification;
use tokio::time::Duration;
use tokio::time::timeout;

const CONNECTION_RPC_DRAIN_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 30);

impl MessageProcessor {
    pub(crate) fn fence_disconnected_controller(&self, connection_id: ConnectionId) {
        self.outgoing.control.connection_closed(connection_id, || {
            self.controlled_thread_manager.fence_execution()
        });
    }

    pub(crate) async fn connection_closed(
        &self,
        connection_id: ConnectionId,
        session_state: &ConnectionSessionState,
    ) {
        self.fence_disconnected_controller(connection_id);
        let cleanup_controller = self.outgoing.control.begin_cleanup(connection_id);
        if cleanup_controller {
            self.outgoing.fence_pending_requests().await;
            if let Ok(status) = self.outgoing.control.status() {
                self.outgoing
                    .send_server_notification(ServerNotification::ControlStatusChanged(
                        codex_app_server_protocol::ControlStatusChangedNotification(status),
                    ))
                    .await;
            }
        }
        session_state.rpc_gate.close().await;
        session_state.mcp_event_streams.clear().await;
        let drain_timed_out = timeout(
            CONNECTION_RPC_DRAIN_TIMEOUT,
            session_state.rpc_gate.shutdown(),
        )
        .await
        .is_err();
        if drain_timed_out {
            tracing::warn!(
                ?connection_id,
                timeout_seconds = CONNECTION_RPC_DRAIN_TIMEOUT.as_secs(),
                "timed out waiting for connection RPCs to drain"
            );
        }
        let shutdown = if cleanup_controller {
            let starts_drained = self.thread_processor.drain_background_tasks().await.is_ok();
            let report = self
                .controlled_thread_manager
                .shutdown_all_threads_bounded(Duration::from_secs(/*secs*/ 10))
                .await;
            Some(
                if !drain_timed_out
                    && starts_drained
                    && report.timed_out.is_empty()
                    && report.submit_failed.is_empty()
                {
                    codex_app_server_protocol::ControlShutdownState::SessionsStopped
                } else {
                    codex_app_server_protocol::ControlShutdownState::Incomplete
                },
            )
        } else {
            None
        };
        self.outgoing.connection_closed(connection_id).await;
        self.fs_processor.connection_closed(connection_id).await;
        self.command_exec_processor
            .connection_closed(connection_id)
            .await;
        self.process_exec_processor
            .connection_closed(connection_id)
            .await;
        self.thread_processor.connection_closed(connection_id).await;
        if let Some(shutdown) = shutdown {
            self.outgoing.control.shutdown_finished(shutdown);
            if let Ok(status) = self.outgoing.control.status() {
                self.outgoing
                    .send_server_notification(ServerNotification::ControlStatusChanged(
                        codex_app_server_protocol::ControlStatusChangedNotification(status),
                    ))
                    .await;
            }
        }
    }
}
