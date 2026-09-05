use super::*;
use codex_app_server_protocol::ThreadReadyParams;
use codex_app_server_protocol::ThreadReadyResponse;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_ready(
        &self,
        params: ThreadReadyParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|err| invalid_request(format!("invalid thread id: {err}")))?;
        let thread = self
            .thread_manager
            .get_thread(thread_id)
            .await
            .map_err(|err| {
                invalid_request(format!("thread is not available in this server: {err}"))
            })?;
        thread.acknowledge_client_readiness().await;
        Ok(Some(
            ThreadReadyResponse {
                thread_id: params.thread_id,
                ready: true,
            }
            .into(),
        ))
    }
}
