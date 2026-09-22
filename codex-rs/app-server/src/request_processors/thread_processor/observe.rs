use super::*;
use codex_app_server_protocol::ThreadObserveParams;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_observe(
        &self,
        request_id: ConnectionRequestId,
        params: ThreadObserveParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        if !self.outgoing.control.enabled() {
            return Err(crate::control::control_error(
                "observer attachment requires a controlled service",
            ));
        }
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|_| invalid_request("invalid thread id"))?;
        let thread = self
            .thread_manager
            .get_thread(thread_id)
            .await
            .map_err(|_| invalid_request("observer target is not loaded in this service"))?;
        let state = self.thread_state_manager.thread_state(thread_id).await;
        let sender = {
            let state = state.lock().await;
            if !state.listener_matches(&thread) {
                return Err(invalid_request("observer target has no active listener"));
            }
            state
                .listener_command_tx()
                .ok_or_else(|| invalid_request("observer target has no active listener"))?
        };
        let metadata = self
            .read_thread_view(thread_id, /*include_turns*/ false)
            .await
            .map_err(thread_read_view_error)?;
        sender
            .send(crate::thread_state::ThreadListenerCommand::Observe {
                request_id,
                thread: Box::new(metadata),
            })
            .map_err(|_| invalid_request("observer target closed during attachment"))?;
        Ok(None)
    }
}
