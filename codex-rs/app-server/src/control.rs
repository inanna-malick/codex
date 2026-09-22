//! Process-scoped custody for explicitly controlled services.

use std::collections::HashSet;
use std::sync::Mutex;

use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ControlShutdownState;
use codex_app_server_protocol::ControlState;
use codex_app_server_protocol::ControlStatus;
use codex_app_server_protocol::JSONRPCErrorError;
use uuid::Uuid;

use crate::outgoing_message::ConnectionId;

pub(crate) struct Control {
    token: Option<String>,
    instance_id: String,
    state: Mutex<State>,
}

struct State {
    controller: Option<ConnectionId>,
    live_connections: HashSet<ConnectionId>,
    fenced: bool,
    cleanup_started: bool,
    shutdown: ControlShutdownState,
    observed: HashSet<(ConnectionId, String)>,
}

impl Default for Control {
    fn default() -> Self {
        Self {
            token: None,
            instance_id: Uuid::new_v4().to_string(),
            state: Mutex::new(State {
                controller: None,
                live_connections: HashSet::new(),
                fenced: false,
                cleanup_started: false,
                shutdown: ControlShutdownState::NotStarted,
                observed: HashSet::new(),
            }),
        }
    }
}

impl Control {
    pub(crate) fn controlled(token: String) -> Self {
        Self {
            token: Some(token),
            ..Self::default()
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.token.is_some()
    }

    pub(crate) fn acquire(
        &self,
        connection: ConnectionId,
        token: &str,
    ) -> Result<ControlStatus, JSONRPCErrorError> {
        if self.token.as_deref() != Some(token) {
            return Err(control_error("controller credential rejected"));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.live_connections.contains(&connection)
            || state.fenced
            || state.controller.is_some_and(|owner| owner != connection)
        {
            return Err(control_error(
                "controller custody unavailable; no replacement in this service instance",
            ));
        }
        state.controller = Some(connection);
        drop(state);
        self.status()
    }

    pub(crate) fn status(&self) -> Result<ControlStatus, JSONRPCErrorError> {
        if !self.enabled() {
            return Err(control_error("service was not launched in controlled mode"));
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(ControlStatus {
            instance_id: self.instance_id.clone(),
            state: if state.fenced {
                ControlState::Fenced
            } else if state.controller.is_some() {
                ControlState::Controlled
            } else {
                ControlState::AwaitingController
            },
            shutdown: state.shutdown.clone(),
            reconciliation_required: state.fenced,
        })
    }

    /// Serialize response consumption with revocation. Never await inside `action`.
    pub(crate) fn with_authority<T>(
        &self,
        connection: ConnectionId,
        action: impl FnOnce() -> T,
    ) -> Option<T> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.enabled() || (!state.fenced && state.controller == Some(connection)) {
            Some(action())
        } else {
            None
        }
    }

    pub(crate) fn request_connections(&self, ordinary: &[ConnectionId]) -> Vec<ConnectionId> {
        if !self.enabled() {
            return ordinary.to_vec();
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.fenced {
            Vec::new()
        } else {
            state.controller.into_iter().collect()
        }
    }

    pub(crate) fn authorize(
        &self,
        connection: ConnectionId,
        request: &ClientRequest,
    ) -> Result<(), JSONRPCErrorError> {
        if self.with_authority(connection, || ()).is_some() {
            return Ok(());
        }
        let thread_id = match request {
            ClientRequest::ControlAcquire { .. }
            | ClientRequest::ControlStatusRead { .. }
            | ClientRequest::ControlPendingList { .. } => return Ok(()),
            ClientRequest::ThreadObserve { .. } => return Ok(()),
            ClientRequest::ThreadRead { params, .. } => &params.thread_id,
            ClientRequest::ThreadTurnsList { params, .. } => &params.thread_id,
            ClientRequest::ThreadItemsList { params, .. } => &params.thread_id,
            ClientRequest::ThreadUnsubscribe { params, .. } => &params.thread_id,
            _ => {
                return Err(control_error(
                    "observer connection cannot mutate controlled execution",
                ));
            }
        };
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.observed.contains(&(connection, thread_id.clone())) {
            Ok(())
        } else {
            Err(control_error(
                "observe the loaded execution before reading its history",
            ))
        }
    }

    pub(crate) fn connection_opened(&self, connection: ConnectionId) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .live_connections
            .insert(connection);
    }

    pub(crate) fn observe(&self, connection: ConnectionId, thread_id: String) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.live_connections.contains(&connection) {
            return false;
        }
        state.observed.insert((connection, thread_id));
        true
    }

    /// Revocation and executor cancellation happen before connection cleanup can await.
    pub(crate) fn connection_closed(&self, connection: ConnectionId, fence: impl FnOnce()) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.live_connections.remove(&connection);
        state.observed.retain(|(id, _)| *id != connection);
        if !self.enabled() || state.controller != Some(connection) || state.fenced {
            return false;
        }
        state.fenced = true;
        state.shutdown = ControlShutdownState::Draining;
        fence();
        true
    }

    pub(crate) fn begin_cleanup(&self, connection: ConnectionId) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.fenced || state.controller != Some(connection) || state.cleanup_started {
            return false;
        }
        state.cleanup_started = true;
        true
    }

    pub(crate) fn shutdown_finished(&self, state: ControlShutdownState) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .shutdown = state;
    }
}

pub(crate) fn control_error(message: &str) -> JSONRPCErrorError {
    JSONRPCErrorError {
        code: -32010,
        message: message.to_string(),
        data: Some(serde_json::json!({"reason": "controlUnavailable"})),
    }
}

#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
