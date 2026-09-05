use codex_app_server_protocol::ThreadForkParams;

use super::AppServerSession;
use crate::cli::Cli;

#[derive(Default)]
pub(super) struct CliFork {
    pub(super) destination_local: bool,
    through_call_id: Option<String>,
    after_call_id: Option<String>,
    override_model: bool,
    override_provider: bool,
    override_effort: bool,
}

impl AppServerSession {
    pub(crate) fn with_cli_fork(mut self, cli: &Cli) -> Self {
        let keys: Vec<_> = cli
            .config_overrides
            .raw_overrides
            .iter()
            .filter_map(|item| item.split_once('=').map(|(key, _)| key.trim()))
            .collect();
        self.cli_fork = CliFork {
            destination_local: cli.fork_destination_local,
            through_call_id: cli.fork_through_call.clone(),
            after_call_id: cli.fork_after_call.clone(),
            override_model: cli.model.is_some() || keys.contains(&"model"),
            override_provider: cli.oss || keys.contains(&"model_provider"),
            override_effort: keys.contains(&"model_reasoning_effort"),
        };
        self
    }
}

impl CliFork {
    pub(super) fn configure(&self, params: &mut ThreadForkParams) {
        params.through_call_id = self.through_call_id.clone();
        params.after_call_id = self.after_call_id.clone();
        if !self.destination_local {
            return;
        }
        params.defer_goal_continuation = true;
        params.require_client_readiness = true;
        if !self.override_model {
            params.model = None;
        }
        if !self.override_provider {
            params.model_provider = None;
        }
        if let Some(config) = params.config.as_mut() {
            if !self.override_model {
                config.remove("model");
            }
            if !self.override_provider {
                config.remove("model_provider");
            }
            if !self.override_effort {
                config.remove("model_reasoning_effort");
            }
        }
    }
}
