use super::*;
use clap::Parser;
use codex_app_server_protocol::ThreadForkParams;
use pretty_assertions::assert_eq;

#[test]
fn destination_local_cli_fork_carries_boundary_and_readiness_policy() {
    let mut cli = Cli::parse_from(["codex"]);
    cli.fork_destination_local = true;
    cli.fork_after_call = Some("call_child".to_string());
    let cli_fork = CliFork::from_cli(&cli);
    let mut params = ThreadForkParams {
        model: Some("gpt-6-luna".to_string()),
        config: Some(
            [
                ("model".to_string(), serde_json::json!("gpt-6-luna")),
                (
                    "model_reasoning_effort".to_string(),
                    serde_json::json!("medium"),
                ),
            ]
            .into(),
        ),
        ..ThreadForkParams::default()
    };

    cli_fork.configure(&mut params);

    assert_eq!(params.after_call_id.as_deref(), Some("call_child"));
    assert!(params.require_client_readiness);
    assert!(params.defer_goal_continuation);
    assert_eq!(params.model, None);
    let config = params.config.expect("fork config");
    assert!(!config.contains_key("model"));
    assert!(!config.contains_key("model_reasoning_effort"));
}

#[tokio::test]
async fn cli_session_constructor_preserves_destination_local_fork_options() -> color_eyre::Result<()>
{
    let home = tempfile::tempdir()?;
    let config = crate::legacy_core::config::ConfigBuilder::default()
        .codex_home(home.path().to_path_buf())
        .build()
        .await?;
    let initial = crate::start_embedded_app_server_for_picker(&config).await?;
    let mut cli = Cli::parse_from(["codex"]);
    cli.fork_destination_local = true;
    cli.fork_after_call = Some("call_child".to_string());

    let session = AppServerSession::new_for_cli(initial.client, initial.thread_params_mode, &cli);

    assert!(session.cli_fork.destination_local);
    session.shutdown().await?;
    Ok(())
}
