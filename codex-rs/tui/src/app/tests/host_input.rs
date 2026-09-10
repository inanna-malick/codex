use super::session_lifecycle_requests::make_history_test_app;
use super::session_lifecycle_requests::start_recording_app_server;
use crate::host_dynamic_tools::HostDynamicTools;
use crate::host_dynamic_tools::spawn_host_with_input;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_protocol::ThreadId;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
async fn hosted_input_reaches_existing_app_server_and_rejects_other_threads()
-> color_eyre::Result<()> {
    let (mut app, codex_home) = make_history_test_app().await?;
    use core_test_support::responses;
    let provider =
        app_test_support::create_mock_responses_server_sequence(vec![responses::sse(vec![
            responses::ev_response_created("input-response"),
            responses::ev_assistant_message("input-answer", "acknowledged"),
            responses::ev_completed("input-response"),
        ])])
        .await;
    // Thread/start reloads provider settings from disk.
    app_test_support::write_mock_responses_config_toml(
        codex_home.path(),
        &provider.uri(),
        &Default::default(),
        /*auto_compact_limit*/ 100_000,
        /*requires_openai_auth*/ Some(false),
        "mock_provider",
        "compact",
    )?;
    app.config = crate::legacy_core::config::ConfigBuilder::default()
        .codex_home(codex_home.path().to_path_buf())
        .fallback_cwd(Some(codex_home.path().to_path_buf()))
        .build()
        .await?;
    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let socket = directory.path().join("host.sock");
    let input = directory.path().join("input.sock");
    let (callbacks, host_task) =
        spawn_host_with_input(&socket, /*request_count*/ 2, input.clone())?;
    let host = HostDynamicTools::connect(Some(AbsolutePathBuf::from_absolute_path(socket)?))
        .await?
        .expect("configured host");
    let (app_server, requests, proxy) = start_recording_app_server(
        &app.config,
        /*blocked_thread_list*/ None,
        /*failed_thread_name*/ None,
    )
    .await?;
    let mut app_server = app_server.with_host_dynamic_tools(Some(host));
    let started = app_server.start_thread(&app.config).await?;
    let thread = started.session.thread_id;
    let _registration = callbacks.recv()?;
    let attachment = callbacks.recv()?;
    assert_eq!(attachment.body["protocolVersion"], json!(3));
    assert_eq!(attachment.body["threadId"], json!(thread));
    assert_eq!(attachment.body["inputControlSocket"], json!(input));
    assert_eq!(attachment.body["launchId"], json!("launch-test"));
    assert_eq!(attachment.body["inputControlNonce"], json!("nonce-test"));
    assert_eq!(attachment.body["sessionGeneration"], json!(1));
    assert!(
        attachment.body["applicationInstanceId"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    let binding = json!({
        "protocolVersion": 4,
        "launchId": attachment.body["launchId"],
        "instanceId": attachment.body["applicationInstanceId"],
        "generation": attachment.body["sessionGeneration"],
        "nonce": attachment.body["inputControlNonce"],
    });
    let query = json!({
        "operation": "query",
        "binding": binding.clone(),
        "producer_id": "run/inbox/actor-1.1",
        "sequence": 1,
    });
    let client = reqwest::Client::builder()
        .unix_socket(input.as_path())
        .no_proxy()
        .build()?;
    let response = client
        .post("http://localhost/v1/input/control")
        .json(&query)
        .send()
        .await?;
    let status = response.status();
    let response_body = response.bytes().await?;
    assert_eq!(
        (status, String::from_utf8_lossy(&response_body).into_owned()),
        (
            reqwest::StatusCode::OK,
            serde_json::to_string(&json!({"binding": binding, "outcome": "evidenceUnavailable"}))?
        )
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response_body)?,
        json!({"binding": binding, "outcome": "evidenceUnavailable"})
    );
    let mut stale_query = query.clone();
    stale_query["binding"]["generation"] = json!(2);
    assert_eq!(
        client
            .post("http://localhost/v1/input/control")
            .json(&stale_query)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let payload = json!({"threadId":thread,"clientUserMessageId":"update-7","message":"correct the contract"});
    assert_eq!(
        client
            .post("http://localhost/v1/input")
            .json(&payload)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    let mut wrong_thread = payload.clone();
    wrong_thread["threadId"] = json!(ThreadId::new());
    assert_eq!(
        client
            .post("http://localhost/v1/input")
            .json(&wrong_thread)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let mut override_config = payload;
    override_config["model"] = json!("another-model");
    assert_eq!(
        client
            .post("http://localhost/v1/input")
            .json(&override_config)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );
    let turns: Vec<TurnStartParams> = requests
        .lock()
        .expect("recorder")
        .iter()
        .filter(|request| request.method == "turn/start")
        .map(|request| serde_json::from_value(request.params.clone().expect("params")).unwrap())
        .collect();
    assert_eq!(
        turns,
        vec![TurnStartParams {
            thread_id: thread.to_string(),
            client_user_message_id: Some("update-7".into()),
            input: vec![UserInput::Text {
                text: "correct the contract".into(),
                text_elements: Vec::new()
            }],
            ..Default::default()
        }]
    );
    let rollout = started.session.rollout_path.expect("durable hosted thread");
    tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 10), async {
        loop {
            let content = tokio::fs::read_to_string(&rollout).await?;
            if content
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .any(|record| {
                    record["type"] == "event_msg"
                        && record["payload"]["type"] == "item_completed"
                        && record["payload"]["item"]["type"] == "UserMessage"
                        && record["payload"]["item"]["client_id"] == "update-7"
                })
            {
                return Ok::<_, std::io::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(/*millis*/ 20)).await;
        }
    })
    .await??;
    app_server.shutdown().await?;
    tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), async {
        while tokio::net::UnixStream::connect(&input).await.is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    proxy.await??;
    host_task.join().expect("host task")?;
    Ok(())
}

#[tokio::test]
async fn occupied_input_socket_preserves_tui_thread_and_queue_attachment() -> color_eyre::Result<()>
{
    let (app, _codex_home) = make_history_test_app().await?;
    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let socket = directory.path().join("host.sock");
    let input = directory.path().join("input.sock");
    let occupied = tokio::net::UnixListener::bind(&input)?;
    let (callbacks, host_task) =
        spawn_host_with_input(&socket, /*request_count*/ 2, input.clone())?;
    let host = HostDynamicTools::connect(Some(AbsolutePathBuf::from_absolute_path(socket)?))
        .await?
        .expect("configured host");
    let (app_server, _, proxy) = start_recording_app_server(
        &app.config,
        /*blocked_thread_list*/ None,
        /*failed_thread_name*/ None,
    )
    .await?;
    let mut app_server = app_server.with_host_dynamic_tools(Some(host));
    let started = app_server.start_thread(&app.config).await?;
    let _registration = callbacks.recv()?;
    let attachment = callbacks.recv()?;
    assert_eq!(
        attachment.body,
        json!({"protocolVersion":3,"threadId":started.session.thread_id})
    );
    // The foreign socket still owns its path; startup did not unlink it.
    let connection = tokio::net::UnixStream::connect(input).await?;
    let _accepted = occupied.accept().await?;
    drop(connection);
    app_server.shutdown().await?;
    proxy.await??;
    host_task.join().expect("host task")?;
    Ok(())
}
