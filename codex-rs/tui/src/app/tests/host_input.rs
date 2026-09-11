use super::make_test_app_with_channels;
use super::session_lifecycle_requests::make_history_test_app;
use super::session_lifecycle_requests::start_recording_app_server;
use crate::app_event::AppEvent;
use crate::host_dynamic_tools::HostDynamicTools;
use crate::host_dynamic_tools::spawn_cancellable_host_with_input;
use crate::host_dynamic_tools::spawn_host_with_input;
use codex_app_server_client::AppServerEvent;
use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_protocol::DynamicToolCallParams;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::UserInput;
use codex_protocol::ThreadId;
use codex_utils_absolute_path::AbsolutePathBuf;
use pretty_assertions::assert_eq;
use serde_json::json;
use sha2::Digest;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

fn host_input_digest(thread: ThreadId, actor: &str, payload: &[u8]) -> String {
    let mut digest = sha2::Sha256::new();
    digest.update(b"tidepool-interactive-input-v1\0");
    digest.update([0]);
    for field in [thread.to_string().as_bytes(), actor.as_bytes()] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    digest.update([0]);
    digest.update((payload.len() as u64).to_be_bytes());
    digest.update(payload);
    let bytes: [u8; 32] = digest.finalize().into();
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

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
async fn hosted_actor_input_reconciles_early_not_sleeping_before_admission()
-> color_eyre::Result<()> {
    let (mut app, mut events, _ops) = make_test_app_with_channels().await;
    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let host_socket = directory.path().join("host.sock");
    let input_socket = directory.path().join("input.sock");
    let (host_requests, host_task) =
        spawn_cancellable_host_with_input(&host_socket, input_socket.clone())?;
    let host = HostDynamicTools::connect(Some(AbsolutePathBuf::from_absolute_path(host_socket)?))
        .await?
        .expect("configured host");
    let state_db =
        crate::init_state_db_for_app_server_target(&app.config, &crate::AppServerTarget::Embedded)
            .await?;
    let embedded = crate::start_embedded_app_server_with(
        codex_arg0::Arg0DispatchPaths::default(),
        app.config.clone(),
        Vec::new(),
        codex_config::LoaderOverrides::without_managed_config_for_tests(),
        /*strict_config*/ false,
        codex_config::CloudConfigBundleLoader::default(),
        codex_feedback::CodexFeedback::new(),
        /*log_db*/ None,
        state_db,
        Arc::clone(&app.environment_manager),
        |mut args| {
            args.experimental_api = false;
            codex_app_server_client::InProcessAppServerClient::start(args)
        },
    )
    .await?;
    assert!(embedded.request_handle().host_input_control().is_some());
    let thread = ThreadId::new();
    host.attach_primary_with_input(
        thread,
        AppServerRequestHandle::InProcess(embedded.request_handle()),
    )
    .await?;
    let registration =
        host_requests.recv_timeout(std::time::Duration::from_secs(/*secs*/ 5))?;
    assert_eq!(registration.path, "/v1/dynamic-tools/registration");
    let attachment = host_requests.recv_timeout(std::time::Duration::from_secs(/*secs*/ 5))?;
    assert_eq!(attachment.path, "/v1/dynamic-tools/session");

    let (app_server, requests, proxy) = start_recording_app_server(
        &app.config,
        /*blocked_thread_list*/ None,
        /*failed_thread_name*/ None,
    )
    .await?;
    let mut app_server = app_server.with_host_dynamic_tools(Some(host));
    app.active_thread_id = Some(thread);
    let request_id = RequestId::Integer(703);
    let params = DynamicToolCallParams {
        context_call_id: Some("actor-sleep".to_string()),
        thread_id: thread.to_string(),
        turn_id: "turn-host".to_string(),
        call_id: "call-host".to_string(),
        namespace: Some("tidepool_actor".to_string()),
        tool: "haskell".to_string(),
        arguments: serde_json::Value::String("sleep".to_string()),
    };
    assert_eq!(
        app_server
            .host_dynamic_tools()
            .expect("host")
            .routing(&params),
        crate::host_dynamic_tools::HostDynamicToolRouting::Forward
    );
    app.handle_app_server_event(
        &app_server,
        AppServerEvent::ServerRequest(Box::new(ServerRequest::DynamicToolCall {
            request_id: request_id.clone(),
            params,
        })),
    )
    .await;
    assert!(app.dynamic_tool_tasks.contains_key(&request_id));
    let call = tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), async {
        loop {
            if let Ok(request) = host_requests.try_recv() {
                break request;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(call.path, "/v1/dynamic-tools/call");

    let binding = json!({
        "protocolVersion": 4,
        "launchId": attachment.body["launchId"],
        "instanceId": attachment.body["applicationInstanceId"],
        "generation": attachment.body["sessionGeneration"],
        "nonce": attachment.body["inputControlNonce"],
    });
    let actor = "actor-3.1";
    let payload = b"actor notification";
    let submit = json!({
        "operation": "submit",
        "binding": binding,
        "envelope": {
            "producerId": "run/inbox/actor-3.1",
            "sequence": 1,
            "purpose": "notification",
            "mode": "queueOnly",
            "target": {
                "conversation": thread,
                "actor": actor,
                "correlation": null,
            },
            "payload": payload,
            "contentDigest": host_input_digest(thread, actor, payload),
        }
    });
    let client = reqwest::Client::builder()
        .unix_socket(input_socket)
        .no_proxy()
        .build()?;
    let mut ingress = tokio::spawn(async move {
        client
            .post("http://localhost/v1/input/control")
            .json(&submit)
            .send()
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(/*millis*/ 50)).await;
    if ingress.is_finished() {
        let response = (&mut ingress).await??;
        let status = response.status();
        let body = response.text().await?;
        color_eyre::eyre::bail!("actor ingress returned before exact settlement: {status} {body}");
    }
    let first_cancel = tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), async {
        loop {
            if let Ok(request) = host_requests.try_recv() {
                break request;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(first_cancel.path, "/v1/dynamic-tools/cancel");

    let completion =
        tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), events.recv())
            .await?
            .expect("original hosted call terminal event");
    assert!(
        !ingress.is_finished(),
        "terminal proof must not admit actor input before original request settlement"
    );
    let AppEvent::DynamicToolCallCompleted {
        request_id: completed_id,
        response,
    } = completion
    else {
        panic!("expected original dynamic-tool completion")
    };
    assert_eq!(completed_id, request_id);
    for expected_attempt in 2..=3 {
        let retry = tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), async {
            loop {
                if let Ok(request) = host_requests.try_recv() {
                    break request;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(
            retry.path, "/v1/dynamic-tools/cancel",
            "attempt {expected_attempt} must re-observe the exact actor evaluation"
        );
        assert_eq!(retry.body, first_cancel.body);
    }

    let mut tui = crate::tui::test_support::make_test_tui()?;
    app.handle_event(
        &mut tui,
        &mut app_server,
        AppEvent::DynamicToolCallCompleted {
            request_id,
            response,
        },
    )
    .await?;
    let ingress_response =
        tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), &mut ingress).await???;
    assert_eq!(ingress_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        ingress_response.json::<serde_json::Value>().await?["outcome"],
        json!("admitted")
    );
    assert!(
        requests
            .lock()
            .expect("request recorder")
            .iter()
            .any(|request| request.method == "server/request/response"),
        "original dynamic-tool request must settle before actor input admission"
    );

    app_server.shutdown().await?;
    embedded.shutdown().await?;
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
