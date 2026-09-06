use codex_core::StartThreadOptions;
use codex_core::TurnInputRequest;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn execution_fence_rejects_input_and_new_threads_even_after_readiness() -> anyhow::Result<()>
{
    let server = responses::start_mock_server().await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let provider = responses::mount_sse_once(
        &server,
        responses::sse(vec![responses::ev_completed("must-not-run")]),
    )
    .await;
    test.thread_manager.fence_execution();
    test.codex.acknowledge_client_readiness().await;
    let error = test
        .codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "must not infer".to_string(),
            text_elements: Vec::new(),
        }]))
        .await
        .expect_err("fenced input");
    assert!(error.to_string().contains("execution owner disconnected"));
    let result = test
        .thread_manager
        .start_thread(StartThreadOptions::new(test.config.clone()))
        .await;
    assert!(result.is_err());
    assert_eq!(provider.requests().len(), 0);
    Ok(())
}

#[tokio::test]
async fn execution_fence_cancels_inflight_provider_before_late_tool_dispatch() -> anyhow::Result<()>
{
    use codex_protocol::protocol::EventMsg;
    use core_test_support::streaming_sse::StreamingSseChunk;
    use core_test_support::streaming_sse::start_streaming_sse_server;
    use core_test_support::wait_for_event;
    use tokio::time::Duration;
    use tokio::time::timeout;

    let (release, gate) = tokio::sync::oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![vec![
        StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![responses::ev_response_created("inflight")]),
        },
        StreamingSseChunk {
            gate: Some(gate),
            body: responses::sse(vec![
                responses::ev_function_call(
                    "late-command",
                    "exec_command",
                    r#"{"cmd":"echo must-not-execute"}"#,
                ),
                responses::ev_completed("inflight"),
            ]),
        },
    ]])
    .await;
    let test = test_codex().build_with_streaming_server(&server).await?;
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "begin".to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    timeout(
        Duration::from_secs(/*secs*/ 10),
        server.wait_for_request_count(/*count*/ 1),
    )
    .await?;
    test.thread_manager.fence_execution();
    let _ = release.send(());
    timeout(
        Duration::from_secs(/*secs*/ 10),
        test.codex.shutdown_and_wait(),
    )
    .await??;
    wait_for_event(&test.codex, |event| {
        assert!(
            !matches!(event, EventMsg::ExecCommandBegin(_)),
            "late tool executed after fence"
        );
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;
    wait_for_event(&test.codex, |event| {
        assert!(
            !matches!(event, EventMsg::ExecCommandBegin(_)),
            "late tool executed after abort"
        );
        matches!(event, EventMsg::ShutdownComplete)
    })
    .await;
    let history = std::fs::read_to_string(test.codex.rollout_path().expect("persisted execution"))?;
    assert!(history.contains("Unrecorded external-effect outcomes are uncertain"));
    assert!(!history.contains("The user interrupted the previous turn on purpose"));
    assert_eq!(server.requests().await.len(), 1);
    server.shutdown().await;
    Ok(())
}
