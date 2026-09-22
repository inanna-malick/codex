use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadReadyResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn fork_readiness_holds_queue_and_rearms_on_resume_legacy() -> Result<()> {
    fork_readiness_holds_queue_and_rearms_on_resume(ThreadHistoryMode::Legacy).await
}

#[tokio::test]
async fn fork_readiness_holds_queue_and_rearms_on_resume_paginated() -> Result<()> {
    fork_readiness_holds_queue_and_rearms_on_resume(ThreadHistoryMode::Paginated).await
}

async fn fork_readiness_holds_queue_and_rearms_on_resume(mode: ThreadHistoryMode) -> Result<()> {
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-6-astra")
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let id = app
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(mode),
            ..Default::default()
        })
        .await?;
    let parent: ThreadStartResponse = app.read_response(id).await?;
    super::thread_fork_effort::infer(&mut app, &server, &parent.thread.id, "source evidence")
        .await?;
    let id = app
        .send_thread_fork_request(ThreadForkParams {
            thread_id: parent.thread.id,
            require_client_readiness: true,
            exclude_turns: true,
            config: Some([("model_reasoning_effort".to_string(), json!("low"))].into()),
            ..Default::default()
        })
        .await?;
    let child: ThreadForkResponse = app.read_response(id).await?;
    let assignment =
        json!([{"type": "text", "text": "destination assignment", "textElements": []}]);
    let id = app
        .send_request(
            "turn/start",
            Some(json!({"threadId": child.thread.id, "input": assignment})),
        )
        .await?;
    let error = app
        .read_stream_until_error_message(RequestId::Integer(id))
        .await?;
    assert!(error.error.message.contains("readiness"));
    let queued = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("assignment"),
            responses::ev_assistant_message("done", "implemented"),
            responses::ev_completed("assignment"),
        ]),
    )
    .await;
    let id = app.send_request("thread/queue/add", Some(json!({
        "threadId": child.thread.id, "input": assignment, "clientUserMessageId": "assignment-1",
    }))).await?;
    let _: Value = app.read_response(id).await?;
    assert_eq!(queued.requests().len(), 0);
    let id = app
        .send_request(
            "thread/queue/list",
            Some(json!({"threadId": child.thread.id})),
        )
        .await?;
    let list: Value = app.read_response(id).await?;
    assert_eq!(list["data"].as_array().unwrap().len(), 1);
    for _ in 0..2 {
        let id = app
            .send_request("thread/ready", Some(json!({"threadId": child.thread.id})))
            .await?;
        let ready: ThreadReadyResponse = app.read_response(id).await?;
        assert_eq!(
            ready,
            ThreadReadyResponse {
                thread_id: child.thread.id.clone(),
                ready: true
            }
        );
    }
    app.read_stream_until_notification_message("turn/completed")
        .await?;
    assert_eq!(
        queued
            .single_request()
            .inputs_of_type("configuration_update")
            .last()
            .unwrap()["reasoning"]["effort"],
        "low"
    );
    let id = app
        .send_request(
            "thread/queue/list",
            Some(json!({"threadId": child.thread.id})),
        )
        .await?;
    let list: Value = app.read_response(id).await?;
    assert_eq!(list["data"], json!([]));
    app.shutdown_gracefully().await?;

    let mut resumed = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let id = resumed
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: child.thread.id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let _: ThreadResumeResponse = resumed.read_response(id).await?;
    let id = resumed
        .send_request(
            "turn/start",
            Some(json!({"threadId": child.thread.id, "input": assignment})),
        )
        .await?;
    let error = resumed
        .read_stream_until_error_message(RequestId::Integer(id))
        .await?;
    assert!(error.error.message.contains("readiness"));
    let id = resumed
        .send_request("thread/ready", Some(json!({"threadId": child.thread.id})))
        .await?;
    let _: ThreadReadyResponse = resumed.read_response(id).await?;
    // An idle readiness acknowledgment has not caused another provider call.
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    let id = resumed
        .send_request(
            "thread/ready",
            Some(json!({"threadId": "00000000-0000-0000-0000-000000000000"})),
        )
        .await?;
    let error = resumed
        .read_stream_until_error_message(RequestId::Integer(id))
        .await?;
    assert_eq!(error.error.code, -32600);
    assert!(error.error.message.contains("not available"));
    super::thread_fork_effort::infer(&mut resumed, &server, &child.thread.id, "continue work")
        .await?;
    Ok(())
}
