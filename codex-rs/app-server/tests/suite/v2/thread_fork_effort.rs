use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_app_server_protocol::DynamicToolCustomSpec;
use codex_app_server_protocol::DynamicToolSpec;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_protocol::openai_models::ReasoningEffort;
use core_test_support::responses;
use core_test_support::responses::ResponsesRequest;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use wiremock::MockServer;

pub(super) async fn infer(
    app: &mut TestAppServer,
    server: &MockServer,
    thread_id: &str,
    text: &str,
) -> Result<ResponsesRequest> {
    let mock = responses::mount_sse_once(
        server,
        responses::sse(vec![
            responses::ev_response_created(text),
            responses::ev_reasoning_item(
                &format!("rs_{}", text.replace(' ', "_")),
                &["retained evidence"],
                &[],
            ),
            responses::ev_assistant_message(
                &format!("msg_{}", text.replace(' ', "_")),
                "accumulated understanding",
            ),
            responses::ev_completed(text),
        ]),
    )
    .await;
    let id = app
        .send_turn_start_request(TurnStartParams {
            thread_id: thread_id.to_string(),
            input: vec![UserInput::Text {
                text: text.to_string(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = app.read_response(id).await?;
    app.read_stream_until_notification_message("turn/completed")
        .await?;
    Ok(mock.single_request())
}

fn updates(request: &ResponsesRequest) -> Vec<Value> {
    request.inputs_of_type("configuration_update")
}

#[tokio::test]
async fn fork_effort_preserves_prefix_and_resume_legacy() -> Result<()> {
    fork_effort_preserves_prefix_and_resume(ThreadHistoryMode::Legacy).await
}

#[tokio::test]
async fn fork_effort_preserves_prefix_and_resume_paginated() -> Result<()> {
    fork_effort_preserves_prefix_and_resume(ThreadHistoryMode::Paginated).await
}

async fn fork_effort_preserves_prefix_and_resume(history_mode: ThreadHistoryMode) -> Result<()> {
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-6-astra")
        .with_root_config("model_reasoning_effort = \"medium\"")
        .write(home.path())?;
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let id = app
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(history_mode),
            dynamic_tools: Some(vec![DynamicToolSpec::Custom(DynamicToolCustomSpec {
                name: "hosted_eval".to_string(),
                description: "Evaluate code in a resident host".to_string(),
                defer_loading: false,
                format: None,
            })]),
            config: Some([("model_reasoning_effort".to_string(), json!("high"))].into()),
            ..Default::default()
        })
        .await?;
    let parent: ThreadStartResponse = app.read_response(id).await?;
    let tool_call = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("tool"),
            responses::ev_custom_tool_call("call_evidence", "hosted_eval", "collect evidence"),
            responses::ev_completed("tool"),
        ]),
    )
    .await;
    let id = app
        .send_turn_start_request(TurnStartParams {
            thread_id: parent.thread.id.clone(),
            input: vec![UserInput::Text {
                text: "scaffold".to_string(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = app.read_response(id).await?;
    let ServerRequest::DynamicToolCall { request_id, .. } =
        app.read_stream_until_request_message().await?
    else {
        panic!("expected hosted tool call");
    };
    let continuation = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("after_tool"),
            responses::ev_reasoning_item("rs_scaffold", &["tool evidence understood"], &[]),
            responses::ev_assistant_message("msg_scaffold", "scaffold complete"),
            responses::ev_completed("after_tool"),
        ]),
    )
    .await;
    app.send_response(
        request_id,
        serde_json::to_value(DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: "hosted evidence".to_string(),
            }],
            success: true,
        })?,
    )
    .await?;
    app.read_stream_until_notification_message("turn/completed")
        .await?;
    assert_eq!(
        updates(&tool_call.single_request()),
        updates(&continuation.single_request())
    );
    let parent_request = infer(&mut app, &server, &parent.thread.id, "retain evidence").await?;
    let high = json!({"type": "configuration_update", "reasoning": {"effort": "high"}});
    let low = json!({"type": "configuration_update", "reasoning": {"effort": "low"}});
    assert_eq!(updates(&parent_request), vec![high.clone()]);

    let count = server
        .received_requests()
        .await
        .expect("request recording is enabled")
        .len();
    let id = app
        .send_thread_fork_request(ThreadForkParams {
            thread_id: parent.thread.id.clone(),
            config: Some([("model_reasoning_effort".to_string(), json!("low"))].into()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let child: ThreadForkResponse = app.read_response(id).await?;
    assert_eq!(
        (&child.model, &child.reasoning_effort),
        (&parent.model, &Some(ReasoningEffort::Low))
    );
    let id = app
        .send_thread_fork_request(ThreadForkParams {
            thread_id: child.thread.id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let inherited: ThreadForkResponse = app.read_response(id).await?;
    assert_eq!(inherited.reasoning_effort, Some(ReasoningEffort::Low));
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("request recording is enabled")
            .len(),
        count
    );

    let child_request = infer(&mut app, &server, &child.thread.id, "implement").await?;
    let cache_key = parent_request.body_json()["prompt_cache_key"].clone();
    assert!(cache_key.is_string());
    assert_eq!(child_request.body_json()["prompt_cache_key"], cache_key);
    let routing_id = parent_request.header("session-id").unwrap();
    assert_eq!(child_request.header("session-id"), Some(routing_id.clone()));
    assert_eq!(
        child_request.body_json()["client_metadata"]["session_id"],
        routing_id
    );
    assert_eq!(
        child_request.header("thread-id"),
        Some(child.thread.id.clone())
    );
    let prefix = parent_request.input();
    assert_eq!(&child_request.input()[..prefix.len()], prefix.as_slice());
    assert_eq!(updates(&child_request), vec![high.clone(), low.clone()]);
    assert_eq!(
        child_request.body_json()["reasoning"],
        parent_request.body_json()["reasoning"]
    );
    // The copied prefix includes reasoning and assistant output, not just user messages.
    assert_eq!(child_request.inputs_of_type("reasoning").len(), 2);
    assert_eq!(
        child_request.inputs_of_type("custom_tool_call_output"),
        continuation
            .single_request()
            .inputs_of_type("custom_tool_call_output")
    );
    let child_again = infer(
        &mut app,
        &server,
        &child.thread.id,
        "continue implementation",
    )
    .await?;
    assert_eq!(updates(&child_again), vec![high.clone(), low.clone()]);
    let parent_again = infer(&mut app, &server, &parent.thread.id, "continue planning").await?;
    assert_eq!(updates(&parent_again), vec![high.clone()]);
    let complete_parent_context = parent_again.input();
    let inherited_end = complete_parent_context
        .iter()
        .position(|item| item["id"] == "msg_retain_evidence")
        .expect("parent's final output is retained")
        + 1;
    assert_eq!(
        &child_request.input()[..inherited_end],
        &complete_parent_context[..inherited_end]
    );

    let id = app
        .send_thread_fork_request(ThreadForkParams {
            thread_id: child.thread.id.clone(),
            config: Some([("model_reasoning_effort".to_string(), json!("high"))].into()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let grandchild: ThreadForkResponse = app.read_response(id).await?;
    let grandchild_request = infer(&mut app, &server, &grandchild.thread.id, "integrate").await?;
    assert_eq!(
        grandchild_request.body_json()["prompt_cache_key"],
        cache_key
    );
    assert_eq!(
        &grandchild_request.input()[..child_again.input().len()],
        child_again.input().as_slice()
    );
    assert_eq!(
        updates(&grandchild_request),
        vec![high.clone(), low.clone(), high.clone()]
    );

    drop(app);
    let mut app = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    // This child has never inferred: its selected effort must survive the fork
    // response and process restart, not depend on a later TurnContext item.
    let id = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: inherited.thread.id,
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let never_run: ThreadResumeResponse = app.read_response(id).await?;
    assert_eq!(never_run.reasoning_effort, Some(ReasoningEffort::Low));
    let first_resumed = infer(
        &mut app,
        &server,
        &never_run.thread.id,
        "first resumed work",
    )
    .await?;
    assert_eq!(first_resumed.body_json()["prompt_cache_key"], cache_key);
    assert_eq!(first_resumed.header("session-id"), Some(routing_id.clone()));
    assert_eq!(
        first_resumed.body_json()["client_metadata"]["session_id"],
        routing_id
    );
    let id = app
        .send_thread_resume_request(ThreadResumeParams {
            thread_id: child.thread.id.clone(),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let resumed: ThreadResumeResponse = app.read_response(id).await?;
    assert_eq!(resumed.reasoning_effort, Some(ReasoningEffort::Low));
    let request = infer(&mut app, &server, &child.thread.id, "resumed work").await?;
    assert_eq!(updates(&request), vec![high, low]);
    assert_eq!(request.body_json()["prompt_cache_key"], cache_key);
    assert_eq!(request.header("session-id"), Some(routing_id.clone()));
    assert_eq!(
        request.body_json()["client_metadata"]["session_id"],
        routing_id
    );
    assert_eq!(
        &request.input()[..child_again.input().len()],
        child_again.input().as_slice()
    );
    Ok(())
}

#[tokio::test]
async fn fork_effort_rejects_invalid_configuration_without_inference() -> Result<()> {
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
        .send_thread_start_request_with_auto_env(ThreadStartParams::default())
        .await?;
    let parent: ThreadStartResponse = app.read_response(id).await?;
    infer(&mut app, &server, &parent.thread.id, "materialize").await?;
    let count = server
        .received_requests()
        .await
        .expect("request recording is enabled")
        .len();
    for invalid in [json!(""), json!(5)] {
        let id = app
            .send_thread_fork_request(ThreadForkParams {
                thread_id: parent.thread.id.clone(),
                config: Some([("model_reasoning_effort".to_string(), invalid)].into()),
                exclude_turns: true,
                ..Default::default()
            })
            .await?;
        let error = app
            .read_stream_until_error_message(codex_app_server_protocol::RequestId::Integer(id))
            .await?;
        assert_eq!(error.error.code, -32600);
        assert!(error.error.message.contains("model_reasoning_effort"));
    }
    let oversized = "x".repeat(/*n*/ 129);
    let id = app
        .send_thread_fork_request(ThreadForkParams {
            thread_id: parent.thread.id,
            config: Some([("model_reasoning_effort".to_string(), json!(oversized))].into()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let child: ThreadForkResponse = app.read_response(id).await?;
    assert_eq!(
        child.reasoning_effort,
        Some(ReasoningEffort::Custom(oversized))
    );
    let id = app
        .send_turn_start_request(TurnStartParams {
            thread_id: child.thread.id,
            input: vec![UserInput::Text {
                text: "work".to_string(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let started: TurnStartResponse = app.read_response(id).await?;
    let completed: codex_app_server_protocol::TurnCompletedNotification =
        app.read_notification("turn/completed").await?;
    assert_eq!(
        (completed.turn.id, completed.turn.status),
        (
            started.turn.id,
            codex_app_server_protocol::TurnStatus::Failed
        )
    );
    assert!(
        completed
            .turn
            .error
            .expect("failed turn has an error")
            .message
            .contains("128-byte")
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("request recording is enabled")
            .len(),
        count
    );
    Ok(())
}
