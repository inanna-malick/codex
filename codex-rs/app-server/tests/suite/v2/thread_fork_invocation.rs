use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_app_server_protocol::DynamicToolCustomSpec;
use codex_app_server_protocol::DynamicToolSpec;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ThreadForkParams;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_protocol::openai_models::ReasoningEffort;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

use super::thread_fork_effort::infer;

#[tokio::test]
async fn destination_forks_pending_invocation_legacy() -> Result<()> {
    destination_forks_pending_invocation(ThreadHistoryMode::Legacy).await
}

#[tokio::test]
async fn destination_forks_pending_invocation_paginated() -> Result<()> {
    destination_forks_pending_invocation(ThreadHistoryMode::Paginated).await
}

async fn destination_forks_pending_invocation(mode: ThreadHistoryMode) -> Result<()> {
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-6-astra")
        .write(home.path())?;
    let mut source = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let id = source
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(mode),
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
    let parent: ThreadStartResponse = source.read_response(id).await?;
    let invocation = responses::ev_custom_tool_call(
        "unfold_1",
        "hosted_eval",
        "fork accumulated context (a, b)",
    );
    let parent_mock = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("parent"),
            invocation.clone(),
            responses::ev_completed("parent"),
        ]),
    )
    .await;
    let id = source
        .send_turn_start_request(TurnStartParams {
            thread_id: parent.thread.id.clone(),
            input: vec![UserInput::Text {
                text: "understand and scaffold".to_string(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = source.read_response(id).await?;
    let ServerRequest::DynamicToolCall {
        request_id: pending_parent,
        ..
    } = source.read_stream_until_request_message().await?
    else {
        panic!("expected hosted invocation");
    };

    // A separate owner reads the same storage while the source is awaiting its hosted response.
    let mut destination = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let id = destination
        .send_thread_fork_request(ThreadForkParams {
            thread_id: parent.thread.id.clone(),
            through_call_id: Some("unfold_1".to_string()),
            expected_dynamic_tools: Some(vec![]),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let rejected = destination
        .read_stream_until_error_message(RequestId::Integer(id))
        .await?;
    assert_eq!(rejected.error.code, -32600);
    assert!(rejected.error.message.contains("declarations do not match"));
    let mut inherited_prefix = None;
    let mut children = Vec::new();
    for effort in [None, Some("low")] {
        let id = destination
            .send_thread_fork_request(ThreadForkParams {
                thread_id: parent.thread.id.clone(),
                through_call_id: Some("unfold_1".to_string()),
                exclude_turns: true,
                defer_goal_continuation: true,
                config: effort
                    .map(|effort| [("model_reasoning_effort".to_string(), json!(effort))].into()),
                ..Default::default()
            })
            .await?;
        let child: ThreadForkResponse = destination.read_response(id).await?;
        assert_eq!(
            child.reasoning_effort,
            Some(if effort.is_some() {
                ReasoningEffort::Low
            } else {
                ReasoningEffort::High
            })
        );
        // Forking itself has not spent a model turn.
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1 + children.len()
        );
        let request = infer(
            &mut destination,
            &server,
            &child.thread.id,
            "child assignment",
        )
        .await?;
        let input = request.input();
        let boundary = input
            .iter()
            .position(|item| item["type"] == "custom_tool_call" && item["call_id"] == "unfold_1")
            .unwrap();
        let prefix = input[..=boundary].to_vec();
        let source_input = parent_mock.single_request().input();
        assert_eq!(&prefix[..source_input.len()], source_input.as_slice());
        assert_eq!(prefix[boundary]["input"], invocation["item"]["input"]);
        if let Some(expected) = &inherited_prefix {
            assert_eq!(&prefix, expected);
        } else {
            inherited_prefix = Some(prefix);
        }
        assert_eq!(input[boundary + 1]["type"], "custom_tool_call_output");
        assert_eq!(input[boundary + 1]["call_id"], "unfold_1");
        assert!(
            input[boundary + 1]["output"]
                .to_string()
                .contains("child-local protocol closure")
        );
        assert!(
            !input
                .iter()
                .any(|item| item.to_string().contains("<turn_aborted>"))
        );
        let updates = request.inputs_of_type("configuration_update");
        assert_eq!(
            updates.last().unwrap()["reasoning"]["effort"],
            effort.unwrap_or("high")
        );
        children.push(child);
    }

    // Recursive capture remains possible while the original ancestor call is unresolved.
    let recursive = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("recursive"),
            responses::ev_custom_tool_call("unfold_2", "hosted_eval", "fork again"),
            responses::ev_completed("recursive"),
        ]),
    )
    .await;
    let id = destination
        .send_turn_start_request(TurnStartParams {
            thread_id: children[1].thread.id.clone(),
            input: vec![UserInput::Text {
                text: "scaffold children".to_string(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = destination.read_response(id).await?;
    let ServerRequest::DynamicToolCall {
        request_id: pending_child,
        ..
    } = destination.read_stream_until_request_message().await?
    else {
        panic!("expected recursive hosted invocation");
    };
    let mut grandchild_owner = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let id = grandchild_owner
        .send_thread_fork_request(ThreadForkParams {
            thread_id: children[1].thread.id.clone(),
            through_call_id: Some("unfold_2".to_string()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let grandchild: ThreadForkResponse = grandchild_owner.read_response(id).await?;
    let request = infer(
        &mut grandchild_owner,
        &server,
        &grandchild.thread.id,
        "recursive assignment",
    )
    .await?;
    let recursive_prefix = recursive.single_request().input();
    assert_eq!(
        &request.input()[..recursive_prefix.len()],
        recursive_prefix.as_slice()
    );
    assert_eq!(grandchild.reasoning_effort, Some(ReasoningEffort::Low));

    // Neither descendant settled the parent's operation: its real result still resumes it.
    let parent_continuation = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("parent_result"),
            responses::ev_assistant_message("parent_done", "parent advances"),
            responses::ev_completed("parent_result"),
        ]),
    )
    .await;
    source
        .send_response(
            pending_parent,
            serde_json::to_value(DynamicToolCallResponse {
                content_items: vec![DynamicToolCallOutputContentItem::InputText {
                    text: "actual source result".to_string(),
                }],
                success: true,
            })?,
        )
        .await?;
    source
        .read_stream_until_notification_message("turn/completed")
        .await?;
    assert_eq!(
        parent_continuation
            .single_request()
            .inputs_of_type("configuration_update"),
        vec![json!({"type": "configuration_update", "reasoning": {"effort": "high"}})]
    );
    assert!(
        !parent_continuation
            .single_request()
            .input()
            .iter()
            .any(|item| item.to_string().contains("child-local protocol closure"))
    );

    let id = grandchild_owner
        .send_thread_fork_request(ThreadForkParams {
            thread_id: parent.thread.id.clone(),
            through_call_id: Some("unfold_1".to_string()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let late: ThreadForkResponse = grandchild_owner.read_response(id).await?;
    let request = infer(
        &mut grandchild_owner,
        &server,
        &late.thread.id,
        "later sibling",
    )
    .await?;
    let prefix = inherited_prefix.unwrap();
    assert_eq!(&request.input()[..prefix.len()], prefix.as_slice());
    assert!(
        !request
            .input()
            .iter()
            .any(|item| item.to_string().contains("actual source result"))
    );

    let id = grandchild_owner
        .send_thread_fork_request(ThreadForkParams {
            thread_id: parent.thread.id,
            through_call_id: Some("missing-call".to_string()),
            exclude_turns: true,
            ..Default::default()
        })
        .await?;
    let error = grandchild_owner
        .read_stream_until_error_message(RequestId::Integer(id))
        .await?;
    assert_eq!(error.error.code, -32600);
    assert!(error.error.message.contains("no durable invocation"));
    // Finish the remaining hosted request cleanly; no parent call was interrupted by forking.
    let _completion = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("child_done"),
            responses::ev_completed("child_done"),
        ]),
    )
    .await;
    destination
        .send_response(
            pending_child,
            serde_json::to_value(DynamicToolCallResponse {
                content_items: vec![],
                success: true,
            })?,
        )
        .await?;
    destination
        .read_stream_until_notification_message("turn/completed")
        .await?;
    Ok(())
}

#[tokio::test]
async fn destination_forks_completed_invocation_legacy() -> Result<()> {
    destination_forks_completed_invocation(ThreadHistoryMode::Legacy).await
}

#[tokio::test]
async fn destination_forks_completed_invocation_paginated() -> Result<()> {
    destination_forks_completed_invocation(ThreadHistoryMode::Paginated).await
}

async fn destination_forks_completed_invocation(mode: ThreadHistoryMode) -> Result<()> {
    let server = responses::start_mock_server().await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_model("gpt-6-astra")
        .write(home.path())?;
    let mut source = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let id = source
        .send_thread_start_request_with_auto_env(ThreadStartParams {
            history_mode: Some(mode),
            dynamic_tools: Some(vec![DynamicToolSpec::Custom(DynamicToolCustomSpec {
                name: "hosted_eval".into(),
                description: "Evaluate code".into(),
                defer_loading: false,
                format: None,
            })]),
            ..Default::default()
        })
        .await?;
    let parent: ThreadStartResponse = source.read_response(id).await?;
    responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("call"),
            responses::ev_custom_tool_call("unfold_done", "hosted_eval", "unfold then bind"),
            responses::ev_completed("call"),
        ]),
    )
    .await;
    let id = source
        .send_turn_start_request(TurnStartParams {
            thread_id: parent.thread.id.clone(),
            input: vec![UserInput::Text {
                text: "fork".into(),
                text_elements: vec![],
            }],
            ..Default::default()
        })
        .await?;
    let _: TurnStartResponse = source.read_response(id).await?;
    let ServerRequest::DynamicToolCall { request_id, .. } =
        source.read_stream_until_request_message().await?
    else {
        panic!("expected invocation");
    };
    let mut destination = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let params = ThreadForkParams {
        thread_id: parent.thread.id.clone(),
        after_call_id: Some("unfold_done".into()),
        exclude_turns: true,
        ..Default::default()
    };
    let id = destination.send_thread_fork_request(params.clone()).await?;
    let error = destination
        .read_stream_until_error_message(RequestId::Integer(id))
        .await?;
    assert_eq!(error.error.code, -32600);
    let continuation = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("continued"),
            responses::ev_assistant_message("later", "parent advanced beyond the boundary"),
            responses::ev_completed("continued"),
        ]),
    )
    .await;
    source
        .send_response(
            request_id,
            serde_json::to_value(DynamicToolCallResponse {
                content_items: vec![DynamicToolCallOutputContentItem::InputText {
                    text: "real result with final bindings".into(),
                }],
                success: true,
            })?,
        )
        .await?;
    source
        .read_stream_until_notification_message("turn/completed")
        .await?;
    let expected = continuation.single_request().input();
    for _ in 0..2 {
        let id = destination.send_thread_fork_request(params.clone()).await?;
        let child: ThreadForkResponse = destination.read_response(id).await?;
        let request = infer(
            &mut destination,
            &server,
            &child.thread.id,
            "child assignment",
        )
        .await?;
        let input = request.input();
        assert_eq!(&input[..expected.len()], expected.as_slice());
        assert!(
            input
                .iter()
                .any(|item| item.to_string().contains("real result with final bindings"))
        );
        assert!(!input.iter().any(|item| {
            item.to_string()
                .contains("parent advanced beyond the boundary")
                || item.to_string().contains("child-local protocol closure")
                || item.to_string().contains("<turn_aborted>")
        }));
    }
    Ok(())
}
