use super::*;

use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::DynamicToolCallStatus;

fn dynamic_tool_item(
    namespace: Option<&str>,
    status: DynamicToolCallStatus,
    content_items: Option<Vec<DynamicToolCallOutputContentItem>>,
    success: Option<bool>,
) -> AppServerThreadItem {
    AppServerThreadItem::DynamicToolCall {
        id: "call-haskell".to_string(),
        namespace: namespace.map(str::to_string),
        tool: "haskell".to_string(),
        arguments: json!(
            "main = do\n  putStrLn \"{\\\"nested\\\":true}\"\n  putStrLn \\\\tmp\n  putStrLn \"λ\""
        ),
        status,
        content_items,
        success,
        duration_ms: None,
    }
}

#[tokio::test]
async fn live_dynamic_tool_call_updates_the_visible_transcript() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let _ = drain_insert_history(&mut rx);

    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: dynamic_tool_item(
                Some("tidepool_actor"),
                DynamicToolCallStatus::InProgress,
                None,
                None,
            ),
            started_at_ms: 0,
        }),
        /*replay_kind*/ None,
    );

    insta::assert_snapshot!(active_blob(&chat), @r#"
    • tidepool_actor.haskell Running
      ├ Input
      │ ```
      │ main = do
      │   putStrLn "{\"nested\":true}"
      │   putStrLn \\tmp
      │   putStrLn "λ"
      │ ```
    "#);

    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: dynamic_tool_item(
                Some("tidepool_actor"),
                DynamicToolCallStatus::Completed,
                Some(vec![DynamicToolCallOutputContentItem::InputText {
                    text: "{\"status\":\"rejected\"}\nGHC diagnostic".to_string(),
                }]),
                Some(true),
            ),
            completed_at_ms: 25,
        }),
        /*replay_kind*/ None,
    );

    assert!(chat.transcript.active_cell.is_none());
    let cells = drain_insert_history(&mut rx);
    let [cell] = cells.as_slice() else {
        panic!("expected one completed dynamic-tool cell, got {cells:?}");
    };
    insta::assert_snapshot!(lines_to_single_string(cell), @r#"
    • tidepool_actor.haskell Completed · 0ms
      ├ Input
      │ ```
      │ main = do
      │   putStrLn "{\"nested\":true}"
      │   putStrLn \\tmp
      │   putStrLn "λ"
      │ ```
      └ Result
        {"status":"rejected"}
        GHC diagnostic
    "#);
}

#[tokio::test]
async fn resumed_dynamic_tool_call_is_rendered() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let _ = drain_insert_history(&mut rx);

    chat.replay_thread_item(
        dynamic_tool_item(
            Some("tidepool_actor"),
            DynamicToolCallStatus::Failed,
            Some(vec![DynamicToolCallOutputContentItem::InputText {
                text: "host result unavailable".to_string(),
            }]),
            Some(false),
        ),
        "turn-1".to_string(),
        ReplayKind::ThreadSnapshot,
    );

    let cells = drain_insert_history(&mut rx);
    let [cell] = cells.as_slice() else {
        panic!("expected one replayed dynamic-tool cell, got {cells:?}");
    };
    let rendered = lines_to_single_string(cell);
    assert!(rendered.contains("tidepool_actor.haskell Failed"));
    assert!(rendered.contains("host result unavailable"));
    assert!(rendered.contains("main = do"));
}

#[tokio::test]
async fn tui_internal_dynamic_tools_keep_their_existing_rendering_path() {
    let (mut chat, mut rx, _op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let _ = drain_insert_history(&mut rx);

    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: dynamic_tool_item(
                Some(crate::dynamic_tools::NAMESPACE),
                DynamicToolCallStatus::InProgress,
                None,
                None,
            ),
            started_at_ms: 0,
        }),
        /*replay_kind*/ None,
    );
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item: dynamic_tool_item(
                Some(crate::dynamic_tools::NAMESPACE),
                DynamicToolCallStatus::Completed,
                Some(Vec::new()),
                Some(true),
            ),
            completed_at_ms: 1,
        }),
        /*replay_kind*/ None,
    );

    assert!(chat.transcript.active_cell.is_none());
    assert!(drain_insert_history(&mut rx).is_empty());
}
