use super::*;
use crate::runtime::test_support::unique_temp_dir;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;

async fn runtime(home: &std::path::Path) -> Arc<StateRuntime> {
    StateRuntime::init(
        crate::SqliteConfig::new_for_testing(home.abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("state runtime")
}

#[tokio::test]
async fn ready_completion_survives_runtime_restart_and_acknowledges_once() {
    let home = unique_temp_dir();
    let thread_id = ThreadId::new();
    let key = HostToolCompletionKey {
        thread_id,
        context_call_id: "call-1".to_string(),
    };
    let first = runtime(&home).await;
    assert_eq!(
        HostToolCompletionState::Pending,
        first
            .thread_queue()
            .register_host_tool_completion(&key)
            .await
            .unwrap()
            .state
    );
    assert_eq!(
        HostToolCompletionState::Ready,
        first
            .thread_queue()
            .mark_host_tool_completion_ready(&key)
            .await
            .unwrap()
            .state
    );
    first.close().await;

    let resumed = runtime(&home).await;
    assert_eq!(
        vec![HostToolCompletionRecord {
            key: key.clone(),
            state: HostToolCompletionState::Ready,
        }],
        resumed
            .thread_queue()
            .list_unresolved_host_tool_completions(thread_id)
            .await
            .unwrap()
    );
    assert_eq!(
        HostToolCompletionState::Acknowledged,
        resumed
            .thread_queue()
            .acknowledge_host_tool_completion(&key)
            .await
            .unwrap()
            .state
    );
    assert_eq!(
        HostToolCompletionState::Acknowledged,
        resumed
            .thread_queue()
            .acknowledge_host_tool_completion(&key)
            .await
            .unwrap()
            .state
    );
    assert!(
        resumed
            .thread_queue()
            .list_unresolved_host_tool_completions(thread_id)
            .await
            .unwrap()
            .is_empty()
    );
}
