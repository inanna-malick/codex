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

#[tokio::test]
async fn exact_thread_call_keys_and_terminal_states_do_not_alias() {
    let home = unique_temp_dir();
    let state = runtime(&home).await;
    let first_thread = ThreadId::new();
    let second_thread = ThreadId::new();
    let first = HostToolCompletionKey {
        thread_id: first_thread,
        context_call_id: "shared-call".to_string(),
    };
    let second = HostToolCompletionKey {
        thread_id: second_thread,
        context_call_id: "shared-call".to_string(),
    };
    state
        .thread_queue()
        .register_host_tool_completion(&first)
        .await
        .unwrap();
    state
        .thread_queue()
        .register_host_tool_completion(&second)
        .await
        .unwrap();

    assert_eq!(
        HostToolCompletionState::ReattachedWithoutCompletion,
        state
            .thread_queue()
            .mark_host_tool_completion_reattached(&first)
            .await
            .unwrap()
            .state
    );
    assert!(
        state
            .thread_queue()
            .mark_host_tool_completion_ready(&first)
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot transition")
    );
    assert_eq!(
        vec![HostToolCompletionRecord {
            key: second,
            state: HostToolCompletionState::Pending,
        }],
        state
            .thread_queue()
            .list_unresolved_host_tool_completions(second_thread)
            .await
            .unwrap()
    );
    assert!(
        state
            .thread_queue()
            .list_unresolved_host_tool_completions(first_thread)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn unresolved_capacity_still_allows_idempotent_existing_registration() {
    let home = unique_temp_dir();
    let state = runtime(&home).await;
    let thread_id = ThreadId::new();
    let mut first = None;
    for index in 0..MAX_UNRESOLVED_HOST_TOOL_COMPLETIONS {
        let key = HostToolCompletionKey {
            thread_id,
            context_call_id: format!("call-{index}"),
        };
        state
            .thread_queue()
            .register_host_tool_completion(&key)
            .await
            .unwrap();
        first.get_or_insert(key);
    }
    let first = first.unwrap();
    assert_eq!(
        HostToolCompletionState::Pending,
        state
            .thread_queue()
            .register_host_tool_completion(&first)
            .await
            .unwrap()
            .state
    );
    let overflow = HostToolCompletionKey {
        thread_id,
        context_call_id: "overflow".to_string(),
    };
    assert!(
        state
            .thread_queue()
            .register_host_tool_completion(&overflow)
            .await
            .unwrap_err()
            .to_string()
            .contains("too many unresolved")
    );
}
