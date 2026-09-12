use super::*;
use crate::runtime::test_support::unique_temp_dir;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio::time::timeout;

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
async fn registration_distinguishes_first_admission_from_existing_key() {
    let home = unique_temp_dir();
    let state = runtime(&home).await;
    let key = HostToolCompletionKey {
        thread_id: ThreadId::new(),
        context_call_id: "call-admission".to_string(),
    };
    let record = HostToolCompletionRecord {
        key: key.clone(),
        state: HostToolCompletionState::Pending,
    };

    assert_eq!(
        HostToolCompletionRegistration::New(record.clone()),
        state
            .thread_queue()
            .register_host_tool_call(&key)
            .await
            .unwrap()
    );
    assert_eq!(
        HostToolCompletionRegistration::Existing(record),
        state
            .thread_queue()
            .register_host_tool_call(&key)
            .await
            .unwrap()
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
    assert_eq!(
        HostToolCompletionState::ReattachedWithoutCompletion,
        state
            .thread_queue()
            .mark_host_tool_completion_ready(&first)
            .await
            .unwrap()
            .state
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
async fn reconcile_intent_is_durable_and_ready_wins_stale_transitions() {
    let home = unique_temp_dir();
    let first = runtime(&home).await;
    let thread_id = ThreadId::new();
    let key = HostToolCompletionKey {
        thread_id,
        context_call_id: "interrupted-call".to_string(),
    };
    first
        .thread_queue()
        .register_host_tool_completion(&key)
        .await
        .unwrap();
    assert_eq!(
        HostToolCompletionState::ReconcilePending,
        first
            .thread_queue()
            .mark_host_tool_completion_reconcile(&key)
            .await
            .unwrap()
            .state
    );
    first.close().await;

    let resumed = runtime(&home).await;
    let reconcile_record = HostToolCompletionRecord {
        key: key.clone(),
        state: HostToolCompletionState::ReconcilePending,
    };
    assert_eq!(
        Some(reconcile_record.clone()),
        resumed
            .thread_queue()
            .read_host_tool_completion(&key)
            .await
            .unwrap()
    );
    assert_eq!(
        vec![reconcile_record],
        resumed
            .thread_queue()
            .list_unresolved_host_tool_completions(thread_id)
            .await
            .unwrap()
    );
    assert_eq!(
        HostToolCompletionState::Ready,
        resumed
            .thread_queue()
            .mark_host_tool_completion_ready(&key)
            .await
            .unwrap()
            .state
    );
    assert_eq!(
        HostToolCompletionState::Ready,
        resumed
            .thread_queue()
            .mark_host_tool_completion_reconcile(&key)
            .await
            .unwrap()
            .state
    );
    assert_eq!(
        HostToolCompletionState::Ready,
        resumed
            .thread_queue()
            .mark_host_tool_completion_ready(&key)
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
    assert_eq!(
        HostToolCompletionState::Acknowledged,
        resumed
            .thread_queue()
            .mark_host_tool_completion_reconcile(&key)
            .await
            .unwrap()
            .state
    );
}

#[tokio::test]
async fn reconcile_can_settle_as_reattached_but_cannot_be_acknowledged() {
    let home = unique_temp_dir();
    let state = runtime(&home).await;
    let key = HostToolCompletionKey {
        thread_id: ThreadId::new(),
        context_call_id: "reattached-call".to_string(),
    };
    state
        .thread_queue()
        .register_host_tool_completion(&key)
        .await
        .unwrap();
    state
        .thread_queue()
        .mark_host_tool_completion_reconcile(&key)
        .await
        .unwrap();
    assert_eq!(
        HostToolCompletionState::ReattachedWithoutCompletion,
        state
            .thread_queue()
            .mark_host_tool_completion_reattached(&key)
            .await
            .unwrap()
            .state
    );
    assert_eq!(
        HostToolCompletionState::ReattachedWithoutCompletion,
        state
            .thread_queue()
            .mark_host_tool_completion_ready(&key)
            .await
            .unwrap()
            .state
    );
    let error = state
        .thread_queue()
        .acknowledge_host_tool_completion(&key)
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<HostToolCompletionError>(),
        Some(HostToolCompletionError::Conflict {
            current: HostToolCompletionState::ReattachedWithoutCompletion,
            requested: HostToolCompletionState::Acknowledged,
        })
    ));
}

#[tokio::test]
async fn cancellation_before_dispatch_is_a_distinct_terminal_state() {
    let home = unique_temp_dir();
    let state = runtime(&home).await;
    let key = HostToolCompletionKey {
        thread_id: ThreadId::new(),
        context_call_id: "cancelled-before-dispatch".to_string(),
    };
    state
        .thread_queue()
        .register_host_tool_completion(&key)
        .await
        .unwrap();
    state
        .thread_queue()
        .mark_host_tool_completion_reconcile(&key)
        .await
        .unwrap();
    assert_eq!(
        HostToolCompletionState::NotSubmitted,
        state
            .thread_queue()
            .mark_host_tool_completion_not_submitted(&key)
            .await
            .unwrap()
            .state
    );
    assert_eq!(
        HostToolCompletionState::NotSubmitted,
        state
            .thread_queue()
            .mark_host_tool_completion_ready(&key)
            .await
            .unwrap()
            .state
    );
    assert!(
        state
            .thread_queue()
            .list_unresolved_host_tool_completions(key.thread_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn recovery_state_migration_preserves_existing_terminal_rows() {
    let home = unique_temp_dir();
    tokio::fs::create_dir_all(&home).await.unwrap();
    let sqlite = crate::SqliteConfig::new_for_testing(home.abs());
    let pool = sqlite
        .open_read_write_pool(&home.join("migration.sqlite"))
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../../queue_migrations/0006_host_tool_completions.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    for (index, state) in ["acknowledged", "reattached_without_completion"]
        .into_iter()
        .enumerate()
    {
        sqlx::query(
            "INSERT INTO host_tool_completions
             (thread_id, context_call_id, state, created_at_ms, updated_at_ms)
             VALUES ('thread', ?, ?, 1, 1)",
        )
        .bind(format!("call-{index}"))
        .bind(state)
        .execute(&pool)
        .await
        .unwrap();
    }

    sqlx::raw_sql(include_str!(
        "../../queue_migrations/0007_host_tool_completion_recovery_states.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let states: Vec<String> =
        sqlx::query_scalar("SELECT state FROM host_tool_completions ORDER BY context_call_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        vec![
            "acknowledged".to_string(),
            "reattached_without_completion".to_string(),
        ],
        states
    );
    for state in ["reconcile_pending", "not_submitted"] {
        sqlx::query(
            "INSERT INTO host_tool_completions
             (thread_id, context_call_id, state, created_at_ms, updated_at_ms)
             VALUES ('thread', ?, ?, 1, 1)",
        )
        .bind(state)
        .bind(state)
        .execute(&pool)
        .await
        .unwrap();
    }
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
    let error = state
        .thread_queue()
        .register_host_tool_completion(&overflow)
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<HostToolCompletionError>(),
        Some(HostToolCompletionError::Capacity)
    ));
}

#[tokio::test]
async fn missing_and_storage_failures_are_typed() {
    let home = unique_temp_dir();
    let state = runtime(&home).await;
    let key = HostToolCompletionKey {
        thread_id: ThreadId::new(),
        context_call_id: "missing".to_string(),
    };
    let missing = state
        .thread_queue()
        .mark_host_tool_completion_reconcile(&key)
        .await
        .unwrap_err();
    assert!(matches!(
        missing.downcast_ref::<HostToolCompletionError>(),
        Some(HostToolCompletionError::Missing)
    ));

    sqlx::query("DROP TABLE host_tool_completions")
        .execute(state.thread_queue().pool.as_ref())
        .await
        .unwrap();
    let storage = state
        .thread_queue()
        .read_host_tool_completion(&key)
        .await
        .unwrap_err();
    let typed = storage
        .downcast_ref::<HostToolCompletionError>()
        .expect("typed storage error");
    assert!(matches!(typed, HostToolCompletionError::Storage { .. }));
    assert!(!typed.retryable());
    assert!(
        storage
            .chain()
            .any(|cause| cause.downcast_ref::<sqlx::Error>().is_some())
    );
}

#[tokio::test]
async fn locked_storage_failure_is_typed_as_retryable() {
    let home = unique_temp_dir();
    let owner = runtime(&home).await;
    let key = HostToolCompletionKey {
        thread_id: ThreadId::new(),
        context_call_id: "locked".to_string(),
    };
    owner
        .thread_queue()
        .register_host_tool_completion(&key)
        .await
        .unwrap();
    let sqlite = crate::SqliteConfig::new_for_testing(home.abs());
    let competing_pool = sqlite
        .open_read_write_pool(&sqlite.queue_db_path())
        .await
        .unwrap();
    let mut connections = Vec::new();
    for _ in 0..5 {
        let mut connection = competing_pool.acquire().await.unwrap();
        sqlx::query("PRAGMA busy_timeout = 0")
            .execute(connection.as_mut())
            .await
            .unwrap();
        connections.push(connection);
    }
    drop(connections);
    let competing_store = SqliteQueueStore::new(Arc::new(competing_pool));
    let writer = owner
        .thread_queue()
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();

    let storage = competing_store
        .mark_host_tool_completion_reconcile(&key)
        .await
        .unwrap_err();
    let typed = storage
        .downcast_ref::<HostToolCompletionError>()
        .expect("typed storage error");
    assert!(matches!(typed, HostToolCompletionError::Storage { .. }));
    assert!(typed.retryable());
    assert!(
        storage
            .chain()
            .any(|cause| cause.downcast_ref::<sqlx::Error>().is_some())
    );

    writer.rollback().await.unwrap();
}

#[tokio::test]
async fn read_modify_write_operations_wait_for_an_independent_writer() {
    let home = unique_temp_dir();
    let first = runtime(&home).await;
    let second = runtime(&home).await;
    let thread_id = ThreadId::new();
    let registered = HostToolCompletionKey {
        thread_id,
        context_call_id: "registered".to_string(),
    };
    first
        .thread_queue()
        .register_host_tool_completion(&registered)
        .await
        .unwrap();
    first
        .thread_queue()
        .mark_host_tool_completion_ready(&registered)
        .await
        .unwrap();

    let writer = second
        .thread_queue()
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    let mut transition = {
        let first = Arc::clone(&first);
        let registered = registered.clone();
        tokio::spawn(async move {
            first
                .thread_queue()
                .acknowledge_host_tool_completion(&registered)
                .await
        })
    };
    assert!(
        timeout(Duration::from_millis(100), &mut transition)
            .await
            .is_err(),
        "transition should wait for the existing writer"
    );
    writer.commit().await.unwrap();

    assert_eq!(
        HostToolCompletionState::Acknowledged,
        timeout(Duration::from_secs(5), transition)
            .await
            .expect("transition should resume after the writer commits")
            .unwrap()
            .unwrap()
            .state
    );

    let writer = second
        .thread_queue()
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    let new_key = HostToolCompletionKey {
        thread_id,
        context_call_id: "new".to_string(),
    };
    let mut registration = {
        let first = Arc::clone(&first);
        let new_key = new_key.clone();
        tokio::spawn(async move {
            first
                .thread_queue()
                .register_host_tool_completion(&new_key)
                .await
        })
    };
    assert!(
        timeout(Duration::from_millis(100), &mut registration)
            .await
            .is_err(),
        "registration should wait for the existing writer"
    );
    writer.commit().await.unwrap();

    assert_eq!(
        HostToolCompletionState::Pending,
        timeout(Duration::from_secs(5), registration)
            .await
            .expect("registration should resume after the writer commits")
            .unwrap()
            .unwrap()
            .state
    );
}
