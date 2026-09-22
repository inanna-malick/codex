use super::*;
use crate::migrations::QUEUE_MIGRATOR;
use crate::runtime::test_support::test_thread_metadata;
use crate::runtime::test_support::unique_temp_dir;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use sqlx::migrate::Migrator;
use std::borrow::Cow;
use std::future::Future;
use std::time::Duration;
use tokio::time::timeout;

async fn runtime_with_thread() -> (Arc<StateRuntime>, ThreadId) {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(
        crate::SqliteConfig::new_for_testing(home.as_path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("state runtime");
    let thread_id = ThreadId::new();
    let metadata = test_thread_metadata(home.as_path(), thread_id, home.clone());
    runtime.upsert_thread(&metadata).await.unwrap();
    (runtime, thread_id)
}

fn host_operation(thread_id: ThreadId, producer_id: &str, sequence: u64) -> HostInputOperation {
    HostInputOperation {
        thread_id,
        producer_id: producer_id.to_string(),
        sequence,
        purpose: "assignment".to_string(),
        mode: "queueOnly".to_string(),
        target_json: "{\"actor\":\"actor-1\",\"conversation\":\"thread\",\"correlation\":null}"
            .to_string(),
        content_digest: "digest-v1".to_string(),
        payload: r#"{"host":true}"#.to_string(),
    }
}

async fn completes_after_independent_writer<T>(
    writer_runtime: &Arc<StateRuntime>,
    operation: impl Future<Output = anyhow::Result<T>> + Send + 'static,
) -> T
where
    T: Send + 'static,
{
    let writer = writer_runtime
        .thread_queue()
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    let mut operation = tokio::spawn(operation);
    assert!(
        timeout(Duration::from_millis(100), &mut operation)
            .await
            .is_err(),
        "operation should wait for the existing writer"
    );
    writer.commit().await.unwrap();
    timeout(Duration::from_secs(5), operation)
        .await
        .expect("operation should resume after the writer commits")
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn read_modify_write_operations_wait_for_an_independent_writer() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let other = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();

    let first = runtime
        .thread_queue()
        .enqueue(thread_id, r#"{"first":true}"#)
        .await
        .unwrap();
    let second = runtime
        .thread_queue()
        .enqueue(thread_id, r#"{"second":true}"#)
        .await
        .unwrap();
    let ordered_ids = vec![second.id.clone(), first.id.clone()];
    completes_after_independent_writer(&other, {
        let runtime = Arc::clone(&runtime);
        let ordered_ids = ordered_ids.clone();
        async move {
            runtime
                .thread_queue()
                .reorder(thread_id, &ordered_ids)
                .await
        }
    })
    .await;
    assert!(
        runtime
            .thread_queue()
            .delete(thread_id, &first.id)
            .await
            .unwrap()
    );
    assert!(
        runtime
            .thread_queue()
            .delete(thread_id, &second.id)
            .await
            .unwrap()
    );

    let admitted = host_operation(thread_id, "run/inbox/actor-1.1", 1);
    assert!(matches!(
        completes_after_independent_writer(&other, {
            let runtime = Arc::clone(&runtime);
            let admitted = admitted.clone();
            async move { runtime.thread_queue().admit_host_input(&admitted).await }
        })
        .await,
        HostInputAdmission::Admitted(_)
    ));
    let admitted_item = runtime
        .thread_queue()
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        HostInputState::Dispatching,
        completes_after_independent_writer(&other, {
            let runtime = Arc::clone(&runtime);
            let item_id = admitted_item.id.clone();
            async move {
                runtime
                    .thread_queue()
                    .claim_host_queue_item(thread_id, &item_id)
                    .await
            }
        })
        .await
        .unwrap()
        .state
    );

    let withdrawn = host_operation(thread_id, "run/inbox/actor-2.1", 1);
    runtime
        .thread_queue()
        .admit_host_input(&withdrawn)
        .await
        .unwrap();
    assert!(matches!(
        completes_after_independent_writer(&other, {
            let runtime = Arc::clone(&runtime);
            let withdrawn = withdrawn.clone();
            async move {
                runtime
                    .thread_queue()
                    .withdraw_host_input(thread_id, &withdrawn.producer_id, withdrawn.sequence)
                    .await
            }
        })
        .await,
        Some(HostInputWithdrawal::Withdrawn(_))
    ));

    let acknowledged = host_operation(thread_id, "run/inbox/actor-3.1", 1);
    runtime
        .thread_queue()
        .admit_host_input(&acknowledged)
        .await
        .unwrap();
    let acknowledged_item = runtime
        .thread_queue()
        .list_page(thread_id, /*offset*/ 0, /*limit*/ MAX_QUEUE_ITEMS)
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.id == format!("host:{}:1", acknowledged.producer_id))
        .unwrap();
    runtime
        .thread_queue()
        .claim_host_queue_item(thread_id, &acknowledged_item.id)
        .await
        .unwrap();
    runtime
        .thread_queue()
        .confirm_host_input_presented(
            thread_id,
            &acknowledged_item.id,
            &acknowledged.producer_id,
            acknowledged.sequence,
        )
        .await
        .unwrap();
    assert_eq!(
        HostInputState::Presented,
        completes_after_independent_writer(&other, {
            let runtime = Arc::clone(&runtime);
            let acknowledged = acknowledged.clone();
            async move {
                runtime
                    .thread_queue()
                    .acknowledge_host_input(
                        thread_id,
                        &acknowledged.producer_id,
                        acknowledged.sequence,
                    )
                    .await
            }
        })
        .await
        .unwrap()
        .state
    );
}

#[tokio::test]
async fn host_input_admission_is_idempotent_and_rejects_changed_content() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let operation = host_operation(thread_id, "run-a/inbox/actor-1.1", 7);
    assert!(matches!(
        queue.admit_host_input(&operation).await.unwrap(),
        HostInputAdmission::Admitted(_)
    ));
    assert!(matches!(
        queue.admit_host_input(&operation).await.unwrap(),
        HostInputAdmission::Existing(_)
    ));
    let mut changed = operation.clone();
    changed.payload = r#"{"host":"changed"}"#.to_string();
    assert_eq!(
        HostInputAdmission::Conflict,
        queue.admit_host_input(&changed).await.unwrap()
    );
    let mut changed_purpose = operation.clone();
    changed_purpose.purpose = "notification".to_string();
    assert_eq!(
        HostInputAdmission::Conflict,
        queue.admit_host_input(&changed_purpose).await.unwrap()
    );
    assert_eq!(
        Some(HostInputRecord {
            operation,
            state: HostInputState::Ready,
        }),
        queue
            .observe_host_input("run-a/inbox/actor-1.1", 7)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn distinct_run_scopes_do_not_alias_and_seal_quarantines_ready_input() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = host_operation(thread_id, "run-a/inbox/actor-1.1", 1);
    let second = host_operation(thread_id, "run-b/inbox/actor-1.1", 1);
    assert!(matches!(
        queue.admit_host_input(&first).await.unwrap(),
        HostInputAdmission::Admitted(_)
    ));
    assert!(matches!(
        queue.admit_host_input(&second).await.unwrap(),
        HostInputAdmission::Admitted(_)
    ));
    queue
        .seal_host_input_producer(thread_id, &first.producer_id)
        .await
        .unwrap();
    assert_eq!(
        HostInputState::Rejected,
        queue
            .observe_host_input(&first.producer_id, first.sequence)
            .await
            .unwrap()
            .unwrap()
            .state
    );
    assert_eq!(
        HostInputState::Ready,
        queue
            .observe_host_input(&second.producer_id, second.sequence)
            .await
            .unwrap()
            .unwrap()
            .state
    );
    assert_eq!(
        HostInputAdmission::Existing(HostInputRecord {
            operation: first.clone(),
            state: HostInputState::Rejected,
        }),
        queue.admit_host_input(&first).await.unwrap()
    );
}

#[tokio::test]
async fn withdrawal_is_a_durable_negative_fence() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let operation = host_operation(thread_id, "run/inbox/actor-1.1", 2);
    queue.admit_host_input(&operation).await.unwrap();
    assert!(matches!(
        queue
            .withdraw_host_input(thread_id, &operation.producer_id, operation.sequence)
            .await
            .unwrap(),
        Some(HostInputWithdrawal::Withdrawn(_))
    ));
    assert!(matches!(
        queue.admit_host_input(&operation).await.unwrap(),
        HostInputAdmission::Existing(HostInputRecord {
            state: HostInputState::Withdrawn,
            ..
        })
    ));
    assert!(
        queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn withdrawal_before_delayed_submit_persists_a_negative_tombstone() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let operation = host_operation(thread_id, "run/inbox/actor-1.1", 1);
    assert_eq!(
        Some(HostInputWithdrawal::Tombstoned),
        runtime
            .thread_queue()
            .withdraw_host_input(thread_id, &operation.producer_id, operation.sequence)
            .await
            .unwrap()
    );
    assert_eq!(
        HostInputAdmission::Withdrawn,
        runtime
            .thread_queue()
            .admit_host_input(&operation)
            .await
            .unwrap()
    );
    assert_eq!(
        None,
        runtime
            .thread_queue()
            .acknowledge_host_input(thread_id, &operation.producer_id, operation.sequence)
            .await
            .unwrap()
    );
    assert_eq!(
        HostInputAdmission::Compacted,
        runtime
            .thread_queue()
            .admit_host_input(&operation)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn dispatch_claim_survives_queue_consumption_and_restart() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let operation = host_operation(thread_id, "run/inbox/actor-1.1", 3);
    let queue = runtime.thread_queue();
    queue.admit_host_input(&operation).await.unwrap();
    let queued = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await
        .unwrap();
    assert!(matches!(
        queue
            .claim_host_queue_item(thread_id, &queued[0].id)
            .await
            .unwrap(),
        Some(HostInputRecord {
            state: HostInputState::Dispatching,
            ..
        })
    ));
    assert!(queue.delete(thread_id, &queued[0].id).await.unwrap());

    let reopened = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();
    assert_eq!(
        HostInputState::Dispatching,
        reopened
            .thread_queue()
            .observe_host_input(&operation.producer_id, operation.sequence)
            .await
            .unwrap()
            .unwrap()
            .state
    );
    assert!(matches!(
        reopened
            .thread_queue()
            .withdraw_host_input(thread_id, &operation.producer_id, operation.sequence)
            .await
            .unwrap(),
        Some(HostInputWithdrawal::Unknown(HostInputRecord {
            state: HostInputState::Unknown,
            ..
        }))
    ));
}

#[tokio::test]
async fn confirmed_presentation_atomically_consumes_only_the_claimed_queue_row() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let operation = host_operation(thread_id, "run/inbox/actor-1.1", 3);
    let queue = runtime.thread_queue();
    queue.admit_host_input(&operation).await.unwrap();
    let queued = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await
        .unwrap();
    queue
        .claim_host_queue_item(thread_id, &queued[0].id)
        .await
        .unwrap()
        .expect("host claim");

    assert!(
        queue
            .confirm_host_input_presented(
                thread_id,
                "different-queue-row",
                &operation.producer_id,
                operation.sequence,
            )
            .await
            .is_err()
    );
    assert_eq!(
        HostInputState::Dispatching,
        queue
            .observe_host_input(&operation.producer_id, operation.sequence)
            .await
            .unwrap()
            .unwrap()
            .state
    );
    assert_eq!(queued, queue.list_page(thread_id, 0, 1).await.unwrap());

    let presented = queue
        .confirm_host_input_presented(
            thread_id,
            &queued[0].id,
            &operation.producer_id,
            operation.sequence,
        )
        .await
        .unwrap();
    assert_eq!(HostInputState::Presented, presented.state);
    assert!(queue.list_page(thread_id, 0, 1).await.unwrap().is_empty());

    let reopened = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();
    assert_eq!(
        HostInputState::Presented,
        reopened
            .thread_queue()
            .observe_host_input(&operation.producer_id, operation.sequence)
            .await
            .unwrap()
            .unwrap()
            .state
    );
}

#[tokio::test]
async fn presentation_acknowledgement_is_idempotent_after_lost_response() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let operation = host_operation(thread_id, "run/inbox/actor-1.1", 1);
    let queue = runtime.thread_queue();
    queue.admit_host_input(&operation).await.unwrap();
    let queued = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await
        .unwrap();
    queue
        .claim_host_queue_item(thread_id, &queued[0].id)
        .await
        .unwrap();

    queue
        .confirm_host_input_presented(
            thread_id,
            &queued[0].id,
            &operation.producer_id,
            operation.sequence,
        )
        .await
        .unwrap();

    let record = queue
        .acknowledge_host_input(thread_id, &operation.producer_id, operation.sequence)
        .await
        .unwrap()
        .expect("acknowledged operation");
    assert_eq!(HostInputState::Presented, record.state);
    assert_eq!(
        None,
        queue
            .acknowledge_host_input(thread_id, &operation.producer_id, operation.sequence)
            .await
            .unwrap()
    );
    let reopened = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();
    let queue = reopened.thread_queue();
    assert_eq!(
        None,
        queue
            .observe_host_input(&operation.producer_id, operation.sequence)
            .await
            .unwrap()
    );
    assert_eq!(
        HostInputAdmission::Compacted,
        queue.admit_host_input(&operation).await.unwrap()
    );
    assert!(
        queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn acknowledgement_preserves_nonterminal_evidence_and_terminal_prefix() {
    for state in [
        HostInputState::Ready,
        HostInputState::Dispatching,
        HostInputState::Unknown,
    ] {
        let (runtime, thread_id) = runtime_with_thread().await;
        let queue = runtime.thread_queue();
        let first = host_operation(thread_id, "run/inbox/actor-1.1", 1);
        let second = host_operation(thread_id, &first.producer_id, 2);
        queue.admit_host_input(&first).await.unwrap();
        queue
            .withdraw_host_input(thread_id, &first.producer_id, 1)
            .await
            .unwrap();
        queue.admit_host_input(&second).await.unwrap();
        let queued = queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap();
        if state != HostInputState::Ready {
            queue
                .claim_host_queue_item(thread_id, &queued[0].id)
                .await
                .unwrap();
        }
        if state == HostInputState::Unknown {
            queue
                .withdraw_host_input(thread_id, &second.producer_id, 2)
                .await
                .unwrap();
        }

        assert!(
            queue
                .acknowledge_host_input(thread_id, &second.producer_id, 2)
                .await
                .is_err(),
            "{state:?} cannot be promoted or compacted by host acknowledgement"
        );
        assert_eq!(
            Some(HostInputRecord {
                operation: first.clone(),
                state: HostInputState::Withdrawn
            }),
            queue
                .observe_host_input(&first.producer_id, 1)
                .await
                .unwrap()
        );
        assert_eq!(
            Some(HostInputRecord {
                operation: second,
                state
            }),
            queue
                .observe_host_input(&first.producer_id, 2)
                .await
                .unwrap()
        );
        assert_eq!(
            queued,
            queue
                .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
                .await
                .unwrap()
        );
        // The failed prefix transaction must leave even its terminal row unacknowledged.
        assert_eq!(
            Some(HostInputRecord {
                operation: first.clone(),
                state: HostInputState::Withdrawn
            }),
            queue
                .acknowledge_host_input(thread_id, &first.producer_id, 1)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn prefix_acknowledgement_rejects_ready_and_noncontiguous_rows() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = host_operation(thread_id, "run/inbox/actor-1.1", 1);
    let third = host_operation(thread_id, "run/inbox/actor-1.1", 3);
    queue.admit_host_input(&first).await.unwrap();
    queue.admit_host_input(&third).await.unwrap();
    assert!(
        queue
            .acknowledge_host_input(thread_id, &first.producer_id, 1)
            .await
            .is_err()
    );
    let queued = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 2)
        .await
        .unwrap();
    queue
        .claim_host_queue_item(thread_id, &queued[0].id)
        .await
        .unwrap();
    assert!(
        queue
            .acknowledge_host_input(thread_id, &first.producer_id, 3)
            .await
            .is_err()
    );
    assert_eq!(
        Some(HostInputRecord {
            operation: first,
            state: HostInputState::Dispatching,
        }),
        queue
            .observe_host_input("run/inbox/actor-1.1", 1)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn competing_runtimes_preserve_fifo_queue_order() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let other = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();
    let queue = runtime.thread_queue();
    let other_queue = other.thread_queue();
    let (first, second) = tokio::join!(
        queue.enqueue(thread_id, r#"{"first":true}"#),
        other_queue.enqueue(thread_id, r#"{"second":true}"#),
    );
    let mut expected = vec![first.unwrap(), second.unwrap()];
    expected.sort_by(|first, second| first.id.cmp(&second.id));
    let mut actual = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 2)
        .await
        .unwrap();
    actual.sort_by(|first, second| first.id.cmp(&second.id));
    assert_eq!(expected, actual);
}

#[tokio::test]
async fn migrating_existing_queue_backfills_thread_revisions() {
    let home = unique_temp_dir();
    tokio::fs::create_dir_all(&home).await.unwrap();
    let sqlite = crate::SqliteConfig::new_for_testing(home.as_path().abs());
    let queue_path = sqlite.queue_db_path();
    let old_queue_migrator = Migrator {
        migrations: Cow::Owned(vec![QUEUE_MIGRATOR.migrations[0].clone()]),
        ignore_missing: false,
        locking: true,
        no_tx: false,
        table_name: QUEUE_MIGRATOR.table_name.clone(),
        create_schemas: QUEUE_MIGRATOR.create_schemas.clone(),
    };
    let pool = sqlite.open_read_write_pool(&queue_path).await.unwrap();
    old_queue_migrator.run(&pool).await.unwrap();

    let thread_id = ThreadId::new();
    let queued = QueuedUserSubmissionRecord {
        id: Uuid::now_v7().to_string(),
        thread_id,
        payload: r#"{"existing":true}"#.to_string(),
    };
    sqlx::query(
        "INSERT INTO queued_items
         (id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms)
         VALUES (?, ?, ?, 0, 0, 0)",
    )
    .bind(&queued.id)
    .bind(thread_id.to_string())
    .bind(&queued.payload)
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let runtime = StateRuntime::init(sqlite, "test-provider".to_string())
        .await
        .unwrap();
    let queue = runtime.thread_queue();
    assert_eq!(
        vec![(thread_id, 1)],
        queue
            .changes_since(/*revision*/ 0, &[thread_id])
            .await
            .unwrap()
    );
    assert_eq!(
        vec![queued],
        queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn queue_revisions_identify_changed_threads_after_updates_and_deletions() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"first":true}"#).await.unwrap();
    let first_revision = queue
        .changes_since(/*revision*/ 0, &[thread_id])
        .await
        .unwrap()[0]
        .1;
    queue
        .update(thread_id, &first.id, r#"{"updated":true}"#)
        .await
        .unwrap();
    let updated_revision = queue
        .changes_since(first_revision, &[thread_id])
        .await
        .unwrap()[0]
        .1;
    let other_thread_id = ThreadId::new();
    queue
        .enqueue(other_thread_id, r#"{"other":true}"#)
        .await
        .unwrap();
    let newly_loaded_changes = queue
        .changes_since(/*revision*/ 0, &[other_thread_id])
        .await
        .unwrap();
    assert_eq!(
        vec![(thread_id, updated_revision), newly_loaded_changes[0]],
        queue
            .changes_since(first_revision, &[thread_id, other_thread_id])
            .await
            .unwrap()
    );
    assert!(queue.delete(thread_id, &first.id).await.unwrap());
    assert!(
        queue
            .changes_since(updated_revision, &[thread_id])
            .await
            .unwrap()
            .iter()
            .any(|(changed_thread, _)| *changed_thread == thread_id)
    );
}

#[tokio::test]
async fn fifo_dispatch_preserves_edits_reordering_and_pagination() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
    let second = queue.enqueue(thread_id, r#"{"n":2}"#).await.unwrap();
    let third = queue.enqueue(thread_id, r#"{"n":3}"#).await.unwrap();

    let updated = queue
        .update(thread_id, &first.id, r#"{"n":"edited"}"#)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.id, updated.id);
    let error = queue
        .reorder(thread_id, std::slice::from_ref(&first.id))
        .await
        .unwrap_err();
    assert_eq!(
        std::io::ErrorKind::InvalidInput,
        error.downcast_ref::<std::io::Error>().unwrap().kind()
    );

    let ordered_ids = vec![third.id, first.id, second.id];
    queue.reorder(thread_id, &ordered_ids).await.unwrap();

    let items = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 3)
        .await
        .unwrap();
    let page = queue
        .list_page(thread_id, /*offset*/ 1, /*limit*/ 1)
        .await
        .unwrap();
    assert_eq!(vec![items[1].clone()], page);
    assert_eq!(r#"{"n":"edited"}"#, items[1].payload);

    for item in items {
        assert!(queue.delete(thread_id, &item.id).await.unwrap());
    }
    assert!(
        queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn queue_operations_cannot_mutate_another_threads_messages() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
    let other_thread_id = ThreadId::new();
    let other = queue.enqueue(other_thread_id, r#"{"n":2}"#).await.unwrap();
    let other_id = &other.id;

    assert_eq!(
        None,
        queue
            .update(thread_id, other_id, r#"{"n":3}"#)
            .await
            .unwrap()
    );
    assert!(!queue.delete(thread_id, other_id).await.unwrap());
    assert!(
        queue
            .reorder(thread_id, std::slice::from_ref(other_id))
            .await
            .is_err()
    );
    let (items, other_items) = tokio::join!(
        queue.list_page(thread_id, /*offset*/ 0, /*limit*/ 1),
        queue.list_page(other_thread_id, /*offset*/ 0, /*limit*/ 1),
    );
    assert_eq!(
        (vec![first], vec![other]),
        (items.unwrap(), other_items.unwrap())
    );
}

#[tokio::test]
async fn deleting_a_thread_removes_its_queue() {
    let (runtime, thread_id) = runtime_with_thread().await;
    runtime
        .thread_queue()
        .enqueue(thread_id, r#"{"n":1}"#)
        .await
        .unwrap();

    assert_eq!(1, runtime.delete_thread(thread_id).await.unwrap());
    assert!(
        runtime
            .thread_queue()
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn concurrent_inserts_enforce_the_queue_limit() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let other = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();

    for _ in 0..MAX_QUEUE_ITEMS - 1 {
        runtime
            .thread_queue()
            .enqueue(thread_id, r#"{"n":1}"#)
            .await
            .unwrap();
    }
    let (first, second) = tokio::join!(
        runtime.thread_queue().enqueue(thread_id, r#"{"n":2}"#),
        other.thread_queue().enqueue(thread_id, r#"{"n":3}"#),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(
        MAX_QUEUE_ITEMS,
        runtime
            .thread_queue()
            .list_page(thread_id, /*offset*/ 0, /*limit*/ MAX_QUEUE_ITEMS)
            .await
            .unwrap()
            .len()
    );
}
