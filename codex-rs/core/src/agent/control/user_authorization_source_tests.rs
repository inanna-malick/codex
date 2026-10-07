//! Exact original-envelope replay must not consume the root evidence cap twice.
use super::*;
use crate::StartThreadOptions;
use crate::ThreadManager;
use crate::config::test_config;
use crate::context_manager::ContextManager;
use codex_history::CodexHarnessMetadata;
use codex_history::ResponseItemEnvelope;
use codex_history::RolloutItem;
use codex_login::CodexAuth;
use codex_protocol::ResponseItemId;
use codex_protocol::models::InternalChatMessageMetadataPassthrough;
use codex_protocol::protocol::SessionSource;
use codex_utils_output_truncation::TruncationPolicy;
use std::sync::Arc;

fn evidence(role: &str, index: u64) -> ResponseItemEnvelope {
    let text = format!("{role} source {index}: keep this distinct evidence.");
    ResponseItemEnvelope {
        item: ResponseItem::Message {
            id: Some(ResponseItemId::from_server(format!("{role}-{index}"))),
            role: role.to_owned(),
            content: vec![if role == "user" {
                ContentItem::InputText { text }
            } else {
                ContentItem::OutputText { text }
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: Some(
                InternalChatMessageMetadataPassthrough {
                    turn_id: Some(format!("turn-{index}")),
                    content_item_kinds: (role == "user").then(|| {
                        vec![codex_protocol::models::ContentItemKind(
                            "user.text".to_owned(),
                        )]
                    }),
                    ..Default::default()
                },
            ),
        },
        metadata: Some(CodexHarnessMetadata {
            user_input_order: Some(index),
            ..Default::default()
        }),
    }
}

async fn resumed_snapshot(user_count: u64, assistant_copies: usize) -> GuardianRootSnapshot {
    let mut config = test_config().await;
    let _ = config
        .features
        .enable(codex_features::Feature::MultiAgentV2);
    let home = tempfile::tempdir().unwrap();
    config.codex_home = home.path().to_path_buf().try_into().unwrap();
    config.cwd = home.path().to_path_buf().try_into().unwrap();
    let features = crate::config::ManagedFeatures::from(config.features.clone());
    let mut history = ContextManager::for_session(&SessionSource::Cli, &features);
    let mut originals = (0..user_count)
        .map(|index| evidence("user", index))
        .collect::<Vec<_>>();
    originals.push(evidence("assistant", user_count));
    history.record_annotated_items(&mut originals, TruncationPolicy::Tokens(10_000));
    assert!(originals.iter().all(|item| {
        item.metadata
            .as_ref()
            .unwrap()
            .retained_source
            .as_ref()
            .is_some_and(|source| source.complete)
    }));
    let assistant = originals.last().unwrap().clone();
    originals.extend(std::iter::repeat_n(assistant, assistant_copies - 1));
    let state_db = crate::init_state_db(&config).await;
    assert!(state_db.is_some(), "durable thread access requires SQLite");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        state_db,
    );
    let stored = manager
        .start_thread(StartThreadOptions {
            history_mode: Some(codex_protocol::protocol::ThreadHistoryMode::Legacy),
            ..StartThreadOptions::new(config.clone())
        })
        .await
        .expect("create stored root");
    let root_id = stored.thread_id;
    let persisted = originals
        .into_iter()
        .map(RolloutItem::ResponseItem)
        .collect::<Vec<_>>();
    stored
        .thread
        .append_rollout_items(&persisted)
        .await
        .expect("persist originals");
    stored.thread.ensure_rollout_materialized().await;
    stored
        .thread
        .flush_rollout()
        .await
        .expect("flush durable originals");
    let rollout_path = stored.thread.rollout_path().expect("stored root rollout");
    stored
        .thread
        .shutdown_and_wait()
        .await
        .expect("close stored root");
    manager.remove_thread(&root_id).await;
    let root = manager
        .resume_legacy_thread_from_rollout(
            config.clone(),
            rollout_path,
            manager.auth_manager(),
            None,
            codex_protocol::mcp::ClientMcpExtensions::default(),
        )
        .await
        .expect("resume durable originals");
    let retained = root.thread.session.clone_history().await;
    assert_eq!(
        retained.retained_context().ordered_entries().count(),
        user_count as usize + 1
    );
    assert_eq!(
        root.thread.multi_agent_version(),
        Some(MultiAgentVersion::V2)
    );
    let control = root
        .thread
        .session
        .services
        .local_agent_runtime
        .control(root.thread.session.services.agent_control.identity());
    let state = control.runtime.upgrade().unwrap();
    let worker = state
        .spawn_new_thread_with_source(
            config,
            control,
            SessionSource::SubAgent(codex_protocol::protocol::SubAgentSource::Other(
                "source-worker".to_owned(),
            )),
            None,
            Vec::new(),
            Some(root_id),
            None,
            Some(codex_protocol::protocol::ThreadSource::Subagent),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("create worker in the resumed tree");
    let snapshot = worker
        .thread
        .guardian_root_snapshot()
        .await
        .expect("root projection available to another agent");
    manager
        .shutdown_all_threads_bounded(std::time::Duration::from_secs(5))
        .await;
    snapshot
}

// Session startup futures need more stack than the default libtest thread. Keep
// the resource requirement local, including when run without RUST_MIN_STACK.
fn snapshot_on_test_stack(user_count: u64, assistant_copies: usize) -> GuardianRootSnapshot {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(resumed_snapshot(user_count, assistant_copies))
        })
        .unwrap()
        .join()
        .unwrap()
}

#[test]
fn replayed_assistant_source_does_not_displace_distinct_root_instructions() {
    let snapshot = snapshot_on_test_stack(8, 11);
    let users = snapshot
        .messages
        .iter()
        .filter_map(|message| match message {
            GuardianRootMessage::User(text) => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        users,
        (0..8)
            .map(|index| format!("user source {index}: keep this distinct evidence."))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        snapshot
            .messages
            .iter()
            .filter(|message| matches!(message, GuardianRootMessage::Assistant(_)))
            .count(),
        1
    );
    assert!(
        !snapshot
            .messages
            .contains(&GuardianRootMessage::IncompleteRootInstructions)
    );
    assert!(
        !snapshot
            .messages
            .contains(&GuardianRootMessage::IncompleteAssistantContext)
    );
}
