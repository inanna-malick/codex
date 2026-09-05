//! Resolve fork defaults from the parent owner, not the app-server's launch settings.

use codex_app_server_protocol::ThreadForkParams;
use codex_core::ThreadManager;
use codex_thread_store::StoredThread;

pub(super) async fn inherit(
    params: &mut ThreadForkParams,
    source: &StoredThread,
    threads: &ThreadManager,
) {
    let (model, provider, effort) = if let Ok(parent) = threads.get_thread(source.thread_id).await {
        let settings = parent.thread_settings_snapshot().await;
        (
            Some(settings.model),
            settings.model_provider_id,
            settings.reasoning_effort,
        )
    } else {
        (
            source.model.clone(),
            source.model_provider.clone(),
            source.reasoning_effort.clone(),
        )
    };
    let overrides = params.config.get_or_insert_default();
    let changes_model = params.model.is_some()
        || overrides.contains_key("model")
        || params.model_provider.is_some()
        || overrides.contains_key("model_provider");
    if params.model.is_none() && !overrides.contains_key("model") {
        params.model = model;
    }
    if params.model_provider.is_none() && !overrides.contains_key("model_provider") {
        params.model_provider = Some(provider);
    }
    // A model switch may have a different effort vocabulary/default. Effort-only
    // forks accept the same model-defined strings as ordinary configuration.
    if !changes_model && let Some(effort) = effort {
        overrides
            .entry("model_reasoning_effort".to_string())
            .or_insert_with(|| serde_json::json!(effort));
    }
}
