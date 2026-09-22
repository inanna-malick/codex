use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadInjectItemsParams;
use codex_app_server_protocol::ThreadInjectItemsResponse;
use codex_protocol::ThreadId;
use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::models::FunctionCallOutputContentItem;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;

use super::HostDynamicTools;
use super::completions::completion_is_closed;
use super::completions::store_error;
use super::recovery::Intervention;

impl HostDynamicTools {
    /// Restore the terminal receipt through native history ownership before
    /// acknowledging the boundary that permits deferred forks to start.
    pub(super) async fn restore_completion(
        &self,
        thread_id: &str,
        call_id: &str,
        reply: DynamicToolCallResponse,
    ) -> color_eyre::Result<()> {
        let thread = ThreadId::from_string(thread_id)?;
        let metadata = self
            .state_db
            .get_thread(thread)
            .await
            .map_err(store_error)?
            .ok_or_else(|| Intervention("recovery thread is not persisted".into()))?;
        let (items, actual_thread, errors) =
            RolloutRecorder::load_rollout_items(&metadata.rollout_path).await?;
        if actual_thread != Some(thread) || errors != 0 {
            return Err(Intervention(
                "recovery history identity or integrity is unconfirmed".into(),
            )
            .into());
        }
        if completion_is_closed(&items, call_id)? {
            return Ok(());
        }
        let custom = items
            .iter()
            .find_map(|item| match item {
                RolloutItem::ResponseItem(item) => match &item.item {
                    ResponseItem::CustomToolCall { call_id: id, .. } if id == call_id => Some(true),
                    ResponseItem::FunctionCall { call_id: id, .. } if id == call_id => Some(false),
                    _ => None,
                },
                _ => None,
            })
            .ok_or_else(|| {
                Intervention(format!(
                    "cannot recover call {call_id}: original invocation is absent"
                ))
            })?;
        let output = FunctionCallOutputPayload {
            body: FunctionCallOutputBody::ContentItems(
                reply
                    .content_items
                    .into_iter()
                    .map(|item| {
                        FunctionCallOutputContentItem::from(
                            codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem::from(
                                item,
                            ),
                        )
                    })
                    .collect(),
            ),
            success: Some(reply.success),
        };
        let item = ResponseItem::from(if custom {
            ResponseInputItem::CustomToolCallOutput {
                name: None,
                call_id: call_id.to_owned(),
                output,
            }
        } else {
            ResponseInputItem::FunctionCallOutput {
                call_id: call_id.to_owned(),
                output,
            }
        });
        let handle = self.recovery.handle.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
            .ok_or_else(|| color_eyre::eyre::eyre!("native history transport is not attached; retained completion awaits reconnection"))?;
        let _: ThreadInjectItemsResponse = tokio::time::timeout(
            super::SETTLEMENT_REQUEST_TIMEOUT,
            handle.request_typed(ClientRequest::ThreadInjectItems {
                request_id: RequestId::String(format!("host-recovery-{}", uuid::Uuid::new_v4())),
                params: ThreadInjectItemsParams {
                    thread_id: thread_id.to_owned(),
                    items: vec![serde_json::to_value(item)?],
                    terminal_call_id: Some(call_id.to_owned()),
                },
            }),
        )
        .await
        .map_err(|_| {
            color_eyre::eyre::eyre!(
                "native history recovery acknowledgment timed out; exact receipt remains retained"
            )
        })??;
        let (items, actual_thread, errors) =
            RolloutRecorder::load_rollout_items(&metadata.rollout_path).await?;
        if actual_thread != Some(thread) || errors != 0 {
            return Err(Intervention(
                "persisted recovery history failed identity/integrity verification".into(),
            )
            .into());
        }
        if !completion_is_closed(&items, call_id)? {
            color_eyre::eyre::bail!(
                "recovered call {call_id} awaits the remaining tool outputs at its native context boundary"
            );
        }
        Ok(())
    }
}
