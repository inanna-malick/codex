use super::*;
use crate::HostToolCompletionKey;
use crate::HostToolCompletionRecord;
use crate::HostToolCompletionState;

const MAX_UNRESOLVED_HOST_TOOL_COMPLETIONS: i64 = 256;

impl SqliteQueueStore {
    /// Durably register an exact hosted-call boundary before its work is sent.
    /// Repeating the same key returns its retained state without demotion.
    pub async fn register_host_tool_completion(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(record) = read_host_tool_completion(transaction.as_mut(), key).await? {
            transaction.rollback().await?;
            return Ok(record);
        }
        let unresolved: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM host_tool_completions
             WHERE thread_id = ? AND state IN ('pending', 'ready')",
        )
        .bind(key.thread_id.to_string())
        .fetch_one(transaction.as_mut())
        .await?;
        if unresolved >= MAX_UNRESOLVED_HOST_TOOL_COMPLETIONS {
            transaction.rollback().await?;
            anyhow::bail!("too many unresolved hosted tool completions");
        }
        let now_ms = datetime_to_epoch_millis(Utc::now());
        sqlx::query(
            "INSERT INTO host_tool_completions (
                thread_id, context_call_id, state, created_at_ms, updated_at_ms
             ) VALUES (?, ?, 'pending', ?, ?)",
        )
        .bind(key.thread_id.to_string())
        .bind(&key.context_call_id)
        .bind(now_ms)
        .bind(now_ms)
        .execute(transaction.as_mut())
        .await?;
        let record = read_host_tool_completion(transaction.as_mut(), key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("registered host tool completion disappeared"))?;
        transaction.commit().await?;
        Ok(record)
    }

    pub async fn mark_host_tool_completion_ready(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        transition_host_tool_completion(
            self.pool.as_ref(),
            key,
            HostToolCompletionState::Pending,
            HostToolCompletionState::Ready,
        )
        .await
    }

    pub async fn acknowledge_host_tool_completion(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        transition_host_tool_completion(
            self.pool.as_ref(),
            key,
            HostToolCompletionState::Ready,
            HostToolCompletionState::Acknowledged,
        )
        .await
    }

    pub async fn mark_host_tool_completion_reattached(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        transition_host_tool_completion(
            self.pool.as_ref(),
            key,
            HostToolCompletionState::Pending,
            HostToolCompletionState::ReattachedWithoutCompletion,
        )
        .await
    }

    pub async fn list_unresolved_host_tool_completions(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Vec<HostToolCompletionRecord>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT context_call_id, state FROM host_tool_completions
             WHERE thread_id = ? AND state IN ('pending', 'ready')
             ORDER BY created_at_ms, context_call_id",
        )
        .bind(thread_id.to_string())
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.into_iter()
            .map(|(context_call_id, state)| {
                Ok(HostToolCompletionRecord {
                    key: HostToolCompletionKey {
                        thread_id,
                        context_call_id,
                    },
                    state: HostToolCompletionState::from_str(&state)?,
                })
            })
            .collect()
    }
}

async fn transition_host_tool_completion(
    pool: &SqlitePool,
    key: &HostToolCompletionKey,
    from: HostToolCompletionState,
    to: HostToolCompletionState,
) -> anyhow::Result<HostToolCompletionRecord> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let record = read_host_tool_completion(transaction.as_mut(), key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("host tool completion is not registered"))?;
    if record.state == to {
        transaction.rollback().await?;
        return Ok(record);
    }
    if record.state != from {
        transaction.rollback().await?;
        anyhow::bail!(
            "host tool completion cannot transition from {} to {}",
            record.state.as_str(),
            to.as_str()
        );
    }
    sqlx::query(
        "UPDATE host_tool_completions SET state = ?, updated_at_ms = ?
         WHERE thread_id = ? AND context_call_id = ? AND state = ?",
    )
    .bind(to.as_str())
    .bind(datetime_to_epoch_millis(Utc::now()))
    .bind(key.thread_id.to_string())
    .bind(&key.context_call_id)
    .bind(from.as_str())
    .execute(transaction.as_mut())
    .await?;
    transaction.commit().await?;
    Ok(HostToolCompletionRecord {
        key: key.clone(),
        state: to,
    })
}

async fn read_host_tool_completion(
    connection: &mut SqliteConnection,
    key: &HostToolCompletionKey,
) -> anyhow::Result<Option<HostToolCompletionRecord>> {
    let row: Option<String> = sqlx::query_scalar(
        "SELECT state FROM host_tool_completions
         WHERE thread_id = ? AND context_call_id = ?",
    )
    .bind(key.thread_id.to_string())
    .bind(&key.context_call_id)
    .fetch_optional(connection)
    .await?;
    row.map(|state| {
        Ok(HostToolCompletionRecord {
            key: key.clone(),
            state: HostToolCompletionState::from_str(&state)?,
        })
    })
    .transpose()
}

#[cfg(test)]
#[path = "host_tool_completions_tests.rs"]
mod tests;
