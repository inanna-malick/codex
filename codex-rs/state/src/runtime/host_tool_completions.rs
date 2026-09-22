use super::*;
use crate::HostToolCompletionError;
use crate::HostToolCompletionKey;
use crate::HostToolCompletionRecord;
use crate::HostToolCompletionRegistration;
use crate::HostToolCompletionState;

const MAX_UNRESOLVED_HOST_TOOL_COMPLETIONS: i64 = 256;

impl SqliteQueueStore {
    /// Durably register an exact hosted-call boundary before its work is sent.
    /// Repeating the same key returns its retained state without demotion.
    pub async fn register_host_tool_completion(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        Ok(match self.register_host_tool_call(key).await? {
            HostToolCompletionRegistration::New(record)
            | HostToolCompletionRegistration::Existing(record) => record,
        })
    }

    /// Registers the exact call key and tells the dispatcher whether it owns
    /// the first admission for that key.
    pub async fn register_host_tool_call(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRegistration> {
        let mut transaction = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(HostToolCompletionError::storage)?;
        if let Some(record) =
            read_host_tool_completion_in_connection(transaction.as_mut(), key).await?
        {
            transaction
                .rollback()
                .await
                .map_err(HostToolCompletionError::storage)?;
            return Ok(HostToolCompletionRegistration::Existing(record));
        }
        let unresolved: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM host_tool_completions
             WHERE thread_id = ? AND state IN ('pending', 'reconcile_pending', 'ready')",
        )
        .bind(key.thread_id.to_string())
        .fetch_one(transaction.as_mut())
        .await
        .map_err(HostToolCompletionError::storage)?;
        if unresolved >= MAX_UNRESOLVED_HOST_TOOL_COMPLETIONS {
            transaction
                .rollback()
                .await
                .map_err(HostToolCompletionError::storage)?;
            return Err(HostToolCompletionError::Capacity.into());
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
        .await
        .map_err(HostToolCompletionError::storage)?;
        let record = read_host_tool_completion_in_connection(transaction.as_mut(), key)
            .await?
            .ok_or(HostToolCompletionError::Missing)?;
        transaction
            .commit()
            .await
            .map_err(HostToolCompletionError::storage)?;
        Ok(HostToolCompletionRegistration::New(record))
    }

    pub async fn read_host_tool_completion(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<Option<HostToolCompletionRecord>> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(HostToolCompletionError::storage)?;
        read_host_tool_completion_in_connection(&mut connection, key).await
    }

    pub async fn mark_host_tool_completion_reconcile(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        transition_host_tool_completion(
            self.pool.as_ref(),
            key,
            HostToolCompletionState::ReconcilePending,
        )
        .await
    }

    pub async fn mark_host_tool_completion_ready(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        transition_host_tool_completion(self.pool.as_ref(), key, HostToolCompletionState::Ready)
            .await
    }

    pub async fn acknowledge_host_tool_completion(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        transition_host_tool_completion(
            self.pool.as_ref(),
            key,
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
            HostToolCompletionState::ReattachedWithoutCompletion,
        )
        .await
    }

    pub async fn mark_host_tool_completion_not_submitted(
        &self,
        key: &HostToolCompletionKey,
    ) -> anyhow::Result<HostToolCompletionRecord> {
        transition_host_tool_completion(
            self.pool.as_ref(),
            key,
            HostToolCompletionState::NotSubmitted,
        )
        .await
    }

    pub async fn list_unresolved_host_tool_completions(
        &self,
        thread_id: ThreadId,
    ) -> anyhow::Result<Vec<HostToolCompletionRecord>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT context_call_id, state FROM host_tool_completions
             WHERE thread_id = ? AND state IN ('pending', 'reconcile_pending', 'ready')
             ORDER BY created_at_ms, context_call_id",
        )
        .bind(thread_id.to_string())
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(HostToolCompletionError::storage)?;
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
    requested: HostToolCompletionState,
) -> anyhow::Result<HostToolCompletionRecord> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(HostToolCompletionError::storage)?;
    let record = read_host_tool_completion_in_connection(transaction.as_mut(), key)
        .await?
        .ok_or(HostToolCompletionError::Missing)?;
    if transition_is_stale_or_duplicate(record.state, requested) {
        transaction
            .rollback()
            .await
            .map_err(HostToolCompletionError::storage)?;
        return Ok(record);
    }
    if !transition_is_allowed(record.state, requested) {
        transaction
            .rollback()
            .await
            .map_err(HostToolCompletionError::storage)?;
        return Err(HostToolCompletionError::Conflict {
            current: record.state,
            requested,
        }
        .into());
    }
    sqlx::query(
        "UPDATE host_tool_completions SET state = ?, updated_at_ms = ?
         WHERE thread_id = ? AND context_call_id = ? AND state = ?",
    )
    .bind(requested.as_str())
    .bind(datetime_to_epoch_millis(Utc::now()))
    .bind(key.thread_id.to_string())
    .bind(&key.context_call_id)
    .bind(record.state.as_str())
    .execute(transaction.as_mut())
    .await
    .map_err(HostToolCompletionError::storage)?;
    transaction
        .commit()
        .await
        .map_err(HostToolCompletionError::storage)?;
    Ok(HostToolCompletionRecord {
        key: key.clone(),
        state: requested,
    })
}

fn transition_is_allowed(
    current: HostToolCompletionState,
    requested: HostToolCompletionState,
) -> bool {
    matches!(
        (current, requested),
        (
            HostToolCompletionState::Pending,
            HostToolCompletionState::ReconcilePending
                | HostToolCompletionState::Ready
                | HostToolCompletionState::ReattachedWithoutCompletion
                | HostToolCompletionState::NotSubmitted
        ) | (
            HostToolCompletionState::ReconcilePending,
            HostToolCompletionState::Ready
                | HostToolCompletionState::ReattachedWithoutCompletion
                | HostToolCompletionState::NotSubmitted
        ) | (
            HostToolCompletionState::Ready,
            HostToolCompletionState::Acknowledged
        )
    )
}

fn transition_is_stale_or_duplicate(
    current: HostToolCompletionState,
    requested: HostToolCompletionState,
) -> bool {
    current == requested
        || matches!(
            (current, requested),
            (
                HostToolCompletionState::Ready | HostToolCompletionState::Acknowledged,
                HostToolCompletionState::ReconcilePending
                    | HostToolCompletionState::Ready
                    | HostToolCompletionState::ReattachedWithoutCompletion
                    | HostToolCompletionState::NotSubmitted
            ) | (
                HostToolCompletionState::ReattachedWithoutCompletion
                    | HostToolCompletionState::NotSubmitted,
                HostToolCompletionState::ReconcilePending
                    | HostToolCompletionState::Ready
                    | HostToolCompletionState::ReattachedWithoutCompletion
                    | HostToolCompletionState::NotSubmitted
            )
        )
}

async fn read_host_tool_completion_in_connection(
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
    .await
    .map_err(HostToolCompletionError::storage)?;
    row.map(|state| {
        Ok(HostToolCompletionRecord {
            key: key.clone(),
            state: HostToolCompletionState::from_str(&state)
                .map_err(HostToolCompletionError::storage)?,
        })
    })
    .transpose()
}

#[cfg(test)]
#[path = "host_tool_completions_tests.rs"]
mod tests;
