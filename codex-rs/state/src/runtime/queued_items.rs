use super::*;
use crate::HostInputAdmission;
use crate::HostInputOperation;
use crate::HostInputRecord;
use crate::HostInputState;
use crate::HostInputWithdrawal;
use crate::MAX_QUEUE_ITEMS;
use crate::QueuedUserSubmissionRecord;
use sqlx::Connection;
use tokio::sync::Mutex;
use uuid::Uuid;

/// SQLite-backed persistence for durable, thread-scoped user messages.
#[derive(Clone)]
pub struct SqliteQueueStore {
    pub(super) pool: Arc<SqlitePool>,
    change_version_connection: Arc<Mutex<Option<SqliteConnection>>>,
}

impl SqliteQueueStore {
    pub(crate) fn new(pool: Arc<SqlitePool>) -> Self {
        Self {
            pool,
            change_version_connection: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn close(&self) {
        let connection = self.change_version_connection.lock().await.take();
        if let Some(connection) = connection
            && let Err(error) = connection.close().await
        {
            tracing::warn!(%error, "failed to close queue change-version connection");
        }
        self.pool.close().await;
    }

    /// Observe queue-database commits through one stable SQLite connection.
    pub async fn change_version(&self) -> anyhow::Result<i64> {
        let mut connection = Arc::clone(&self.change_version_connection)
            .lock_owned()
            .await;
        if connection.is_none() {
            *connection = Some(self.pool.acquire().await?.detach());
        }
        let Some(connection) = connection.as_mut() else {
            unreachable!("queue change-version connection was initialized");
        };
        Ok(sqlx::query_scalar("PRAGMA data_version")
            .fetch_one(connection)
            .await?)
    }

    /// Return changed revisions only for the supplied loaded thread IDs.
    pub async fn changes_since(
        &self,
        revision: i64,
        thread_ids: &[ThreadId],
    ) -> anyhow::Result<Vec<(ThreadId, i64)>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT thread_id, revision FROM queued_thread_revisions WHERE revision > ",
        );
        query.push_bind(revision).push(" AND thread_id IN (");
        let mut separated = query.separated(", ");
        for thread_id in thread_ids {
            separated.push_bind(thread_id.to_string());
        }
        separated.push_unseparated(") ORDER BY revision");
        let rows = query
            .build_query_as::<(String, i64)>()
            .fetch_all(self.pool.as_ref())
            .await?;
        rows.into_iter()
            .map(|(thread_id, revision)| Ok((ThreadId::try_from(thread_id)?, revision)))
            .collect()
    }

    pub async fn enqueue(
        &self,
        thread_id: ThreadId,
        payload_json: &str,
    ) -> anyhow::Result<QueuedUserSubmissionRecord> {
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let row = sqlx::query(
            "INSERT INTO queued_items (
                id, thread_id, payload_json, queue_order,
                created_at_ms, updated_at_ms
             )
             SELECT ?, ?, ?,
                    COALESCE((SELECT MAX(queue_order) FROM queued_items WHERE thread_id = ?), -1) + 1,
                    ?, ?
             WHERE (SELECT COUNT(*) FROM queued_items WHERE thread_id = ?) < ?
             RETURNING id, thread_id, payload_json",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(thread_id.to_string())
        .bind(payload_json)
        .bind(thread_id.to_string())
        .bind(now_ms)
        .bind(now_ms)
        .bind(thread_id.to_string())
        .bind(i64::try_from(MAX_QUEUE_ITEMS)?)
        .fetch_one(self.pool.as_ref())
        .await?;
        QueuedUserSubmissionRecord::try_from_row(&row)
    }

    pub async fn list_page(
        &self,
        thread_id: ThreadId,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<QueuedUserSubmissionRecord>> {
        let rows = sqlx::query(
            "SELECT id, thread_id, payload_json
             FROM queued_items
             WHERE thread_id = ?
             ORDER BY queue_order LIMIT ? OFFSET ?",
        )
        .bind(thread_id.to_string())
        .bind(i64::try_from(limit)?)
        .bind(i64::try_from(offset)?)
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.iter()
            .map(QueuedUserSubmissionRecord::try_from_row)
            .collect()
    }

    pub async fn update(
        &self,
        thread_id: ThreadId,
        item_id: &str,
        payload_json: &str,
    ) -> anyhow::Result<Option<QueuedUserSubmissionRecord>> {
        let row = sqlx::query(
            "UPDATE queued_items
             SET payload_json = ?, updated_at_ms = ?
             WHERE thread_id = ? AND id = ?
             RETURNING id, thread_id, payload_json",
        )
        .bind(payload_json)
        .bind(datetime_to_epoch_millis(Utc::now()))
        .bind(thread_id.to_string())
        .bind(item_id)
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.as_ref()
            .map(QueuedUserSubmissionRecord::try_from_row)
            .transpose()
    }

    pub async fn delete(&self, thread_id: ThreadId, item_id: &str) -> anyhow::Result<bool> {
        Ok(
            sqlx::query("DELETE FROM queued_items WHERE thread_id = ? AND id = ?")
                .bind(thread_id.to_string())
                .bind(item_id)
                .execute(self.pool.as_ref())
                .await?
                .rows_affected()
                > 0,
        )
    }

    pub async fn reorder(&self, thread_id: ThreadId, ordered_ids: &[String]) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT id, queue_order FROM queued_items
             WHERE thread_id = ? ORDER BY queue_order",
        )
        .bind(thread_id.to_string())
        .fetch_all(transaction.as_mut())
        .await?;
        let mut expected_ids = rows.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
        let mut requested_ids = ordered_ids.to_vec();
        expected_ids.sort();
        requested_ids.sort();
        if expected_ids != requested_ids {
            transaction.rollback().await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "queue reorder must include every queued submission exactly once",
            )
            .into());
        }
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let max_queue_order = rows.last().map_or(-1, |(_, queue_order)| *queue_order);
        for (index, item_id) in ordered_ids.iter().enumerate() {
            sqlx::query(
                "UPDATE queued_items SET queue_order = ?, updated_at_ms = ?
                 WHERE thread_id = ? AND id = ?",
            )
            .bind(max_queue_order + i64::try_from(index)? + 1)
            .bind(now_ms)
            .bind(thread_id.to_string())
            .bind(item_id)
            .execute(transaction.as_mut())
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically retain a host operation and its queue row. Repeating an exact
    /// operation returns its retained state; changing frozen content conflicts.
    pub async fn admit_host_input(
        &self,
        operation: &HostInputOperation,
    ) -> anyhow::Result<HostInputAdmission> {
        let mut transaction = self.pool.begin().await?;
        if let Some(record) = read_host_input(
            transaction.as_mut(),
            &operation.producer_id,
            operation.sequence,
        )
        .await?
        {
            transaction.rollback().await?;
            return Ok(if record.operation == *operation {
                HostInputAdmission::Existing(record)
            } else {
                HostInputAdmission::Conflict
            });
        }
        let watermark: Option<i64> = sqlx::query_scalar(
            "SELECT through_sequence FROM host_input_producer_watermarks
             WHERE producer_id = ?",
        )
        .bind(&operation.producer_id)
        .fetch_optional(transaction.as_mut())
        .await?;
        if watermark.is_some_and(|watermark| operation.sequence <= watermark as u64) {
            transaction.rollback().await?;
            return Ok(HostInputAdmission::Compacted);
        }
        let withdrawn: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM host_input_withdrawal_tombstones
             WHERE producer_id = ? AND sequence = ?)",
        )
        .bind(&operation.producer_id)
        .bind(i64::try_from(operation.sequence)?)
        .fetch_one(transaction.as_mut())
        .await?;
        if withdrawn {
            transaction.rollback().await?;
            return Ok(HostInputAdmission::Withdrawn);
        }
        let sealed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM host_input_producer_seals WHERE producer_id = ?)",
        )
        .bind(&operation.producer_id)
        .fetch_one(transaction.as_mut())
        .await?;
        if sealed {
            transaction.rollback().await?;
            return Ok(HostInputAdmission::ProducerSealed);
        }
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM queued_items WHERE thread_id = ?")
                .bind(operation.thread_id.to_string())
                .fetch_one(transaction.as_mut())
                .await?;
        if count >= i64::try_from(MAX_QUEUE_ITEMS)? {
            transaction.rollback().await?;
            return Ok(HostInputAdmission::AtCapacity);
        }
        let item_id = format!("host:{}:{}", operation.producer_id, operation.sequence);
        let now_ms = datetime_to_epoch_millis(Utc::now());
        sqlx::query(
            "INSERT INTO queued_items (
                id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?,
                COALESCE((SELECT MAX(queue_order) FROM queued_items WHERE thread_id = ?), -1) + 1,
                ?, ?)",
        )
        .bind(&item_id)
        .bind(operation.thread_id.to_string())
        .bind(&operation.payload)
        .bind(operation.thread_id.to_string())
        .bind(now_ms)
        .bind(now_ms)
        .execute(transaction.as_mut())
        .await?;
        sqlx::query(
            "INSERT INTO host_input_operations (
                thread_id, producer_id, sequence, purpose, input_mode, target_json,
                content_digest, payload_json,
                state, queue_item_id, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?)",
        )
        .bind(operation.thread_id.to_string())
        .bind(&operation.producer_id)
        .bind(i64::try_from(operation.sequence)?)
        .bind(&operation.purpose)
        .bind(&operation.mode)
        .bind(&operation.target_json)
        .bind(&operation.content_digest)
        .bind(&operation.payload)
        .bind(item_id)
        .bind(now_ms)
        .bind(now_ms)
        .execute(transaction.as_mut())
        .await?;
        transaction.commit().await?;
        Ok(HostInputAdmission::Admitted(HostInputRecord {
            operation: operation.clone(),
            state: HostInputState::Ready,
        }))
    }

    pub async fn observe_host_input(
        &self,
        producer_id: &str,
        sequence: u64,
    ) -> anyhow::Result<Option<HostInputRecord>> {
        let mut connection = self.pool.acquire().await?;
        read_host_input(connection.as_mut(), producer_id, sequence).await
    }

    /// Claim a host-owned queue row before entering the native engine. The
    /// retained `Dispatching` record survives deletion of the queue row.
    pub async fn claim_host_queue_item(
        &self,
        thread_id: ThreadId,
        queue_item_id: &str,
    ) -> anyhow::Result<Option<HostInputRecord>> {
        let mut transaction = self.pool.begin().await?;
        let key: Option<(String, i64)> = sqlx::query_as(
            "SELECT producer_id, sequence FROM host_input_operations
             WHERE thread_id = ? AND queue_item_id = ? AND state = 'ready'",
        )
        .bind(thread_id.to_string())
        .bind(queue_item_id)
        .fetch_optional(transaction.as_mut())
        .await?;
        let Some((producer_id, sequence)) = key else {
            transaction.rollback().await?;
            return Ok(None);
        };
        set_host_input_state(
            &mut transaction,
            &producer_id,
            u64::try_from(sequence)?,
            HostInputState::Dispatching,
        )
        .await?;
        let record = read_host_input(transaction.as_mut(), &producer_id, u64::try_from(sequence)?)
            .await?
            .ok_or_else(|| anyhow::anyhow!("claimed host input record disappeared"))?;
        transaction.commit().await?;
        Ok(Some(record))
    }

    pub async fn mark_host_input_unknown(
        &self,
        producer_id: &str,
        sequence: u64,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        set_host_input_state(
            &mut transaction,
            producer_id,
            sequence,
            HostInputState::Unknown,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Record confirmed native acceptance and consume the exact queue row in
    /// one commit. A stale or mismatched claim leaves both records unchanged.
    pub async fn confirm_host_input_presented(
        &self,
        thread_id: ThreadId,
        queue_item_id: &str,
        producer_id: &str,
        sequence: u64,
    ) -> anyhow::Result<HostInputRecord> {
        let mut transaction = self.pool.begin().await?;
        let now_ms = datetime_to_epoch_millis(Utc::now());
        let updated = sqlx::query(
            "UPDATE host_input_operations SET state = 'presented', updated_at_ms = ?
             WHERE thread_id = ? AND queue_item_id = ? AND producer_id = ? AND sequence = ?
               AND state = 'dispatching'",
        )
        .bind(now_ms)
        .bind(thread_id.to_string())
        .bind(queue_item_id)
        .bind(producer_id)
        .bind(i64::try_from(sequence)?)
        .execute(transaction.as_mut())
        .await?;
        if updated.rows_affected() != 1 {
            transaction.rollback().await?;
            anyhow::bail!("host input presentation does not match the active dispatch claim");
        }
        let deleted = sqlx::query("DELETE FROM queued_items WHERE thread_id = ? AND id = ?")
            .bind(thread_id.to_string())
            .bind(queue_item_id)
            .execute(transaction.as_mut())
            .await?;
        if deleted.rows_affected() != 1 {
            transaction.rollback().await?;
            anyhow::bail!("claimed host input queue row disappeared before presentation");
        }
        let record = read_host_input(transaction.as_mut(), producer_id, sequence)
            .await?
            .ok_or_else(|| anyhow::anyhow!("presented host input record disappeared"))?;
        transaction.commit().await?;
        Ok(record)
    }

    /// Compact a contiguous prefix after the host has durably observed every
    /// terminal outcome. Missing or nonterminal rows make the acknowledgement
    /// fail without changing the retained evidence.
    pub async fn acknowledge_host_input(
        &self,
        thread_id: ThreadId,
        producer_id: &str,
        through_sequence: u64,
    ) -> anyhow::Result<Option<HostInputRecord>> {
        let mut transaction = self.pool.begin().await?;
        let previous: Option<i64> = sqlx::query_scalar(
            "SELECT through_sequence FROM host_input_producer_watermarks
             WHERE producer_id = ?",
        )
        .bind(producer_id)
        .fetch_optional(transaction.as_mut())
        .await?;
        let first = previous.map_or(1, |value| value.saturating_add(1));
        if i64::try_from(through_sequence)? < first {
            transaction.rollback().await?;
            return Ok(None);
        }
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT sequence, state FROM (
                SELECT sequence, state FROM host_input_operations
                 WHERE thread_id = ? AND producer_id = ? AND sequence BETWEEN ? AND ?
                UNION ALL
                SELECT sequence, 'withdrawn' FROM host_input_withdrawal_tombstones
                 WHERE thread_id = ? AND producer_id = ? AND sequence BETWEEN ? AND ?
             ) ORDER BY sequence",
        )
        .bind(thread_id.to_string())
        .bind(producer_id)
        .bind(first)
        .bind(i64::try_from(through_sequence)?)
        .bind(thread_id.to_string())
        .bind(producer_id)
        .bind(first)
        .bind(i64::try_from(through_sequence)?)
        .fetch_all(transaction.as_mut())
        .await?;
        let expected = usize::try_from(i64::try_from(through_sequence)? - first + 1)?;
        if rows.len() != expected
            || rows.iter().enumerate().any(|(offset, (sequence, state))| {
                *sequence != first + offset as i64 || state == "ready"
            })
        {
            transaction.rollback().await?;
            anyhow::bail!("host input acknowledgement is not a contiguous terminal prefix");
        }
        sqlx::query(
            "UPDATE host_input_operations SET state = 'presented', updated_at_ms = ?
             WHERE thread_id = ? AND producer_id = ? AND sequence BETWEEN ? AND ?
               AND state IN ('dispatching', 'unknown')",
        )
        .bind(datetime_to_epoch_millis(Utc::now()))
        .bind(thread_id.to_string())
        .bind(producer_id)
        .bind(first)
        .bind(i64::try_from(through_sequence)?)
        .execute(transaction.as_mut())
        .await?;
        let record = read_host_input(transaction.as_mut(), producer_id, through_sequence).await?;
        sqlx::query(
            "DELETE FROM host_input_withdrawal_tombstones
             WHERE thread_id = ? AND producer_id = ? AND sequence <= ?",
        )
        .bind(thread_id.to_string())
        .bind(producer_id)
        .bind(i64::try_from(through_sequence)?)
        .execute(transaction.as_mut())
        .await?;
        sqlx::query(
            "DELETE FROM queued_items WHERE id IN (
                SELECT queue_item_id FROM host_input_operations
                WHERE thread_id = ? AND producer_id = ? AND sequence <= ?)",
        )
        .bind(thread_id.to_string())
        .bind(producer_id)
        .bind(i64::try_from(through_sequence)?)
        .execute(transaction.as_mut())
        .await?;
        let now_ms = datetime_to_epoch_millis(Utc::now());
        sqlx::query(
            "DELETE FROM host_input_operations
             WHERE thread_id = ? AND producer_id = ? AND sequence <= ?",
        )
        .bind(thread_id.to_string())
        .bind(producer_id)
        .bind(i64::try_from(through_sequence)?)
        .execute(transaction.as_mut())
        .await?;
        sqlx::query(
            "INSERT INTO host_input_producer_watermarks
                (thread_id, producer_id, through_sequence, acknowledged_at_ms)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(producer_id) DO UPDATE SET
                through_sequence = excluded.through_sequence,
                acknowledged_at_ms = excluded.acknowledged_at_ms",
        )
        .bind(thread_id.to_string())
        .bind(producer_id)
        .bind(i64::try_from(through_sequence)?)
        .bind(now_ms)
        .execute(transaction.as_mut())
        .await?;
        transaction.commit().await?;
        Ok(record)
    }

    /// Fence a ready operation against dispatch. Dispatching remains unknown;
    /// absence from `queued_items` is not negative evidence.
    pub async fn withdraw_host_input(
        &self,
        thread_id: ThreadId,
        producer_id: &str,
        sequence: u64,
    ) -> anyhow::Result<Option<HostInputWithdrawal>> {
        let mut transaction = self.pool.begin().await?;
        let Some(mut record) = read_host_input(transaction.as_mut(), producer_id, sequence).await?
        else {
            sqlx::query(
                "INSERT INTO host_input_withdrawal_tombstones
                    (thread_id, producer_id, sequence, withdrawn_at_ms)
                 VALUES (?, ?, ?, ?) ON CONFLICT(producer_id, sequence) DO NOTHING",
            )
            .bind(thread_id.to_string())
            .bind(producer_id)
            .bind(i64::try_from(sequence)?)
            .bind(datetime_to_epoch_millis(Utc::now()))
            .execute(transaction.as_mut())
            .await?;
            transaction.commit().await?;
            return Ok(Some(HostInputWithdrawal::Tombstoned));
        };
        let outcome = match record.state {
            HostInputState::Ready => {
                sqlx::query(
                    "DELETE FROM queued_items WHERE id = (
                        SELECT queue_item_id FROM host_input_operations
                        WHERE producer_id = ? AND sequence = ?)",
                )
                .bind(producer_id)
                .bind(i64::try_from(sequence)?)
                .execute(transaction.as_mut())
                .await?;
                set_host_input_state(
                    &mut transaction,
                    producer_id,
                    sequence,
                    HostInputState::Withdrawn,
                )
                .await?;
                record.state = HostInputState::Withdrawn;
                HostInputWithdrawal::Withdrawn(record)
            }
            HostInputState::Dispatching | HostInputState::Unknown => {
                if record.state == HostInputState::Dispatching {
                    set_host_input_state(
                        &mut transaction,
                        producer_id,
                        sequence,
                        HostInputState::Unknown,
                    )
                    .await?;
                    record.state = HostInputState::Unknown;
                }
                HostInputWithdrawal::Unknown(record)
            }
            HostInputState::Presented | HostInputState::Withdrawn | HostInputState::Rejected => {
                HostInputWithdrawal::Existing(record)
            }
        };
        transaction.commit().await?;
        Ok(Some(outcome))
    }

    /// Seal one producer and quarantine all of its undispatched operations.
    pub async fn seal_host_input_producer(
        &self,
        thread_id: ThreadId,
        producer_id: &str,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        let now_ms = datetime_to_epoch_millis(Utc::now());
        sqlx::query(
            "INSERT INTO host_input_producer_seals (thread_id, producer_id, sealed_at_ms)
             VALUES (?, ?, ?) ON CONFLICT(producer_id) DO NOTHING",
        )
        .bind(thread_id.to_string())
        .bind(producer_id)
        .bind(now_ms)
        .execute(transaction.as_mut())
        .await?;
        sqlx::query(
            "DELETE FROM queued_items WHERE id IN (
                SELECT queue_item_id FROM host_input_operations
                WHERE producer_id = ? AND state = 'ready')",
        )
        .bind(producer_id)
        .execute(transaction.as_mut())
        .await?;
        sqlx::query(
            "UPDATE host_input_operations SET state = 'rejected', updated_at_ms = ?
             WHERE producer_id = ? AND state = 'ready'",
        )
        .bind(now_ms)
        .bind(producer_id)
        .execute(transaction.as_mut())
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn delete_thread_queue(&self, thread_id: ThreadId) -> anyhow::Result<bool> {
        Ok(sqlx::query("DELETE FROM queued_items WHERE thread_id = ?")
            .bind(thread_id.to_string())
            .execute(self.pool.as_ref())
            .await?
            .rows_affected()
            > 0)
    }
}

async fn read_host_input<'e, E>(
    executor: E,
    producer_id: &str,
    sequence: u64,
) -> anyhow::Result<Option<HostInputRecord>>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let row = sqlx::query(
        "SELECT thread_id, producer_id, sequence, purpose, input_mode, target_json,
                content_digest, payload_json, state
         FROM host_input_operations WHERE producer_id = ? AND sequence = ?",
    )
    .bind(producer_id)
    .bind(i64::try_from(sequence)?)
    .fetch_optional(executor)
    .await?;
    row.map(|row| {
        Ok(HostInputRecord {
            operation: HostInputOperation {
                thread_id: ThreadId::try_from(row.try_get::<String, _>("thread_id")?)?,
                producer_id: row.try_get("producer_id")?,
                sequence: u64::try_from(row.try_get::<i64, _>("sequence")?)?,
                purpose: row.try_get("purpose")?,
                mode: row.try_get("input_mode")?,
                target_json: row.try_get("target_json")?,
                content_digest: row.try_get("content_digest")?,
                payload: row.try_get("payload_json")?,
            },
            state: HostInputState::from_str(row.try_get("state")?)?,
        })
    })
    .transpose()
}

async fn set_host_input_state(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    producer_id: &str,
    sequence: u64,
    state: HostInputState,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE host_input_operations SET state = ?, updated_at_ms = ?
         WHERE producer_id = ? AND sequence = ?",
    )
    .bind(state.as_str())
    .bind(datetime_to_epoch_millis(Utc::now()))
    .bind(producer_id)
    .bind(i64::try_from(sequence)?)
    .execute(transaction.as_mut())
    .await?;
    Ok(())
}

#[cfg(test)]
#[path = "queued_items_tests.rs"]
mod tests;
