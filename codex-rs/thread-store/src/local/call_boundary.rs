use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;

use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::HistoryPosition;
use codex_rollout::RolloutItem;

use super::rollout_lineage::RolloutLineage;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

#[derive(Clone, Copy)]
pub(super) enum CallBoundaryEnd {
    Invocation,
    Completed,
}

/// Search the durable lineage, not its latest compacted model context. Reference preparation
/// has materialized plain JSONL segments, and the supplied latest position freezes the tail.
pub(super) async fn resolve(
    lineage: RolloutLineage,
    latest: HistoryPosition,
    call_id: String,
    boundary_end: CallBoundaryEnd,
) -> ThreadStoreResult<HistoryPosition> {
    tokio::task::spawn_blocking(move || {
        let mut found = None;
        let mut completed = codex_rollout::CompletedCallBoundary::new(&call_id);
        for segment in lineage.segments() {
            let end = segment.end.unwrap_or(latest);
            let file =
                File::open(&segment.rollout_path).map_err(|err| ThreadStoreError::Internal {
                    message: format!("cannot open fork boundary history: {err}"),
                })?;
            let mut reader = BufReader::new(file.take(end.end_byte_offset));
            let mut bytes = Vec::new();
            let mut offset = 0;
            loop {
                bytes.clear();
                let count = reader.read_until(b'\n', &mut bytes).map_err(|err| {
                    ThreadStoreError::Internal {
                        message: format!("cannot read fork boundary history: {err}"),
                    }
                })?;
                if count == 0 {
                    break;
                }
                offset += count as u64;
                let line = codex_rollout::parse_rollout_line_bytes(&bytes).map_err(|err| {
                    ThreadStoreError::Internal {
                        message: format!("invalid fork boundary history: {err}"),
                    }
                })?;
                let Some(ordinal) = line.ordinal else {
                    continue;
                };
                if ordinal < segment.start_ordinal() || ordinal >= end.end_ordinal_exclusive {
                    continue;
                }
                if let RolloutItem::ResponseItem(item) = line.item {
                    let matches = match boundary_end {
                        CallBoundaryEnd::Invocation => matches!(&item.item,
                            ResponseItem::FunctionCall { call_id: id, .. }
                            | ResponseItem::CustomToolCall { call_id: id, .. } if id == &call_id),
                        CallBoundaryEnd::Completed => {
                            completed.observe(&item.item).map_err(|message| {
                                ThreadStoreError::InvalidRequest {
                                    message: message.to_string(),
                                }
                            })?
                        }
                    };
                    if matches {
                        if found.is_some() && matches!(boundary_end, CallBoundaryEnd::Invocation) {
                            return Err(ThreadStoreError::InvalidRequest {
                                message: format!(
                                    "call id '{call_id}' is ambiguous in source history"
                                ),
                            });
                        }
                        found.get_or_insert(HistoryPosition {
                            thread_id: segment.rollout_id(),
                            end_ordinal_exclusive: ordinal + 1,
                            end_byte_offset: offset,
                        });
                    }
                }
            }
        }
        found.ok_or_else(|| {
            let description = match boundary_end {
                CallBoundaryEnd::Invocation => "invocation",
                CallBoundaryEnd::Completed => "completed tool result",
            };
            ThreadStoreError::InvalidRequest {
                message: format!("no durable {description} found for call id '{call_id}'"),
            }
        })
    })
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to resolve invocation boundary: {err}"),
    })?
}
