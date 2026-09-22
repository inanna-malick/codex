use std::collections::HashSet;

use codex_protocol::models::ResponseItem;

/// Finds the first protocol-closed prefix containing a call's real result.
/// All calls opened before that result must have results of their own.
#[derive(Debug)]
pub struct CompletedCallBoundary {
    target: String,
    seen_target: bool,
    completed_target: bool,
    pending: HashSet<String>,
}

impl CompletedCallBoundary {
    pub fn new(target: &str) -> Self {
        Self {
            target: target.to_owned(),
            seen_target: false,
            completed_target: false,
            pending: HashSet::new(),
        }
    }

    pub fn invocation_id(item: &ResponseItem) -> Option<&str> {
        match item {
            ResponseItem::FunctionCall { call_id, .. }
            | ResponseItem::CustomToolCall { call_id, .. } => Some(call_id),
            ResponseItem::LocalShellCall { call_id, .. }
            | ResponseItem::ToolSearchCall { call_id, .. } => call_id.as_deref(),
            _ => None,
        }
    }

    /// Returns true at a closed boundary after the target result. Duplicate
    /// target invocations are invalid, even if an earlier boundary was found.
    pub fn observe(&mut self, item: &ResponseItem) -> Result<bool, &'static str> {
        let (opened, closed) = match item {
            ResponseItem::FunctionCall { call_id, .. }
            | ResponseItem::CustomToolCall { call_id, .. } => (Some(call_id), None),
            ResponseItem::LocalShellCall { call_id, .. }
            | ResponseItem::ToolSearchCall { call_id, .. } => (call_id.as_ref(), None),
            ResponseItem::FunctionCallOutput { call_id, .. }
            | ResponseItem::ToolSearchOutput { call_id, .. } => (None, call_id.as_ref()),
            ResponseItem::CustomToolCallOutput { call_id, .. } => (None, Some(call_id)),
            _ => (None, None),
        };
        if let Some(id) = opened {
            if id == &self.target {
                if self.seen_target {
                    return Err("call id is ambiguous in source history");
                }
                self.seen_target = true;
            }
            self.pending.insert(id.clone());
        }
        if let Some(id) = closed {
            let was_pending = self.pending.remove(id);
            if id == &self.target && was_pending {
                self.completed_target = true;
            }
        }
        Ok(self.completed_target && self.pending.is_empty())
    }
}

#[cfg(test)]
#[path = "completed_call_tests.rs"]
mod tests;
