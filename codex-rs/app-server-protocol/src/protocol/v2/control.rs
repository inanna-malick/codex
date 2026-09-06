use crate::JsonSchema;
use crate::RequestId;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControlAcquireParams {
    pub token: String,
}

impl std::fmt::Debug for ControlAcquireParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlAcquireParams")
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum ControlState {
    AwaitingController,
    Controlled,
    Fenced,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", export_to = "v2/")]
pub enum ControlShutdownState {
    NotStarted,
    Draining,
    /// Session loops and accepted request/startup tasks stopped; process exit is unconfirmed.
    SessionsStopped,
    /// Session/request cleanup failed or timed out; launcher teardown remains required.
    Incomplete,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControlStatus {
    pub instance_id: String,
    pub state: ControlState,
    pub shutdown: ControlShutdownState,
    /// Fencing never establishes the outcomes of effects dispatched before it.
    pub reconciliation_required: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControlStatusParams {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControlPendingListParams {
    #[ts(optional = nullable)]
    pub cursor: Option<String>,
    #[ts(optional = nullable)]
    pub limit: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControlPendingEntry {
    pub request_id: RequestId,
    pub thread_id: Option<String>,
    pub method: String,
    pub turn_id: Option<String>,
    pub call_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ControlPendingListResponse {
    pub data: Vec<ControlPendingEntry>,
    pub next_cursor: Option<String>,
    /// Entries are pending in a live service and uncertain after fencing.
    pub state: ControlState,
    /// The bounded recovery snapshot may omit entries; it is not an effect ledger.
    pub truncated: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadObserveParams {
    pub thread_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadObserveResponse {
    /// Metadata only. Page completed history with thread/turns/list and thread/items/list.
    pub thread: super::Thread,
    /// Listener-ordered snapshot; subsequent notifications follow this response.
    pub active_turn: Option<super::Turn>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(transparent)]
#[ts(export_to = "v2/")]
pub struct ControlAcquireResponse(pub ControlStatus);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(transparent)]
#[ts(export_to = "v2/")]
pub struct ControlStatusReadResponse(pub ControlStatus);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(transparent)]
#[ts(export_to = "v2/")]
pub struct ControlStatusChangedNotification(pub ControlStatus);
