//! Per-response usage metadata reported by the upstream service, without aggregation.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use ts_rs::TS;

/// Usage metadata reported for one upstream response.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq, TS, JsonSchema)]
pub struct ResponseUsageMetadata {
    pub amount: Option<String>,
    pub metadata: Option<Value>,
    /// Provider explanation of the prompt-cache outcome for this response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub prompt_cache_diagnostics: Option<Value>,
    /// Provider-selected prompt-cache mode, lifetime, and comparison response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub prompt_cache_options: Option<Value>,
}
