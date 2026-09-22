use super::CompletedCallBoundary;
use codex_protocol::models::ResponseItem;
use serde_json::json;

fn call(id: &str) -> ResponseItem {
    serde_json::from_value(
        json!({"type":"custom_tool_call", "call_id":id, "name":"haskell", "input":"pure ()"}),
    )
    .unwrap()
}
fn result(id: &str) -> ResponseItem {
    serde_json::from_value(
        json!({"type":"custom_tool_call_output", "call_id":id, "output":"committed"}),
    )
    .unwrap()
}

#[test]
fn completed_call_waits_for_the_whole_open_batch() {
    let mut boundary = CompletedCallBoundary::new("fork");
    let observed: Vec<_> = [
        call("fork"),
        call("sibling"),
        result("fork"),
        result("sibling"),
    ]
    .iter()
    .map(|item| boundary.observe(item).unwrap())
    .collect();
    assert_eq!(observed, vec![false, false, false, true]);
}

#[test]
fn incomplete_or_unmatched_result_never_supplies_a_boundary() {
    let mut boundary = CompletedCallBoundary::new("fork");
    for item in [result("fork"), call("fork"), call("other"), result("other")] {
        assert!(!boundary.observe(&item).unwrap());
    }
}

#[test]
fn duplicate_invocation_is_rejected_after_a_completed_prefix() {
    let mut boundary = CompletedCallBoundary::new("fork");
    boundary.observe(&call("fork")).unwrap();
    assert!(boundary.observe(&result("fork")).unwrap());
    assert!(boundary.observe(&call("fork")).is_err());
}
