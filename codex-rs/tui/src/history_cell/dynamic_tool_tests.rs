use super::*;

use pretty_assertions::assert_eq;
use serde_json::json;

fn rendered(lines: Vec<Line<'static>>) -> String {
    lines
        .into_iter()
        .map(|line| {
            line.spans
                .into_iter()
                .map(|span| span.content.into_owned())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn custom_dynamic_tool_renders_raw_source_and_result() {
    let input = "main = do\n  putStrLn \"JSON: {\\\"ok\\\":true}\"\n  putStrLn \\\\server\n  putStrLn \"λ\"";
    let mut cell = DynamicToolCallCell::new(
        "call-1".to_string(),
        Some("tidepool_actor".to_string()),
        "haskell".to_string(),
        json!(input),
        /*animations_enabled*/ false,
    );
    cell.complete(
        Duration::from_millis(25),
        vec![DynamicToolCallOutputContentItem::InputText {
            text: "{\"status\":\"rejected\"}\nGHC: type mismatch".to_string(),
        }],
        /*success*/ true,
    );

    insta::assert_snapshot!(rendered(cell.display_lines(/*width*/ 80)), @r#"
    • tidepool_actor.haskell Completed · 25ms
      ├ Input
      │ ```
      │ main = do
      │   putStrLn "JSON: {\"ok\":true}"
      │   putStrLn \\server
      │   putStrLn "λ"
      │ ```
      └ Result
        {"status":"rejected"}
        GHC: type mismatch
    "#);
}

#[test]
fn large_dynamic_tool_blocks_are_collapsed_only_in_the_main_view() {
    let input = (1..=10)
        .map(|line| format!("line {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let cell = DynamicToolCallCell::new(
        "call-2".to_string(),
        None,
        "evaluate".to_string(),
        json!(input),
        /*animations_enabled*/ false,
    );

    let display = rendered(cell.display_lines(/*width*/ 80));
    let transcript = rendered(cell.transcript_lines(/*width*/ 80));
    assert!(display.contains("2 lines hidden"));
    assert!(!display.contains("line 10"));
    assert!(transcript.contains("line 10"));
    assert!(!transcript.contains("lines hidden"));
}

#[test]
fn function_dynamic_tool_arguments_remain_structured_json() {
    let cell = DynamicToolCallCell::new(
        "call-3".to_string(),
        None,
        "lookup".to_string(),
        json!({"query": "needle"}),
        /*animations_enabled*/ false,
    );

    let lines = rendered(cell.display_lines(/*width*/ 80));
    assert_eq!(
        lines,
        "• lookup Running\n  ├ Input\n  │ ```json\n  │ {\n  │   \"query\": \"needle\"\n  │ }\n  │ ```"
    );
}
