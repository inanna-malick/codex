use super::*;
use crate::test_backend::VT100Backend;
use pretty_assertions::assert_eq;

fn observer() -> Observer {
    Observer {
        thread_id: "019e72f4-e09a-70f2-b2c2-a153a57b8cc0".to_string(),
        cwd: AbsolutePathBuf::current_dir().unwrap(),
        items: Vec::new(),
        older: None,
        older_items: None,
        status: "Connected".to_string(),
        scroll_back: 0,
    }
}

fn message(text: &str) -> ThreadItem {
    ThreadItem::AgentMessage {
        id: "message".to_string(),
        text: text.to_string(),
        phase: None,
        memory_citation: None,
        delivery: None,
        questions: None,
    }
}

#[test]
fn observer_transcript_snapshot_and_completion_replaces_partial_output() {
    let mut observer = observer();
    observer.upsert(message("Working"));
    observer.upsert(message("Work completed."));
    assert_eq!(observer.items, vec![message("Work completed.")]);
    let mut terminal = Terminal::new(VT100Backend::new(/*width*/ 90, /*height*/ 8)).unwrap();
    terminal.draw(|frame| observer.render(frame)).unwrap();
    insta::assert_snapshot!("observer_connected", terminal.backend().to_string());
}

#[test]
fn observer_fenced_snapshot() {
    let mut observer = observer();
    observer.notification(ServerNotification::ControlStatusChanged(
        codex_app_server_protocol::ControlStatusChangedNotification(
            codex_app_server_protocol::ControlStatus {
                instance_id: "service".to_string(),
                state: ControlState::Fenced,
                shutdown: codex_app_server_protocol::ControlShutdownState::Incomplete,
                reconciliation_required: true,
            },
        ),
    ));
    let mut terminal = Terminal::new(VT100Backend::new(/*width*/ 120, /*height*/ 8)).unwrap();
    terminal.draw(|frame| observer.render(frame)).unwrap();
    insta::assert_snapshot!("observer_fenced", terminal.backend().to_string());
}

#[test]
fn paging_history_keeps_live_completion() {
    let mut observer = observer();
    observer.upsert(message("Working"));
    observer.older_items = Some(vec![message("Older page")]);
    observer.upsert(message("Completed while viewing history"));
    assert_eq!(observer.older_items, Some(vec![message("Older page")]));
    assert_eq!(
        observer.items,
        vec![message("Completed while viewing history")]
    );
}

#[test]
fn returning_live_restarts_history_instead_of_skipping_visited_pages() {
    let mut observer = observer();
    observer.older_items = Some(vec![message("History")]);
    observer.older = Some("next-page".to_string());
    assert_eq!(
        observer.key(KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE)),
        Action::History(Some("next-page".to_string()))
    );
    assert_eq!(
        observer.key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE)),
        Action::None
    );
    assert_eq!(
        observer.key(KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE)),
        Action::History(None)
    );
    observer.older_items = Some(Vec::new());
    observer.older = None;
    assert_eq!(
        observer.key(KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE)),
        Action::None
    );
    observer.key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE));
    assert_eq!(
        observer.key(KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE)),
        Action::History(None)
    );
}
