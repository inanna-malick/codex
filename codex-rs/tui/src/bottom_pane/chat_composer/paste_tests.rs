//! Raw paste tabs remain draft text until the user explicitly submits or queues it.

use super::tests::new_test_composer;
use super::*;
use pretty_assertions::assert_eq;
use ratatui::Terminal;
use ratatui::backend::TestBackend;

#[test]
fn paste_burst_tabs_preserve_multiline_draft() {
    for running in [false, true] {
        for first_line in ["x", "界", "first line\n"] {
            let (mut composer, _rx) = new_test_composer();
            composer.set_task_running(running);
            let mut now = Instant::now();
            let payload = format!("{first_line}\t\tsecond line\n\tthird line\n");

            for ch in payload.chars() {
                let (result, _) = match ch {
                    '\t' => {
                        assert!(composer.handle_paste_tab(KeyEvent::from(KeyCode::Tab), now));
                        (InputResult::None, true)
                    }
                    '\n' => composer.handle_submission_with_time(/*should_queue*/ false, now),
                    ch => composer
                        .handle_input_basic_with_time(KeyEvent::from(KeyCode::Char(ch)), now),
                };
                assert_eq!(result, InputResult::None);
                now += Duration::from_millis(/*millis*/ 1);
            }

            composer.handle_paste_burst_flush(now + PasteBurst::recommended_active_flush_delay());
            assert_eq!(composer.current_text(), payload);

            if running && first_line == "first line\n" {
                let mut terminal = Terminal::new(TestBackend::new(60, 8)).unwrap();
                terminal
                    .draw(|frame| composer.render(frame.area(), frame.buffer_mut()))
                    .unwrap();
                insta::assert_snapshot!("paste_burst_tab_indentation", terminal.backend());
            }

            let (result, _) = composer.handle_key_event(KeyEvent::from(KeyCode::Tab));
            let expected = if running {
                InputResult::Queued {
                    text: payload.trim().to_string(),
                    text_elements: Vec::new(),
                    action: QueuedInputAction::Plain,
                    pending_pastes: Vec::new(),
                }
            } else {
                InputResult::Submitted {
                    text: payload.trim().to_string(),
                    text_elements: Vec::new(),
                }
            };
            assert_eq!(result, expected);
        }
    }
}

#[test]
fn paste_burst_tabs_refresh_idle_timeout() {
    for first_char in ['x', '界'] {
        let (mut composer, _rx) = new_test_composer();
        let mut now = Instant::now();
        composer.handle_input_basic_with_time(KeyEvent::from(KeyCode::Char(first_char)), now);
        assert!(composer.handle_paste_tab(KeyEvent::from(KeyCode::Tab), now));

        for _ in 0..3 {
            now += PasteBurst::recommended_active_flush_delay() / 2;
            assert!(composer.handle_paste_tab(KeyEvent::from(KeyCode::Tab), now));
        }

        composer.handle_paste_burst_flush(now + PasteBurst::recommended_active_flush_delay());
        assert_eq!(composer.current_text(), format!("{first_char}\t\t\t\t"));
        assert!(!composer.handle_paste_tab(
            KeyEvent::from(KeyCode::Tab),
            now + PasteBurst::recommended_active_flush_delay()
        ));
    }
}

#[test]
fn paste_burst_tab_does_not_accept_a_completion() {
    let (mut composer, _rx) = new_test_composer();
    composer.insert_str("/");
    assert!(matches!(composer.popups.active, ActivePopup::Command(_)));
    composer
        .draft
        .paste_burst
        .begin_with_retro_grabbed("review this".to_string(), Instant::now());

    let (result, _) = composer.handle_key_event(KeyEvent::from(KeyCode::Tab));

    assert_eq!(result, InputResult::None);
    let pasted = composer
        .draft
        .paste_burst
        .flush_before_modified_input()
        .unwrap();
    composer.handle_paste(pasted);
    assert_eq!(composer.current_text(), "/review this\t");
}

#[test]
fn paste_burst_expired_before_tab_still_queues() {
    let (mut composer, _rx) = new_test_composer();
    composer.set_task_running(/*running*/ true);
    composer.handle_input_basic_with_time(
        KeyEvent::from(KeyCode::Char('x')),
        Instant::now() - Duration::from_secs(/*secs*/ 1),
    );

    let (result, _) = composer.handle_key_event(KeyEvent::from(KeyCode::Tab));

    assert_eq!(
        result,
        InputResult::Queued {
            text: "x".to_string(),
            text_elements: Vec::new(),
            action: QueuedInputAction::Plain,
            pending_pastes: Vec::new(),
        }
    );
}

#[test]
fn paste_burst_modified_queue_binding_still_dispatches() {
    let (mut composer, _rx) = new_test_composer();
    composer.set_task_running(/*running*/ true);
    composer.queue_keys = vec![key_hint::ctrl(KeyCode::Char('q'))];
    composer.handle_input_basic_with_time(KeyEvent::from(KeyCode::Char('x')), Instant::now());

    let (result, _) =
        composer.handle_key_event(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::CONTROL));

    assert_eq!(
        result,
        InputResult::Queued {
            text: "x".to_string(),
            text_elements: Vec::new(),
            action: QueuedInputAction::Plain,
            pending_pastes: Vec::new(),
        }
    );
}

#[test]
fn vim_replace_recovery_snapshot_matches_pending_key_flush() {
    let mut recovery_mismatches = Vec::new();
    for (initial, cursor, replace, payload, expected) in [
        ("abcd", 0, true, "X", "Xbcd"),
        ("abcd", 0, true, "XYZ", "XYZd"),
        ("prefix abcd", 7, true, "XYZ", "prefix XYZd"),
        ("abcd", 0, false, "XYZ", "XYZabcd"),
        ("abcd", 2, false, "X", "abXcd"),
    ] {
        let (mut composer, _rx) = new_test_composer();
        composer.set_image_paste_enabled(false);
        composer.set_text_content(initial.to_owned(), Vec::new(), Vec::new());
        composer.set_vim_enabled(true);
        composer.draft.textarea.set_cursor(cursor);
        composer.handle_key_event(KeyCode::Char(if replace { 'R' } else { 'i' }).into());
        let now = Instant::now();
        for ch in payload.chars() {
            composer.handle_input_basic_with_time(KeyCode::Char(ch).into(), now);
        }
        assert!(composer.is_in_paste_burst());
        let before = composer.draft_snapshot();
        let recovery = composer.recovery_snapshot();
        assert_eq!(
            composer.draft_snapshot(),
            before,
            "snapshot must not edit live text"
        );
        assert!(
            composer.is_in_paste_burst(),
            "snapshot must not consume pending keys"
        );
        composer.handle_paste_burst_flush(now + PasteBurst::recommended_active_flush_delay());
        assert_eq!(composer.current_text(), expected);
        if recovery.text != composer.current_text() {
            recovery_mismatches.push(format!(
                "{initial:?} at {cursor}, replace={replace}, payload={payload:?}: saved {:?}, flushed {:?}",
                recovery.text, composer.current_text()
            ));
        }
    }

    // Recovery should preserve the live editor's atomic-marker placement.
    let (mut composer, _rx) = new_test_composer();
    composer.set_image_paste_enabled(false);
    composer.insert_str("a");
    composer.draft.textarea.insert_element("<image>");
    composer.insert_str("bcde");
    composer.set_vim_enabled(true);
    composer.draft.textarea.set_cursor(1);
    composer.handle_key_event(KeyCode::Char('R').into());
    let now = Instant::now();
    for ch in "XYZ".chars() {
        composer.handle_input_basic_with_time(KeyCode::Char(ch).into(), now);
    }
    let before = composer.draft_snapshot();
    let recovery = composer.recovery_snapshot();
    assert_eq!(composer.draft_snapshot(), before);
    composer.handle_paste_burst_flush(now + PasteBurst::recommended_active_flush_delay());
    assert_eq!(composer.current_text(), "a<image>XYZe");
    if recovery.text != composer.current_text()
        || recovery.text_elements != composer.draft_snapshot().text_elements
    {
        recovery_mismatches.push(format!(
            "marker: saved {:?} with {:?}, flushed {:?} with {:?}",
            recovery.text,
            recovery.text_elements,
            composer.current_text(),
            composer.draft_snapshot().text_elements
        ));
    }
    eprintln!(
        "validated all 6 live-result and snapshot-immutability fixtures, including 2 Insert controls"
    );
    assert!(
        recovery_mismatches.is_empty(),
        "pending recovery mismatches: {recovery_mismatches:#?}"
    );
}
