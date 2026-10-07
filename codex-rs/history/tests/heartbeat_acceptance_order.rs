use codex_history::RetainedContext;
use codex_history::RetainedContextEntry;
use codex_history::RetainedContextEvent;
use codex_history::RetainedContextOrder;
use codex_history::RetainedInputSource;
use codex_history::RetainedUserMessage;
use codex_history::UserInputOrigin;
use codex_history::VerifiedAnswer;
use codex_history::VerifiedQuestionAnswer;

fn heartbeat(order: u64, body: &str) -> RetainedUserMessage {
    RetainedUserMessage {
        turn_id: format!("turn-{order}"),
        message_id: Some(format!("message-{order}")),
        text: format!(
            "<heartbeat>\n  <automation_id>monitor</automation_id>\n  <current_time_iso>2026-10-06T00:00:{order:02}Z</current_time_iso>\n  <instructions>\n{body}\n  </instructions>\n</heartbeat>\n"
        ),
        complete: true,
        origin: UserInputOrigin::Heartbeat,
        phase: None,
    }
}

fn record(context: &mut RetainedContext, order: u64, body: &str) {
    context.record_user_message(
        heartbeat(order, body),
        RetainedInputSource::Local(Some(order)),
    );
}

fn messages(context: &RetainedContext) -> Vec<(RetainedContextOrder, String)> {
    context
        .ordered_entries()
        .filter_map(|(order, entry)| match entry {
            RetainedContextEntry::UserMessage(message) => Some((order, message.text.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn delayed_repeat_preserves_earliest_order_relative_to_answer() {
    let mut context = RetainedContext::default();
    record(&mut context, 2, "Monitor.");
    context.record(&RetainedContextEvent::VerifiedAnswer {
        answer: VerifiedAnswer {
            turn_id: "answer-turn".to_owned(),
            call_id: "question".to_owned(),
            questions: vec![VerifiedQuestionAnswer {
                question: "Continue monitoring?".to_owned(),
                answer: "Stop monitoring.".to_owned(),
            }],
        },
        acceptance_order: Some(1),
    });
    record(&mut context, 0, "Monitor.");
    assert_eq!(
        context
            .ordered_entries()
            .map(|(order, _)| order)
            .collect::<Vec<_>>(),
        vec![
            RetainedContextOrder::Local(0),
            RetainedContextOrder::Local(1)
        ]
    );
}

#[test]
fn delayed_change_preserves_later_reversion() {
    let mut context = RetainedContext::default();
    for (order, body) in [(0, "Monitor."), (2, "Monitor."), (1, "Stop.")] {
        record(&mut context, order, body);
    }
    assert!(context.user_messages_complete());
    let expected = [(0, "Monitor."), (1, "Stop."), (2, "Monitor.")].map(|(order, body)| {
        (
            RetainedContextOrder::Local(order),
            heartbeat(order, body).text,
        )
    });
    assert_eq!(messages(&context), expected);
}

#[test]
fn delayed_earlier_repeat_preserves_later_version_order() {
    let mut context = RetainedContext::default();
    for (order, body) in [(0, "Monitor."), (2, "Stop."), (1, "Monitor."), (3, "Stop.")] {
        record(&mut context, order, body);
    }
    let expected = [(0, "Monitor."), (2, "Stop.")].map(|(order, body)| {
        (
            RetainedContextOrder::Local(order),
            heartbeat(order, body).text,
        )
    });
    assert_eq!(messages(&context), expected);
}
