use super::*;
use pretty_assertions::assert_eq;

#[tokio::test(start_paused = true)]
async fn admission_expires_without_becoming_a_later_execution() {
    let recovery = Recovery::default();
    recovery.start_recovering();
    let result = recovery.admit().await;
    assert!(result.unwrap_err().starts_with("Not submitted:"));
    assert_eq!(
        recovery.recovered(recovery.generation()),
        RecoveryPassCompletion::Recovered { reported: true }
    );
    assert_eq!(recovery.admit().await, Ok(()));
}

#[tokio::test]
async fn waiting_admission_resumes_on_recovery() {
    let recovery = Arc::new(Recovery::default());
    recovery.start_recovering();
    let generation = recovery.generation();
    let waiting = {
        let recovery = Arc::clone(&recovery);
        tokio::spawn(async move { recovery.admit().await })
    };
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    assert_eq!(
        recovery.recovered(generation),
        RecoveryPassCompletion::Recovered { reported: false }
    );
    assert_eq!(waiting.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn intervention_blocks_admission_but_can_be_reconciled() {
    let recovery = Recovery::default();
    recovery.failed(&Intervention("session identity mismatch".into()).into());
    let generation = recovery.generation();
    assert!(
        recovery
            .admit()
            .await
            .unwrap_err()
            .contains("identity mismatch")
    );
    assert_eq!(
        recovery.recovered(generation),
        RecoveryPassCompletion::Recovered { reported: true }
    );
    assert_eq!(recovery.admit().await, Ok(()));
}

#[tokio::test]
async fn recovery_notices_are_bounded_and_not_repeated_per_retry() {
    let recovery = Recovery::default();
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    *recovery.events.lock().unwrap() = Some(AppEventSender::new(sender));
    let error = color_eyre::eyre::eyre!("completion transport temporarily unavailable");
    recovery.start_recovering();
    recovery.failed(&error);
    recovery.failed(&error);
    recovery
        .reported
        .store(true, std::sync::atomic::Ordering::Release);
    let generation = recovery.generation();
    assert_eq!(
        recovery.recovered(generation),
        RecoveryPassCompletion::Recovered { reported: true }
    );
    assert_eq!(
        recovery.recovered(generation),
        RecoveryPassCompletion::Recovered { reported: false }
    );
    let mut text = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        if let AppEvent::InsertHistoryCell(cell) = event {
            text.push(
                cell.display_lines(80)
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
        }
    }
    assert_eq!(text.len(), 2);
    insta::assert_snapshot!(text.join("\n\n"));
}

#[test]
fn stale_recovery_pass_cannot_clear_a_newer_fence() {
    let recovery = Recovery::default();
    recovery.start_recovering();
    let stale_generation = recovery.generation();
    recovery.failed(&Intervention("history integrity is unconfirmed".into()).into());

    assert_eq!(
        recovery.recovered(stale_generation),
        RecoveryPassCompletion::Stale
    );
    assert_eq!(
        recovery.health.borrow().clone(),
        Health::NeedsIntervention("history integrity is unconfirmed".into())
    );
}

#[test]
fn dispatch_gate_observes_recovery_transition_and_cancellation() {
    let recovery = Recovery::default();
    let admission = HostedCallAdmission::default();
    recovery.start_recovering();

    assert_eq!(
        recovery.try_dispatch(&admission),
        DispatchOutcome::Recovering
    );
    assert_eq!(
        recovery.recovered(recovery.generation()),
        RecoveryPassCompletion::Recovered { reported: false }
    );
    assert_eq!(
        recovery.try_dispatch(&admission),
        DispatchOutcome::Dispatched
    );

    let cancelled = HostedCallAdmission::default();
    cancelled.cancel();
    assert_eq!(
        recovery.try_dispatch(&cancelled),
        DispatchOutcome::Cancelled
    );
}

#[test]
fn transient_failure_does_not_weaken_intervention_fence() {
    let recovery = Recovery::default();
    recovery.failed(&Intervention("manual repair required".into()).into());
    recovery.failed(&color_eyre::eyre::eyre!("temporary transport failure"));

    assert_eq!(
        recovery.health.borrow().clone(),
        Health::NeedsIntervention("manual repair required".into())
    );
}
