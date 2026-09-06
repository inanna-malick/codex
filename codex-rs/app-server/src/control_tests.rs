use super::*;
use pretty_assertions::assert_eq;

#[test]
fn disconnected_connections_cannot_acquire_or_observe() {
    let control = Control::controlled("credential".to_string());
    let connection = ConnectionId(1);
    control.connection_opened(connection);
    control.connection_closed(connection, || panic!("no controller to fence"));
    assert!(control.acquire(connection, "credential").is_err());
    assert!(!control.observe(connection, "thread".to_string()));
    assert_eq!(
        control.status().unwrap().state,
        ControlState::AwaitingController
    );
}

#[test]
fn only_lost_controller_can_drain_fenced_service() {
    let control = Control::controlled("credential".to_string());
    let controller = ConnectionId(1);
    let observer = ConnectionId(2);
    control.connection_opened(controller);
    control.connection_opened(observer);
    control.acquire(controller, "credential").unwrap();
    assert!(control.connection_closed(controller, || {}));
    assert!(!control.connection_closed(observer, || panic!("observer fenced execution")));
    assert!(!control.begin_cleanup(observer));
    assert!(control.begin_cleanup(controller));
    assert!(!control.begin_cleanup(controller));
    assert!(control.with_authority(controller, || ()).is_none());
}
