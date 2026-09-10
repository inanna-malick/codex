#![cfg(unix)]

use super::*;
use codex_app_server_client::AppServerRequestHandle;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::JSONRPCMessage;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, PartialEq)]
pub(crate) struct RecordedRequest {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) body: Value,
}

fn read_request(mut stream: &UnixStream) -> std::io::Result<RecordedRequest> {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut buffer = [0_u8; 4096];
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request ended before its headers",
            ));
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let mut request_line = headers
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace();
    let method = request_line.next().unwrap_or_default().to_string();
    let path = request_line.next().unwrap_or_default().to_string();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or_default();
    while bytes.len() < header_end + content_length {
        let mut buffer = [0_u8; 4096];
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    let body = if content_length == 0 {
        Value::Null
    } else {
        serde_json::from_slice(&bytes[header_end..header_end + content_length])?
    };
    Ok(RecordedRequest { method, path, body })
}

fn write_response(mut stream: &UnixStream, status: &str, body: &[u8]) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

pub(crate) fn spawn_host(
    socket_path: &std::path::Path,
    request_count: usize,
) -> std::io::Result<(
    mpsc::Receiver<RecordedRequest>,
    std::thread::JoinHandle<std::io::Result<()>>,
)> {
    spawn_host_with_completion_delay(socket_path, request_count, Duration::ZERO)
}

fn spawn_host_with_completion_delay(
    socket_path: &std::path::Path,
    request_count: usize,
    completion_delay: Duration,
) -> std::io::Result<(
    mpsc::Receiver<RecordedRequest>,
    std::thread::JoinHandle<std::io::Result<()>>,
)> {
    spawn_host_configured(
        socket_path,
        request_count,
        completion_delay,
        /*input_control_socket*/ None,
    )
}

pub(crate) fn spawn_host_with_input(
    socket_path: &std::path::Path,
    request_count: usize,
    input_control_socket: std::path::PathBuf,
) -> std::io::Result<(
    mpsc::Receiver<RecordedRequest>,
    std::thread::JoinHandle<std::io::Result<()>>,
)> {
    spawn_host_configured(
        socket_path,
        request_count,
        Duration::ZERO,
        Some(input_control_socket),
    )
}

#[tokio::test]
async fn reattach_rejects_stale_generation_and_remote_reports_unavailable() -> color_eyre::Result<()>
{
    let websocket_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let websocket_url = format!("ws://{}", websocket_listener.local_addr()?);
    let websocket_task = tokio::spawn(async move {
        let (stream, _) = websocket_listener.accept().await?;
        let mut socket = tokio_tungstenite::accept_async(stream).await?;
        while let Some(message) = socket.next().await {
            let Message::Text(text) = message? else {
                continue;
            };
            let JSONRPCMessage::Request(request) = serde_json::from_str(&text)? else {
                continue;
            };
            let result = match request.method.as_str() {
                "initialize" => json!({"userAgent": "host-input-test"}),
                "shutdown" => Value::Null,
                method => panic!("unexpected remote request: {method}"),
            };
            socket
                .send(Message::Text(
                    json!({"id": request.id, "result": result})
                        .to_string()
                        .into(),
                ))
                .await?;
        }
        color_eyre::Result::<()>::Ok(())
    });
    let remote = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url,
            auth_token: None,
        },
        client_name: "host-input-test".to_string(),
        client_version: "0.0.0".to_string(),
        experimental_api: false,
        mcp_server_openai_form_elicitation: false,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 8,
    })
    .await?;

    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let host_socket = directory.path().join("host.sock");
    let input_socket = directory.path().join("input.sock");
    let (callbacks, host_task) =
        spawn_host_with_input(&host_socket, /*request_count*/ 3, input_socket.clone())?;
    let host = HostDynamicTools::connect(Some(AbsolutePathBuf::from_absolute_path(host_socket)?))
        .await?
        .expect("configured host");
    let thread = ThreadId::new();
    let handle = AppServerRequestHandle::Remote(remote.request_handle());
    host.attach_primary_with_input(thread, handle.clone())
        .await?;
    let _registration = callbacks.recv()?;
    let first_attachment = callbacks.recv()?;
    host.attach_primary_with_input(thread, handle).await?;
    let second_attachment = callbacks.recv()?;
    assert_eq!(first_attachment.body["sessionGeneration"], json!(1));
    assert_eq!(second_attachment.body["sessionGeneration"], json!(2));
    assert_eq!(
        first_attachment.body["applicationInstanceId"],
        second_attachment.body["applicationInstanceId"]
    );

    let query = |attachment: &RecordedRequest| {
        json!({
            "operation": "query",
            "binding": {
                "protocolVersion": 4,
                "launchId": attachment.body["launchId"],
                "instanceId": attachment.body["applicationInstanceId"],
                "generation": attachment.body["sessionGeneration"],
                "nonce": attachment.body["inputControlNonce"],
            },
            "producer_id": "run/inbox/actor-1.1",
            "sequence": 1,
        })
    };
    let client = reqwest::Client::builder()
        .unix_socket(input_socket)
        .no_proxy()
        .build()?;
    assert_eq!(
        client
            .post("http://localhost/v1/input/control")
            .json(&query(&first_attachment))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let response = client
        .post("http://localhost/v1/input/control")
        .json(&query(&second_attachment))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await?["outcome"],
        json!("evidenceUnavailable")
    );

    drop(host);
    remote.shutdown().await?;
    websocket_task.await??;
    host_task.join().expect("host task")?;
    Ok(())
}

fn spawn_host_configured(
    socket_path: &std::path::Path,
    request_count: usize,
    completion_delay: Duration,
    input_control_socket: Option<std::path::PathBuf>,
) -> std::io::Result<(
    mpsc::Receiver<RecordedRequest>,
    std::thread::JoinHandle<std::io::Result<()>>,
)> {
    let listener = UnixListener::bind(socket_path)?;
    let (request_tx, request_rx) = mpsc::channel();
    let task = std::thread::spawn(move || {
        for _ in 0..request_count {
            let (stream, _) = listener.accept()?;
            let request = read_request(&stream)?;
            if request.path == "/v1/dynamic-tools/completed" {
                std::thread::sleep(completion_delay);
            }
            let response = match request.path.as_str() {
                REGISTRATION_PATH => Some((
                    "200 OK",
                    serde_json::to_vec(&json!({
                        "protocolVersion": 3,
                        "dynamicTools": [{
                            "type": "custom",
                            "name": "evaluate",
                            "description": "Evaluate source",
                            "deferLoading": false
                        }],
                        "scope": "primaryThread",
                        "inputControlSocket": input_control_socket,
                        "launchId": "launch-test",
                        "inputControlNonce": "nonce-test",
                    }))?,
                )),
                SESSION_PATH | "/v1/dynamic-tools/completed" => {
                    Some(("204 No Content", Vec::new()))
                }
                CALL_PATH => Some((
                    "200 OK",
                    serde_json::to_vec(&json!({
                        "contentItems": [{"type": "inputText", "text": "accepted"}],
                        "success": true
                    }))?,
                )),
                _ => None,
            };
            request_tx.send(request).map_err(std::io::Error::other)?;
            let Some((status, body)) = response else {
                write_response(&stream, "404 Not Found", &[])?;
                continue;
            };
            write_response(&stream, status, &body)?;
        }
        Ok(())
    });
    Ok((request_rx, task))
}

#[tokio::test]
async fn custom_call_round_trips_exact_decoded_source_and_ids() -> color_eyre::Result<()> {
    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let socket_path = directory.path().join("host.sock");
    let (requests, task) = spawn_host(&socket_path, 3)?;
    let host = HostDynamicTools::connect(Some(AbsolutePathBuf::from_absolute_path(&socket_path)?))
        .await?
        .expect("configured host");
    let thread_id = ThreadId::new();
    host.attach_primary(thread_id).await?;
    let source = "module Main where\n\nvalue = \"{\\\"nested\\\":true}\"\nemoji = \"λ🌊\"\n";
    let params = DynamicToolCallParams {
        context_call_id: Some("outer-exec".into()),
        thread_id: thread_id.to_string(),
        turn_id: "turn-α".to_string(),
        call_id: "call-1".to_string(),
        namespace: None,
        tool: "evaluate".to_string(),
        arguments: Value::String(source.to_string()),
    };
    assert_eq!(host.routing(&params), HostDynamicToolRouting::Forward);
    let mut wrong_kind = params.clone();
    wrong_kind.arguments = json!({"program": source});
    assert_eq!(host.routing(&wrong_kind), HostDynamicToolRouting::Reject);
    let mut wrong_thread = params.clone();
    wrong_thread.thread_id = ThreadId::new().to_string();
    assert_eq!(host.routing(&wrong_thread), HostDynamicToolRouting::Reject);
    let mut unknown = params.clone();
    unknown.tool = "someone_elses_tool".to_string();
    assert_eq!(host.routing(&unknown), HostDynamicToolRouting::Unregistered);
    let response = host.call(&params).await?;
    assert_eq!(
        response,
        DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: "accepted".to_string(),
            }],
            success: true,
        }
    );

    let registration = requests.recv()?;
    assert_eq!(
        registration,
        RecordedRequest {
            method: "GET".to_string(),
            path: REGISTRATION_PATH.to_string(),
            body: Value::Null,
        }
    );
    let session = requests.recv()?;
    assert_eq!(session.method, "POST");
    assert_eq!(session.path, SESSION_PATH);
    assert_eq!(
        session.body,
        json!({"protocolVersion": 3, "threadId": thread_id})
    );
    let call = requests.recv()?;
    assert_eq!(call.method, "POST");
    assert_eq!(call.path, CALL_PATH);
    assert_eq!(
        call.body,
        json!({
            "protocolVersion": 3,
            "threadId": thread_id,
            "turnId": "turn-α",
            "callId": "call-1",
            "contextCallId": "outer-exec",
            "namespace": null,
            "tool": "evaluate",
            "arguments": source,
        })
    );
    task.join().expect("host thread panicked")?;
    Ok(())
}

#[test]
fn registration_rejects_duplicates_and_tui_namespace() {
    let custom = |name: &str| {
        serde_json::from_value::<DynamicToolSpec>(json!({
            "type": "custom",
            "name": name,
            "description": "test"
        }))
        .expect("custom spec")
    };
    let registration = HostDynamicToolRegistration {
        input_control_socket: None,
        launch_id: String::new(),
        input_control_nonce: String::new(),
        protocol_version: PROTOCOL_VERSION,
        dynamic_tools: vec![custom("same"), custom("same")],
        scope: HostDynamicToolScope::PrimaryThread,
    };
    assert!(validate_registration(&registration).is_err());

    let registration = HostDynamicToolRegistration {
        input_control_socket: None,
        launch_id: String::new(),
        input_control_nonce: String::new(),
        protocol_version: PROTOCOL_VERSION,
        dynamic_tools: vec![
            serde_json::from_value(json!({
                "type": "namespace",
                "name": "codex_tui",
                "description": "collision",
                "tools": [{"type": "custom", "name": "host", "description": "test"}]
            }))
            .expect("namespace spec"),
        ],
        scope: HostDynamicToolScope::PrimaryThread,
    };
    assert!(validate_registration(&registration).is_err());
}

#[tokio::test]
async fn completion_waits_for_sibling_result_and_acknowledges_once() -> color_eyre::Result<()> {
    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let socket = directory.path().join("host.sock");
    let (requests, task) = spawn_host(&socket, 3)?;
    let host = HostDynamicTools::connect(Some(AbsolutePathBuf::from_absolute_path(&socket)?))
        .await?
        .expect("configured host");
    let thread = ThreadId::new();
    host.attach_primary(thread).await?;
    requests.recv()?;
    requests.recv()?;
    host.completions.lock().unwrap().register("outer".into())?;
    let notify = |item: Value| codex_app_server_protocol::RawResponseItemCompletedNotification {
        thread_id: thread.to_string(),
        turn_id: "turn".into(),
        item: serde_json::from_value(item).unwrap(),
    };
    for id in ["outer", "sibling"] {
        host.observe_completion(&notify(json!({"type":"function_call", "call_id":id,
            "name":"exec", "arguments":"{}"})))
            .await?;
    }
    host.observe_completion(&notify(
        json!({"type":"function_call_output", "call_id":"outer", "output":"actual result"}),
    ))
    .await?;
    assert!(requests.try_recv().is_err());
    let last = notify(json!({"type":"function_call_output", "call_id":"sibling", "output":"done"}));
    host.observe_completion(&last).await?;
    assert_eq!(
        requests.recv()?,
        RecordedRequest {
            method: "POST".into(),
            path: "/v1/dynamic-tools/completed".into(),
            body: json!({"protocolVersion":3, "threadId":thread, "contextCallId":"outer"}),
        }
    );
    host.observe_completion(&last).await?;
    task.join().expect("host thread panicked")?;
    Ok(())
}

#[tokio::test]
async fn interrupted_turn_settles_pending_host_effects_without_a_completion()
-> color_eyre::Result<()> {
    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let socket = directory.path().join("host.sock");
    let (requests, task) = spawn_host(&socket, 3)?;
    let host = HostDynamicTools::connect(Some(AbsolutePathBuf::from_absolute_path(&socket)?))
        .await?
        .expect("configured host");
    let thread = ThreadId::new();
    host.attach_primary(thread).await?;
    requests.recv()?;
    requests.recv()?;
    host.completions
        .lock()
        .unwrap()
        .register("interrupted".into())?;
    host.settle_turn(&thread.to_string()).await?;
    assert_eq!(
        requests.recv()?,
        RecordedRequest {
            method: "POST".into(),
            path: SESSION_PATH.into(),
            body: json!({"protocolVersion":3,"threadId":thread}),
        }
    );
    host.settle_turn(&thread.to_string()).await?;
    task.join().expect("host thread panicked")?;
    Ok(())
}

#[tokio::test]
async fn completion_accepts_acknowledgement_after_five_seconds() -> color_eyre::Result<()> {
    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let socket = directory.path().join("host.sock");
    let (_requests, task) = spawn_host_with_completion_delay(
        &socket,
        /*request_count*/ 3,
        Duration::from_secs(6),
    )?;
    let host = HostDynamicTools::connect(Some(AbsolutePathBuf::from_absolute_path(&socket)?))
        .await?
        .expect("configured host");
    let thread = ThreadId::new();
    host.attach_primary(thread).await?;
    host.completions.lock().unwrap().register("outer".into())?;
    for item in [
        json!({"type":"function_call", "call_id":"outer", "name":"exec", "arguments":"{}"}),
        json!({"type":"function_call_output", "call_id":"outer", "output":"done"}),
    ] {
        host.observe_completion(
            &codex_app_server_protocol::RawResponseItemCompletedNotification {
                thread_id: thread.to_string(),
                turn_id: "turn".into(),
                item: serde_json::from_value(item)?,
            },
        )
        .await?;
    }
    assert!(!host.is_disabled());
    // No pending acknowledgement remains to trigger another host request.
    host.settle_turn(&thread.to_string()).await?;
    task.join().expect("host thread panicked")?;
    Ok(())
}
