#![cfg(unix)]

use super::*;
use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::sync::mpsc;

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
    let listener = UnixListener::bind(socket_path)?;
    let (request_tx, request_rx) = mpsc::channel();
    let task = std::thread::spawn(move || {
        for _ in 0..request_count {
            let (stream, _) = listener.accept()?;
            let request = read_request(&stream)?;
            let response = match request.path.as_str() {
                REGISTRATION_PATH => Some((
                    "200 OK",
                    serde_json::to_vec(&json!({
                        "protocolVersion": 2,
                        "dynamicTools": [{
                            "type": "custom",
                            "name": "evaluate",
                            "description": "Evaluate source",
                            "deferLoading": false
                        }],
                        "scope": "primaryThread"
                    }))?,
                )),
                SESSION_PATH => Some(("204 No Content", Vec::new())),
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
        json!({"protocolVersion": 2, "threadId": thread_id})
    );
    let call = requests.recv()?;
    assert_eq!(call.method, "POST");
    assert_eq!(call.path, CALL_PATH);
    assert_eq!(
        call.body,
        json!({
            "protocolVersion": 2,
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
        protocol_version: PROTOCOL_VERSION,
        dynamic_tools: vec![custom("same"), custom("same")],
        scope: HostDynamicToolScope::PrimaryThread,
    };
    assert!(validate_registration(&registration).is_err());

    let registration = HostDynamicToolRegistration {
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
