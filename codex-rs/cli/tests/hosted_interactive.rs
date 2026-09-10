#![cfg(unix)]

use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use codex_utils_pty::TerminalSize;
use codex_utils_pty::combine_output_receivers;
use codex_utils_pty::spawn_pty_process;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug, PartialEq)]
struct HostRequest {
    method: String,
    path: String,
    body: Value,
}

fn read_request(mut stream: &UnixStream) -> Result<HostRequest> {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut buffer = [0_u8; 4096];
        let read = stream.read(&mut buffer)?;
        anyhow::ensure!(read != 0, "request ended before its headers");
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let mut request_line = headers
        .lines()
        .next()
        .context("host request line")?
        .split_whitespace();
    let method = request_line
        .next()
        .context("host request method")?
        .to_string();
    let path = request_line
        .next()
        .context("host request path")?
        .to_string();
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
        anyhow::ensure!(read != 0, "request ended before its body");
        bytes.extend_from_slice(&buffer[..read]);
    }
    let body = if content_length == 0 {
        Value::Null
    } else {
        serde_json::from_slice(&bytes[header_end..header_end + content_length])?
    };
    Ok(HostRequest { method, path, body })
}

fn write_response(mut stream: &UnixStream, status: &str, body: &[u8]) -> Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    Ok(())
}

fn spawn_host(
    socket: &Path,
    input_sockets: Vec<std::path::PathBuf>,
) -> Result<(
    mpsc::Receiver<HostRequest>,
    std::thread::JoinHandle<Result<()>>,
)> {
    let listener = UnixListener::bind(socket)?;
    let (sender, receiver) = mpsc::channel();
    let task = std::thread::spawn(move || {
        let mut registration_index = 0;
        for _ in 0..input_sockets.len() * 2 {
            let (stream, _) = listener.accept()?;
            let request = read_request(&stream)?;
            let response = match request.path.as_str() {
                "/v1/dynamic-tools/registration" => {
                    let input_socket = input_sockets
                        .get(registration_index)
                        .context("unexpected extra host registration")?;
                    registration_index += 1;
                    (
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
                        "inputControlSocket": input_socket,
                        "launchId": "launch-test",
                        "inputControlNonce": "nonce-test",
                        }))?,
                    )
                }
                "/v1/dynamic-tools/session" => ("204 No Content", Vec::new()),
                path => anyhow::bail!("unexpected host request: {path}"),
            };
            sender.send(request)?;
            write_response(&stream, response.0, &response.1)?;
        }
        Ok(())
    });
    Ok((receiver, task))
}

fn post_input(socket: &Path, path: &str, payload: &Value) -> Result<String> {
    let body = serde_json::to_vec(payload)?;
    let deadline = Instant::now() + Duration::from_secs(/*secs*/ 10);
    let mut stream = loop {
        match UnixStream::connect(socket) {
            Ok(stream) => break stream,
            Err(error) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(/*millis*/ 20));
                let _ = error;
            }
            Err(error) => return Err(error.into()),
        }
    };
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response.lines().next().unwrap_or_default().to_string())
}

async fn wait_for_output(
    output: &mut tokio::sync::broadcast::Receiver<Vec<u8>>,
    captured: &mut String,
    expected: &str,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 30), async {
        while let Ok(bytes) = output.recv().await {
            captured.push_str(&String::from_utf8_lossy(&bytes));
            if captured.contains(expected) {
                return Ok(());
            }
        }
        anyhow::bail!("TUI exited before rendering {expected:?}: {captured}")
    })
    .await
    .with_context(|| format!("TUI output timed out waiting for {expected:?}: {captured}"))?
}

fn rollout_contains_correlated_input(root: &Path, client_id: &str) -> Result<bool> {
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            if rollout_contains_correlated_input(&path, client_id)? {
                return Ok(true);
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == "jsonl")
        {
            let content = std::fs::read_to_string(path)?;
            if content
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .any(|record| {
                    record["type"] == "event_msg"
                        && record["payload"]["type"] == "item_completed"
                        && record["payload"]["item"]["type"] == "UserMessage"
                        && record["payload"]["item"]["client_id"] == client_id
                })
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_tui_attaches_host_owner_and_routes_correlated_input() -> Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(/*mode*/ 0o700))?;
    let codex_home = root.path().join("home");
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&codex_home)?;
    std::fs::create_dir(&workspace)?;
    let provider = app_test_support::create_mock_responses_server_sequence_unchecked(vec![
        app_test_support::create_final_assistant_message_sse_response("initial answer")?,
        app_test_support::create_final_assistant_message_sse_response("hosted answer")?,
    ])
    .await;
    MockResponsesConfig::new(&provider.uri())
        .with_extra_config(&format!(
            "check_for_update_on_startup = false\n\n[projects.\"{}\"]\ntrust_level = \"trusted\"",
            workspace.display()
        ))
        .write(&codex_home)?;

    let host_socket = root.path().join("host.sock");
    let first_input_socket = root.path().join("input-1.sock");
    let resumed_input_socket = root.path().join("input-2.sock");
    let (host_requests, host_task) = spawn_host(
        &host_socket,
        vec![first_input_socket.clone(), resumed_input_socket.clone()],
    )?;
    // Release checks set this to the immutable packaged executable. Ordinary
    // crate checks still exercise the binary Cargo built from this checkout.
    let program = std::env::var_os("CODEX_TEST_INTERACTIVE_BIN")
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(|| codex_utils_cargo_bin::cargo_bin("codex"))?;
    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.insert("CODEX_HOME".to_string(), codex_home.display().to_string());
    env.insert("OPENAI_API_KEY".to_string(), "dummy".to_string());
    env.insert("TERM".to_string(), "xterm-256color".to_string());
    let spawned = spawn_pty_process(
        program.to_str().context("codex binary path")?,
        &[
            "--host-dynamic-tools-socket".to_string(),
            host_socket.display().to_string(),
            "--no-alt-screen".to_string(),
            "-C".to_string(),
            workspace.display().to_string(),
            "initial request".to_string(),
        ],
        &workspace,
        &env,
        /*arg0*/ &None,
        TerminalSize {
            rows: 40,
            cols: 120,
        },
        &[],
    )
    .await?;
    let mut output = combine_output_receivers(spawned.stdout_rx, spawned.stderr_rx);
    let mut captured = String::new();
    wait_for_output(&mut output, &mut captured, "initial answer").await?;

    let registration = host_requests.recv_timeout(Duration::from_secs(/*secs*/ 5))?;
    assert_eq!(registration.method, "GET");
    assert_eq!(registration.path, "/v1/dynamic-tools/registration");
    let attachment = host_requests.recv_timeout(Duration::from_secs(/*secs*/ 5))?;
    assert_eq!(attachment.method, "POST");
    assert_eq!(attachment.path, "/v1/dynamic-tools/session");
    let thread_id = attachment.body["threadId"]
        .as_str()
        .context("attached thread id")?;
    assert_eq!(attachment.body["protocolVersion"], json!(3));
    assert_eq!(attachment.body["threadId"], json!(thread_id));
    assert_eq!(
        attachment.body["inputControlSocket"],
        json!(first_input_socket)
    );
    assert_eq!(attachment.body["launchId"], json!("launch-test"));
    assert_eq!(attachment.body["inputControlNonce"], json!("nonce-test"));
    assert_eq!(attachment.body["sessionGeneration"], json!(1));
    let first_instance = attachment.body["applicationInstanceId"]
        .as_str()
        .context("first application instance")?
        .to_string();

    spawned.session.request_terminate();
    tokio::time::timeout(Duration::from_secs(/*secs*/ 10), spawned.exit_rx)
        .await
        .context("initial full TUI did not exit after termination")??;

    let resumed = spawn_pty_process(
        program.to_str().context("codex binary path")?,
        &[
            "resume".to_string(),
            thread_id.to_string(),
            "--host-dynamic-tools-socket".to_string(),
            host_socket.display().to_string(),
            "--no-alt-screen".to_string(),
            "-C".to_string(),
            workspace.display().to_string(),
        ],
        &workspace,
        &env,
        /*arg0*/ &None,
        TerminalSize {
            rows: 40,
            cols: 120,
        },
        &[],
    )
    .await?;
    let mut resumed_output = combine_output_receivers(resumed.stdout_rx, resumed.stderr_rx);
    let resumed_registration = host_requests.recv_timeout(Duration::from_secs(/*secs*/ 10))?;
    assert_eq!(resumed_registration.method, "GET");
    assert_eq!(resumed_registration.path, "/v1/dynamic-tools/registration");
    let resumed_attachment = host_requests.recv_timeout(Duration::from_secs(/*secs*/ 10))?;
    assert_eq!(resumed_attachment.method, "POST");
    assert_eq!(resumed_attachment.path, "/v1/dynamic-tools/session");
    assert_eq!(resumed_attachment.body["protocolVersion"], json!(3));
    assert_eq!(resumed_attachment.body["threadId"], json!(thread_id));
    assert_eq!(
        resumed_attachment.body["inputControlSocket"],
        json!(resumed_input_socket)
    );
    assert_eq!(resumed_attachment.body["launchId"], json!("launch-test"));
    assert_eq!(
        resumed_attachment.body["inputControlNonce"],
        json!("nonce-test")
    );
    assert_eq!(resumed_attachment.body["sessionGeneration"], json!(1));
    let resumed_instance = resumed_attachment.body["applicationInstanceId"]
        .as_str()
        .context("resumed application instance")?;
    assert_ne!(resumed_instance, first_instance);

    let query = |instance: &str| {
        json!({
            "operation": "query",
            "binding": {
                "protocolVersion": 4,
                "launchId": "launch-test",
                "instanceId": instance,
                "generation": 1,
                "nonce": "nonce-test",
            },
            "producer_id": "run/inbox/actor-1.1",
            "sequence": 1,
        })
    };
    assert_eq!(
        post_input(
            &resumed_input_socket,
            "/v1/input/control",
            &query(&first_instance),
        )?,
        "HTTP/1.1 409 Conflict"
    );
    assert_eq!(
        post_input(
            &resumed_input_socket,
            "/v1/input/control",
            &query(resumed_instance),
        )?,
        "HTTP/1.1 200 OK"
    );

    let client_id = "native-fixture-input-1";
    let status = tokio::task::spawn_blocking({
        let input_socket = resumed_input_socket.clone();
        let payload = json!({
            "threadId": thread_id,
            "clientUserMessageId": client_id,
            "message": "hosted correction",
        });
        move || post_input(&input_socket, "/v1/input", &payload)
    })
    .await??;
    assert_eq!(status, "HTTP/1.1 202 Accepted");
    wait_for_output(&mut resumed_output, &mut captured, "hosted answer").await?;

    let requests = provider
        .received_requests()
        .await
        .context("provider requests")?;
    assert_eq!(requests.len(), 2);
    let second_request: Value = serde_json::from_slice(&requests[1].body)?;
    assert!(
        second_request["input"]
            .to_string()
            .contains("hosted correction"),
        "host input did not reach the provider: {second_request}"
    );
    assert!(rollout_contains_correlated_input(&codex_home, client_id)?);

    resumed.session.request_terminate();
    if tokio::time::timeout(Duration::from_secs(/*secs*/ 10), resumed.exit_rx)
        .await
        .is_err()
    {
        resumed.session.request_terminate();
        anyhow::bail!("resumed full TUI did not exit after termination: {captured}");
    }
    host_task.join().expect("host task panicked")?;
    Ok(())
}
