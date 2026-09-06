use anyhow::Context;
use anyhow::Result;
use app_test_support::MockResponsesConfig;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::process::Stdio;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn native_observer_attach_exit_and_reattach_preserve_readiness_and_service() -> Result<()> {
    #[cfg(windows)]
    if !codex_utils_pty::conpty_supported() {
        return Ok(());
    }
    let home = TempDir::new()?;
    let provider = wiremock::MockServer::start().await;
    MockResponsesConfig::new(&provider.uri()).write(home.path())?;
    let token = "test-observer-controller-credential-0123456789";
    let token_file = home.path().join("controller-token");
    std::fs::write(&token_file, token)?;
    let program = codex_utils_cargo_bin::cargo_bin("codex")?;
    let mut service = tokio::process::Command::new(&program)
        .args([
            "app-server",
            "--listen",
            "ws://127.0.0.1:0",
            "--controller-token-file",
        ])
        .arg(&token_file)
        .env("CODEX_HOME", home.path())
        .env("RUST_LOG", "trace")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut lines = BufReader::new(service.stderr.take().context("service stderr")?).lines();
    let endpoint = timeout(Duration::from_secs(/*secs*/ 60), async {
        while let Some(line) = lines.next_line().await? {
            if let Some(address) = line
                .split_whitespace()
                .find_map(|part| part.strip_prefix("ws://"))
                .and_then(|address| address.parse::<std::net::SocketAddr>().ok())
            {
                return Ok::<_, anyhow::Error>(format!("ws://{address}"));
            }
        }
        anyhow::bail!("service exited before listening")
    })
    .await??;
    let drain = tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    let (mut controller, _) = tokio_tungstenite::connect_async(&endpoint).await?;
    let mut observed_thread = String::new();
    // Use actual native protocol routing, with no mock app-server peer.
    for (id, method, params) in [
        (
            1,
            "initialize",
            json!({"clientInfo":{"name":"observer_process_test","version":"1"},"capabilities":{"experimentalApi":true}}),
        ),
        (2, "control/acquire", json!({"token":token})),
        (
            3,
            "thread/start",
            json!({"dynamicTools":[{"type":"function","name":"effect","description":"Hosted effect","inputSchema":{"type":"object","properties":{}}}]}),
        ),
    ] {
        controller
            .send(Message::Text(
                json!({"id":id,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await?;
        loop {
            let Message::Text(text) = timeout(Duration::from_secs(/*secs*/ 60), controller.next())
                .await?
                .context("controller closed")??
            else {
                continue;
            };
            let reply: Value = serde_json::from_str(&text)?;
            if reply["id"] == id && reply.get("method").is_none() {
                assert!(reply.get("result").is_some(), "{reply}");
                if id == 3 {
                    let thread = reply["result"]["thread"]["id"]
                        .as_str()
                        .context("thread id")?;
                    for _ in 0..2 {
                        observe_once(&program, home.path(), thread, &endpoint, "").await?;
                        assert!(service.try_wait()?.is_none());
                    }
                    observed_thread = thread.to_string();
                    controller.send(Message::Text(json!({"id":4,"method":"turn/start","params":{"threadId":thread,"input":[{"type":"text","text":"must stay gated"}]}}).to_string().into())).await?;
                }
                break;
            }
        }
    }
    loop {
        let Message::Text(text) = timeout(Duration::from_secs(/*secs*/ 10), controller.next())
            .await?
            .context("controller closed")??
        else {
            continue;
        };
        let reply: Value = serde_json::from_str(&text)?;
        if reply["id"] == 4 {
            assert!(
                reply["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("readiness"),
                "{reply}"
            );
            break;
        }
    }
    assert_eq!(
        provider
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path().ends_with("/responses"))
            .count(),
        0
    );
    eprintln!("readiness attachment verified; starting hosted call");
    let first_response = [
        json!({"type":"response.created","response":{"id":"hosted"}}),
        json!({"type":"response.output_item.done","item":{"type":"function_call","call_id":"observer-effect","name":"effect","arguments":"{}"}}),
        json!({"type":"response.completed","response":{"id":"hosted","usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}),
    ].into_iter().map(|event| format!("event: {}\ndata: {event}\n\n", event["type"].as_str().unwrap())).collect::<String>();
    wiremock::Mock::given(wiremock::matchers::path("/v1/responses"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(first_response),
        )
        .up_to_n_times(1)
        .mount(&provider)
        .await;
    rpc(
        &mut controller,
        5,
        "thread/ready",
        json!({"threadId":observed_thread}),
    )
    .await?;
    rpc(&mut controller, 6, "turn/start", json!({"threadId":observed_thread,"input":[{"type":"text","text":"perform hosted effect"}]})).await?;
    eprintln!("turn accepted; awaiting hosted call");
    let call = loop {
        let event = receive(&mut controller).await?;
        if event["method"] == "item/tool/call" {
            break event;
        }
    };
    eprintln!("hosted call pending; attaching observers");
    for _ in 0..2 {
        observe_once(&program, home.path(), &observed_thread, &endpoint, "").await?;
        let pending = rpc(&mut controller, 7, "control/pending/list", json!({})).await?;
        assert!(
            pending["result"]["data"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["requestId"] == call["id"])
        );
        assert!(service.try_wait()?.is_none());
    }
    wiremock::Mock::given(wiremock::matchers::path("/v1/responses"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(
                    app_test_support::create_final_assistant_message_sse_response(
                        "ObserverCompletionVerified",
                    )?,
                ),
        )
        .mount(&provider)
        .await;
    controller.send(Message::Text(json!({"id":call["id"],"result":{"contentItems":[{"type":"inputText","text":"recorded"}],"success":true}}).to_string().into())).await?;
    loop {
        if receive(&mut controller).await?["method"] == "turn/completed" {
            break;
        }
    }
    eprintln!("turn completed; checking observer history");
    observe_once(
        &program,
        home.path(),
        &observed_thread,
        &endpoint,
        "ObserverCompletionVerified",
    )
    .await?;
    assert_eq!(
        provider
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path().ends_with("/responses"))
            .count(),
        2
    );
    service.kill().await?;
    drain.await?;
    Ok(())
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn receive(client: &mut Client) -> Result<Value> {
    loop {
        let Message::Text(text) = timeout(Duration::from_secs(/*secs*/ 30), client.next())
            .await?
            .context("connection closed")??
        else {
            continue;
        };
        let value: Value = serde_json::from_str(&text)?;
        if value["method"] == "currentTime/read" {
            client
                .send(Message::Text(
                    json!({"id":value["id"],"result":{"currentTimeAt":1780000000_i64}})
                        .to_string()
                        .into(),
                ))
                .await?;
        } else {
            return Ok(value);
        }
    }
}

async fn rpc(client: &mut Client, id: i64, method: &str, params: Value) -> Result<Value> {
    client
        .send(Message::Text(
            json!({"id":id,"method":method,"params":params})
                .to_string()
                .into(),
        ))
        .await?;
    loop {
        let value = receive(client).await?;
        if value["id"] == id && value.get("method").is_none() {
            assert!(value.get("result").is_some(), "{value}");
            return Ok(value);
        }
        assert!(value.get("id").is_none(), "unexpected request: {value}");
    }
}

async fn observe_once(
    program: &std::path::Path,
    home: &std::path::Path,
    thread: &str,
    endpoint: &str,
    expected: &str,
) -> Result<()> {
    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.insert("CODEX_HOME".to_string(), home.display().to_string());
    env.insert("TERM".to_string(), "xterm-256color".to_string());
    env.insert("RUST_LOG".to_string(), "trace".to_string());
    let spawned = codex_utils_pty::spawn_pty_process(
        program.to_str().context("program path")?,
        &[
            "observe".to_string(),
            thread.to_string(),
            "--remote".to_string(),
            endpoint.to_string(),
        ],
        home,
        &env,
        /*arg0*/ &None,
        codex_utils_pty::TerminalSize {
            rows: 24,
            cols: 120,
        },
        &[],
    )
    .await?;
    let mut output = spawned.stdout_rx;
    let writer = spawned.session.writer_sender();
    let mut screen = String::new();
    timeout(Duration::from_secs(/*secs*/ 15), async {
        while let Some(bytes) = output.recv().await {
            screen.push_str(&String::from_utf8_lossy(&bytes));
            if screen.contains("Observer") && screen.contains("only") && screen.contains(expected) {
                return Ok::<_, anyhow::Error>(());
            }
        }
        anyhow::bail!("observer {program:?} exited before rendering: {screen}")
    })
    .await
    .with_context(|| format!("observer timed out waiting for {expected:?}; output: {screen}"))??;
    // Paste and Enter have no submission path in this UI.
    writer
        .send(b"\x1b[200~do not submit\x1b[201~".to_vec())
        .await?;
    writer.send(b"\r".to_vec()).await?;
    writer.send(b"q".to_vec()).await?;
    assert_eq!(
        timeout(Duration::from_secs(/*secs*/ 10), spawned.exit_rx).await??,
        0
    );
    Ok(())
}
