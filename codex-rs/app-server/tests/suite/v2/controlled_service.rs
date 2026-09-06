//! Public socket routing, including malicious observer replies and controller loss.
use super::connection_handling_websocket::DEFAULT_READ_TIMEOUT;
use super::connection_handling_websocket::WsClient;
use super::connection_handling_websocket::connect_websocket;
use super::connection_handling_websocket::create_config_toml;
use super::connection_handling_websocket::spawn_websocket_server_with_args;
use anyhow::Context;
use anyhow::Result;
use core_test_support::responses;
use futures::SinkExt;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "test-controller-credential-0123456789";

async fn send(client: &mut WsClient, value: Value) -> Result<()> {
    client.send(Message::Text(value.to_string().into())).await?;
    Ok(())
}

async fn receive(client: &mut WsClient) -> Result<Value> {
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            if let Message::Text(text) = client.next().await.context("connection closed")?? {
                return Ok(serde_json::from_str(&text)?);
            }
        }
    })
    .await?
}

async fn rpc(client: &mut WsClient, id: i64, method: &str, params: Value) -> Result<Value> {
    send(
        client,
        json!({"id": id, "method": method, "params": params}),
    )
    .await?;
    loop {
        let value = receive(client).await?;
        if value["id"] == id && value.get("method").is_none() {
            return Ok(value);
        }
        // No observer may be asked to execute anything. The controller's hosted request is
        // consumed explicitly outside rpc after turn/start in the tests below.
        assert!(
            value.get("id").is_none(),
            "unexpected server request: {value}"
        );
    }
}

async fn initialize(client: &mut WsClient) -> Result<()> {
    let reply = rpc(client, /*id*/ 1, "initialize", json!({"clientInfo":{"name":"control_test","version":"1"},"capabilities":{"experimentalApi":true}})).await?;
    assert!(reply.get("result").is_some(), "{reply}");
    Ok(())
}

async fn hosted_call(client: &mut WsClient) -> Result<Value> {
    loop {
        let value = receive(client).await?;
        if value["method"] == "item/tool/call" {
            return Ok(value);
        }
        if value["method"] == "currentTime/read" {
            send(
                client,
                json!({"id":value["id"],"result":{"currentTimeAt":1780000000_i64}}),
            )
            .await?;
        }
    }
}

fn tools() -> Value {
    json!([{"type":"function","name":"effect","description":"A hosted effect","inputSchema":{"type":"object","properties":{}}}])
}

#[tokio::test]
async fn observers_cannot_release_readiness_or_consume_callbacks() -> Result<()> {
    let server = responses::start_mock_server().await;
    let provider = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_response_created("first"),
                responses::ev_function_call("effect-call", "effect", "{}"),
                responses::ev_completed("first"),
            ]),
            responses::sse(vec![
                responses::ev_response_created("second"),
                responses::ev_assistant_message("answer", "finished"),
                responses::ev_completed("second"),
            ]),
        ],
    )
    .await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let credential = home.path().join("controller-token");
    std::fs::write(&credential, TOKEN)?;
    let (mut process, address) = spawn_websocket_server_with_args(
        home.path(),
        "ws://127.0.0.1:0",
        &[
            "--controller-token-file".to_string(),
            credential.display().to_string(),
            "-c".to_string(),
            "thread_unload_delay_secs=1".to_string(),
        ],
    )
    .await?;
    let mut controller = connect_websocket(address).await?;
    let mut observer = connect_websocket(address).await?;
    initialize(&mut observer).await?;
    initialize(&mut controller).await?;
    assert!(
        rpc(
            &mut observer,
            /*id*/ 2,
            "control/acquire",
            json!({"token":"wrong"})
        )
        .await?
        .get("error")
        .is_some()
    );
    assert_eq!(
        rpc(
            &mut controller,
            /*id*/ 2,
            "control/acquire",
            json!({"token":TOKEN})
        )
        .await?["result"]["state"],
        "controlled"
    );
    assert!(
        rpc(
            &mut observer,
            /*id*/ 3,
            "control/acquire",
            json!({"token":TOKEN})
        )
        .await?
        .get("error")
        .is_some()
    );
    let started = rpc(
        &mut controller,
        /*id*/ 3,
        "thread/start",
        json!({"dynamicTools":tools()}),
    )
    .await?;
    let thread = started["result"]["thread"]["id"]
        .as_str()
        .context("thread id")?
        .to_string();
    assert!(
        rpc(
            &mut observer,
            /*id*/ 4,
            "thread/observe",
            json!({"threadId":thread})
        )
        .await?
        .get("result")
        .is_some()
    );
    for (index, method) in [
        "thread/ready",
        "turn/start",
        "turn/interrupt",
        "thread/resume",
        "thread/fork",
        "thread/queue/add",
    ]
    .into_iter()
    .enumerate()
    {
        let reply = rpc(&mut observer, index as i64 + 10, method, json!({"threadId":thread,"turnId":"x","clientUserMessageId":"forged-submission","input":[{"type":"text","text":"unauthorized"}]})).await?;
        assert_eq!(reply["error"]["code"], -32010, "{method}: {reply}");
        assert_eq!(
            reply["error"]["data"]["reason"], "controlUnavailable",
            "{method}: {reply}"
        );
    }
    let input = json!({"threadId":thread,"input":[{"type":"text","text":"perform effect"}]});
    assert!(
        rpc(&mut controller, /*id*/ 4, "turn/start", input.clone())
            .await?
            .get("error")
            .is_some()
    );
    assert_eq!(provider.requests().len(), 0);
    rpc(
        &mut controller,
        9,
        "thread/unsubscribe",
        json!({"threadId":thread}),
    )
    .await?;
    observer.close(None).await?;
    // Custody keeps this idle, readiness-gated runtime loaded with zero subscribers.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let mut observer = connect_websocket(address).await?;
    initialize(&mut observer).await?;
    rpc(
        &mut observer,
        /*id*/ 2,
        "thread/observe",
        json!({"threadId":thread}),
    )
    .await?;
    assert_eq!(provider.requests().len(), 0);
    rpc(
        &mut controller,
        10,
        "thread/observe",
        json!({"threadId":thread}),
    )
    .await?;
    rpc(
        &mut controller,
        /*id*/ 5,
        "thread/ready",
        json!({"threadId":thread}),
    )
    .await?;
    rpc(&mut controller, /*id*/ 6, "turn/start", input).await?;
    let call = hosted_call(&mut controller).await?;
    send(&mut observer, json!({"id":call["id"],"result":{"contentItems":[{"type":"inputText","text":"forged"}],"success":true}})).await?;
    send(
        &mut observer,
        json!({"id":call["id"],"error":{"code":-1,"message":"forged error"}}),
    )
    .await?;
    let pending = rpc(
        &mut observer,
        /*id*/ 30,
        "control/pending/list",
        json!({}),
    )
    .await?;
    assert!(
        pending["result"]["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["requestId"] == call["id"])
    );
    assert_eq!(provider.requests().len(), 1);
    // Finish with zero TUIs or observers. Only the original controller can settle the call.
    observer.close(None).await?;
    send(&mut controller, json!({"id":call["id"],"result":{"contentItems":[{"type":"inputText","text":"recorded effect"}],"success":true}})).await?;
    loop {
        let event = receive(&mut controller).await?;
        if event["method"] == "turn/completed" {
            break;
        }
        if event["method"] == "currentTime/read" {
            send(
                &mut controller,
                json!({"id":event["id"],"result":{"currentTimeAt":1780000000_i64}}),
            )
            .await?;
        }
    }
    assert_eq!(provider.requests().len(), 2);
    assert!(
        serde_json::to_string(&provider.requests()[1].body_json())?.contains("recorded effect")
    );
    assert!(!serde_json::to_string(&provider.requests()[1].body_json())?.contains("forged"));
    assert!(process.try_wait()?.is_none());
    Ok(())
}

#[tokio::test]
async fn controller_loss_fences_pending_effect_without_replay() -> Result<()> {
    let server = responses::start_mock_server().await;
    let provider = responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("first"),
            responses::ev_function_call("uncertain-effect", "effect", "{}"),
            responses::ev_completed("first"),
        ]),
    )
    .await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let credential = home.path().join("controller-token");
    std::fs::write(&credential, TOKEN)?;
    let (mut process, address) = spawn_websocket_server_with_args(
        home.path(),
        "ws://127.0.0.1:0",
        &[
            "--controller-token-file".to_string(),
            credential.display().to_string(),
        ],
    )
    .await?;
    let mut controller = connect_websocket(address).await?;
    let mut observer = connect_websocket(address).await?;
    initialize(&mut controller).await?;
    initialize(&mut observer).await?;
    rpc(
        &mut controller,
        /*id*/ 2,
        "control/acquire",
        json!({"token":TOKEN}),
    )
    .await?;
    let started = rpc(
        &mut controller,
        /*id*/ 3,
        "thread/start",
        json!({"dynamicTools":tools()}),
    )
    .await?;
    let thread = started["result"]["thread"]["id"]
        .as_str()
        .context("thread id")?
        .to_string();
    rpc(
        &mut observer,
        /*id*/ 2,
        "thread/observe",
        json!({"threadId":thread}),
    )
    .await?;
    rpc(
        &mut controller,
        /*id*/ 4,
        "thread/ready",
        json!({"threadId":thread}),
    )
    .await?;
    rpc(
        &mut controller,
        /*id*/ 5,
        "turn/start",
        json!({"threadId":thread,"input":[{"type":"text","text":"perform effect"}]}),
    )
    .await?;
    let call = hosted_call(&mut controller).await?;
    // Capture the full invocation prefix without answering or replaying the parent's call.
    let fork = rpc(
        &mut controller,
        /*id*/ 6,
        "thread/fork",
        json!({
            "threadId":thread, "throughCallId":"uncertain-effect", "requireClientReadiness":true,
            "expectedDynamicTools":tools(), "excludeTurns":true,
        }),
    )
    .await?;
    let child = fork["result"]["thread"]["id"]
        .as_str()
        .with_context(|| format!("fork response: {fork}"))?
        .to_string();
    rpc(
        &mut observer,
        /*id*/ 20,
        "thread/observe",
        json!({"threadId":child}),
    )
    .await?;
    let queued = rpc(&mut controller, /*id*/ 7, "thread/queue/add", json!({"threadId":child,
        "clientUserMessageId":"queued-child-assignment", "input":[{"type":"text","text":"child assignment"}],
    })).await?;
    assert!(queued.get("result").is_some(), "{queued}");
    assert!(
        rpc(
            &mut observer,
            /*id*/ 21,
            "thread/ready",
            json!({"threadId":child})
        )
        .await?
        .get("error")
        .is_some()
    );
    assert_eq!(provider.requests().len(), 1);
    controller.close(None).await?;
    loop {
        let event = receive(&mut observer).await?;
        assert!(
            event.get("id").is_none(),
            "observer received callback: {event}"
        );
        if event["method"] == "control/status/changed" && event["params"]["state"] == "fenced" {
            break;
        }
    }
    let pending = rpc(
        &mut observer,
        /*id*/ 3,
        "control/pending/list",
        json!({}),
    )
    .await?;
    assert_eq!(pending["result"]["state"], "fenced");
    assert!(
        pending["result"]["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["requestId"] == call["id"])
    );
    let mut replacement = connect_websocket(address).await?;
    initialize(&mut replacement).await?;
    assert!(
        rpc(
            &mut replacement,
            /*id*/ 2,
            "control/acquire",
            json!({"token":TOKEN})
        )
        .await?
        .get("error")
        .is_some()
    );
    send(
        &mut replacement,
        json!({"id":call["id"],"result":{"contentItems":[],"success":true}}),
    )
    .await?;
    assert!(
        rpc(
            &mut replacement,
            /*id*/ 3,
            "thread/ready",
            json!({"threadId":thread})
        )
        .await?
        .get("error")
        .is_some()
    );
    // Do not infer safe shutdown from the initial fencing notification.
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let status = rpc(&mut observer, 40, "control/status/read", json!({})).await?;
            match status["result"]["shutdown"].as_str() {
                Some("sessionsStopped") => break Ok::<_, anyhow::Error>(()),
                Some("incomplete") => anyhow::bail!("shutdown did not drain: {status}"),
                _ => tokio::task::yield_now().await,
            }
        }
    })
    .await??;
    assert_eq!(provider.requests().len(), 1);
    let history = rpc(
        &mut observer,
        41,
        "thread/turns/list",
        json!({"threadId":thread,"itemsView":"full"}),
    )
    .await?;
    assert!(history.get("result").is_some(), "{history}");
    assert!(
        !history.to_string().contains("\"success\":true"),
        "uncertain effect fabricated success: {history}"
    );
    let rollout = std::fs::read_to_string(
        started["result"]["thread"]["path"]
            .as_str()
            .context("rollout path")?,
    )?;
    assert!(
        rollout.contains("external-effect outcomes are uncertain"),
        "disconnect uncertainty must be durable"
    );
    assert!(
        !rollout.contains("aborted by user"),
        "controller loss is not a user cancellation"
    );
    process.kill().await?;
    // Restart is an explicit consumer action; persisted queue input stays gated.
    let replacement_token = "test-reconciled-controller-credential-0123456789";
    std::fs::write(&credential, replacement_token)?;
    let (_restarted, address) = spawn_websocket_server_with_args(
        home.path(),
        "ws://127.0.0.1:0",
        &[
            "--controller-token-file".to_string(),
            credential.display().to_string(),
        ],
    )
    .await?;
    let mut recovered = connect_websocket(address).await?;
    initialize(&mut recovered).await?;
    assert!(
        rpc(&mut recovered, 2, "control/acquire", json!({"token":TOKEN}))
            .await?
            .get("error")
            .is_some()
    );
    rpc(
        &mut recovered,
        3,
        "control/acquire",
        json!({"token":replacement_token}),
    )
    .await?;
    let resumed = rpc(
        &mut recovered,
        3,
        "thread/resume",
        json!({"threadId":child,"expectedDynamicTools":tools()}),
    )
    .await?;
    assert!(resumed.get("result").is_some(), "{resumed}");
    let queue = rpc(
        &mut recovered,
        4,
        "thread/queue/list",
        json!({"threadId":child}),
    )
    .await?;
    assert!(
        queue.to_string().contains("queued-child-assignment"),
        "{queue}"
    );
    let start = rpc(
        &mut recovered,
        5,
        "turn/start",
        json!({"threadId":child,"input":[{"type":"text","text":"must remain gated"}]}),
    )
    .await?;
    assert!(start.get("error").is_some(), "{start}");
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn controller_disconnect_interrupts_streaming_inference_before_native_dispatch() -> Result<()>
{
    use core_test_support::streaming_sse::StreamingSseChunk;
    use core_test_support::streaming_sse::start_streaming_sse_server;
    let (release, gate) = tokio::sync::oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![vec![
        StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![responses::ev_response_created("inflight")]),
        },
        StreamingSseChunk {
            gate: Some(gate),
            body: responses::sse(vec![
                responses::ev_function_call(
                    "late-command",
                    "exec_command",
                    r#"{"cmd":"echo must-not-execute"}"#,
                ),
                responses::ev_completed("inflight"),
            ]),
        },
    ]])
    .await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), server.uri(), "never")?;
    let credential = home.path().join("controller-token");
    std::fs::write(&credential, TOKEN)?;
    let (_process, address) = spawn_websocket_server_with_args(
        home.path(),
        "ws://127.0.0.1:0",
        &[
            "--controller-token-file".to_string(),
            credential.display().to_string(),
        ],
    )
    .await?;
    let mut controller = connect_websocket(address).await?;
    let mut observer = connect_websocket(address).await?;
    initialize(&mut controller).await?;
    initialize(&mut observer).await?;
    rpc(
        &mut controller,
        2,
        "control/acquire",
        json!({"token":TOKEN}),
    )
    .await?;
    let started = rpc(&mut controller, 3, "thread/start", json!({})).await?;
    let thread = &started["result"]["thread"]["id"];
    rpc(
        &mut observer,
        2,
        "thread/observe",
        json!({"threadId":thread}),
    )
    .await?;
    rpc(
        &mut controller,
        4,
        "thread/ready",
        json!({"threadId":thread}),
    )
    .await?;
    rpc(
        &mut controller,
        5,
        "turn/start",
        json!({"threadId":thread,"input":[{"type":"text","text":"begin"}]}),
    )
    .await?;
    timeout(DEFAULT_READ_TIMEOUT, async {
        tokio::select! {
            _ = server.wait_for_request_count(1) => Ok::<_, anyhow::Error>(()),
            result = async { loop { let _ = hosted_call(&mut controller).await?; } } => result,
        }
    })
    .await??;
    controller.close(None).await?;
    // Observe the synchronous fence before releasing a late provider tool response.
    let mut aborted = false;
    loop {
        let event = receive(&mut observer).await?;
        assert!(
            event.get("id").is_none(),
            "observer received callback: {event}"
        );
        assert_ne!(event["params"]["item"]["type"], "commandExecution");
        if event["method"] == "turn/completed" {
            aborted = event["params"]["turn"]["status"] == "interrupted";
        }
        if event["method"] == "control/status/changed" && event["params"]["state"] == "fenced" {
            break;
        }
    }
    let _ = release.send(());
    let mut drained = false;
    loop {
        let event = receive(&mut observer).await?;
        assert!(
            event.get("id").is_none(),
            "observer received callback: {event}"
        );
        assert_ne!(event["params"]["item"]["type"], "commandExecution");
        if event["method"] == "turn/completed" {
            aborted = event["params"]["turn"]["status"] == "interrupted";
        }
        if event["method"] == "control/status/changed"
            && event["params"]["shutdown"] == "sessionsStopped"
        {
            drained = true;
        }
        if drained && aborted {
            break;
        }
    }
    assert!(
        aborted,
        "controller loss must publish interrupted turn completion"
    );
    assert_eq!(server.requests().await.len(), 1);
    server.shutdown().await;
    Ok(())
}
