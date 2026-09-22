//! Matched test: launched by Tidepool's delegated command-resource fixture.
#![cfg(target_os = "linux")]

use codex_utils_pty::SpawnedProcess;
use codex_utils_pty::TerminalSize;
use codex_utils_pty::spawn_pipe_process_no_stdin;
use codex_utils_pty::spawn_pty_process;
use codex_utils_pty::workspace_admission::track_process;
use std::collections::HashMap;
use std::path::Path;

async fn output(mut receiver: tokio::sync::mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(chunk) = receiver.recv().await {
        bytes.extend(chunk);
    }
    bytes
}

#[tokio::test]
#[ignore = "requires the matched Tidepool host and delegated cgroup fixture"]
async fn shoal_command_resources() {
    assert!(std::env::var_os("CODEX_COMMAND_RESOURCE_SOCKET").is_some());
    let env: HashMap<String, String> = std::env::vars().collect();
    let mut direct = tokio::process::Command::new("python3");
    direct.args(["-c", "a=bytearray(128*1024*1024)"]);
    let result = codex_utils_pty::workspace_admission::command_output(direct)
        .await
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("resource limit"));
    let mut direct = tokio::process::Command::new("python3");
    direct.args(["-c", "a=bytearray(128*1024*1024)"]);
    let mut child = codex_utils_pty::workspace_admission::spawn_command(direct)
        .await
        .unwrap();
    assert!(
        child
            .wait()
            .await
            .unwrap_err()
            .to_string()
            .contains("resource limit")
    );
    for pty in [false, true] {
        for (script, expected) in [
            (
                "import pathlib; p=pathlib.Path('/proc/self/cgroup').read_text(); assert '/commands/native/' in p; print('membership verified')",
                0,
            ),
            ("a=bytearray(128*1024*1024)", 137),
            ("print('still usable')", 0),
        ] {
            let args = vec!["-c".to_owned(), script.to_owned()];
            let spawned = track_process(async {
                if pty {
                    spawn_pty_process(
                        "python3",
                        &args,
                        Path::new("/tmp"),
                        &env,
                        &None,
                        TerminalSize::default(),
                        &[],
                    )
                    .await
                } else {
                    spawn_pipe_process_no_stdin(
                        "python3",
                        &args,
                        Path::new("/tmp"),
                        &env,
                        &None,
                        &[],
                    )
                    .await
                }
            })
            .await
            .unwrap();
            let SpawnedProcess {
                session,
                stdout_rx,
                stderr_rx,
                exit_rx,
            } = spawned;
            let stdout = tokio::spawn(output(stdout_rx));
            let stderr = tokio::spawn(output(stderr_rx));
            assert_eq!(exit_rx.await.unwrap(), expected);
            drop(session);
            match codex_utils_pty::workspace_admission::availability() {
                codex_utils_pty::workspace_admission::Availability::Ready(owner) => {
                    assert!(
                        matches!(
                            owner.try_snapshot(),
                            codex_utils_pty::workspace_admission::SnapshotAdmission::Ready(_)
                        ),
                        "completed command must permit workspace publication, including after OOM"
                    );
                }
                _ => panic!("managed workspace admission unavailable"),
            }
            let mut bytes = stdout.await.unwrap();
            bytes.extend(stderr.await.unwrap());
            let text = String::from_utf8_lossy(&bytes);
            if expected == 137 {
                assert!(text.contains("resource limit"), "{text}");
            } else {
                assert!(
                    text.contains("verified") || text.contains("still usable"),
                    "{text}"
                );
            }
        }
    }
}
