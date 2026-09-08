use super::*;
use crate::TerminalSize;
use std::collections::HashMap;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires writable delegated cgroup v2"]
async fn writer_admission_tracks_detached_descendants() {
    let owner = Arc::new(WorkspaceAdmission::create().expect("delegated writer scope"));
    let mutation = owner.mutation().await;
    assert!(matches!(owner.try_snapshot(), SnapshotAdmission::Busy));
    drop(mutation);
    let SnapshotAdmission::Ready(snapshot) = owner.try_snapshot() else {
        panic!("idle admission");
    };
    let (started, mut entered) = tokio::sync::oneshot::channel();
    let waiting_owner = owner.clone();
    let waiter = tokio::spawn(async move {
        let _mutation = waiting_owner.mutation().await;
        started.send(()).expect("waiting test receiver");
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut entered)
            .await
            .is_err()
    );
    drop(snapshot);
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("admission resumed")
        .expect("writer entered");
    waiter.await.expect("writer completed");

    let root = std::env::temp_dir().join(format!("codex-writer-admission-{}", std::process::id()));
    std::fs::create_dir(&root).expect("exclusive test directory");
    let env: HashMap<String, String> = std::env::vars().collect();
    enum Launch {
        Pipe,
        Pty,
        Direct,
    }
    for launch in [Launch::Pipe, Launch::Pty, Launch::Direct] {
        let release = root.join("release");
        let args = vec![
            "-c".to_owned(),
            include_str!("workspace_admission_worker.py").to_owned(),
            release.to_str().expect("test path").to_owned(),
        ];
        let (exit, session) = match launch {
            Launch::Direct => {
                let mut command = tokio::process::Command::new("python3");
                command.args(&args).current_dir(&root);
                let SnapshotAdmission::Ready(snapshot) = owner.try_snapshot() else {
                    panic!("idle before direct launch");
                };
                let spawn = owner.spawn_command(command);
                tokio::pin!(spawn);
                assert!(
                    tokio::time::timeout(Duration::from_millis(25), &mut spawn)
                        .await
                        .is_err()
                );
                drop(snapshot);
                let child = spawn.await.expect("direct command");
                let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
                    .await
                    .expect("leader deadline")
                    .expect("leader exit");
                (output.status.code().expect("normal exit"), None)
            }
            Launch::Pipe | Launch::Pty => {
                let spawned = match launch {
                    Launch::Pty => {
                        owner
                            .track_process(crate::spawn_pty_process(
                                "python3",
                                &args,
                                &root,
                                &env,
                                &None,
                                TerminalSize::default(),
                                &[],
                            ))
                            .await
                    }
                    Launch::Pipe => {
                        owner
                            .track_process(crate::spawn_pipe_process_no_stdin(
                                "python3",
                                &args,
                                &root,
                                &env,
                                &None,
                                &[],
                            ))
                            .await
                    }
                    Launch::Direct => unreachable!(),
                }
                .expect("tracked process");
                let exit = tokio::time::timeout(Duration::from_secs(5), spawned.exit_rx)
                    .await
                    .expect("leader deadline")
                    .expect("leader exit");
                (exit, Some(spawned.session))
            }
        };
        assert_eq!(exit, 0);
        assert!(
            matches!(owner.try_snapshot(), SnapshotAdmission::Busy),
            "closed stdio and leader exit do not settle descendants"
        );
        std::fs::write(&release, b"done").expect("release descendant");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match owner.try_snapshot() {
                    SnapshotAdmission::Ready(_) => break,
                    SnapshotAdmission::Busy => tokio::time::sleep(Duration::from_millis(10)).await,
                    SnapshotAdmission::Unavailable(error) => panic!("kernel accounting: {error}"),
                }
            }
        })
        .await
        .expect("descendants settled");
        drop(session);
        std::fs::remove_file(release).expect("remove release marker");
    }
    let missing = tokio::process::Command::new(root.join("missing-executable"));
    assert!(owner.spawn_command(missing).await.is_err());
    assert!(matches!(owner.try_snapshot(), SnapshotAdmission::Ready(_)));
    std::fs::remove_dir_all(root).expect("test cleanup");
    let group = owner.cgroup_path().to_owned();
    drop(owner);
    assert!(!group.exists());
}
