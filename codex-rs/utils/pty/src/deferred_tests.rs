use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::time::timeout;

struct Lease(Arc<AtomicUsize>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn queued_cancellation_drops_launch_without_starting() {
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = Lease(drops.clone());
    let (_release, admission) = oneshot::channel::<()>();
    let process = defer_process::<_, ()>(async move {
        let _lease = lease;
        admission.await?;
        panic!("cancelled launch must never proceed");
    });
    process.session.request_terminate();
    assert_eq!(
        timeout(Duration::from_secs(2), process.exit_rx)
            .await
            .unwrap()
            .unwrap(),
        130
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(process.session.exit_code(), Some(130));
}

#[tokio::test]
async fn root_exit_preserves_pipe_tail_and_lease_until_descendants_close() {
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = Lease(drops.clone());
    let (stdout, stdout_rx) = mpsc::channel(1);
    let (stderr, stderr_rx) = mpsc::channel(1);
    let (done, exit_rx) = oneshot::channel();
    let session =
        defer_process(std::future::pending::<anyhow::Result<(SpawnedProcess, ())>>()).session;
    let mut process = defer_process(async move {
        Ok((
            SpawnedProcess {
                session,
                stdout_rx,
                stderr_rx,
                exit_rx,
            },
            lease,
        ))
    });
    done.send(7).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), &mut process.exit_rx)
            .await
            .unwrap()
            .unwrap(),
        7
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    stdout
        .send(b"last descendant output".to_vec())
        .await
        .unwrap();
    assert_eq!(
        process.stdout_rx.recv().await.unwrap(),
        b"last descendant output"
    );
    drop(stdout);
    drop(stderr);
    timeout(Duration::from_secs(2), async {
        while drops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn deferred_pipe_preserves_stdin_close_and_all_output() {
    let process = defer_process(async {
        crate::spawn_pipe_process(
            "/bin/sh",
            &["-c".into(), "cat; printf done >&2".into()],
            std::path::Path::new("/tmp"),
            &std::collections::HashMap::from([("PATH".into(), std::env::var("PATH").unwrap())]),
            &None,
            &[],
        )
        .await
        .map(|process| (process, ()))
    });
    let SpawnedProcess {
        session,
        mut stdout_rx,
        mut stderr_rx,
        exit_rx,
    } = process;
    let read = tokio::spawn(async move {
        let mut output = Vec::new();
        while let Some(chunk) = stdout_rx.recv().await {
            output.extend(chunk);
        }
        output
    });
    let errors = tokio::spawn(async move {
        let mut output = Vec::new();
        while let Some(chunk) = stderr_rx.recv().await {
            output.extend(chunk);
        }
        output
    });
    let input = vec![b'x'; 1024 * 1024];
    session.writer_sender().send(input.clone()).await.unwrap();
    session.close_stdin();
    assert_eq!(
        timeout(Duration::from_secs(5), exit_rx)
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let output = timeout(Duration::from_secs(5), read)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), input.len());
    assert!(output == input, "output bytes changed");
    assert_eq!(errors.await.unwrap(), b"done");
}
