//! Retain a local launch behind the existing process handle while admission waits.
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::ProcessHandle;
use crate::ProcessSignal;
use crate::SpawnedProcess;
use crate::TerminalSize;
use crate::process::ChildTerminator;

#[derive(Default)]
struct Control {
    session: Option<Arc<ProcessHandle>>,
    size: Option<TerminalSize>,
}

struct DeferredTerminator {
    control: Arc<Mutex<Control>>,
    cancelled: watch::Sender<bool>,
}
impl ChildTerminator for DeferredTerminator {
    fn signal(&mut self, signal: ProcessSignal) -> io::Result<()> {
        let control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &control.session {
            Some(session) => session.signal(signal),
            None => {
                self.cancelled.send_replace(true);
                Ok(())
            }
        }
    }

    fn kill(&mut self) -> io::Result<()> {
        self.cancelled.send_replace(true);
        Ok(())
    }
}

/// Return a controllable handle immediately. `keepalive` belongs to the process
/// lifetime, including queued admission and descendants retaining output pipes.
pub fn defer_process<F, K>(launch: F) -> SpawnedProcess
where
    F: Future<Output = anyhow::Result<(SpawnedProcess, K)>> + Send + 'static,
    K: Send + 'static,
{
    let (stdin, mut input) = mpsc::channel::<Vec<u8>>(32);
    let (stdout, stdout_rx) = mpsc::channel(256);
    let (stderr, stderr_rx) = mpsc::channel(256);
    let (exit, exit_rx) = oneshot::channel();
    let (cancel, mut cancelled) = watch::channel(false);
    let control = Arc::new(Mutex::new(Control::default()));
    let execution_control = control.clone();
    let exited = Arc::new(AtomicBool::new(false));
    let exit_code = Arc::new(Mutex::new(None));
    let execution_exited = exited.clone();
    let execution_exit_code = exit_code.clone();
    let execution = tokio::spawn(async move {
        let publish_exit = |code| {
            *execution_exit_code
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(code);
            execution_exited.store(true, Ordering::SeqCst);
            let _ = exit.send(code);
        };
        let launched = tokio::select! {
            biased;
            _ = wait_cancelled(&mut cancelled) => None,
            result = launch => Some(result),
        };
        let Some(launched) = launched else {
            publish_exit(130);
            return;
        };
        let (
            SpawnedProcess {
                session,
                stdout_rx,
                stderr_rx,
                mut exit_rx,
            },
            _keepalive,
        ) = match launched {
            Ok(launched) => launched,
            Err(error) => {
                let _ = stderr
                    .send(format!("command did not start: {error}\n").into_bytes())
                    .await;
                publish_exit(-1);
                return;
            }
        };
        let session = Arc::new(session);
        {
            let mut control = execution_control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(size) = control.size {
                let _ = session.resize(size);
            }
            control.session = Some(session.clone());
        }
        // Backpressure is intentional. No broadcast hop may silently discard
        // output from a fast compiler before the native output owner sees it.
        let mut output = tokio::spawn(forward(stdout_rx, stdout));
        let mut errors = tokio::spawn(forward(stderr_rx, stderr));
        let writer = session.clone();
        let input_task = tokio::spawn(async move {
            while let Some(bytes) = input.recv().await {
                if writer.writer_sender().send(bytes).await.is_err() {
                    return;
                }
            }
            writer.close_stdin();
        });
        let code = tokio::select! {
            biased;
            _ = wait_cancelled(&mut cancelled) => {
                session.request_terminate();
                exit_rx.await.unwrap_or(-1)
            }
            result = &mut exit_rx => result.unwrap_or(-1),
        };
        input_task.abort();
        publish_exit(code);
        // Root exit and EOF are distinct. Preserve cancellation and the lease
        // while descendants still hold pipes; explicit termination bounds drain.
        let output_abort = output.abort_handle();
        let errors_abort = errors.abort_handle();
        let drained = async {
            let _ = tokio::join!(&mut output, &mut errors);
        };
        tokio::pin!(drained);
        tokio::select! {
            _ = &mut drained => {}
            _ = wait_cancelled(&mut cancelled) => {
                session.request_terminate();
                if tokio::time::timeout(std::time::Duration::from_secs(2), &mut drained).await.is_err() {
                    session.terminate();
                    output_abort.abort();
                    errors_abort.abort();
                }
            }
        }
        execution_control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .session
            .take();
    });
    let resize_control = control.clone();
    let session = ProcessHandle::new(
        stdin,
        Box::new(DeferredTerminator {
            control,
            cancelled: cancel,
        }),
        tokio::spawn(async {}),
        Vec::new(),
        tokio::spawn(async {}),
        execution,
        exited,
        exit_code,
        /*pty_handles*/ None,
        Some(Box::new(move |size| {
            let mut control = resize_control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            control.size = Some(size);
            match &control.session {
                Some(session) => session.resize(size),
                None => Ok(()),
            }
        })),
    );
    SpawnedProcess {
        session,
        stdout_rx,
        stderr_rx,
        exit_rx,
    }
}

async fn forward(mut source: mpsc::Receiver<Vec<u8>>, target: mpsc::Sender<Vec<u8>>) {
    while let Some(bytes) = source.recv().await {
        if target.send(bytes).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
#[path = "deferred_tests.rs"]
mod tests;

async fn wait_cancelled(cancelled: &mut watch::Receiver<bool>) {
    let _ = cancelled.wait_for(|cancelled| *cancelled).await;
}
