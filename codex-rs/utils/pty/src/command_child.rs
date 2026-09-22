//! Keeps the command's resource receipt with the existing Tokio child owner.
use std::io;
use std::ops::Deref;
use std::ops::DerefMut;
use std::process::ExitStatus;
use std::process::Output;
use std::sync::Arc;
use tokio::process::Child;

use crate::workspace_admission::WriterScope;

pub struct CommandChild {
    child: Child,
    scope: Option<Arc<WriterScope>>,
}

impl CommandChild {
    pub(crate) fn new(child: Child, scope: Option<Arc<WriterScope>>) -> Self {
        Self { child, scope }
    }

    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        let status = self.child.wait().await?;
        if let Some(scope) = &self.scope
            && scope.resource_exhausted().await?
        {
            return Err(io::Error::other(
                "command resource limit exceeded (OOM); no automatic retry",
            ));
        }
        Ok(status)
    }

    pub async fn wait_with_output(self) -> io::Result<Output> {
        let mut output = self.child.wait_with_output().await?;
        if let Some(scope) = self.scope
            && scope.resource_exhausted().await?
        {
            use std::os::unix::process::ExitStatusExt;
            output.status = ExitStatus::from_raw(libc::SIGKILL);
            output.stderr.extend_from_slice(
                b"\nCommand resource limit exceeded (OOM); no automatic retry.\n",
            );
        }
        Ok(output)
    }
}

impl Deref for CommandChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.child
    }
}

impl DerefMut for CommandChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}
