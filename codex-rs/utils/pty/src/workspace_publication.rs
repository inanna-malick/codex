//! A single process-owned publication lease survives individual HTTP requests.

use super::SnapshotAdmission;
use super::SnapshotGuard;
use super::WorkspaceAdmission;
use std::io;
use std::num::NonZeroU64;

#[derive(Default)]
pub(super) struct Publication {
    completed: u64,
    active: Option<(NonZeroU64, SnapshotGuard)>,
}

pub enum PublicationAdmission {
    Ready,
    Settled,
    Busy,
    Conflict,
    Unavailable(io::Error),
}

impl WorkspaceAdmission {
    /// Sequences increase within this exact native process. Replaying a completed
    /// sequence never starts another transition. The host persists its sequence
    /// before requesting admission and reconciles lost replies using the same one.
    pub fn begin_publication(&self, sequence: NonZeroU64) -> PublicationAdmission {
        let mut publication = self
            .publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sequence.get() <= publication.completed {
            return PublicationAdmission::Settled;
        }
        if let Some((active, _)) = &publication.active {
            return if *active == sequence {
                PublicationAdmission::Ready
            } else {
                PublicationAdmission::Conflict
            };
        }
        match self.try_snapshot() {
            SnapshotAdmission::Ready(guard) => {
                publication.active = Some((sequence, guard));
                PublicationAdmission::Ready
            }
            SnapshotAdmission::Busy => PublicationAdmission::Busy,
            SnapshotAdmission::Unavailable(error) => PublicationAdmission::Unavailable(error),
        }
    }

    /// Called only after the host confirms that its mount transition is settled.
    /// Refresh the native cwd before releasing writers: its old directory handle
    /// may refer to the generation that has just become an immutable lower layer.
    /// A failed refresh retains admission, so the host can repair and retry.
    pub fn finish_publication(&self, sequence: NonZeroU64) -> PublicationAdmission {
        let mut publication = self
            .publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sequence.get() <= publication.completed {
            return PublicationAdmission::Settled;
        }
        if publication
            .active
            .as_ref()
            .is_none_or(|(active, _)| *active != sequence)
        {
            return PublicationAdmission::Conflict;
        }
        if let Err(error) = std::env::set_current_dir(&self.cwd) {
            return PublicationAdmission::Unavailable(error);
        }
        publication.completed = sequence.get();
        publication.active = None;
        PublicationAdmission::Settled
    }
}
