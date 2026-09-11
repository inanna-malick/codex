//! Bounded job-owned log storage. Temporary files disappear with their owner.
use codex_utils_pty::OutputSegment;
use codex_utils_pty::OutputTail;
use codex_utils_pty::OutputWindow;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

#[derive(Default)]
pub(super) struct RetainedOutput {
    prefix: Option<File>,
    prefix_len: usize,
    prefix_closed: bool,
    tail: OutputTail,
}

impl RetainedOutput {
    pub(super) const PREFIX_LIMIT: usize = 16 * 1024 * 1024;

    pub(super) fn retained_len(&self) -> usize {
        self.prefix_len + self.tail.retained_len()
    }

    pub(super) fn growth(&self, incoming: usize) -> usize {
        let prefix = if self.prefix_closed {
            0
        } else {
            incoming.min(Self::PREFIX_LIMIT - self.prefix_len)
        };
        prefix + incoming.min(OutputTail::CAPACITY - self.tail.retained_len())
    }

    /// Storage exhaustion loses retention, never pipe drainage or stream positions.
    pub(super) fn push(&mut self, bytes: &[u8], allowance: usize) {
        // Keep a diagnostic tail even when the owner cannot retain the whole prefix.
        let tail_allowance = allowance.min(OutputTail::CAPACITY);
        let prefix_allowance = allowance.saturating_sub(tail_allowance);
        let remaining = prefix_allowance.saturating_sub(self.prefix_len);
        if !self.prefix_closed {
            let count = bytes
                .len()
                .min(Self::PREFIX_LIMIT - self.prefix_len)
                .min(remaining);
            if count > 0 {
                let result = (|| -> io::Result<()> {
                    if self.prefix.is_none() {
                        self.prefix = Some(tempfile::tempfile()?);
                    }
                    let file = self
                        .prefix
                        .as_mut()
                        .ok_or_else(|| io::Error::other("output file unavailable"))?;
                    file.seek(SeekFrom::Start(self.prefix_len as u64))?;
                    file.write_all(&bytes[..count])
                })();
                if result.is_ok() {
                    self.prefix_len += count;
                } else {
                    // Dropping the anonymous file also removes partially written data.
                    self.prefix = None;
                    self.prefix_len = 0;
                    self.prefix_closed = true;
                }
            }
            self.prefix_closed |= count < bytes.len();
        }
        self.tail
            .push_limited(bytes, allowance.saturating_sub(self.prefix_len));
    }

    // The owning Jobs mutex serializes reads and writes, including this file cursor.
    pub(super) fn page(&self, offset: Option<u64>, limit: usize) -> io::Result<OutputWindow> {
        let Some(offset) = offset.filter(|offset| *offset < self.prefix_len as u64) else {
            let mut page = self.tail.page(offset, limit);
            if self.prefix_len > 0 {
                // The tail's start is not the owner's earliest retained byte.
                // Actual gaps remain explicit in lost_bytes and page positions.
                page.retained_start = 0;
            }
            return Ok(page);
        };
        let base = offset.saturating_sub(1);
        let count = (self.prefix_len as u64 - base).min(limit.saturating_add(5) as u64) as usize;
        let mut bytes = vec![0; count];
        let mut file = self
            .prefix
            .as_ref()
            .ok_or_else(|| io::Error::other("retained output file unavailable"))?;
        file.seek(SeekFrom::Start(base))?;
        file.read_exact(&mut bytes)?;
        let total = self.tail.total_len();
        let mut page = OutputSegment {
            bytes: &bytes,
            start: base,
            available_end: total,
            leading_fragment: base != 0,
        }
        .page(Some(offset), limit);
        page.retained_start = 0;
        Ok(page)
    }
}

#[cfg(test)]
#[path = "command_output_tests.rs"]
mod tests;
