use std::collections::VecDeque;

/// Bounded retained output, independent of whether the consumer follows live chunks.
#[derive(Default)]
pub struct OutputTail {
    bytes: VecDeque<u8>,
    discarded: bool,
}

impl OutputTail {
    pub const CAPACITY: usize = 256 * 1024;

    pub fn push(&mut self, bytes: &[u8]) {
        let drop = self
            .bytes
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(Self::CAPACITY);
        self.discarded |= drop != 0;
        let retained_drop = drop.min(self.bytes.len());
        self.bytes.drain(..retained_drop);
        self.bytes.extend(&bytes[drop - retained_drop..]);
    }

    pub fn read(&self, limit: usize) -> Vec<u8> {
        self.bytes
            .iter()
            .skip(self.bytes.len().saturating_sub(limit))
            .copied()
            .collect()
    }

    pub fn truncated(&self, limit: usize) -> bool {
        self.discarded || self.bytes.len() > limit
    }
}
