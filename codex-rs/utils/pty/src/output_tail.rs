use std::collections::VecDeque;

/// Bounded retained output, independent of whether the consumer follows live chunks.
#[derive(Default)]
pub struct OutputTail {
    bytes: VecDeque<u8>,
    discarded: bool,
    total: u64,
    first_is_fragment: bool,
}

impl OutputTail {
    pub const CAPACITY: usize = 256 * 1024;

    pub fn push(&mut self, bytes: &[u8]) {
        self.push_limited(bytes, Self::CAPACITY);
    }

    /// Retain at most the owner's remaining allowance while still advancing positions.
    pub fn push_limited(&mut self, bytes: &[u8], allowance: usize) {
        let drop = self
            .bytes
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(allowance.min(Self::CAPACITY));
        self.total = self.total.saturating_add(bytes.len() as u64);
        if drop != 0 {
            let preceding = if drop <= self.bytes.len() {
                self.bytes.get(drop - 1).copied()
            } else {
                bytes.get(drop - self.bytes.len() - 1).copied()
            };
            self.first_is_fragment = preceding != Some(b'\n');
        }
        self.discarded |= drop != 0;
        let retained_drop = drop.min(self.bytes.len());
        self.bytes.drain(..retained_drop);
        self.bytes.extend(&bytes[drop - retained_drop..]);
    }

    pub fn total_len(&self) -> u64 {
        self.total
    }

    pub fn retained_len(&self) -> usize {
        self.bytes.len()
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

/// An immutable byte window with absolute positions in one output stream.
#[derive(Debug)]
pub struct OutputWindow {
    pub bytes: Vec<u8>,
    pub start: u64,
    pub end: u64,
    pub available_end: u64,
    pub retained_start: u64,
    pub lost_bytes: u64,
    pub leading_fragment: bool,
    pub trailing_fragment: bool,
}

/// One contiguous retained segment, possibly followed by a retention gap.
/// Positions always address the original byte stream, independently of decoding.
pub struct OutputSegment<'a> {
    pub bytes: &'a [u8],
    pub start: u64,
    pub available_end: u64,
    pub leading_fragment: bool,
}

impl OutputTail {
    /// `None` selects a tail; an offset selects a forward page. Reads never consume.
    pub fn page(&self, offset: Option<u64>, limit: usize) -> OutputWindow {
        let bytes: Vec<_> = self.bytes.iter().copied().collect();
        OutputSegment {
            bytes: &bytes,
            start: self.total - bytes.len() as u64,
            available_end: self.total,
            leading_fragment: self.first_is_fragment,
        }
        .page(offset, limit)
    }
}

impl OutputSegment<'_> {
    /// Page one segment without splitting valid UTF-8 or hiding an absent prefix.
    pub fn page(&self, offset: Option<u64>, limit: usize) -> OutputWindow {
        let requested = offset.unwrap_or_else(|| self.available_end.saturating_sub(limit as u64));
        let mut start = requested
            .max(self.start)
            .min(self.start + self.bytes.len() as u64);
        let mut index = (start - self.start) as usize;
        let mut end = index.saturating_add(limit).min(self.bytes.len());
        if offset.is_none()
            && index > 0
            && self.bytes.get(index - 1) != Some(&b'\n')
            && let Some(newline) = self.bytes[index..].iter().position(|b| *b == b'\n')
            && index + newline + 1 < end
        {
            index += newline + 1;
            start = self.start + index as u64;
        }
        if end < self.bytes.len() {
            while end > index && self.bytes[end] & 0xc0 == 0x80 {
                end -= 1;
            }
        }
        if offset.is_some()
            && end < self.bytes.len()
            && let Some(newline) = self.bytes[index..end].iter().rposition(|b| *b == b'\n')
        {
            end = index + newline + 1;
        }
        let leading_fragment = if index == 0 {
            self.leading_fragment
        } else {
            self.bytes[index - 1] != b'\n'
        };
        OutputWindow {
            bytes: self.bytes[index..end].to_vec(),
            start,
            end: self.start + end as u64,
            available_end: self.available_end,
            retained_start: self.start,
            lost_bytes: self.start.saturating_sub(requested),
            leading_fragment: index < end && leading_fragment,
            trailing_fragment: end > index && self.bytes[end - 1] != b'\n',
        }
    }
}

#[cfg(test)]
mod page_tests {
    use super::*;
    #[test]
    fn pages_preserve_positions_and_report_retention_gaps() {
        let mut tail = OutputTail::default();
        tail.push(b"one\ntwo\nthree\n");
        let first = tail.page(Some(0), 8);
        assert_eq!(first.bytes, b"one\ntwo\n");
        tail.push(b"four\n");
        assert_eq!(tail.page(Some(first.end), 64).bytes, b"three\nfour\n");
        tail.push(&vec![b'x'; OutputTail::CAPACITY]);
        let gap = tail.page(Some(first.end), 8);
        assert_eq!(gap.lost_bytes, 11);
        assert_eq!(gap.start, 19);
        assert!(gap.trailing_fragment);
    }
    #[test]
    fn empty_and_current_end_pages_do_not_invent_fragments() {
        let mut tail = OutputTail::default();
        let empty = tail.page(Some(0), 8192);
        assert!(empty.bytes.is_empty());
        assert_eq!((empty.start, empty.end, empty.available_end), (0, 0, 0));
        assert!(!empty.leading_fragment && !empty.trailing_fragment);
        tail.push(b"unfinished");
        let end = tail.page(Some(10), 8192);
        assert!(end.bytes.is_empty());
        assert!(!end.leading_fragment && !end.trailing_fragment);
        tail.push(b" line\n");
        assert_eq!(tail.page(Some(end.end), 8192).bytes, b" line\n");
    }

    #[test]
    fn retention_and_tail_line_boundaries_are_distinct() {
        let mut tail = OutputTail::default();
        tail.push(b"discarded\n");
        let retained = vec![b'x'; OutputTail::CAPACITY];
        tail.push(&retained);
        let page = tail.page(Some(0), 8192);
        assert_eq!(page.lost_bytes, 10);
        assert_eq!(page.retained_start, 10);
        assert!(!page.leading_fragment);
        tail.push(b"\nlast\n");
        let end = tail.page(None, 10);
        assert_eq!(end.bytes, b"last\n");
        assert!(!end.leading_fragment && !end.trailing_fragment);
        assert_eq!(end.end, end.available_end);
        assert_eq!(end.lost_bytes, 0);
        assert!(end.retained_start > 0);
    }

    #[test]
    fn invalid_bytes_remain_data_and_pages_are_nonconsuming() {
        let mut tail = OutputTail::default();
        tail.push(b"ok\n\xffbad\n");
        let first = tail.page(Some(0), 8192);
        let repeated = tail.page(Some(0), 8192);
        assert_eq!(first.bytes, repeated.bytes);
        assert_eq!(first.bytes, b"ok\n\xffbad\n");
        assert!(String::from_utf8(first.bytes).is_err());
    }

    #[test]
    fn pages_keep_unicode_and_fragment_long_lines() {
        let mut tail = OutputTail::default();
        tail.push("abλcd\nx\n".as_bytes());
        let first = tail.page(Some(0), 3);
        assert_eq!(first.bytes, b"ab");
        assert!(first.trailing_fragment);
        let second = tail.page(Some(first.end), 8);
        assert_eq!(String::from_utf8(second.bytes).unwrap(), "λcd\nx\n");
        assert!(second.leading_fragment);
    }
}
