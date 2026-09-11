use super::*;
use pretty_assertions::assert_eq;

#[test]
fn complete_output_survives_tail_rotation_and_repeated_reads() {
    let mut output = RetainedOutput::default();
    let data = "row λ\n".repeat(90_000).into_bytes();
    output.push(&data, RetainedOutput::PREFIX_LIMIT + OutputTail::CAPACITY);
    let page = output.page(Some(0), 1024 * 1024).unwrap();
    assert_eq!(page.bytes, data);
    assert_eq!(
        (page.start, page.end, page.lost_bytes),
        (0, data.len() as u64, 0)
    );
    assert_eq!(output.page(Some(0), 1024 * 1024).unwrap().bytes, page.bytes);
}

#[test]
fn prefix_and_tail_expose_the_gap_without_stopping_positions() {
    let mut output = RetainedOutput::default();
    let allowance = OutputTail::CAPACITY + 16;
    output.push(b"first-line\nnext\n", allowance);
    output.push(&vec![b'x'; OutputTail::CAPACITY + 50], allowance);
    assert!(output.retained_len() <= allowance);
    let first = output.page(Some(0), 65536).unwrap();
    assert_eq!(first.bytes, b"first-line\nnext\n");
    let gap = output.page(Some(first.end), 64).unwrap();
    assert_eq!(gap.lost_bytes, 50);
    assert_eq!(gap.start, first.end + 50);
    assert!(gap.leading_fragment && gap.trailing_fragment);
    output.push(b"\nlast\n", allowance);
    assert_eq!(output.page(None, 6).unwrap().bytes, b"last\n");
}

#[test]
fn no_allowance_still_drains_and_reports_lost_bytes() {
    let mut output = RetainedOutput::default();
    output.push(b"unretained", 0);
    assert_eq!(output.retained_len(), 0);
    let page = output.page(Some(0), 65536).unwrap();
    assert!(page.bytes.is_empty());
    assert_eq!(
        (page.start, page.end, page.available_end, page.lost_bytes),
        (10, 10, 10, 10)
    );
    output.push(b"later", 64);
    let page = output.page(Some(0), 65536).unwrap();
    assert_eq!(page.bytes, b"later");
    assert_eq!(page.lost_bytes, 10);
}

#[test]
fn prefix_pages_preserve_unicode_and_partial_lines() {
    let mut output = RetainedOutput::default();
    output.push("αβ\nγδ\n".as_bytes(), 1024 * 1024);
    let first = output.page(Some(0), 6).unwrap();
    assert_eq!(first.bytes, "αβ\n".as_bytes());
    let next = output.page(Some(first.end), 6).unwrap();
    assert_eq!(next.bytes, "γδ\n".as_bytes());
    assert!(!next.leading_fragment && !next.trailing_fragment);
}

#[test]
fn sustained_output_bounds_disk_and_memory_independently_of_total_output() {
    let mut output = RetainedOutput::default();
    let chunk = vec![b'x'; 64 * 1024];
    let allowance = RetainedOutput::PREFIX_LIMIT + OutputTail::CAPACITY;
    for _ in 0..300 {
        output.push(&chunk, allowance);
        assert!(output.retained_len() <= allowance);
    }
    assert_eq!(
        output.prefix.as_ref().unwrap().metadata().unwrap().len(),
        RetainedOutput::PREFIX_LIMIT as u64
    );
    let tail = output.page(None, 64 * 1024).unwrap();
    assert_eq!(tail.available_end, 300 * chunk.len() as u64);
    assert_eq!(tail.end, tail.available_end);
    assert_eq!(output.retained_len(), allowance);
}
