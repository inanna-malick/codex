CREATE TABLE host_input_producer_watermarks (
    thread_id TEXT NOT NULL,
    producer_id TEXT PRIMARY KEY NOT NULL,
    through_sequence INTEGER NOT NULL CHECK (through_sequence > 0),
    acknowledged_at_ms INTEGER NOT NULL
);

CREATE TABLE host_input_withdrawal_tombstones (
    thread_id TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    withdrawn_at_ms INTEGER NOT NULL,
    PRIMARY KEY (producer_id, sequence)
);
