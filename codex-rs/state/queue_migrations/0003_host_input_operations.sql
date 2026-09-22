CREATE TABLE host_input_operations (
    thread_id TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    content_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'ready', 'dispatching', 'presented', 'withdrawn', 'rejected', 'unknown'
    )),
    queue_item_id TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (producer_id, sequence)
);

CREATE TABLE host_input_producer_seals (
    thread_id TEXT NOT NULL,
    producer_id TEXT PRIMARY KEY NOT NULL,
    sealed_at_ms INTEGER NOT NULL
);

CREATE INDEX host_input_operations_thread_state_idx
    ON host_input_operations(thread_id, state);
