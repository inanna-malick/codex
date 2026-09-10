CREATE TABLE host_tool_completions (
    thread_id TEXT NOT NULL,
    context_call_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'ready', 'acknowledged', 'reattached_without_completion'
    )),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (thread_id, context_call_id)
);

CREATE INDEX host_tool_completions_unresolved_by_thread
    ON host_tool_completions(thread_id, state)
    WHERE state IN ('pending', 'ready');
