ALTER TABLE host_tool_completions RENAME TO host_tool_completions_before_recovery_states;

CREATE TABLE host_tool_completions (
    thread_id TEXT NOT NULL,
    context_call_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'reconcile_pending', 'ready', 'acknowledged',
        'reattached_without_completion', 'not_submitted'
    )),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (thread_id, context_call_id)
);

INSERT INTO host_tool_completions (
    thread_id, context_call_id, state, created_at_ms, updated_at_ms
)
SELECT thread_id, context_call_id, state, created_at_ms, updated_at_ms
FROM host_tool_completions_before_recovery_states;

DROP TABLE host_tool_completions_before_recovery_states;

CREATE INDEX host_tool_completions_unresolved_by_thread
    ON host_tool_completions(thread_id, state)
    WHERE state IN ('pending', 'reconcile_pending', 'ready');
