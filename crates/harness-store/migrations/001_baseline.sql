-- Baseline schema for the session store, squashed from the pre-release
-- migration history (v1 base tables + v2 provenance columns + v3 history
-- markers). Development-stage policy: no deployed databases exist, so schema
-- changes before release rewrite THIS file; after release, never edit it —
-- append a new versioned file under sql/ and register it in
-- `MIGRATIONS` (src/turso.rs) instead.
--
-- Each registered file must be runnable inside one transaction (the runner
-- wraps DDL and its _session_migrations record together) and idempotent
-- under CREATE IF NOT EXISTS so a retried migration cannot half-apply.

CREATE TABLE IF NOT EXISTS sessions (
    session_id    TEXT PRIMARY KEY,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS units (
    event_id        TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    branch_id       TEXT NOT NULL,
    epoch           INTEGER NOT NULL,
    event_order     INTEGER NOT NULL,
    message_seq     INTEGER NOT NULL,
    role            TEXT NOT NULL,
    kind            TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    parent_event_id TEXT,
    created_at_ms   INTEGER NOT NULL,
    -- Provenance (run-tagged appends): untagged appends leave run_id and
    -- idempotency_key NULL; every row carries its source family and the
    -- content hash verified on read.
    run_id          TEXT,
    source_kind     TEXT NOT NULL,
    tool_call_id    TEXT,
    idempotency_key TEXT,
    integrity_hash  TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id),
    UNIQUE(session_id, branch_id, epoch, event_order)
);

CREATE TABLE IF NOT EXISTS session_heads (
    session_id       TEXT PRIMARY KEY,
    branch_id        TEXT NOT NULL,
    epoch            INTEGER NOT NULL,
    head_event_id    TEXT,
    last_event_order INTEGER NOT NULL,
    last_message_seq INTEGER NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS run_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    event_json      TEXT NOT NULL,
    timestamp_ms    INTEGER NOT NULL,
    idempotency_key TEXT UNIQUE NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id),
    UNIQUE(run_id, seq)
);

CREATE TABLE IF NOT EXISTS history_markers (
    session_id         TEXT NOT NULL,
    branch_id          TEXT NOT NULL,
    from_epoch         INTEGER NOT NULL,
    to_epoch           INTEGER NOT NULL,
    cutoff_event_id    TEXT,
    cutoff_event_order INTEGER,
    boundary           TEXT NOT NULL,
    occurred_at_ms     INTEGER NOT NULL,
    PRIMARY KEY (session_id, branch_id, to_epoch),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_units_session_order
    ON units(session_id, event_order);

CREATE INDEX IF NOT EXISTS idx_units_run
    ON units(run_id);

-- Provenance dedupe is run-scoped: untagged appends (NULL key) are exempt.
CREATE UNIQUE INDEX IF NOT EXISTS idx_units_idempotency
    ON units(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_run_events_run_seq
    ON run_events(run_id, seq);

CREATE INDEX IF NOT EXISTS idx_history_markers_session
    ON history_markers(session_id, branch_id);

-- Recall-store FTS: turso has no SQLite FTS5 module, so the tantivy-backed
-- experimental index syntax powers the ngram search (requires
-- `experimental_index_method(true)` on the connection builder).
CREATE INDEX IF NOT EXISTS idx_run_events_fts
    ON run_events USING fts (event_json)
    WITH (tokenizer = 'ngram');
