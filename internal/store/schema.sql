CREATE TABLE IF NOT EXISTS chats (
    jid TEXT PRIMARY KEY,
    kind TEXT NOT NULL, -- dm|group|broadcast|newsletter|unknown
    name TEXT,
    last_message_ts INTEGER,
    archived INTEGER NOT NULL DEFAULT 0,
    pinned INTEGER NOT NULL DEFAULT 0,
    muted_until INTEGER NOT NULL DEFAULT 0,
    unread INTEGER NOT NULL DEFAULT 0,
    unread_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS contacts (
    jid TEXT PRIMARY KEY,
    phone TEXT,
    push_name TEXT,
    full_name TEXT,
    first_name TEXT,
    business_name TEXT,
    system_name TEXT,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS groups (
    jid TEXT PRIMARY KEY,
    name TEXT,
    owner_jid TEXT,
    created_ts INTEGER,
    is_parent INTEGER NOT NULL DEFAULT 0,
    linked_parent_jid TEXT,
    left_at INTEGER,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS group_participants (
    group_jid TEXT NOT NULL,
    user_jid TEXT NOT NULL,
    role TEXT,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (group_jid, user_jid),
    FOREIGN KEY (group_jid) REFERENCES groups(jid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS contact_aliases (
    jid TEXT PRIMARY KEY,
    alias TEXT NOT NULL,
    notes TEXT,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS contact_tags (
    jid TEXT NOT NULL,
    tag TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (jid, tag)
);

CREATE TABLE IF NOT EXISTS messages (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_jid TEXT NOT NULL,
    chat_name TEXT,
    msg_id TEXT NOT NULL,
    sender_jid TEXT,
    sender_name TEXT,
    ts INTEGER NOT NULL,
    from_me INTEGER NOT NULL,
    text TEXT,
    display_text TEXT,
    quoted_msg_id TEXT,
    quoted_sender_jid TEXT,
    is_forwarded INTEGER NOT NULL DEFAULT 0,
    forwarding_score INTEGER NOT NULL DEFAULT 0,
    reaction_to_id TEXT,
    reaction_emoji TEXT,
    media_type TEXT,
    media_caption TEXT,
    filename TEXT,
    mime_type TEXT,
    direct_path TEXT,
    media_key BLOB,
    file_sha256 BLOB,
    file_enc_sha256 BLOB,
    file_length INTEGER,
    local_path TEXT,
    downloaded_at INTEGER,
    media_unavailable_at INTEGER,
    revoked INTEGER NOT NULL DEFAULT 0,
    deleted_for_me INTEGER NOT NULL DEFAULT 0,
    deleted_at INTEGER,
    deletion_reason TEXT,
    payload_purged_at INTEGER,
    edited INTEGER NOT NULL DEFAULT 0,
    edited_ts INTEGER NOT NULL DEFAULT 0,
    buttons TEXT,
    UNIQUE(chat_jid, msg_id),
    FOREIGN KEY (chat_jid) REFERENCES chats(jid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_jid, ts);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);

CREATE TABLE IF NOT EXISTS message_payload_purges (
    chat_jid TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    purged_at INTEGER NOT NULL,
    deleted_at INTEGER NOT NULL,
    deletion_reason TEXT NOT NULL,
    PRIMARY KEY (chat_jid, msg_id)
);

CREATE TABLE IF NOT EXISTS message_local_media_aliases (
    chat_jid TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    local_path TEXT NOT NULL,
    downloaded_at INTEGER,
    PRIMARY KEY (chat_jid, msg_id, local_path),
    FOREIGN KEY (chat_jid, msg_id) REFERENCES messages(chat_jid, msg_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS status_messages (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    msg_id TEXT NOT NULL UNIQUE,
    ts INTEGER NOT NULL,
    from_me INTEGER NOT NULL,
    sender_jid TEXT,
    sender_name TEXT,
    text TEXT,
    media_type TEXT,
    media_caption TEXT,
    filename TEXT,
    mime_type TEXT,
    direct_path TEXT,
    media_key BLOB,
    file_sha256 BLOB,
    file_enc_sha256 BLOB,
    file_length INTEGER,
    background_color TEXT,
    font INTEGER
);

CREATE INDEX IF NOT EXISTS idx_status_messages_ts ON status_messages(ts);

CREATE TABLE IF NOT EXISTS call_events (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_jid TEXT NOT NULL,
    chat_name TEXT,
    sender_jid TEXT,
    sender_name TEXT,
    call_id TEXT NOT NULL,
    msg_id TEXT,
    event_type TEXT NOT NULL,
    direction TEXT,
    media TEXT,
    outcome TEXT,
    reason TEXT,
    call_type TEXT,
    duration_secs INTEGER NOT NULL DEFAULT 0,
    ts INTEGER NOT NULL,
    participants TEXT,
    UNIQUE(chat_jid, call_id, event_type, ts),
    FOREIGN KEY (chat_jid) REFERENCES chats(jid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_call_events_chat_ts ON call_events(chat_jid, ts);
CREATE INDEX IF NOT EXISTS idx_call_events_ts ON call_events(ts);

CREATE TABLE IF NOT EXISTS starred (
    chat_jid TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    sender_jid TEXT,
    from_me INTEGER NOT NULL DEFAULT 0,
    starred_at INTEGER NOT NULL,
    PRIMARY KEY (chat_jid, msg_id)
);

CREATE INDEX IF NOT EXISTS idx_starred_starred_at ON starred(starred_at);

CREATE TABLE IF NOT EXISTS polls (
    chat_jid TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    sender_jid TEXT,
    question TEXT NOT NULL,
    options_json TEXT NOT NULL,
    selectable_count INTEGER NOT NULL DEFAULT 1,
    created_ts INTEGER NOT NULL,
    PRIMARY KEY (chat_jid, msg_id)
);

CREATE INDEX IF NOT EXISTS idx_polls_chat_ts ON polls(chat_jid, created_ts);

CREATE TABLE IF NOT EXISTS message_locations (
    chat_jid TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    name TEXT,
    address TEXT,
    is_live INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chat_jid, msg_id)
);

CREATE TABLE IF NOT EXISTS poll_votes (
    chat_jid TEXT NOT NULL,
    poll_msg_id TEXT NOT NULL,
    voter_jid TEXT NOT NULL,
    vote_msg_id TEXT NOT NULL,
    selected_options_json TEXT NOT NULL,
    ts INTEGER NOT NULL,
    PRIMARY KEY (chat_jid, poll_msg_id, voter_jid)
);

CREATE INDEX IF NOT EXISTS idx_poll_votes_poll ON poll_votes(chat_jid, poll_msg_id);

-- Append-only protocol event ledger. Rows may never be updated or deleted:
-- state changes (scrub, identity resolution, supersession) are expressed as
-- new events, and readers compute the effective set.
CREATE TABLE IF NOT EXISTS ledger_events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    source         TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    wa_key         TEXT NOT NULL DEFAULT '',
    chat_jid       TEXT NOT NULL DEFAULT '',
    msg_id         TEXT NOT NULL DEFAULT '',
    sender_jid     TEXT NOT NULL DEFAULT '',
    server_ts      INTEGER NOT NULL DEFAULT 0,
    event_ts       INTEGER NOT NULL DEFAULT 0,
    received_at    INTEGER NOT NULL,
    raw_hash       TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    rules_version  TEXT NOT NULL,
    dedup_key      TEXT NOT NULL,
    causal_refs    TEXT NOT NULL DEFAULT '[]',
    causes         TEXT NOT NULL DEFAULT '[]',
    batch_id       INTEGER NOT NULL DEFAULT 0,
    flags          INTEGER NOT NULL DEFAULT 0,
    -- Snapshot/tombstone payload for events without retained raw bytes
    -- (legacy_snapshot, scrub) so projectors remain replayable.
    snapshot       TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_ledger_dedup_key ON ledger_events(dedup_key);
CREATE INDEX IF NOT EXISTS idx_ledger_wa_key ON ledger_events(wa_key);
CREATE INDEX IF NOT EXISTS idx_ledger_chat_jid ON ledger_events(chat_jid);
CREATE INDEX IF NOT EXISTS idx_ledger_msg_id ON ledger_events(msg_id);
CREATE INDEX IF NOT EXISTS idx_ledger_event_ts ON ledger_events(event_ts);

-- Raw protocol envelope bytes, kept separate from the immutable event rows so
-- payload purge can erase content (delete this row + append a scrub event)
-- without mutating ledger_events.
CREATE TABLE IF NOT EXISTS ledger_raw (
    event_id  TEXT PRIMARY KEY,
    encoding  TEXT NOT NULL,
    raw_bytes BLOB NOT NULL,
    FOREIGN KEY (event_id) REFERENCES ledger_events(event_id)
);

CREATE TABLE IF NOT EXISTS projector_checkpoints (
    view              TEXT PRIMARY KEY,
    last_seq          INTEGER NOT NULL,
    projector_version TEXT NOT NULL,
    updated_at        INTEGER NOT NULL
);

-- Per-view projection mode: shadow (write only to shadow tables) or live.
CREATE TABLE IF NOT EXISTS ledger_view_state (
    view TEXT PRIMARY KEY,
    mode TEXT NOT NULL DEFAULT 'shadow'
);

CREATE TABLE IF NOT EXISTS ledger_batches (
    batch_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at INTEGER NOT NULL,
    source     TEXT NOT NULL DEFAULT ''
);

-- Late causal resolution: links appended after the event row when its target
-- arrives later. Insert-only; event.causes covers refs resolved at append time.
CREATE TABLE IF NOT EXISTS ledger_causal_links (
    event_id  TEXT NOT NULL,
    ref_index INTEGER NOT NULL,
    target_id TEXT NOT NULL,
    PRIMARY KEY (event_id, ref_index)
);

CREATE INDEX IF NOT EXISTS idx_ledger_links_target ON ledger_causal_links(target_id);

-- INSERT OR REPLACE deletes rows without firing DELETE triggers, so guard the
-- BEFORE INSERT path as well. The trigger fires before REPLACE resolves its
-- uniqueness conflict and therefore blocks the overwrite.
CREATE TRIGGER IF NOT EXISTS ledger_events_no_replace
BEFORE INSERT ON ledger_events
WHEN EXISTS (SELECT 1 FROM ledger_events WHERE event_id = NEW.event_id)
BEGIN
    SELECT RAISE(ABORT, 'ledger event_id already exists: ledger_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS ledger_events_no_update
BEFORE UPDATE ON ledger_events
BEGIN
    SELECT RAISE(ABORT, 'ledger_events is append-only: express state changes as new events');
END;

CREATE TRIGGER IF NOT EXISTS ledger_events_no_delete
BEFORE DELETE ON ledger_events
BEGIN
    SELECT RAISE(ABORT, 'ledger_events is append-only: deletes are not allowed');
END;

CREATE TRIGGER IF NOT EXISTS ledger_links_no_replace
BEFORE INSERT ON ledger_causal_links
WHEN EXISTS (
    SELECT 1 FROM ledger_causal_links
    WHERE event_id = NEW.event_id AND ref_index = NEW.ref_index
)
BEGIN
    SELECT RAISE(ABORT, 'causal link already exists: ledger_causal_links is insert-only');
END;

CREATE TRIGGER IF NOT EXISTS ledger_links_no_update
BEFORE UPDATE ON ledger_causal_links
BEGIN
    SELECT RAISE(ABORT, 'ledger_causal_links is insert-only');
END;

CREATE TRIGGER IF NOT EXISTS ledger_links_no_delete
BEFORE DELETE ON ledger_causal_links
BEGIN
    SELECT RAISE(ABORT, 'ledger_causal_links is insert-only');
END;
