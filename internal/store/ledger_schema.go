package store

import "fmt"

// ledgerSchemaSQL mirrors the ledger objects at the end of schema.sql. All
// statements are idempotent so migration 27 is safe on fresh databases, where
// the objects already exist from the core schema.
const ledgerSchemaSQL = `
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
    snapshot       TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_ledger_dedup_key ON ledger_events(dedup_key);
CREATE INDEX IF NOT EXISTS idx_ledger_wa_key ON ledger_events(wa_key);
CREATE INDEX IF NOT EXISTS idx_ledger_chat_jid ON ledger_events(chat_jid);
CREATE INDEX IF NOT EXISTS idx_ledger_msg_id ON ledger_events(msg_id);
CREATE INDEX IF NOT EXISTS idx_ledger_event_ts ON ledger_events(event_ts);

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

CREATE TABLE IF NOT EXISTS ledger_view_state (
    view TEXT PRIMARY KEY,
    mode TEXT NOT NULL DEFAULT 'shadow'
);

CREATE TABLE IF NOT EXISTS ledger_batches (
    batch_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at INTEGER NOT NULL,
    source     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS ledger_causal_links (
    event_id  TEXT NOT NULL,
    ref_index INTEGER NOT NULL,
    target_id TEXT NOT NULL,
    PRIMARY KEY (event_id, ref_index)
);

CREATE INDEX IF NOT EXISTS idx_ledger_links_target ON ledger_causal_links(target_id);

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
`

func migrateEventLedger(d *DB) error {
	if _, err := d.sql.Exec(ledgerSchemaSQL); err != nil {
		return fmt.Errorf("create event ledger: %w", err)
	}
	return nil
}
