package store

import (
	"strings"
	"testing"
)

func TestLedgerTablesExistOnFreshOpen(t *testing.T) {
	db := openTestDB(t)

	for _, table := range []string{
		"ledger_events",
		"ledger_raw",
		"projector_checkpoints",
		"ledger_view_state",
		"ledger_batches",
		"ledger_causal_links",
	} {
		exists, err := db.tableExists(table)
		if err != nil {
			t.Fatalf("tableExists(%q): %v", table, err)
		}
		if !exists {
			t.Fatalf("expected %q to exist", table)
		}
	}

	eventCols, err := tableColumns(db.sql, "ledger_events")
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{
		"seq", "event_id", "source", "event_type", "wa_key", "chat_jid", "msg_id",
		"sender_jid", "server_ts", "event_ts", "received_at", "raw_hash",
		"parser_version", "rules_version", "dedup_key", "causal_refs", "causes",
		"batch_id", "flags", "snapshot",
	} {
		if !eventCols[col] {
			t.Fatalf("expected ledger_events column %q", col)
		}
	}

	var version int
	if err := db.sql.QueryRow(
		`SELECT version FROM schema_migrations WHERE name = 'event ledger'`,
	).Scan(&version); err != nil {
		t.Fatalf("migration 27 recorded: %v", err)
	}
	if version != 27 {
		t.Fatalf("event ledger version = %d, want 27", version)
	}
}

func TestLedgerEventsAppendOnlyTriggers(t *testing.T) {
	db := openTestDB(t)

	_, err := db.sql.Exec(`
		INSERT INTO ledger_events(
			event_id, source, event_type, raw_hash, parser_version,
			rules_version, dedup_key, received_at
		) VALUES ('evt-1', 'live', 'message', 'hash', 'pv', 'rv', 'dk', 1)
	`)
	if err != nil {
		t.Fatalf("insert ledger event: %v", err)
	}

	if _, err := db.sql.Exec(
		`UPDATE ledger_events SET source = 'history' WHERE event_id = 'evt-1'`,
	); err == nil {
		t.Fatal("expected UPDATE on ledger_events to be rejected")
	}

	if _, err := db.sql.Exec(
		`DELETE FROM ledger_events WHERE event_id = 'evt-1'`,
	); err == nil {
		t.Fatal("expected DELETE on ledger_events to be rejected")
	}

	// REPLACE implies a DELETE and must be rejected as well.
	if _, err := db.sql.Exec(`
		INSERT OR REPLACE INTO ledger_events(
			event_id, source, event_type, raw_hash, parser_version,
			rules_version, dedup_key, received_at
		) VALUES ('evt-1', 'history', 'message', 'hash', 'pv', 'rv', 'dk', 1)
	`); err == nil {
		t.Fatal("expected REPLACE on ledger_events to be rejected")
	}

	var source string
	if err := db.sql.QueryRow(
		`SELECT source FROM ledger_events WHERE event_id = 'evt-1'`,
	).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if source != "live" {
		t.Fatalf("row mutated despite trigger, source = %q", source)
	}
}

func TestLedgerCausalLinksInsertOnlyTriggers(t *testing.T) {
	db := openTestDB(t)

	if _, err := db.sql.Exec(`
		INSERT INTO ledger_events(
			event_id, source, event_type, raw_hash, parser_version,
			rules_version, dedup_key, received_at
		) VALUES ('evt-1', 'live', 'message', 'hash', 'pv', 'rv', 'dk', 1)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.sql.Exec(
		`INSERT INTO ledger_causal_links(event_id, ref_index, target_id) VALUES('evt-1', 0, 'evt-0')`,
	); err != nil {
		t.Fatalf("insert causal link: %v", err)
	}

	if _, err := db.sql.Exec(
		`UPDATE ledger_causal_links SET target_id = 'evt-9' WHERE event_id = 'evt-1'`,
	); err == nil {
		t.Fatal("expected UPDATE on ledger_causal_links to be rejected")
	}
	if _, err := db.sql.Exec(
		`DELETE FROM ledger_causal_links WHERE event_id = 'evt-1'`,
	); err == nil {
		t.Fatal("expected DELETE on ledger_causal_links to be rejected")
	}
}

func TestMigration27RepairsMissingLedger(t *testing.T) {
	db := openTestDB(t)
	path := db.path

	// Simulate a store that never got migration 27: drop all ledger objects
	// and forget the migration record.
	if _, err := db.sql.Exec(`
		DROP TRIGGER ledger_events_no_update;
		DROP TRIGGER ledger_events_no_delete;
		DROP TRIGGER ledger_links_no_update;
		DROP TRIGGER ledger_links_no_delete;
		DROP TABLE ledger_causal_links;
		DROP TABLE ledger_raw;
		DROP TABLE ledger_events;
		DROP TABLE projector_checkpoints;
		DROP TABLE ledger_view_state;
		DROP TABLE ledger_batches;
		DELETE FROM schema_migrations WHERE version = 27;
	`); err != nil {
		t.Fatalf("strip ledger: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("Open repaired store: %v", err)
	}
	defer reopened.Close()

	for _, table := range []string{
		"ledger_events", "ledger_raw", "projector_checkpoints",
		"ledger_view_state", "ledger_batches", "ledger_causal_links",
	} {
		exists, err := reopened.tableExists(table)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("migration 27 did not recreate %q", table)
		}
	}

	_, err = reopened.sql.Exec(`
		INSERT INTO ledger_events(
			event_id, source, event_type, raw_hash, parser_version,
			rules_version, dedup_key, received_at
		) VALUES ('evt-1', 'live', 'message', 'hash', 'pv', 'rv', 'dk', 1)
	`)
	if err != nil {
		t.Fatalf("insert after repair: %v", err)
	}
	if _, err := reopened.sql.Exec(
		`UPDATE ledger_events SET flags = 1 WHERE event_id = 'evt-1'`,
	); err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("append-only trigger not restored, err = %v", err)
	}
}
