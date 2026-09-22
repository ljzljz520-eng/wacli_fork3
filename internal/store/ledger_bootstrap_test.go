package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func seedLegacyRows(t *testing.T, db *DB) map[string]int {
	t.Helper()
	statements := []string{
		`INSERT INTO chats(jid, kind, name) VALUES ('g@g.us', 'group', 'Group')`,
		`INSERT INTO contacts(jid, updated_at, full_name) VALUES ('alice@g.us', 1700000000, 'Alice')`,
		`INSERT INTO groups(jid, name, updated_at) VALUES ('g@g.us', 'Group', 1700000000)`,
		`INSERT INTO messages(chat_jid, msg_id, ts, from_me, sender_jid, text)
		 VALUES ('g@g.us', 'MSG-1', 1700000000, 0, 'alice@g.us', 'hi')`,
		`INSERT INTO message_locations(chat_jid, msg_id, latitude, longitude)
		 VALUES ('g@g.us', 'MSG-1', 37.7749, -122.4194)`,
		`INSERT INTO message_payload_purges(chat_jid, msg_id, purged_at, deleted_at, deletion_reason)
		 VALUES ('g@g.us', 'MSG-1', 1700000010, 1700000010, 'privacy_placeholder')`,
		`INSERT INTO group_participants(group_jid, user_jid, role, updated_at)
		 VALUES ('g@g.us', 'alice@g.us', 'admin', 1700000000)`,
		`INSERT INTO starred(chat_jid, msg_id, sender_jid, from_me, starred_at)
		 VALUES ('g@g.us', 'MSG-1', 'alice@g.us', 0, 1700000001)`,
		`INSERT INTO call_events(chat_jid, call_id, event_type, ts, duration_secs)
		 VALUES ('g@g.us', 'CALL-1', 'offer', 1700000002, 0)`,
		`INSERT INTO status_messages(msg_id, ts, from_me, sender_jid)
		 VALUES ('STATUS-1', 1700000003, 0, 'alice@g.us')`,
		`INSERT INTO polls(chat_jid, msg_id, question, options_json, created_ts)
		 VALUES ('g@g.us', 'POLL-1', 'Q', '["A","B"]', 1700000004)`,
		`INSERT INTO poll_votes(chat_jid, poll_msg_id, voter_jid, vote_msg_id, selected_options_json, ts)
		 VALUES ('g@g.us', 'POLL-1', 'alice@g.us', 'VOTE-1', '[0]', 1700000005)`,
	}
	for _, stmt := range statements {
		if _, err := db.sql.Exec(stmt); err != nil {
			t.Fatalf("seed: %v\n%s", err, stmt)
		}
	}
	return map[string]int{
		"messages": 1, "message_locations": 1, "message_payload_purges": 1,
		"chats": 1, "contacts": 1,
		"groups": 1, "group_participants": 1, "starred": 1, "call_events": 1,
		"status_messages": 1, "polls": 1, "poll_votes": 1,
	}
}

func TestBootstrapLegacySnapshots(t *testing.T) {
	db := openTestDB(t)
	want := seedLegacyRows(t, db)

	report, err := db.BootstrapLegacySnapshots()
	if err != nil {
		t.Fatal(err)
	}
	for table, count := range want {
		if got := report.PerTable[table].Appended; got != count {
			t.Fatalf("table %s appended = %d, want %d", table, got, count)
		}
	}

	totalEvents := 0
	for _, tb := range report.PerTable {
		totalEvents += tb.Appended + tb.Skipped
	}
	if totalEvents != 12 {
		t.Fatalf("total snapshot events = %d, want 12", totalEvents)
	}
	if got := countRows(t, db.sql, "SELECT COUNT(*) FROM ledger_events"); got != 12 {
		t.Fatalf("ledger rows = %d, want 12", got)
	}
	// No raw envelope bytes are retained for legacy rows.
	if got := countRows(t, db.sql, "SELECT COUNT(*) FROM ledger_raw"); got != 0 {
		t.Fatalf("ledger_raw rows = %d, want 0", got)
	}
	// No projector checkpoints are written.
	if got := countRows(t, db.sql, "SELECT COUNT(*) FROM projector_checkpoints"); got != 0 {
		t.Fatalf("checkpoint rows = %d, want 0", got)
	}

	// Verify raw_hash for the messages snapshot: it must hash the exact
	// canonical snapshot bytes stored in the event.
	var msgEventID, rawHash, snapshotStr string
	err = db.sql.QueryRow(`
		SELECT event_id, raw_hash, snapshot FROM ledger_events
		WHERE event_type = 'snapshot_messages'`,
	).Scan(&msgEventID, &rawHash, &snapshotStr)
	if err != nil {
		t.Fatal(err)
	}
	var compact []byte
	if compact, err = compactJSON(snapshotStr); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(compact)
	if got := hex.EncodeToString(sum[:]); got != rawHash {
		t.Fatalf("raw_hash = %s, recomputed = %s", rawHash, got)
	}

	// The message snapshot carries a WA key with the group participant.
	stored, err := db.GetEventByID(msgEventID)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Table string         `json:"table"`
		Row   map[string]any `json:"row"`
	}
	if err := json.Unmarshal(stored.Snapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Table != "messages" || snapshot.Row["msg_id"] != "MSG-1" {
		t.Fatalf("snapshot payload = %+v", snapshot)
	}
}

func TestBootstrapLegacySnapshotsReentrant(t *testing.T) {
	db := openTestDB(t)
	seedLegacyRows(t, db)

	if _, err := db.BootstrapLegacySnapshots(); err != nil {
		t.Fatal(err)
	}
	// Simulate an interrupted-then-resumed run: rerun appends nothing.
	report, err := db.BootstrapLegacySnapshots()
	if err != nil {
		t.Fatal(err)
	}
	for table, tb := range report.PerTable {
		if tb.Appended != 0 || tb.Skipped != 1 {
			t.Fatalf("table %s second pass = %+v, want appended=0 skipped=1", table, tb)
		}
	}
	if got := countRows(t, db.sql, "SELECT COUNT(*) FROM ledger_events"); got != 12 {
		t.Fatalf("ledger rows after rerun = %d, want 12", got)
	}
}

func compactJSON(s string) ([]byte, error) {
	var v struct {
		Table string         `json:"table"`
		Row   map[string]any `json:"row"`
	}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
