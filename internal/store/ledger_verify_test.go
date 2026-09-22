package store

import (
	"context"
	"testing"
)

const verifyBaseSQL = `
INSERT INTO chats(jid, kind) VALUES ('c1@s.whatsapp.net','dm');
INSERT INTO messages(chat_jid, msg_id, ts, from_me, text)
 VALUES ('c1@s.whatsapp.net','m1',100,0,'hello');
`

func verifyCheckSet(t *testing.T, db *DB) map[string]bool {
	t.Helper()
	report, err := db.VerifyLedger(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, v := range report.Violations {
		out[v.Check] = true
	}
	return out
}

// TR-17.1: a healthy minimal store has zero false positives.
func TestVerifyLedgerHealthy(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.sql.Exec(verifyBaseSQL); err != nil {
		t.Fatal(err)
	}
	// A deleted message must not disturb the checker (FTS row removed by
	// trigger).
	if _, err := db.sql.Exec(
		`UPDATE messages SET deleted_at=110 WHERE msg_id='m1'`); err != nil {
		t.Fatal(err)
	}
	if checks := verifyCheckSet(t, db); len(checks) != 0 {
		t.Fatalf("healthy store produced violations: %+v", checks)
	}
}

// TR-17.1: each fault class is detected and located.
func TestVerifyLedgerFaultInjection(t *testing.T) {
	insertLedgerEvent := func(db *DB, eventID, eventType, source string) {
		t.Helper()
		_, err := db.sql.Exec(`INSERT INTO ledger_events
			(event_id,source,event_type,received_at,raw_hash,parser_version,
				rules_version,dedup_key)
			VALUES (?, ?, ?, 1, 'h','pv','rv','dk')`, eventID, source, eventType)
		if err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		name        string
		requiresFTS bool
		setup       func(t *testing.T, db *DB)
		want        string
	}{
		{
			name: "message without chat",
			setup: func(t *testing.T, db *DB) {
				db.sql.SetMaxOpenConns(1)
				if _, err := db.sql.Exec(`PRAGMA foreign_keys=OFF;
					DELETE FROM chats WHERE jid='c1@s.whatsapp.net';
					PRAGMA foreign_keys=ON`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkMessageChat,
		},
		{
			name: "participant without group",
			setup: func(t *testing.T, db *DB) {
				db.sql.SetMaxOpenConns(1)
				if _, err := db.sql.Exec(`PRAGMA foreign_keys=OFF;
					INSERT INTO group_participants
					 (group_jid,user_jid,role,updated_at)
					 VALUES ('ghost@g.us','u1','member',1);
					PRAGMA foreign_keys=ON`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkRosterGroup,
		},
		{
			name: "fts missing row", requiresFTS: true,
			setup: func(t *testing.T, db *DB) {
				if _, err := db.sql.Exec(`DELETE FROM messages_fts`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkFTSMissing,
		},
		{
			name: "fts extra row", requiresFTS: true,
			setup: func(t *testing.T, db *DB) {
				if _, err := db.sql.Exec(`INSERT INTO messages_fts(rowid,text)
				 VALUES (99999,'ghost')`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkFTSExtra,
		},
		{
			name: "checkpoint ahead",
			setup: func(t *testing.T, db *DB) {
				if _, err := db.sql.Exec(`INSERT INTO projector_checkpoints
				 (view,last_seq,projector_version,updated_at)
				 VALUES ('messages',50,'v',1)`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkCheckpointAhead,
		},
		{
			name: "checkpoint lag",
			setup: func(t *testing.T, db *DB) {
				insertLedgerEvent(db, "e1", "message", "live")
				// The live event without raw trips missing_raw: insert raw so
				// only the lag violation is the target.
				if _, err := db.sql.Exec(`INSERT INTO ledger_raw
				 (event_id,encoding,raw_bytes) VALUES ('e1','proto',X'00')`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.sql.Exec(`INSERT INTO projector_checkpoints
				 (view,last_seq,projector_version,updated_at)
				 VALUES ('messages',0,'v',1)`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkCheckpointLag,
		},
		{
			name: "checkpoint fork",
			setup: func(t *testing.T, db *DB) {
				if _, err := db.sql.Exec(`INSERT INTO projector_checkpoints
				 (view,last_seq,projector_version,updated_at) VALUES
				 ('messages',0,'v',1),('chats',1,'v',1)`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkCheckpointFork,
		},
		{
			name: "raw hash mismatch",
			setup: func(t *testing.T, db *DB) {
				insertLedgerEvent(db, "e1", "message", "live")
				if _, err := db.sql.Exec(`INSERT INTO ledger_raw
				 (event_id,encoding,raw_bytes) VALUES ('e1','proto',X'00')`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkRawHash,
		},
		{
			name: "dangling causal ref",
			setup: func(t *testing.T, db *DB) {
				if _, err := db.sql.Exec(`INSERT INTO ledger_causal_links
				 (event_id,ref_index,target_id) VALUES ('e9',0,'missing')`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkDangling,
		},
		{
			name: "causal cycle",
			setup: func(t *testing.T, db *DB) {
				if _, err := db.sql.Exec(`INSERT INTO ledger_causal_links
				 (event_id,ref_index,target_id) VALUES
				 ('a',0,'b'),('b',0,'a')`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkCycle,
		},
		{
			name: "scrub with raw",
			setup: func(t *testing.T, db *DB) {
				insertLedgerEvent(db, "s1", "scrub", "scrub")
				if _, err := db.sql.Exec(`INSERT INTO ledger_raw
				 (event_id,encoding,raw_bytes) VALUES ('s1','json',X'00')`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkScrubWithRaw,
		},
		{
			name: "event without raw or scrub",
			setup: func(t *testing.T, db *DB) {
				insertLedgerEvent(db, "e1", "message", "live")
			},
			want: checkMissingRaw,
		},
		{
			name: "unread inconsistent",
			setup: func(t *testing.T, db *DB) {
				if _, err := db.sql.Exec(`UPDATE chats
				 SET unread=0, unread_count=5 WHERE jid='c1@s.whatsapp.net'`); err != nil {
					t.Fatal(err)
				}
			},
			want: checkUnread,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			if _, err := db.sql.Exec(verifyBaseSQL); err != nil {
				t.Fatal(err)
			}
			if tc.requiresFTS && !db.ftsEnabled {
				t.Skip("FTS not enabled in this build")
			}
			tc.setup(t, db)
			checks := verifyCheckSet(t, db)
			if !checks[tc.want] {
				t.Fatalf("expected violation %s, got %+v", tc.want, checks)
			}
		})
	}
}

// TR-17.2: after a full synced-fixture bootstrap + rebuild, both active and
// shadow pass with zero violations, including the raw/scrub accounting.
func TestVerifyRebuiltParity(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.sql.Exec(verifyBaseSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BootstrapLegacySnapshots(); err != nil {
		t.Fatal(err)
	}
	if err := db.ResetShadowSchema(context.Background(), db.ftsEnabled); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", ShadowSuffix} {
		report, err := db.VerifyLedger(context.Background(), suffix)
		if err != nil {
			t.Fatal(err)
		}
		// Shadow has no checkpoints yet for this manual setup; structural
		// checks still apply.
		if report.HasViolations() {
			t.Fatalf("%q violations: %+v", suffix, report.Violations)
		}
	}
}
