package app

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/openclaw/wacli/internal/store"
)

const rebuildFixtureSQL = `
INSERT INTO chats (jid, kind, name, last_message_ts, unread, unread_count) VALUES
 ('15551234567@s.whatsapp.net','dm','Alice',200,1,1),
 ('1111111111@g.us','group','Team',120,0,0);

INSERT INTO contacts (jid, phone, push_name, full_name, first_name, updated_at) VALUES
 ('15551234567@s.whatsapp.net','15551234567','Alice','Alice','A',100);

INSERT INTO groups (jid, name, owner_jid, created_ts, updated_at) VALUES
 ('1111111111@g.us','Team','15551234567@s.whatsapp.net',50,100);

INSERT INTO group_participants (group_jid, user_jid, role, updated_at) VALUES
 ('1111111111@g.us','15551234567@s.whatsapp.net','admin',100),
 ('1111111111@g.us','15550000000@s.whatsapp.net','member',100);

INSERT INTO messages
 (chat_jid, chat_name, msg_id, sender_jid, sender_name, ts, from_me, text, display_text,
  media_type, media_caption, filename, local_path, downloaded_at, payload_purged_at)
VALUES
 ('15551234567@s.whatsapp.net','Alice','m1','15551234567@s.whatsapp.net','Alice',
   100,0,'hello','hello',NULL,NULL,NULL,NULL,NULL,NULL),
 ('15551234567@s.whatsapp.net','Alice','m2','15551234567@s.whatsapp.net','Alice',
   200,0,NULL,NULL,'image','cap','a.jpg','/tmp/x',190,201),
 ('15551234567@s.whatsapp.net','Alice','m3','15551234567@s.whatsapp.net','Alice',
   150,0,'gone','gone',NULL,NULL,NULL,NULL,NULL,NULL),
 ('1111111111@g.us','Team','g1','15551234567@s.whatsapp.net','Alice',
   120,0,'hi group','hi group',NULL,NULL,NULL,NULL,NULL,NULL);
UPDATE messages SET deleted_at=160 WHERE msg_id='m3';

INSERT INTO message_locations
 (chat_jid, msg_id, latitude, longitude, name, address, is_live)
VALUES ('15551234567@s.whatsapp.net','m1',1.5,-2.5,'home','addr',1);

INSERT INTO message_payload_purges
 (chat_jid, msg_id, purged_at, deleted_at, deletion_reason)
VALUES ('15551234567@s.whatsapp.net','m2',201,201,'privacy_placeholder');

INSERT INTO polls
 (chat_jid, msg_id, sender_jid, question, options_json, selectable_count, created_ts)
VALUES ('15551234567@s.whatsapp.net','poll1','15551234567@s.whatsapp.net',
 'Favourite','["A","B"]',1,130);

INSERT INTO poll_votes
 (chat_jid, poll_msg_id, voter_jid, vote_msg_id, selected_options_json, ts)
VALUES ('15551234567@s.whatsapp.net','poll1','15551234567@s.whatsapp.net',
 'vote1','["A"]',140);

INSERT INTO status_messages
 (msg_id, ts, from_me, sender_jid, sender_name, text)
VALUES ('st1',90,1,'15551234567@s.whatsapp.net','me','status text');

INSERT INTO call_events
 (chat_jid, chat_name, sender_jid, sender_name, call_id, event_type,
  direction, media, ts, duration_secs)
VALUES ('15551234567@s.whatsapp.net','Alice','15551234567@s.whatsapp.net','Alice',
 'call1','offer','inbound','audio',80,0);

INSERT INTO starred (chat_jid, msg_id, sender_jid, from_me, starred_at)
VALUES ('15551234567@s.whatsapp.net','m1','15551234567@s.whatsapp.net',0,110);
`

// rebuildCompareTables defines per-table comparison columns (rowid excluded)
// and ordering. Operational alias tables are intentionally excluded.
var rebuildCompareTables = []struct {
	table   string
	columns string
	orderBy string
}{
	{"chats", "*", "jid"},
	{"contacts", "*", "jid"},
	{"groups", "*", "jid"},
	{"group_participants", "*", "group_jid,user_jid"},
	{"messages", "chat_jid,chat_name,msg_id,sender_jid,sender_name,ts,from_me,text,display_text," +
		"quoted_msg_id,quoted_sender_jid,is_forwarded,forwarding_score,reaction_to_id,reaction_emoji," +
		"media_type,media_caption,filename,mime_type,direct_path,media_key,file_sha256,file_enc_sha256," +
		"file_length,local_path,downloaded_at,media_unavailable_at,revoked,deleted_for_me,deleted_at," +
		"deletion_reason,payload_purged_at,edited,edited_ts,buttons", "chat_jid,msg_id"},
	{"message_locations", "*", "chat_jid,msg_id"},
	{"message_payload_purges", "*", "chat_jid,msg_id"},
	{"polls", "*", "chat_jid,msg_id"},
	{"poll_votes", "*", "chat_jid,poll_msg_id,voter_jid"},
	{"status_messages", "msg_id,ts,from_me,sender_jid,sender_name,text,media_type,media_caption," +
		"filename,mime_type,direct_path,media_key,file_sha256,file_enc_sha256,file_length," +
		"background_color,font", "msg_id"},
	{"call_events", "chat_jid,chat_name,sender_jid,sender_name,call_id,msg_id,event_type,direction," +
		"media,outcome,reason,call_type,duration_secs,ts,participants", "chat_jid,call_id,event_type,ts"},
	{"starred", "*", "chat_jid,msg_id"},
}

var rebuildShadowCheckpointKeys = []string{
	"messages_shadow", "chats_shadow", "contacts_shadow", "groups_shadow",
	"polls_shadow", "poll_votes_shadow", "starred_shadow",
	"status_messages_shadow", "call_events_shadow",
}

func dumpComparedRows(t *testing.T, a *App, table, columns, orderBy string) []string {
	t.Helper()
	rows, err := a.db.Query(
		fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", columns, table, orderBy))
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(cols))
		for i, v := range raw {
			switch val := v.(type) {
			case nil:
				parts[i] = "<nil>"
			case []byte:
				parts[i] = "blob:" + string(val)
			default:
				parts[i] = fmt.Sprint(val)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return out
}

func dumpFTSContent(t *testing.T, a *App, table string) []string {
	t.Helper()
	return dumpComparedRows(t, a, table,
		"text,media_caption,filename,chat_name,sender_name,display_text",
		"rowid")
}

// TR-16.1: offline rebuild of a healthy synced store yields row-level parity
// between shadow and active tables (FTS compared by content set), and every
// shadow checkpoint reaches head.
func TestRebuildShadowParity(t *testing.T) {
	a := newTestApp(t)
	ctx := context.Background()
	if _, err := a.db.Exec(rebuildFixtureSQL); err != nil {
		t.Fatal(err)
	}

	headBefore, err := a.db.HeadSeq()
	if err != nil {
		t.Fatal(err)
	}
	if headBefore != 0 {
		t.Fatalf("head seq before bootstrap = %d, want 0", headBefore)
	}
	if _, err := a.db.BootstrapLegacySnapshots(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	head, err := a.db.HeadSeq()
	if err != nil {
		t.Fatal(err)
	}

	report, err := a.RebuildShadow(ctx)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if report.TargetSeq != head {
		t.Fatalf("rebuild target = %d, want head %d", report.TargetSeq, head)
	}

	withFTS := a.db.FTSEnabled()
	for _, cfg := range rebuildCompareTables {
		active := dumpComparedRows(t, a, cfg.table, cfg.columns, cfg.orderBy)
		shadow := dumpComparedRows(t, a, cfg.table+"_shadow", cfg.columns, cfg.orderBy)
		if len(active) != len(shadow) {
			t.Fatalf("%s: active rows %d, shadow rows %d", cfg.table, len(active), len(shadow))
		}
		for i := range active {
			if active[i] != shadow[i] {
				t.Fatalf("%s row %d:\nactive: %s\nshadow: %s", cfg.table, i, active[i], shadow[i])
			}
		}
	}
	if withFTS {
		activeFTS := dumpFTSContent(t, a, "messages_fts")
		shadowFTS := dumpFTSContent(t, a, "messages_fts_shadow")
		sort.Strings(activeFTS)
		sort.Strings(shadowFTS)
		if len(activeFTS) != len(shadowFTS) {
			t.Fatalf("fts active %d rows, shadow %d", len(activeFTS), len(shadowFTS))
		}
		for i := range activeFTS {
			if activeFTS[i] != shadowFTS[i] {
				t.Fatalf("fts content %d: active %s shadow %s", i, activeFTS[i], shadowFTS[i])
			}
		}
	}

	for _, key := range rebuildShadowCheckpointKeys {
		cp, err := a.db.GetCheckpoint(key)
		if err != nil {
			t.Fatalf("checkpoint %s: %v", key, err)
		}
		if cp.LastSeq != head {
			t.Fatalf("checkpoint %s = %d, want head %d", key, cp.LastSeq, head)
		}
		if cp.ProjectorVersion == "" {
			t.Fatalf("checkpoint %s has empty projector version", key)
		}
	}
}

// TR-17.2: the full fixture, bootstrapped and rebuilt, passes the invariant
// checker on both active and shadow (incl. shadow checkpoint frontier and the
// raw/scrub accounting).
func TestVerifyAfterRebuildZeroViolations(t *testing.T) {
	a := newTestApp(t)
	ctx := context.Background()
	if _, err := a.db.Exec(rebuildFixtureSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.BootstrapLegacySnapshots(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.RebuildShadow(ctx); err != nil {
		t.Fatal(err)
	}

	for name, run := range map[string]func(context.Context) (*store.VerifyReport, error){
		"active": a.VerifyLedger,
		"shadow": a.VerifyShadowLedger,
	} {
		report, err := run(ctx)
		if err != nil {
			t.Fatalf("verify %s: %v", name, err)
		}
		if report.HasViolations() {
			t.Fatalf("%s violations: %+v", name, report.Violations)
		}
	}
}

// TR-16.2: two consecutive rebuilds leave every shadow table byte-for-byte
// identical (reset + replay is deterministic).
func TestRebuildShadowDeterministic(t *testing.T) {
	a := newTestApp(t)
	ctx := context.Background()
	if _, err := a.db.Exec(rebuildFixtureSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.BootstrapLegacySnapshots(); err != nil {
		t.Fatal(err)
	}

	if _, err := a.RebuildShadow(ctx); err != nil {
		t.Fatalf("rebuild 1: %v", err)
	}
	first := make(map[string][]string)
	for _, cfg := range rebuildCompareTables {
		first[cfg.table] = dumpComparedRows(t, a, cfg.table+"_shadow", cfg.columns, cfg.orderBy)
	}

	if _, err := a.RebuildShadow(ctx); err != nil {
		t.Fatalf("rebuild 2: %v", err)
	}
	for _, cfg := range rebuildCompareTables {
		second := dumpComparedRows(t, a, cfg.table+"_shadow", cfg.columns, cfg.orderBy)
		if len(second) != len(first[cfg.table]) {
			t.Fatalf("%s: row count changed across rebuilds", cfg.table)
		}
		for i := range second {
			if second[i] != first[cfg.table][i] {
				t.Fatalf("%s row %d differs across rebuilds:\nfirst:  %s\nsecond: %s",
					cfg.table, i, first[cfg.table][i], second[i])
			}
		}
	}
}
