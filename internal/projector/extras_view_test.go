package projector

import (
	"fmt"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
)

// --- shadow schema -----------------------------------------------------------

func createExtrasShadow(t *testing.T, db *store.DB) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS starred_shadow (
			chat_jid TEXT NOT NULL, msg_id TEXT NOT NULL, sender_jid TEXT,
			from_me INTEGER NOT NULL DEFAULT 0, starred_at INTEGER NOT NULL,
			PRIMARY KEY (chat_jid, msg_id))`,
		`CREATE TABLE IF NOT EXISTS status_messages_shadow (
			rowid INTEGER PRIMARY KEY AUTOINCREMENT,
			msg_id TEXT NOT NULL UNIQUE, ts INTEGER NOT NULL, from_me INTEGER NOT NULL,
			sender_jid TEXT, sender_name TEXT, text TEXT,
			media_type TEXT, media_caption TEXT, filename TEXT, mime_type TEXT,
			direct_path TEXT, media_key BLOB, file_sha256 BLOB, file_enc_sha256 BLOB,
			file_length INTEGER, background_color TEXT, font INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS call_events_shadow (
			rowid INTEGER PRIMARY KEY AUTOINCREMENT,
			chat_jid TEXT NOT NULL, chat_name TEXT, sender_jid TEXT, sender_name TEXT,
			call_id TEXT NOT NULL, msg_id TEXT, event_type TEXT NOT NULL, direction TEXT,
			media TEXT, outcome TEXT, reason TEXT, call_type TEXT,
			duration_secs INTEGER NOT NULL DEFAULT 0, ts INTEGER NOT NULL, participants TEXT,
			UNIQUE(chat_jid, call_id, event_type, ts))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("create extras shadow: %v\n%s", err, s)
		}
	}
}

// --- append helpers ----------------------------------------------------------

func appendStarLedgerEvent(t *testing.T, db *store.DB, c ledger.CanonicalStar) bool {
	t.Helper()
	rawBytes, rawHash, err := ledger.HashCanonical(c)
	if err != nil {
		t.Fatalf("hash star: %v", err)
	}
	dk := ledger.DedupKey("star", c.ChatJID, c.MsgID,
		fmt.Sprintf("%t", c.Starred), fmt.Sprintf("%d", c.StarredTS))
	return appendExtraLedgerEvent(t, db, ledger.EventStar, c.ChatJID, c.StarredTS, dk, rawBytes, rawHash)
}

func appendStatusLedgerEvent(t *testing.T, db *store.DB, c ledger.CanonicalStatusMessage) bool {
	t.Helper()
	rawBytes, rawHash, err := ledger.HashCanonical(c)
	if err != nil {
		t.Fatalf("hash status: %v", err)
	}
	return appendExtraLedgerEvent(t, db, ledger.EventStatusMessage,
		"status@broadcast", c.TS, ledger.DedupKey("status", c.MsgID), rawBytes, rawHash)
}

func appendCallLedgerEvent(t *testing.T, db *store.DB, c ledger.CanonicalCallEvent) bool {
	t.Helper()
	rawBytes, rawHash, err := ledger.HashCanonical(c)
	if err != nil {
		t.Fatalf("hash call: %v", err)
	}
	dk := ledger.DedupKey("call-event", c.ChatJID, c.CallID, c.EventType,
		fmt.Sprintf("%d", c.TS), fmt.Sprintf("%t", c.Deleted))
	return appendExtraLedgerEvent(t, db, ledger.EventCall, c.ChatJID, c.TS, dk, rawBytes, rawHash)
}

func appendExtraLedgerEvent(t *testing.T, db *store.DB, eventType, chatJID string,
	eventTS int64, dk string, rawBytes []byte, rawHash string,
) bool {
	batchID, _ := db.StartBatch(ledger.SourceLive)
	now := time.Now().Unix()
	if eventTS == 0 {
		eventTS = now
	}
	_, inserted, err := db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceLive,
		EventType:     eventType,
		ChatJID:       chatJID,
		ServerTS:      eventTS,
		EventTS:       eventTS,
		ReceivedAt:    now,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	if err != nil {
		t.Fatalf("AppendEvent %s: %v", eventType, err)
	}
	return inserted
}

// --- dumps -------------------------------------------------------------------

var starredDumpCols = []string{"chat_jid", "msg_id", "sender_jid", "from_me", "starred_at"}

func starredDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT chat_jid, msg_id, sender_jid, from_me, starred_at
		FROM %s ORDER BY chat_jid, msg_id`, table)
}

var statusDumpCols = []string{
	"msg_id", "ts", "from_me", "sender_jid", "sender_name", "text",
	"media_type", "media_caption", "filename", "mime_type", "direct_path",
	"media_key", "file_sha256", "file_enc_sha256", "file_length",
	"background_color", "font",
}

func statusDumpQuery(table string) string {
	cols := ""
	for i, c := range statusDumpCols {
		if i > 0 {
			cols += ", "
		}
		cols += c
	}
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY msg_id`, cols, table)
}

var callDumpCols = []string{
	"chat_jid", "chat_name", "sender_jid", "sender_name", "call_id",
	"msg_id", "event_type", "direction", "media", "outcome", "reason",
	"call_type", "duration_secs", "ts", "participants",
}

func callDumpQuery(table string) string {
	cols := ""
	for i, c := range callDumpCols {
		if i > 0 {
			cols += ", "
		}
		cols += c
	}
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY chat_jid, call_id, event_type, ts`, cols, table)
}

// --- tests -------------------------------------------------------------------

func TestExtrasViewsFoldParity(t *testing.T) {
	db := openRunnerDB(t)
	createChatsShadow(t, db)
	createExtrasShadow(t, db)
	dm := "5550101@s.whatsapp.net"

	// Star: add, identical redelivery, remove.
	if !appendStarLedgerEvent(t, db, ledger.CanonicalStar{
		ChatJID: dm, MsgID: "m1", SenderJID: dm, Starred: true, StarredTS: 1700000000,
	}) {
		t.Fatal("first star should insert")
	}
	if appendStarLedgerEvent(t, db, ledger.CanonicalStar{
		ChatJID: dm, MsgID: "m1", SenderJID: dm, Starred: true, StarredTS: 1700000000,
	}) {
		t.Fatal("identical star must dedup")
	}
	if !appendStarLedgerEvent(t, db, ledger.CanonicalStar{
		ChatJID: dm, MsgID: "m1", SenderJID: dm, Starred: false, StarredTS: 1700000100,
	}) {
		t.Fatal("unstar should insert")
	}

	// Status: text + media.
	if !appendStatusLedgerEvent(t, db, ledger.CanonicalStatusMessage{
		MsgID: "s1", TS: 1700000200, SenderJID: "alice@status", SenderName: "Alice",
		Text: "hello status",
	}) {
		t.Fatal("first status should insert")
	}
	if !appendStatusLedgerEvent(t, db, ledger.CanonicalStatusMessage{
		MsgID: "s2", TS: 1700000300, FromMe: true,
		MediaType: "image", MediaCaption: "mine", MimeType: "image/jpeg",
		MediaKey:      []byte{0x01, 0x02},
		FileSHA256:    []byte{0x03, 0x04},
		FileEncSHA256: []byte{0x05, 0x06},
		FileLength:    42,
	}) {
		t.Fatal("media status should insert")
	}

	// Call: first event, update with new ts, then delete marker.
	if !appendCallLedgerEvent(t, db, ledger.CanonicalCallEvent{
		ChatJID: dm, ChatName: "Alice", SenderJID: dm, CallID: "call1",
		EventType: "call_log", Direction: "inbound", TS: 1700000400,
		Participants: []ledger.CanonicalCallParticipant{{JID: dm}},
	}) {
		t.Fatal("first call should insert")
	}
	if !appendCallLedgerEvent(t, db, ledger.CanonicalCallEvent{
		ChatJID: dm, ChatName: "Alice", SenderJID: dm, CallID: "call1",
		EventType: "call_log", Direction: "inbound", TS: 1700000500,
		DurationSecs: 30,
		Participants: []ledger.CanonicalCallParticipant{{JID: dm, Outcome: "connected"}},
	}) {
		t.Fatal("call update should insert")
	}
	if !appendCallLedgerEvent(t, db, ledger.CanonicalCallEvent{
		ChatJID: dm, Direction: "inbound", Deleted: true,
	}) {
		t.Fatal("call delete should insert")
	}

	runMessageViews(t, db,
		NewChatsView(db, ""), NewChatsView(db, shadowSuffix),
		NewStarredView(db, ""), NewStarredView(db, shadowSuffix),
		NewStatusMessagesView(db, ""), NewStatusMessagesView(db, shadowSuffix),
		NewCallEventsView(db, ""), NewCallEventsView(db, shadowSuffix),
	)

	// After unstar and call delete those tables are empty; status has 2 rows.
	for _, c := range []struct {
		name       string
		active, sh []map[string]string
		cols       []string
	}{
		{"starred",
			dumpRows(t, db, starredDumpQuery(store.StarredTable)),
			dumpRows(t, db, starredDumpQuery(store.StarredTable+shadowSuffix)),
			starredDumpCols},
		{"status",
			dumpRows(t, db, statusDumpQuery(store.StatusMessagesTable)),
			dumpRows(t, db, statusDumpQuery(store.StatusMessagesTable+shadowSuffix)),
			statusDumpCols},
		{"calls",
			dumpRows(t, db, callDumpQuery(store.CallEventsTable)),
			dumpRows(t, db, callDumpQuery(store.CallEventsTable+shadowSuffix)),
			callDumpCols},
	} {
		if len(c.active) != len(c.sh) {
			t.Fatalf("%s counts active=%d shadow=%d", c.name, len(c.active), len(c.sh))
		}
		for i := range c.active {
			for _, col := range c.cols {
				if c.active[i][col] != c.sh[i][col] {
					t.Fatalf("%s col %s: active=%s shadow=%s",
						c.name, col, c.active[i][col], c.sh[i][col])
				}
			}
		}
	}

	if len(dumpRows(t, db, statusDumpQuery(store.StatusMessagesTable))) != 2 {
		t.Fatal("two status rows should remain")
	}
	if len(dumpRows(t, db, callDumpQuery(store.CallEventsTable))) != 0 {
		t.Fatal("all inbound calls should be deleted")
	}
}

func TestExtrasViewsLegacyParity(t *testing.T) {
	db := openRunnerDB(t)
	createExtrasShadow(t, db)
	now := time.Now().Unix()
	gid := "999@g.us"

	// call_events FK requires the chat to exist first (old call path did
	// UpsertChat before the call row).
	if _, err := db.Exec(`INSERT INTO chats(jid,kind,name,last_message_ts) VALUES(?,?,?,?)`,
		gid, "group", "Legacy Group", now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO starred(chat_jid,msg_id,sender_jid,from_me,starred_at)
		VALUES(?,?,?,?,?)`, gid, "m1", "alice@s.whatsapp.net", 0, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO status_messages
		(msg_id,ts,from_me,sender_jid,sender_name,text,font)
		VALUES(?,?,?,?,?,?,?)`, "leg-s", now, 0, "bob@s.whatsapp.net", "Bob", "legacy", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO call_events
		(chat_jid,chat_name,sender_jid,sender_name,call_id,msg_id,event_type,direction,ts,participants)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		gid, "Legacy Group", "alice@s.whatsapp.net", "Alice", "c1", nil,
		"call_log", "inbound", now, `[{"jid":"alice@s.whatsapp.net"}]`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BootstrapLegacySnapshots(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	runMessageViews(t, db,
		NewStarredView(db, shadowSuffix),
		NewStatusMessagesView(db, shadowSuffix),
		NewCallEventsView(db, shadowSuffix),
	)

	pairs := []struct {
		col                    string
		active, shadow, keyCol string
		cols                   []string
	}{
		{"starred", store.StarredTable, store.StarredTable + shadowSuffix, "chat_jid", starredDumpCols},
		{"status", store.StatusMessagesTable, store.StatusMessagesTable + shadowSuffix, "msg_id", statusDumpCols},
		{"calls", store.CallEventsTable, store.CallEventsTable + shadowSuffix, "call_id", callDumpCols},
	}
	for _, p := range pairs {
		active := dumpRows(t, db, fmt.Sprintf("SELECT * FROM %s", p.active))
		shadow := dumpRows(t, db, fmt.Sprintf("SELECT * FROM %s", p.shadow))
		if len(active) != 1 || len(shadow) != 1 {
			t.Fatalf("%s counts active=%d shadow=%d", p.col, len(active), len(shadow))
		}
		for col, v := range active[0] {
			if shadow[0][col] != v {
				t.Fatalf("%s legacy col %s: active=%s shadow=%s", p.col, col, v, shadow[0][col])
			}
		}
	}
}

func TestExtrasViewsReplayDeterminism(t *testing.T) {
	db := openRunnerDB(t)
	createExtrasShadow(t, db)
	appendStarLedgerEvent(t, db, ledger.CanonicalStar{
		ChatJID: "5550101@s.whatsapp.net", MsgID: "m1", Starred: true, StarredTS: 1700001000,
	})
	appendStatusLedgerEvent(t, db, ledger.CanonicalStatusMessage{
		MsgID: "s1", TS: 1700001100, Text: "status",
	})
	appendCallLedgerEvent(t, db, ledger.CanonicalCallEvent{
		ChatJID: "5550101@s.whatsapp.net", CallID: "c1",
		EventType: "call_log", TS: 1700001200,
	})

	views := []View{
		NewStarredView(db, shadowSuffix),
		NewStatusMessagesView(db, shadowSuffix),
		NewCallEventsView(db, shadowSuffix),
	}
	runMessageViews(t, db, views...)
	first := map[string][]map[string]string{
		"starred": dumpRows(t, db, starredDumpQuery(store.StarredTable+shadowSuffix)),
		"status":  dumpRows(t, db, statusDumpQuery(store.StatusMessagesTable+shadowSuffix)),
		"calls":   dumpRows(t, db, callDumpQuery(store.CallEventsTable+shadowSuffix)),
	}

	for _, v := range views {
		if err := db.SetCheckpoint(v.Name(), 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"starred_shadow", "status_messages_shadow", "call_events_shadow"} {
		if _, err := db.Exec("DELETE FROM " + table); err != nil {
			t.Fatal(err)
		}
	}
	runMessageViews(t, db, views...)
	again := map[string][]map[string]string{
		"starred": dumpRows(t, db, starredDumpQuery(store.StarredTable+shadowSuffix)),
		"status":  dumpRows(t, db, statusDumpQuery(store.StatusMessagesTable+shadowSuffix)),
		"calls":   dumpRows(t, db, callDumpQuery(store.CallEventsTable+shadowSuffix)),
	}
	for name := range first {
		if mustJSON(first[name]) != mustJSON(again[name]) {
			t.Fatalf("%s replay nondeterministic:\nbefore=%s\nafter=%s",
				name, mustJSON(first[name]), mustJSON(again[name]))
		}
	}
}
