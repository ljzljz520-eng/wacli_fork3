package projector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const shadowSuffix = "_shadow"

// --- shadow schema -----------------------------------------------------------

func createMessagesShadow(t *testing.T, db *store.DB) bool {
	t.Helper()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS chats_shadow (
			jid TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT,
			last_message_ts INTEGER, archived INTEGER NOT NULL DEFAULT 0,
			pinned INTEGER NOT NULL DEFAULT 0, muted_until INTEGER NOT NULL DEFAULT 0,
			unread INTEGER NOT NULL DEFAULT 0, unread_count INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE IF NOT EXISTS contacts_shadow (
			jid TEXT PRIMARY KEY, phone TEXT, push_name TEXT, full_name TEXT,
			first_name TEXT, business_name TEXT, system_name TEXT,
			updated_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS messages_shadow (
			rowid INTEGER PRIMARY KEY AUTOINCREMENT,
			chat_jid TEXT NOT NULL, chat_name TEXT, msg_id TEXT NOT NULL,
			sender_jid TEXT, sender_name TEXT, ts INTEGER NOT NULL,
			from_me INTEGER NOT NULL, text TEXT, display_text TEXT,
			quoted_msg_id TEXT, quoted_sender_jid TEXT,
			is_forwarded INTEGER NOT NULL DEFAULT 0,
			forwarding_score INTEGER NOT NULL DEFAULT 0,
			reaction_to_id TEXT, reaction_emoji TEXT,
			media_type TEXT, media_caption TEXT, filename TEXT,
			mime_type TEXT, direct_path TEXT,
			media_key BLOB, file_sha256 BLOB, file_enc_sha256 BLOB,
			file_length INTEGER, local_path TEXT, downloaded_at INTEGER,
			media_unavailable_at INTEGER, revoked INTEGER NOT NULL DEFAULT 0,
			deleted_for_me INTEGER NOT NULL DEFAULT 0, deleted_at INTEGER,
			deletion_reason TEXT, payload_purged_at INTEGER,
			edited INTEGER NOT NULL DEFAULT 0, edited_ts INTEGER NOT NULL DEFAULT 0,
			buttons TEXT, UNIQUE(chat_jid, msg_id))`,
		`CREATE TABLE IF NOT EXISTS message_locations_shadow (
			chat_jid TEXT NOT NULL, msg_id TEXT NOT NULL,
			latitude REAL NOT NULL, longitude REAL NOT NULL,
			name TEXT, address TEXT, is_live INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (chat_jid, msg_id))`,
		`CREATE TABLE IF NOT EXISTS message_payload_purges_shadow (
			chat_jid TEXT NOT NULL, msg_id TEXT NOT NULL,
			purged_at INTEGER NOT NULL, deleted_at INTEGER NOT NULL,
			deletion_reason TEXT NOT NULL,
			PRIMARY KEY (chat_jid, msg_id))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("create shadow schema: %v\n%s", err, s)
		}
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts_shadow
		USING fts5(text, media_caption, filename, chat_name, sender_name, display_text)`); err != nil {
		return false
	}
	return true
}

func copyNamesToShadow(t *testing.T, db *store.DB) {
	t.Helper()
	if _, err := db.Exec(`INSERT OR REPLACE INTO chats_shadow SELECT * FROM chats`); err != nil {
		t.Fatalf("copy chats: %v", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO contacts_shadow SELECT * FROM contacts`); err != nil {
		t.Fatalf("copy contacts: %v", err)
	}
}

func tableExists(db *store.DB, name string) bool {
	var found int
	_ = db.QueryRow(fmt.Sprintf(
		`SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = '%s'`, name)).Scan(&found)
	return found == 1
}

// --- ledger event append helpers (mirror app ingestion) ----------------------

func appendMessageLedgerEvent(t *testing.T, db *store.DB, pm wa.ParsedMessage) {
	t.Helper()
	env, ok := pm.RawEnvelope.(proto.Message)
	if !ok {
		t.Fatal("parsed message has no proto RawEnvelope")
	}
	rawBytes, rawHash, err := ledger.HashProto(env)
	if err != nil {
		t.Fatalf("hash envelope: %v", err)
	}
	chat := pm.Chat.String()
	participant := ""
	if pm.Chat.Server == types.GroupServer && !pm.FromMe {
		participant = pm.SenderJID
	}
	key := ledger.WAKey(chat, pm.FromMe, pm.ID, participant)
	dk := ledger.DedupKey("message", key)

	var refs []ledger.CausalRef
	if pm.ReplyToID != "" {
		refs = append(refs, ledger.WAKeyRef(chat, pm.ReplyToID, pm.ReplyToSenderJID, false))
	}
	if pm.ReactionToID != "" && pm.ReactionToID != pm.ReplyToID {
		refs = append(refs, ledger.WAKeyRef(chat, pm.ReactionToID, "", false))
	}

	flags := int64(0)
	if pm.FromMe {
		flags |= ledger.FlagFromMe
	}
	batchID, err := db.StartBatch(ledger.SourceLive)
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	serverTS := pm.Timestamp.Unix()
	_, _, err = db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceLive,
		EventType:     ledger.EventMessage,
		WAKey:         key,
		ChatJID:       chat,
		MsgID:         pm.ID,
		SenderJID:     pm.SenderJID,
		ServerTS:      serverTS,
		EventTS:       serverTS,
		ReceivedAt:    time.Now().Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		CausalRefs:    refs,
		BatchID:       batchID,
		Flags:         flags,
		RawBytes:      rawBytes,
	})
	if err != nil {
		t.Fatalf("AppendEvent message: %v", err)
	}
}

func appendDeleteForMeEvent(t *testing.T, db *store.DB, chatJID, senderJID, msgID string, fromMe, deleteMedia bool, ts time.Time) {
	t.Helper()
	tsMS := ts.UnixMilli()
	state := ledger.StateEvent{
		Type: ledger.StateDeleteForMe, Chat: chatJID, Sender: senderJID,
		FromMe: fromMe, MsgID: msgID, DeleteMedia: deleteMedia, TimestampMS: tsMS,
	}
	rawBytes, rawHash, err := ledger.HashCanonical(state)
	if err != nil {
		t.Fatalf("hash state: %v", err)
	}
	dk := ledger.DedupKey("delete_for_me", chatJID, senderJID,
		strconv.FormatBool(fromMe), msgID, strconv.FormatInt(tsMS, 10),
		strconv.FormatBool(deleteMedia))
	ref := ledger.WAKeyRef(chatJID, msgID, senderJID, fromMe)
	batchID, _ := db.StartBatch(ledger.SourceAppState)
	_, _, err = db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceAppState,
		EventType:     ledger.EventDeleteForMe,
		ChatJID:       chatJID,
		MsgID:         msgID,
		SenderJID:     senderJID,
		ServerTS:      ts.Unix(),
		EventTS:       ts.Unix(),
		ReceivedAt:    time.Now().Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		CausalRefs:    []ledger.CausalRef{ref},
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	if err != nil {
		t.Fatalf("AppendEvent delete_for_me: %v", err)
	}
}

func sha256SumHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func appendScrubEvent(t *testing.T, db *store.DB, chatJID, msgID string) {
	t.Helper()
	dk := ledger.DedupKey("scrub", chatJID, msgID)
	sum := sha256SumHex(dk)
	_, _, err := db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, sum),
		Source:        ledger.SourceScrub,
		EventType:     ledger.EventScrub,
		ChatJID:       chatJID,
		MsgID:         msgID,
		ServerTS:      time.Now().Unix(),
		EventTS:       time.Now().Unix(),
		ReceivedAt:    time.Now().Unix(),
		RawHash:       sum,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
	})
	if err != nil {
		t.Fatalf("AppendEvent scrub: %v", err)
	}
}

func runMessageViews(t *testing.T, db *store.DB, views ...View) {
	t.Helper()
	runner := New(db, views).WithBatchSize(2)
	if _, err := runner.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// --- deterministic row dumps -------------------------------------------------

func dumpRows(t *testing.T, db *store.DB, query string) []map[string]string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("dump query: %v\n%s", err, query)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	out := make([]map[string]string, 0)
	for rows.Next() {
		vals := make([]any, len(cols))
		for i := range vals {
			vals[i] = new(any)
		}
		if err := rows.Scan(vals...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		m := make(map[string]string, len(cols))
		for i, col := range cols {
			m[col] = normalizeDumpedValue(*vals[i].(*any))
		}
		out = append(out, m)
	}
	return out
}

func normalizeDumpedValue(v any) string {
	switch n := v.(type) {
	case nil:
		return "<null>"
	case int64:
		return "int:" + strconv.FormatInt(n, 10)
	case float64:
		return "float:" + strconv.FormatFloat(n, 'g', -1, 64)
	case []byte:
		return "blob:" + hex.EncodeToString(n)
	case string:
		return "str:" + n
	default:
		return "other:" + fmt.Sprintf("%v", n)
	}
}

func messagesDumpQuery(table string) string {
	cols := ""
	for i, c := range legacyMessageColumns {
		if i > 0 {
			cols += ", "
		}
		cols += c
	}
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY chat_jid, msg_id`, cols, table)
}

func ftsDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT text, media_caption, filename, chat_name, sender_name, display_text
		FROM %s ORDER BY rowid`, table)
}

// --- tests -------------------------------------------------------------------

func TestMessagesViewLegacySnapshotParity(t *testing.T) {
	db := openRunnerDB(t)
	now := time.Now().Unix()
	chat := types.NewJID("5550101", types.DefaultUserServer).String()
	if _, err := db.Exec(`INSERT INTO chats(jid,kind,name,last_message_ts,archived,pinned,muted_until,unread,unread_count)
		VALUES(?,?,?,?,0,0,0,0,0)`, chat, "dm", "Alice", now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO contacts(jid,phone,push_name,full_name,first_name,business_name,updated_at)
		VALUES(?,?,?,?,?,?,?)`, chat, "5550101", "Pushy", "Alice Full", "Ali", "", now); err != nil {
		t.Fatal(err)
	}
	mediaKey := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	if _, err := db.Exec(`INSERT INTO messages
		(chat_jid,chat_name,msg_id,sender_jid,sender_name,ts,from_me,text,display_text,
			is_forwarded,forwarding_score,media_type,media_caption,media_key,
			revoked,deleted_for_me,edited,edited_ts)
		VALUES(?,?,?,?,?,?,0,?,?,0,0,?,?,?,0,0,0,0)`,
		chat, "Alice", "leg1", chat, "Alice Full", now, "legacy text",
		"legacy text", "image", "the caption", mediaKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO message_locations
		(chat_jid,msg_id,latitude,longitude,name,address,is_live)
		VALUES(?,?,37.77,-122.42,?,?,1)`, chat, "legloc", "Tower", "St 1"); err != nil {
		t.Fatal(err)
	}

	report, err := db.BootstrapLegacySnapshots()
	if err != nil {
		t.Fatalf("BootstrapLegacySnapshots: %v", err)
	}
	if report.PerTable[store.MessagesTable].Appended != 1 ||
		report.PerTable[store.MessageLocationsTable].Appended != 1 {
		t.Fatalf("bootstrap counts = %+v", report.PerTable)
	}

	withFTS := createMessagesShadow(t, db)
	copyNamesToShadow(t, db)
	runMessageViews(t, db, NewMessagesView(db, shadowSuffix, withFTS))

	active := dumpRows(t, db, messagesDumpQuery(store.MessagesTable))
	shadow := dumpRows(t, db, messagesDumpQuery(store.MessagesTable+shadowSuffix))
	if len(active) != 1 || len(shadow) != 1 {
		t.Fatalf("row counts active=%d shadow=%d", len(active), len(shadow))
	}
	for _, col := range legacyMessageColumns {
		if active[0][col] != shadow[0][col] {
			t.Fatalf("column %s: active=%s shadow=%s", col, active[0][col], shadow[0][col])
		}
	}

	loc := dumpRows(t, db, `SELECT chat_jid,msg_id,latitude,longitude,name,address,is_live
		FROM message_locations_shadow ORDER BY chat_jid,msg_id`)
	if len(loc) != 1 {
		t.Fatalf("shadow locations = %d, want 1", len(loc))
	}
	if loc[0]["latitude"] != "float:37.77" || loc[0]["name"] != "str:Tower" ||
		loc[0]["is_live"] != "int:1" {
		t.Fatalf("shadow location = %+v", loc[0])
	}

	if withFTS {
		activeFTS := dumpRows(t, db, ftsDumpQuery("messages_fts"))
		shadowFTS := dumpRows(t, db, ftsDumpQuery("messages_fts"+shadowSuffix))
		if len(activeFTS) != len(shadowFTS) {
			t.Fatalf("FTS row counts active=%d shadow=%d", len(activeFTS), len(shadowFTS))
		}
		for i := range activeFTS {
			for col, v := range activeFTS[i] {
				if shadowFTS[i][col] != v {
					t.Fatalf("FTS row %d col %s: active=%s shadow=%s", i, col, v, shadowFTS[i][col])
				}
			}
		}
	}
}

func TestMessagesViewLiveProjectionParity(t *testing.T) {
	db := openRunnerDB(t)
	withFTS := createMessagesShadow(t, db)

	chat := types.NewJID("77700011", types.DefaultUserServer)
	ts := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := db.UpsertChat(chat.String(), "dm", "Alice", ts); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertContact(chat.String(), "77700011", "Pushy", "Alice Full", "Ali", ""); err != nil {
		t.Fatal(err)
	}
	copyNamesToShadow(t, db)

	// m1: peer text.
	m1 := &waE2E.Message{Conversation: proto.String("first message")}
	pm1 := wa.ParseLiveMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "m1", Timestamp: ts,
		},
		Message: m1, RawMessage: m1,
	})
	appendMessageLedgerEvent(t, db, pm1)

	// m2: peer reaction to m1.
	m2 := &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
		Key:  &waCommon.MessageKey{ID: proto.String("m1"), FromMe: proto.Bool(false)},
		Text: proto.String("👍"),
	}}
	pm2 := wa.ParseLiveMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "m2", Timestamp: ts.Add(time.Second),
		},
		Message: m2, RawMessage: m2,
	})
	appendMessageLedgerEvent(t, db, pm2)

	// m3: from-me quote of m1.
	m3 := &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String("my reply"),
		ContextInfo: &waE2E.ContextInfo{
			StanzaID: proto.String("m1"),
		},
	}}
	meSource := types.MessageSource{Chat: chat, IsFromMe: true}
	pm3 := wa.ParseLiveMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: meSource,
			ID:            "m3", Timestamp: ts.Add(2 * time.Second),
		},
		Message: m3, RawMessage: m3,
	})
	appendMessageLedgerEvent(t, db, pm3)

	runMessageViews(t, db,
		NewMessagesView(db, "", withFTS),
		NewMessagesView(db, shadowSuffix, withFTS))

	active := dumpRows(t, db, messagesDumpQuery(store.MessagesTable))
	shadow := dumpRows(t, db, messagesDumpQuery(store.MessagesTable+shadowSuffix))
	if len(active) != 3 || len(shadow) != 3 {
		t.Fatalf("row counts active=%d shadow=%d", len(active), len(shadow))
	}
	for i := range active {
		for _, col := range legacyMessageColumns {
			if active[i][col] != shadow[i][col] {
				t.Fatalf("row %d column %s: active=%s shadow=%s",
					i, col, active[i][col], shadow[i][col])
			}
		}
	}
	wantDisplay := map[string]string{
		"m1": "first message",
		"m2": "Reacted 👍 to first message",
		"m3": "> first message\nmy reply",
	}
	for _, r := range shadow {
		want := "str:" + wantDisplay[r["msg_id"][4:]]
		if r["display_text"] != want {
			t.Fatalf("display %s = %s, want %s", r["msg_id"], r["display_text"], want)
		}
	}

	if withFTS {
		activeFTS := dumpRows(t, db, ftsDumpQuery("messages_fts"))
		shadowFTS := dumpRows(t, db, ftsDumpQuery("messages_fts"+shadowSuffix))
		if len(activeFTS) != 3 || len(shadowFTS) != 3 {
			t.Fatalf("FTS counts active=%d shadow=%d", len(activeFTS), len(shadowFTS))
		}
		for i := range activeFTS {
			for col, v := range activeFTS[i] {
				if shadowFTS[i][col] != v {
					t.Fatalf("FTS row %d col %s mismatch", i, col)
				}
			}
		}
	}
}

func TestMessagesViewDeleteForMe(t *testing.T) {
	db := openRunnerDB(t)
	withFTS := createMessagesShadow(t, db)
	chat := types.NewJID("88800022", types.DefaultUserServer)
	ts := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := db.UpsertChat(chat.String(), "dm", "Bob", ts); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertContact(chat.String(), "88800022", "", "Bob", "", ""); err != nil {
		t.Fatal(err)
	}
	copyNamesToShadow(t, db)

	m1 := &waE2E.Message{Conversation: proto.String("please delete")}
	pm1 := wa.ParseLiveMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "m1", Timestamp: ts,
		},
		Message: m1, RawMessage: m1,
	})
	appendMessageLedgerEvent(t, db, pm1)
	appendDeleteForMeEvent(t, db, chat.String(), chat.String(), "m1", false, true, ts.Add(time.Minute))
	// Ghost message: delete-for-me arrives with no underlying message event.
	appendDeleteForMeEvent(t, db, chat.String(), chat.String(), "ghost", false, false, ts.Add(2*time.Minute))

	runMessageViews(t, db, NewMessagesView(db, shadowSuffix, withFTS))

	rows := dumpRows(t, db, `SELECT msg_id, deleted_for_me, deleted_at, text, display_text
		FROM messages_shadow ORDER BY msg_id`)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	byID := map[string]map[string]string{}
	for _, r := range rows {
		byID[r["msg_id"][4:]] = r
	}
	if byID["m1"]["deleted_for_me"] != "int:1" || byID["m1"]["deleted_at"] == "<null>" {
		t.Fatalf("m1 = %+v, want tombstoned", byID["m1"])
	}
	if byID["m1"]["text"] != "str:please delete" {
		// Mark preserves content columns; only FTS/payload state changes.
		t.Fatalf("m1 text should be preserved, got %s", byID["m1"]["text"])
	}
	if byID["ghost"]["deleted_for_me"] != "int:1" || byID["ghost"]["deleted_at"] == "<null>" {
		t.Fatalf("ghost fallback tombstone = %+v", byID["ghost"])
	}

	if withFTS {
		fts := dumpRows(t, db, ftsDumpQuery("messages_fts"+shadowSuffix))
		if len(fts) != 0 {
			t.Fatalf("FTS rows = %d, want 0 (both tombstoned)", len(fts))
		}
	}
}

func TestMessagesViewScrubAndReplayDeterminism(t *testing.T) {
	db := openRunnerDB(t)
	withFTS := createMessagesShadow(t, db)
	chat := types.NewJID("99900033", types.DefaultUserServer)
	ts := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	if err := db.UpsertChat(chat.String(), "dm", "Carol", ts); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertContact(chat.String(), "99900033", "", "Carol", "", ""); err != nil {
		t.Fatal(err)
	}
	copyNamesToShadow(t, db)

	m1 := &waE2E.Message{
		ImageMessage: &waE2E.ImageMessage{
			Caption:  proto.String("secret photo"),
			Mimetype: proto.String("image/jpeg"),
			MediaKey: []byte{0x10, 0x20, 0x30},
		},
	}
	pm1 := wa.ParseLiveMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "m1", Timestamp: ts,
		},
		Message: m1, RawMessage: m1,
	})
	appendMessageLedgerEvent(t, db, pm1)
	if _, err := db.Exec(`INSERT INTO message_locations
		(chat_jid,msg_id,latitude,longitude,name,address,is_live)
		VALUES(?,?,40,-74,?,'',1)`, chat.String(), "m1", "Spot"); err != nil {
		t.Fatal(err)
	}
	// The location also needs a legacy/location ledger event so shadow gets it:
	// project via legacy snapshot bootstrap for this single table.
	if _, err := db.BootstrapLegacySnapshots(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	appendScrubEvent(t, db, chat.String(), "m1")

	view := NewMessagesView(db, shadowSuffix, withFTS)
	runMessageViews(t, db, view)

	row := dumpRows(t, db, `SELECT chat_jid,msg_id,chat_name,sender_jid,sender_name,
		text,display_text,media_type,media_caption,mime_type,media_key,
		local_path,downloaded_at,deleted_at,payload_purged_at
		FROM messages_shadow`)[0]
	for col, want := range map[string]string{
		"chat_name": "<null>", "sender_jid": "<null>", "sender_name": "<null>",
		"text": "<null>", "display_text": "<null>", "media_type": "<null>",
		"media_caption": "<null>", "mime_type": "<null>", "media_key": "<null>",
		"local_path": "<null>", "downloaded_at": "<null>",
	} {
		if row[col] != want {
			t.Fatalf("after scrub %s = %s, want %s", col, row[col], want)
		}
	}
	if row["deleted_at"] == "<null>" || row["payload_purged_at"] == "<null>" {
		t.Fatalf("scrub must set deleted_at and payload_purged_at: %+v", row)
	}
	locs := dumpRows(t, db, `SELECT * FROM message_locations_shadow`)
	if len(locs) != 0 {
		t.Fatalf("locations after scrub = %d, want 0", len(locs))
	}
	if withFTS {
		if fts := dumpRows(t, db, ftsDumpQuery("messages_fts"+shadowSuffix)); len(fts) != 0 {
			t.Fatalf("FTS after scrub = %d rows, want 0", len(fts))
		}
	}

	// Determinism: reset checkpoint, wipe shadow outputs, replay from seq 0.
	msgSnapshot := dumpRows(t, db, messagesDumpQuery(store.MessagesTable+shadowSuffix))
	locSnapshot := dumpRows(t, db, `SELECT chat_jid,msg_id,latitude,longitude,name,address,is_live
		FROM message_locations_shadow ORDER BY chat_jid,msg_id`)
	var ftsSnapshot []map[string]string
	if withFTS {
		ftsSnapshot = dumpRows(t, db, ftsDumpQuery("messages_fts"+shadowSuffix))
	}

	if err := db.SetCheckpoint(view.Name(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM messages_shadow`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM message_locations_shadow`); err != nil {
		t.Fatal(err)
	}
	if withFTS {
		if _, err := db.Exec(`DROP TABLE messages_fts_shadow`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE VIRTUAL TABLE messages_fts_shadow
			USING fts5(text, media_caption, filename, chat_name, sender_name, display_text)`); err != nil {
			t.Fatal(err)
		}
	}
	runMessageViews(t, db, view)

	msgAgain := dumpRows(t, db, messagesDumpQuery(store.MessagesTable+shadowSuffix))
	if fmt.Sprint(msgSnapshot) != fmt.Sprint(msgAgain) {
		t.Fatalf("message replay nondeterministic:\nbefore=%v\nafter=%v", msgSnapshot, msgAgain)
	}
	locAgain := dumpRows(t, db, `SELECT chat_jid,msg_id,latitude,longitude,name,address,is_live
		FROM message_locations_shadow ORDER BY chat_jid,msg_id`)
	if fmt.Sprint(locSnapshot) != fmt.Sprint(locAgain) {
		t.Fatalf("location replay nondeterministic")
	}
	if withFTS {
		ftsAgain := dumpRows(t, db, ftsDumpQuery("messages_fts"+shadowSuffix))
		if fmt.Sprint(ftsSnapshot) != fmt.Sprint(ftsAgain) {
			t.Fatalf("FTS replay nondeterministic")
		}
	}
}
