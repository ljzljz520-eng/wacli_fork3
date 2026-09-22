package projector

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// --- shadow schema -----------------------------------------------------------

func createChatsShadow(t *testing.T, db *store.DB) {
	t.Helper()
	// Reuse the chats/contacts shadow tables; the extra message shadow tables
	// are harmless.
	createMessagesShadow(t, db)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS groups_shadow (
		jid TEXT PRIMARY KEY, name TEXT, owner_jid TEXT, created_ts INTEGER,
		is_parent INTEGER NOT NULL DEFAULT 0, linked_parent_jid TEXT,
		left_at INTEGER, updated_at INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create groups shadow: %v", err)
	}
}

// --- event append helpers ----------------------------------------------------

func appendMessageLedgerEventWithSource(t *testing.T, db *store.DB, pm wa.ParsedMessage, source string) {
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

	flags := int64(0)
	if pm.FromMe {
		flags |= ledger.FlagFromMe
	}
	batchID, err := db.StartBatch(source)
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	serverTS := pm.Timestamp.Unix()
	_, _, err = db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        source,
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
		BatchID:       batchID,
		Flags:         flags,
		RawBytes:      rawBytes,
	})
	if err != nil {
		t.Fatalf("AppendEvent message (%s): %v", source, err)
	}
}

func appendStateLedgerEvent(t *testing.T, db *store.DB, source, eventType string, state ledger.StateEvent) {
	t.Helper()
	rawBytes, rawHash, err := ledger.HashCanonical(state)
	if err != nil {
		t.Fatalf("hash state: %v", err)
	}
	dk := ledger.DedupKey(source+":"+eventType, state.Type, state.Chat, state.Sender,
		state.MsgID, strconv.FormatBool(state.FromMe), strconv.FormatBool(state.State),
		strconv.FormatInt(state.EndTSMS, 10), strconv.FormatInt(state.Count, 10),
		strconv.FormatInt(state.TimestampMS, 10))
	batchID, _ := db.StartBatch(source)
	_, _, err = db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        source,
		EventType:     eventType,
		ChatJID:       state.Chat,
		SenderJID:     state.Sender,
		ServerTS:      state.TimestampMS / 1000,
		EventTS:       state.TimestampMS / 1000,
		ReceivedAt:    time.Now().Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	if err != nil {
		t.Fatalf("AppendEvent state %s/%s: %v", source, eventType, err)
	}
}

func appendReceiptLedgerEvent(t *testing.T, db *store.DB, receiptType, chatJID string, ids []string, ts time.Time) {
	t.Helper()
	c := ledger.NewCanonicalReceipt(receiptType, chatJID, "", "", ids, ts)
	rawBytes, rawHash, err := ledger.HashCanonical(c)
	if err != nil {
		t.Fatalf("hash receipt: %v", err)
	}
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	dk := ledger.DedupKey("receipt", append([]string{
		receiptType, chatJID, strconv.FormatInt(ts.UnixMilli(), 10),
	}, sorted...)...)
	batchID, _ := db.StartBatch(ledger.SourceReceipt)
	_, _, err = db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceReceipt,
		EventType:     ledger.EventReceipt,
		ChatJID:       chatJID,
		ServerTS:      ts.Unix(),
		EventTS:       ts.Unix(),
		ReceivedAt:    time.Now().Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	if err != nil {
		t.Fatalf("AppendEvent receipt: %v", err)
	}
}

// --- row helpers --------------------------------------------------------------

func chatRow(t *testing.T, db *store.DB, table, jid string) map[string]string {
	t.Helper()
	rows := dumpRows(t, db, fmt.Sprintf(`SELECT jid, kind, name, last_message_ts,
		archived, pinned, muted_until, unread, unread_count
		FROM %s WHERE jid = '%s'`, table, jid))
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func newLiveMessageEvent(chat, sender types.JID, id string, ts time.Time, msg *waE2E.Message) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: sender},
			ID:            id, Timestamp: ts,
		},
		Message: msg, RawMessage: msg,
	}
}

func newGroupMessageEvent(group, sender types.JID, id string, ts time.Time, msg *waE2E.Message, fromMe bool) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: group, Sender: sender, IsFromMe: fromMe},
			ID:            id, Timestamp: ts,
		},
		Message: msg, RawMessage: msg,
	}
}

// --- tests --------------------------------------------------------------------

func TestChatsViewUnreadFold(t *testing.T) {
	db := openRunnerDB(t)
	createChatsShadow(t, db)
	chat := types.NewJID("77700999", types.DefaultUserServer)
	base := time.Date(2025, 5, 6, 7, 8, 9, 0, time.UTC)
	if err := db.UpsertContact(chat.String(), "77700999", "Pushy", "Alice Full", "Ali", ""); err != nil {
		t.Fatal(err)
	}
	copyNamesToShadow(t, db)

	runner := New(db, []View{NewChatsView(db, ""), NewChatsView(db, shadowSuffix)}).WithBatchSize(2)
	catchUp := func(marker, count int64) {
		t.Helper()
		if _, err := runner.Run(context.Background(), 0); err != nil {
			t.Fatalf("Run: %v", err)
		}
		for _, table := range []string{store.ChatsTable, store.ChatsTable + shadowSuffix} {
			row := chatRow(t, db, table, chat.String())
			if row == nil {
				t.Fatalf("%s: chat row missing", table)
			}
			if row["unread"] != "int:"+strconv.FormatInt(marker, 10) ||
				row["unread_count"] != "int:"+strconv.FormatInt(count, 10) {
				t.Fatalf("%s: unread=%s count=%s, want marker=%d count=%d",
					table, row["unread"], row["unread_count"], marker, count)
			}
		}
	}

	// 1. History snapshot: explicit count=3.
	appendStateLedgerEvent(t, db, ledger.SourceHistory, ledger.EventMarkRead, ledger.StateEvent{
		Type: ledger.StateUnreadCount, Chat: chat.String(), Count: 3,
	})
	catchUp(1, 3)

	// 2. History message: no unread effect.
	hist := &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID: proto.String(chat.String()),
			ID:        proto.String("m_h1"),
			FromMe:    proto.Bool(false),
		},
		MessageTimestamp: proto.Uint64(uint64(base.Add(30 * time.Second).Unix())),
		Message:          &waE2E.Message{Conversation: proto.String("history text")},
	}
	pmh := wa.ParseHistoryMessage(chat.String(), hist)
	appendMessageLedgerEventWithSource(t, db, pmh, ledger.SourceHistory)
	catchUp(1, 3)

	// 3. Live peer message: increment.
	pm1 := wa.ParseLiveMessage(newLiveMessageEvent(chat, chat, "m_l1", base.Add(time.Minute),
		&waE2E.Message{Conversation: proto.String("live one")}))
	appendMessageLedgerEventWithSource(t, db, pm1, ledger.SourceLive)
	catchUp(1, 4)

	// 4. Mark-read (remote app state): both clear.
	appendStateLedgerEvent(t, db, ledger.SourceAppState, ledger.EventMarkRead, ledger.StateEvent{
		Type: ledger.StateMarkRead, Chat: chat.String(), State: true,
		TimestampMS: base.Add(90 * time.Second).UnixMilli(),
	})
	catchUp(0, 0)

	// 5. Another live peer message.
	pm2 := wa.ParseLiveMessage(newLiveMessageEvent(chat, chat, "m_l2", base.Add(2*time.Minute),
		&waE2E.Message{Conversation: proto.String("live two")}))
	appendMessageLedgerEventWithSource(t, db, pm2, ledger.SourceLive)
	catchUp(1, 1)

	// 6. Read-self receipt: clear.
	appendReceiptLedgerEvent(t, db, string(types.ReceiptTypeReadSelf), chat.String(),
		[]string{"m_l2"}, base.Add(150*time.Second))
	catchUp(0, 0)

	// 7. Explicit mark-unread: marker=1, count normalized to 1.
	appendStateLedgerEvent(t, db, ledger.SourceAppState, ledger.EventMarkRead, ledger.StateEvent{
		Type: ledger.StateMarkRead, Chat: chat.String(), State: false,
		TimestampMS: base.Add(180 * time.Second).UnixMilli(),
	})
	catchUp(1, 1)

	// 8. Later history snapshot overrides with explicit count=2.
	appendStateLedgerEvent(t, db, ledger.SourceHistory, ledger.EventMarkRead, ledger.StateEvent{
		Type: ledger.StateUnreadCount, Chat: chat.String(), Count: 2,
	})
	catchUp(1, 2)

	// Final row checks.
	for _, table := range []string{store.ChatsTable, store.ChatsTable + shadowSuffix} {
		row := chatRow(t, db, table, chat.String())
		if row["kind"] != "str:dm" || row["name"] != "str:Alice Full" {
			t.Fatalf("%s: kind/name = %s/%s", table, row["kind"], row["name"])
		}
		wantTS := "int:" + strconv.FormatInt(base.Add(2*time.Minute).Unix(), 10)
		if row["last_message_ts"] != wantTS {
			t.Fatalf("%s: last_message_ts = %s, want %s", table, row["last_message_ts"], wantTS)
		}
	}
}

func TestChatsViewOverlappingHistoryLiveDedup(t *testing.T) {
	db := openRunnerDB(t)
	createChatsShadow(t, db)
	chat := types.NewJID("77700888", types.DefaultUserServer)
	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	// Live first.
	env := &waE2E.Message{Conversation: proto.String("overlap")}
	pml := wa.ParseLiveMessage(newLiveMessageEvent(chat, chat, "m_x", base, env))
	appendMessageLedgerEventWithSource(t, db, pml, ledger.SourceLive)

	// Same message replayed through history (different envelope encoding,
	// same WA key): it must not increment unread.
	hist := &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID: proto.String(chat.String()),
			ID:        proto.String("m_x"),
			FromMe:    proto.Bool(false),
		},
		MessageTimestamp: proto.Uint64(uint64(base.Unix())),
		Message:          env,
	}
	pmh := wa.ParseHistoryMessage(chat.String(), hist)
	appendMessageLedgerEventWithSource(t, db, pmh, ledger.SourceHistory)

	runMessageViews(t, db, NewChatsView(db, shadowSuffix))
	row := chatRow(t, db, store.ChatsTable+shadowSuffix, chat.String())
	if row == nil {
		t.Fatal("chat missing")
	}
	if row["unread"] != "int:1" || row["unread_count"] != "int:1" {
		t.Fatalf("unread=%s count=%s, history replay must not increment",
			row["unread"], row["unread_count"])
	}
}

func TestChatsViewStatusBroadcastAndGroup(t *testing.T) {
	db := openRunnerDB(t)
	createChatsShadow(t, db)
	base := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)

	// Live status broadcast message: no chats row at all.
	statusChat := types.StatusBroadcastJID
	pmStatus := wa.ParseLiveMessage(newLiveMessageEvent(statusChat, statusChat, "s1", base,
		&waE2E.Message{Conversation: proto.String("my status")}))
	appendMessageLedgerEventWithSource(t, db, pmStatus, ledger.SourceLive)

	// Group with a known subject.
	group := types.NewJID("111222333", types.GroupServer)
	member := types.NewJID("77700555", types.DefaultUserServer)
	for _, table := range []string{store.GroupsTable, store.GroupsTable + shadowSuffix} {
		if _, err := db.Exec(fmt.Sprintf(
			`INSERT INTO %s(jid,name,updated_at) VALUES(?, ?, ?)`, table),
			group.String(), "Friends", base.Unix()); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}

	pmg := wa.ParseLiveMessage(newGroupMessageEvent(group, member, "g1", base.Add(time.Minute),
		&waE2E.Message{Conversation: proto.String("hi group")}, false))
	appendMessageLedgerEventWithSource(t, db, pmg, ledger.SourceLive)

	// Own group message: no increment.
	pmgMe := wa.ParseLiveMessage(newGroupMessageEvent(group, member, "g2", base.Add(2*time.Minute),
		&waE2E.Message{Conversation: proto.String("my reply")}, true))
	appendMessageLedgerEventWithSource(t, db, pmgMe, ledger.SourceLive)

	runMessageViews(t, db, NewChatsView(db, shadowSuffix))

	if row := chatRow(t, db, store.ChatsTable+shadowSuffix, statusChat.String()); row != nil {
		t.Fatalf("status broadcast must not create a chats row: %+v", row)
	}
	row := chatRow(t, db, store.ChatsTable+shadowSuffix, group.String())
	if row == nil {
		t.Fatal("group chat missing")
	}
	if row["kind"] != "str:group" || row["name"] != "str:Friends" {
		t.Fatalf("group kind/name = %s/%s", row["kind"], row["name"])
	}
	if row["unread"] != "int:1" || row["unread_count"] != "int:1" {
		t.Fatalf("group unread=%s count=%s, want 1/1", row["unread"], row["unread_count"])
	}
}

func TestChatsViewLegacySnapshotNormalization(t *testing.T) {
	db := openRunnerDB(t)
	createChatsShadow(t, db)
	now := time.Now().Unix()
	c1 := types.NewJID("55500001", types.DefaultUserServer).String()
	c2 := types.NewJID("55500002", types.DefaultUserServer).String()
	if _, err := db.Exec(`INSERT INTO chats
		(jid,kind,name,last_message_ts,archived,pinned,muted_until,unread,unread_count)
		VALUES(?,?,?,?,'1',0,0,1,0),(?,?,?,?,'0',0,0,0,5)`,
		c1, "dm", "Marked", now, c2, "dm", "Read", now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BootstrapLegacySnapshots(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	runMessageViews(t, db, NewChatsView(db, shadowSuffix))

	r1 := chatRow(t, db, store.ChatsTable+shadowSuffix, c1)
	if r1["unread"] != "int:1" || r1["unread_count"] != "int:1" {
		t.Fatalf("c1 marker=1 must normalize count to 1: %s/%s", r1["unread"], r1["unread_count"])
	}
	if r1["archived"] != "int:1" || r1["name"] != "str:Marked" {
		t.Fatalf("c1 other columns must be preserved: %+v", r1)
	}
	r2 := chatRow(t, db, store.ChatsTable+shadowSuffix, c2)
	if r2["unread"] != "int:0" || r2["unread_count"] != "int:0" {
		t.Fatalf("c2 marker=0 must clear count: %s/%s", r2["unread"], r2["unread_count"])
	}
}

func TestChatsViewArchivePinMute(t *testing.T) {
	db := openRunnerDB(t)
	createChatsShadow(t, db)
	chat := types.NewJID("77700777", types.DefaultUserServer)
	base := time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC)

	pm := wa.ParseLiveMessage(newLiveMessageEvent(chat, chat, "m1", base,
		&waE2E.Message{Conversation: proto.String("hi")}))
	appendMessageLedgerEventWithSource(t, db, pm, ledger.SourceLive)

	appendStateLedgerEvent(t, db, ledger.SourceAppState, ledger.EventArchive, ledger.StateEvent{
		Type: ledger.StateArchive, Chat: chat.String(), State: true,
		TimestampMS: base.Add(time.Minute).UnixMilli(),
	})
	appendStateLedgerEvent(t, db, ledger.SourceAppState, ledger.EventPin, ledger.StateEvent{
		Type: ledger.StatePin, Chat: chat.String(), State: true,
		TimestampMS: base.Add(2 * time.Minute).UnixMilli(),
	})
	muteUntil := base.Add(24 * time.Hour).UnixMilli()
	appendStateLedgerEvent(t, db, ledger.SourceAppState, ledger.EventMute, ledger.StateEvent{
		Type: ledger.StateMute, Chat: chat.String(), EndTSMS: muteUntil,
		TimestampMS: base.Add(3 * time.Minute).UnixMilli(),
	})

	runMessageViews(t, db, NewChatsView(db, shadowSuffix))
	row := chatRow(t, db, store.ChatsTable+shadowSuffix, chat.String())
	if row["archived"] != "int:1" || row["pinned"] != "int:1" ||
		row["muted_until"] != "int:"+strconv.FormatInt(muteUntil, 10) {
		t.Fatalf("state columns = %+v", row)
	}

	// Unarchive / unmute.
	appendStateLedgerEvent(t, db, ledger.SourceAppState, ledger.EventArchive, ledger.StateEvent{
		Type: ledger.StateArchive, Chat: chat.String(), State: false,
		TimestampMS: base.Add(4 * time.Minute).UnixMilli(),
	})
	appendStateLedgerEvent(t, db, ledger.SourceAppState, ledger.EventMute, ledger.StateEvent{
		Type: ledger.StateMute, Chat: chat.String(), EndTSMS: 0,
		TimestampMS: base.Add(5 * time.Minute).UnixMilli(),
	})
	runMessageViews(t, db, NewChatsView(db, shadowSuffix))
	row = chatRow(t, db, store.ChatsTable+shadowSuffix, chat.String())
	if row["archived"] != "int:0" || row["muted_until"] != "int:0" {
		t.Fatalf("cleared state columns = %+v", row)
	}
}
