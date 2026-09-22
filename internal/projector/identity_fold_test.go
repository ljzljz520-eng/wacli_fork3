package projector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
)

// TestIdentityResolutionFoldParity (TR-14.1): the event-driven fold applied
// by the projector views must leave the shadow tables byte-identical to the
// active tables folded directly through store.MigrateLIDToPN. The scenario is
// the same split-storage case covered by the store test, plus groups metadata.
func TestIdentityResolutionFoldParity(t *testing.T) {
	db := openRunnerDB(t)
	withFTS := createMessagesShadow(t, db)
	createGroupsShadow(t, db)
	createPollsShadow(t, db)
	createAliasesShadow(t, db)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	pn := "15551234567@s.whatsapp.net"
	lid := "999123456789@lid"
	group := "120363000000@g.us"

	// Chats.
	if err := db.UpsertChat(pn, "dm", "Alice", base); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertChat(lid, "unknown", lid, base.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertChat(group, "group", "Project", base); err != nil {
		t.Fatal(err)
	}
	if err := db.SetChatUnreadCount(pn, 2); err != nil {
		t.Fatal(err)
	}
	if err := db.SetChatUnreadCount(lid, 3); err != nil {
		t.Fatal(err)
	}

	// Messages: destination-only dupe, conflicting dupe under lid, lid-only
	// row, and a group message carrying lid sender references.
	if err := db.UpsertMessage(store.UpsertMessageParams{
		ChatJID: pn, MsgID: "dupe", Timestamp: base,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertMessage(store.UpsertMessageParams{
		ChatJID: lid, ChatName: "Alice LID", MsgID: "dupe",
		SenderJID: lid, SenderName: "Alice", Timestamp: base.Add(5 * time.Second),
		Text: "from lid",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertMessage(store.UpsertMessageParams{
		ChatJID: lid, ChatName: "Alice LID", MsgID: "lid-only",
		SenderJID: lid, SenderName: "Alice", Timestamp: base.Add(6 * time.Second),
		Text: "only on lid", QuotedMsgID: "quoted-lid-only", QuotedSenderJID: lid,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertMessage(store.UpsertMessageParams{
		ChatJID: group, MsgID: "group", SenderJID: lid,
		Timestamp: base.Add(7 * time.Second), Text: "group message",
		QuotedMsgID: "quoted-group", QuotedSenderJID: lid,
	}); err != nil {
		t.Fatal(err)
	}

	// Locations: lid-only message carries a location.
	if err := db.UpsertMessageLocation(store.MessageLocation{
		ChatJID: lid, MsgID: "lid-only",
		Latitude: 37.7749, Longitude: -122.4194,
		Name: "Home", Address: "548 Market", IsLive: false,
	}); err != nil {
		t.Fatal(err)
	}

	// Groups metadata: LID owner and LID participant conflicting with a PN row.
	if err := db.UpsertGroupWithHierarchy(group, "Project", lid, base, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := db.ReplaceGroupParticipants(group, []store.GroupParticipant{
		{GroupJID: group, UserJID: lid, Role: "admin", UpdatedAt: base.Add(2 * time.Second)},
		{GroupJID: group, UserJID: pn, Role: "member", UpdatedAt: base.Add(3 * time.Second)},
	}); err != nil {
		t.Fatal(err)
	}

	// Polls and votes: conflicting poll rows and votes under both identity keys.
	if err := db.UpsertPoll(store.Poll{
		ChatJID: lid, MsgID: "poll", SenderJID: lid, Question: "Dinner?",
		Options: []string{"yes", "no"}, SelectableCount: 1,
		CreatedAt: base.Add(8 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertPoll(store.Poll{
		ChatJID: pn, MsgID: "poll", SenderJID: pn, Question: "Dinner?",
		Options: []string{"yes", "no", "maybe"}, SelectableCount: 1,
		CreatedAt: base.Add(9 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertPoll(store.Poll{
		ChatJID: group, MsgID: "group-poll", SenderJID: lid, Question: "Group?",
		Options: []string{"yes", "no"}, SelectableCount: 1,
		CreatedAt: base.Add(8 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertPollVote(store.PollVote{
		ChatJID: lid, PollMsgID: "poll", VoterJID: lid, VoteMsgID: "older-vote",
		Selected: []string{"yes"}, VotedAt: base.Add(8 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertPollVote(store.PollVote{
		ChatJID: pn, PollMsgID: "poll", VoterJID: pn, VoteMsgID: "newer-vote",
		Selected: []string{"no"}, VotedAt: base.Add(9 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	// Duplicate the seeded active tables into the shadows.
	copyActiveTablesToShadow(t, db, withFTS)

	// Reference fold: direct store migration on the active tables.
	if err := db.MigrateLIDToPN(lid, pn); err != nil {
		t.Fatalf("active MigrateLIDToPN: %v", err)
	}

	// Event fold: one identity_resolution event, projected by the shadow views.
	res := ledger.CanonicalIdentityResolution{
		LID: lid, PN: pn, ResolvedTS: base.Add(11 * time.Second).Unix(),
	}
	rawBytes, rawHash, err := ledger.HashCanonical(res)
	if err != nil {
		t.Fatal(err)
	}
	dk := ledger.DedupKey("identity", lid, pn)
	batchID, _ := db.StartBatch(ledger.SourceIdentityResolution)
	if _, _, err := db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceIdentityResolution,
		EventType:     ledger.EventIdentityResolve,
		ChatJID:       lid,
		ServerTS:      res.ResolvedTS,
		EventTS:       res.ResolvedTS,
		ReceivedAt:    base.Add(11 * time.Second).Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	}); err != nil {
		t.Fatalf("append identity event: %v", err)
	}

	views := []View{
		NewMessagesView(db, shadowSuffix, withFTS),
		NewChatsView(db, shadowSuffix),
		NewContactsView(db, shadowSuffix),
		NewGroupsView(db, shadowSuffix),
		NewPollsView(db, shadowSuffix),
		NewPollVotesView(db, shadowSuffix),
	}
	runner := New(db, views).WithBatchSize(2)
	if _, err := runner.Run(context.Background(), 0); err != nil {
		t.Fatalf("shadow fold run: %v", err)
	}

	// Compare every materialized table. Active FTS rowids differ from shadow
	// rowids (independent AUTOINCREMENT), so FTS is compared by content set.
	tablePairs := []struct {
		name  string
		query func(string) string
	}{
		{"chats", chatsDumpQuery},
		{"messages", messagesDumpQuery},
		{"message_locations", locationsDumpQuery},
		{"message_payload_purges", purgesDumpQuery},
		{"groups", groupsDumpQuery},
		{"group_participants", participantsDumpQuery},
		{"polls", pollDumpQuery},
		{"poll_votes", voteDumpQuery},
	}
	for _, pair := range tablePairs {
		active := dumpRows(t, db, pair.query(pair.name))
		shadow := dumpRows(t, db, pair.query(pair.name+shadowSuffix))
		assertSameRows(t, pair.name, active, shadow)
	}
	if withFTS {
		activeFTS := dumpRows(t, db, ftsContentDumpQuery(store.MessagesTable))
		shadowFTS := dumpRows(t, db, ftsContentDumpQuery(store.MessagesTable+shadowSuffix))
		assertSameRows(t, "messages_fts", activeFTS, shadowFTS)
	}
}

// --- helpers -----------------------------------------------------------------

func createAliasesShadow(t *testing.T, db *store.DB) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS message_local_media_aliases_shadow (
		chat_jid TEXT NOT NULL, msg_id TEXT NOT NULL, local_path TEXT NOT NULL,
		downloaded_at INTEGER,
		PRIMARY KEY (chat_jid, msg_id, local_path))`); err != nil {
		t.Fatal(err)
	}
}

func copyActiveTablesToShadow(t *testing.T, db *store.DB, withFTS bool) {
	t.Helper()
	pairs := []string{
		"chats", "contacts", "messages", "message_locations",
		"message_payload_purges", "groups", "group_participants",
		"polls", "poll_votes", "message_local_media_aliases",
	}
	for _, table := range pairs {
		query := fmt.Sprintf(
			"INSERT OR REPLACE INTO %s_shadow SELECT * FROM %s", table, table)
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("copy %s to shadow: %v", table, err)
		}
	}
	if withFTS {
		query := `INSERT INTO messages_fts_shadow(rowid, text, media_caption, filename, chat_name, sender_name, display_text)
			SELECT rowid, text, media_caption, filename, chat_name, sender_name, display_text
			FROM messages_fts`
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("copy fts to shadow: %v", err)
		}
	}
}

func assertSameRows(t *testing.T, label string, active, shadow []map[string]string) {
	t.Helper()
	if len(active) != len(shadow) {
		t.Fatalf("%s row count: active=%d shadow=%d\nactive=%v\nshadow=%v",
			label, len(active), len(shadow), active, shadow)
	}
	for i := range active {
		a, s := active[i], shadow[i]
		if len(a) != len(s) {
			t.Fatalf("%s row %d column count: active=%v shadow=%v", label, i, a, s)
		}
		for col, av := range a {
			if sv, ok := s[col]; !ok || sv != av {
				t.Fatalf("%s row %d col %s: active=%s shadow=%s",
					label, i, col, av, sv)
			}
		}
	}
}

func chatsDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT jid, kind, name, last_message_ts, archived, pinned,
		muted_until, unread, unread_count FROM %s ORDER BY jid`, table)
}

func locationsDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT chat_jid, msg_id, latitude, longitude, name, address,
		is_live FROM %s ORDER BY chat_jid, msg_id`, table)
}

func purgesDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT chat_jid, msg_id, purged_at, deleted_at,
		deletion_reason FROM %s ORDER BY chat_jid, msg_id`, table)
}

// ftsContentDumpQuery dumps FTS content joined through the backing messages
// table identity-independent: content-only, sorted by text for set comparison.
func ftsContentDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT text, media_caption, filename, chat_name, sender_name,
		display_text FROM %s ORDER BY text, display_text`, table)
}
