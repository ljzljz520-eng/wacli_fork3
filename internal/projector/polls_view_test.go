package projector

import (
	"fmt"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
)

// --- shadow schema -----------------------------------------------------------

func createPollsShadow(t *testing.T, db *store.DB) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS polls_shadow (
			chat_jid TEXT NOT NULL, msg_id TEXT NOT NULL, sender_jid TEXT,
			question TEXT NOT NULL, options_json TEXT NOT NULL,
			selectable_count INTEGER NOT NULL DEFAULT 1, created_ts INTEGER NOT NULL,
			PRIMARY KEY (chat_jid, msg_id))`,
		`CREATE TABLE IF NOT EXISTS poll_votes_shadow (
			chat_jid TEXT NOT NULL, poll_msg_id TEXT NOT NULL, voter_jid TEXT NOT NULL,
			vote_msg_id TEXT NOT NULL, selected_options_json TEXT NOT NULL, ts INTEGER NOT NULL,
			PRIMARY KEY (chat_jid, poll_msg_id, voter_jid))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("create polls shadow: %v\n%s", err, s)
		}
	}
}

// --- append helpers ----------------------------------------------------------

func appendPollLedgerEvent(t *testing.T, db *store.DB, c ledger.CanonicalPoll) bool {
	t.Helper()
	if c.Options == nil {
		c.Options = []string{}
	}
	return appendPollCanonical(t, db, ledger.EventPoll, c.ChatJID, c,
		c.CreatedTS, ledger.DedupKey("poll", c.ChatJID, c.MsgID))
}

func appendPollOptionLedgerEvent(t *testing.T, db *store.DB, c ledger.CanonicalPollOptionAdd) bool {
	t.Helper()
	return appendPollCanonical(t, db, ledger.EventPollOption, c.ChatJID, c,
		0, ledger.DedupKey("poll-option", c.ChatJID, c.PollMsgID, c.Option))
}

func appendPollVoteLedgerEvent(t *testing.T, db *store.DB, c ledger.CanonicalPollVote) bool {
	t.Helper()
	if c.Selected == nil {
		c.Selected = []string{}
	}
	kind := "poll-vote"
	if c.Deleted {
		kind = "poll-vote-delete"
	}
	dk := ledger.DedupKey(kind, c.ChatJID, c.PollMsgID, c.VoterJID,
		fmt.Sprintf("%d", c.VotedTSMS))
	eventTS := time.UnixMilli(c.VotedTSMS).Unix()
	return appendPollCanonical(t, db, ledger.EventPollVote, c.ChatJID, c, eventTS, dk)
}

func appendPollCanonical(t *testing.T, db *store.DB, eventType, chatJID string,
	payload any, eventTS int64, dk string,
) bool {
	t.Helper()
	rawBytes, rawHash, err := ledger.HashCanonical(payload)
	if err != nil {
		t.Fatalf("hash %s: %v", eventType, err)
	}
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

var pollDumpCols = []string{
	"chat_jid", "msg_id", "sender_jid", "question", "options_json",
	"selectable_count", "created_ts",
}

func pollDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT chat_jid, msg_id, sender_jid, question, options_json,
		selectable_count, created_ts FROM %s ORDER BY chat_jid, msg_id`, table)
}

var voteDumpCols = []string{
	"chat_jid", "poll_msg_id", "voter_jid", "vote_msg_id",
	"selected_options_json", "ts",
}

func voteDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT chat_jid, poll_msg_id, voter_jid, vote_msg_id,
		selected_options_json, ts FROM %s
		ORDER BY chat_jid, poll_msg_id, voter_jid`, table)
}

// --- tests -------------------------------------------------------------------

func TestPollsViewsFoldParity(t *testing.T) {
	db := openRunnerDB(t)
	createMessagesShadow(t, db) // provides message_payload_purges_shadow
	createPollsShadow(t, db)
	gid := "123456@g.us"

	// Poll creation + identical redelivery.
	poll := ledger.CanonicalPoll{
		ChatJID:         gid,
		MsgID:           "poll1",
		SenderJID:       "alice@s.whatsapp.net",
		Question:        "Pick one",
		Options:         []string{"A", "B"},
		SelectableCount: 1,
		CreatedTS:       1700000000,
	}
	if !appendPollLedgerEvent(t, db, poll) {
		t.Fatal("first poll should insert")
	}
	if appendPollLedgerEvent(t, db, poll) {
		t.Fatal("identical poll must dedup")
	}
	// Single-option addition + duplicate.
	add := ledger.CanonicalPollOptionAdd{
		ChatJID: gid, PollMsgID: "poll1", Option: "C",
	}
	if !appendPollOptionLedgerEvent(t, db, add) {
		t.Fatal("option add should insert")
	}
	if appendPollOptionLedgerEvent(t, db, add) {
		t.Fatal("identical option add must dedup")
	}

	// Vote 1: selection, then newer-ts update, then retraction.
	if !appendPollVoteLedgerEvent(t, db, ledger.CanonicalPollVote{
		ChatJID: gid, PollMsgID: "poll1", VoterJID: "alice@s.whatsapp.net",
		VoteMsgID: "vote1", Selected: []string{"A"},
		VotedTSMS: 1700000100000,
	}) {
		t.Fatal("first vote should insert")
	}
	if !appendPollVoteLedgerEvent(t, db, ledger.CanonicalPollVote{
		ChatJID: gid, PollMsgID: "poll1", VoterJID: "alice@s.whatsapp.net",
		VoteMsgID: "vote2", Selected: []string{"B"},
		VotedTSMS: 1700000200000,
	}) {
		t.Fatal("newer vote should insert")
	}
	if !appendPollVoteLedgerEvent(t, db, ledger.CanonicalPollVote{
		ChatJID: gid, PollMsgID: "poll1", VoterJID: "alice@s.whatsapp.net",
		VoteMsgID: "", Selected: []string{},
		VotedTSMS: 1700000300000, Deleted: true,
	}) {
		t.Fatal("vote retraction should insert")
	}
	// Vote 2: unknown hash only.
	if !appendPollVoteLedgerEvent(t, db, ledger.CanonicalPollVote{
		ChatJID: gid, PollMsgID: "poll1", VoterJID: "bob@s.whatsapp.net",
		VoteMsgID: "vote3", Selected: []string{},
		UnknownHashes: []string{"deadbeef"},
		VotedTSMS:     1700000400000,
	}) {
		t.Fatal("unknown-hash vote should insert")
	}

	runMessageViews(t, db,
		NewPollsView(db, ""), NewPollsView(db, shadowSuffix),
		NewPollVotesView(db, ""), NewPollVotesView(db, shadowSuffix),
	)

	pairs := []struct {
		name           string
		active, shadow []map[string]string
		cols           []string
	}{
		{"polls",
			dumpRows(t, db, pollDumpQuery(store.PollsTable)),
			dumpRows(t, db, pollDumpQuery(store.PollsTable+shadowSuffix)),
			pollDumpCols},
		{"votes",
			dumpRows(t, db, voteDumpQuery(store.PollVotesTable)),
			dumpRows(t, db, voteDumpQuery(store.PollVotesTable+shadowSuffix)),
			voteDumpCols},
	}
	for _, p := range pairs {
		if len(p.active) != len(p.shadow) {
			t.Fatalf("%s counts active=%d shadow=%d", p.name, len(p.active), len(p.shadow))
		}
		for i := range p.active {
			for _, col := range p.cols {
				if p.active[i][col] != p.shadow[i][col] {
					t.Fatalf("%s col %s: active=%s shadow=%s",
						p.name, col, p.active[i][col], p.shadow[i][col])
				}
			}
		}
	}

	polls := dumpRows(t, db, pollDumpQuery(store.PollsTable))
	if len(polls) != 1 || polls[0]["options_json"] != `str:["A","B","C"]` {
		t.Fatalf("poll options should be A,B,C, got %s", mustJSON(polls))
	}
	votes := dumpRows(t, db, voteDumpQuery(store.PollVotesTable))
	if len(votes) != 1 {
		t.Fatalf("alice retracted; only bob vote remains, got %d", len(votes))
	}
	if votes[0]["voter_jid"] != "str:bob@s.whatsapp.net" ||
		votes[0]["selected_options_json"] !=
			`str:{"selected":[],"unknown_hashes":["deadbeef"]}` {
		t.Fatalf("bob vote must carry unknown_hashes wrapper, got %s",
			votes[0]["selected_options_json"])
	}
}

func TestPollsViewsLegacyParity(t *testing.T) {
	db := openRunnerDB(t)
	createPollsShadow(t, db)
	now := time.Now().Unix()
	gid := "999@g.us"

	if _, err := db.Exec(`INSERT INTO polls
		(chat_jid,msg_id,sender_jid,question,options_json,selectable_count,created_ts)
		VALUES(?,?,?,?,?,?,?)`,
		gid, "p1", "alice@s.whatsapp.net", "Legacy?",
		`["X","Y"]`, 2, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO poll_votes
		(chat_jid,poll_msg_id,voter_jid,vote_msg_id,selected_options_json,ts)
		VALUES(?,?,?,?,?,?)`,
		gid, "p1", "alice@s.whatsapp.net", "v1",
		`{"selected":["X"],"unknown_hashes":[]}`, now*1000); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BootstrapLegacySnapshots(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	runMessageViews(t, db,
		NewPollsView(db, shadowSuffix),
		NewPollVotesView(db, shadowSuffix),
	)

	for _, c := range []struct {
		name, active, shadow string
	}{
		{"polls", store.PollsTable, store.PollsTable + shadowSuffix},
		{"votes", store.PollVotesTable, store.PollVotesTable + shadowSuffix},
	} {
		active := dumpRows(t, db, fmt.Sprintf("SELECT * FROM %s", c.active))
		shadow := dumpRows(t, db, fmt.Sprintf("SELECT * FROM %s", c.shadow))
		if len(active) != 1 || len(shadow) != 1 {
			t.Fatalf("%s counts active=%d shadow=%d", c.name, len(active), len(shadow))
		}
		for col, v := range active[0] {
			if shadow[0][col] != v {
				t.Fatalf("%s legacy col %s: active=%s shadow=%s",
					c.name, col, v, shadow[0][col])
			}
		}
	}
}

func TestPollsViewsReplayDeterminism(t *testing.T) {
	db := openRunnerDB(t)
	createMessagesShadow(t, db)
	createPollsShadow(t, db)
	gid := "777@g.us"
	appendPollLedgerEvent(t, db, ledger.CanonicalPoll{
		ChatJID: gid, MsgID: "p1", Question: "Q",
		Options: []string{"A"}, CreatedTS: 1700001000,
	})
	appendPollOptionLedgerEvent(t, db, ledger.CanonicalPollOptionAdd{
		ChatJID: gid, PollMsgID: "p1", Option: "B",
	})
	appendPollVoteLedgerEvent(t, db, ledger.CanonicalPollVote{
		ChatJID: gid, PollMsgID: "p1", VoterJID: "alice@s.whatsapp.net",
		VoteMsgID: "v1", Selected: []string{"A"}, VotedTSMS: 1700001100000,
	})

	views := []View{
		NewPollsView(db, shadowSuffix),
		NewPollVotesView(db, shadowSuffix),
	}
	runMessageViews(t, db, views...)
	first := map[string][]map[string]string{
		"polls": dumpRows(t, db, pollDumpQuery(store.PollsTable+shadowSuffix)),
		"votes": dumpRows(t, db, voteDumpQuery(store.PollVotesTable+shadowSuffix)),
	}

	for _, v := range views {
		if err := db.SetCheckpoint(v.Name(), 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"polls_shadow", "poll_votes_shadow"} {
		if _, err := db.Exec("DELETE FROM " + table); err != nil {
			t.Fatal(err)
		}
	}
	runMessageViews(t, db, views...)
	again := map[string][]map[string]string{
		"polls": dumpRows(t, db, pollDumpQuery(store.PollsTable+shadowSuffix)),
		"votes": dumpRows(t, db, voteDumpQuery(store.PollVotesTable+shadowSuffix)),
	}
	for name := range first {
		if mustJSON(first[name]) != mustJSON(again[name]) {
			t.Fatalf("%s replay nondeterministic:\nbefore=%s\nafter=%s",
				name, mustJSON(first[name]), mustJSON(again[name]))
		}
	}
}
