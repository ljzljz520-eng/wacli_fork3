package projector

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
)

// --- shadow schema -----------------------------------------------------------

func createGroupsShadow(t *testing.T, db *store.DB) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS groups_shadow (
			jid TEXT PRIMARY KEY, name TEXT, owner_jid TEXT, created_ts INTEGER,
			is_parent INTEGER NOT NULL DEFAULT 0, linked_parent_jid TEXT,
			left_at INTEGER, updated_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS group_participants_shadow (
			group_jid TEXT NOT NULL, user_jid TEXT NOT NULL,
			role TEXT NOT NULL DEFAULT 'member', updated_at INTEGER NOT NULL,
			PRIMARY KEY (group_jid, user_jid))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("create groups shadow: %v\n%s", err, s)
		}
	}
}

// --- event append helper -----------------------------------------------------

func appendGroupLedgerEvent(t *testing.T, db *store.DB, g ledger.CanonicalGroup) bool {
	t.Helper()
	rawBytes, rawHash, err := ledger.HashCanonical(g)
	if err != nil {
		t.Fatalf("hash group: %v", err)
	}
	dk := ledger.DedupKey("group", g.JID, rawHash)
	batchID, _ := db.StartBatch(ledger.SourceLive)
	now := time.Now().Unix()
	_, inserted, err := db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceLive,
		EventType:     ledger.EventGroup,
		ChatJID:       g.JID,
		ServerTS:      now,
		EventTS:       now,
		ReceivedAt:    now,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	if err != nil {
		t.Fatalf("AppendEvent group: %v", err)
	}
	return inserted
}

// --- dumps -------------------------------------------------------------------

var groupsDumpColumns = []string{
	"jid", "name", "owner_jid", "created_ts", "is_parent",
	"linked_parent_jid", "left_at", "updated_at",
}

func groupsDumpQuery(table string) string {
	cols := ""
	for i, c := range groupsDumpColumns {
		if i > 0 {
			cols += ", "
		}
		cols += c
	}
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY jid`, cols, table)
}

func participantsDumpQuery(table string) string {
	return fmt.Sprintf(`SELECT group_jid, user_jid, role, updated_at
		FROM %s ORDER BY group_jid, user_jid`, table)
}

// --- tests -------------------------------------------------------------------

func TestGroupsViewSnapshotFoldRosterAndLeave(t *testing.T) {
	db := openRunnerDB(t)
	createGroupsShadow(t, db)
	g1 := "1111111111@g.us"
	g2 := "2222222222@g.us"
	community := "3333333333@g.us"

	// First snapshot with roster (unsorted on purpose).
	if !appendGroupLedgerEvent(t, db, ledger.CanonicalGroup{
		JID:       g1,
		Name:      "Budget",
		OwnerJID:  "owner@s.whatsapp.net",
		CreatedTS: 1700000000,
		Participants: []ledger.CanonicalGroupParticipant{
			{UserJID: "bob@s.whatsapp.net", Role: "member"},
			{UserJID: "alice@s.whatsapp.net", Role: "admin"},
		},
	}) {
		t.Fatal("first snapshot should insert")
	}
	// Identical redelivery collapses through dedup.
	if appendGroupLedgerEvent(t, db, ledger.CanonicalGroup{
		JID:       g1,
		Name:      "Budget",
		OwnerJID:  "owner@s.whatsapp.net",
		CreatedTS: 1700000000,
		Participants: []ledger.CanonicalGroupParticipant{
			{UserJID: "bob@s.whatsapp.net", Role: "member"},
			{UserJID: "alice@s.whatsapp.net", Role: "admin"},
		},
	}) {
		t.Fatal("identical snapshot must be deduplicated")
	}
	// Second snapshot: renamed, roster replaced (bob gone, carol added).
	if !appendGroupLedgerEvent(t, db, ledger.CanonicalGroup{
		JID:       g1,
		Name:      "Budget Renamed",
		OwnerJID:  "owner@s.whatsapp.net",
		CreatedTS: 1700000000,
		Participants: []ledger.CanonicalGroupParticipant{
			{UserJID: "alice@s.whatsapp.net", Role: "admin"},
			{UserJID: "carol@s.whatsapp.net", Role: "member"},
		},
	}) {
		t.Fatal("second snapshot should insert")
	}
	// Plain group g2.
	if !appendGroupLedgerEvent(t, db, ledger.CanonicalGroup{
		JID: g2, Name: "Trips",
		Participants: []ledger.CanonicalGroupParticipant{
			{UserJID: "dan@s.whatsapp.net", Role: "member"},
		},
	}) {
		t.Fatal("g2 snapshot should insert")
	}
	// Leave marker for g2.
	if !appendGroupLedgerEvent(t, db, ledger.CanonicalGroup{
		JID: g2, LeftAt: 1700000999,
	}) {
		t.Fatal("leave marker should insert")
	}
	// Community: is_parent must clear linked parent.
	if !appendGroupLedgerEvent(t, db, ledger.CanonicalGroup{
		JID: community, Name: "Acme Community", IsParent: true,
		LinkedParentJID: "4444444444@g.us",
		Participants: []ledger.CanonicalGroupParticipant{
			{UserJID: "owner@s.whatsapp.net", Role: "superadmin"},
		},
	}) {
		t.Fatal("community snapshot should insert")
	}

	runMessageViews(t, db,
		NewGroupsView(db, ""),
		NewGroupsView(db, shadowSuffix))

	groupsActive := dumpRows(t, db, groupsDumpQuery(store.GroupsTable))
	groupsShadow := dumpRows(t, db, groupsDumpQuery(store.GroupsTable+shadowSuffix))
	partsActive := dumpRows(t, db, participantsDumpQuery(store.GroupParticipantsTable))
	partsShadow := dumpRows(t, db, participantsDumpQuery(store.GroupParticipantsTable+shadowSuffix))

	if len(groupsActive) != 3 || len(groupsShadow) != 3 {
		t.Fatalf("group counts active=%d shadow=%d", len(groupsActive), len(groupsShadow))
	}
	for i := range groupsActive {
		for _, col := range groupsDumpColumns {
			if groupsActive[i][col] != groupsShadow[i][col] {
				t.Fatalf("groups %s col %s: active=%s shadow=%s",
					groupsActive[i]["jid"], col, groupsActive[i][col], groupsShadow[i][col])
			}
		}
	}
	if len(partsActive) != len(partsShadow) {
		t.Fatalf("participant counts active=%d shadow=%d", len(partsActive), len(partsShadow))
	}
	for i := range partsActive {
		for _, col := range []string{"group_jid", "user_jid", "role", "updated_at"} {
			if partsActive[i][col] != partsShadow[i][col] {
				t.Fatalf("participants col %s: active=%s shadow=%s",
					col, partsActive[i][col], partsShadow[i][col])
			}
		}
	}

	// Final-state assertions on the active tables.
	byJID := map[string]map[string]string{}
	for _, row := range groupsActive {
		byJID[row["jid"]] = row
	}
	if byJID["str:"+g1]["name"] != "str:Budget Renamed" {
		t.Fatalf("g1 name = %s", byJID["str:"+g1]["name"])
	}
	if byJID["str:"+g2]["left_at"] != "int:1700000999" {
		t.Fatalf("g2 left_at = %s", byJID["str:"+g2]["left_at"])
	}
	if byJID["str:"+community]["is_parent"] != "int:1" ||
		byJID["str:"+community]["linked_parent_jid"] != "<null>" {
		t.Fatalf("community hierarchy = %+v", byJID["str:"+community])
	}

	// Roster membership: g1 = alice + carol only (bob removed).
	members := map[string]string{}
	for _, row := range partsActive {
		if row["group_jid"] == "str:"+g1 {
			members[row["user_jid"]] = row["role"]
		}
	}
	if len(members) != 2 || members["str:alice@s.whatsapp.net"] != "str:admin" ||
		members["str:carol@s.whatsapp.net"] != "str:member" {
		t.Fatalf("g1 roster = %#v", members)
	}
}

func TestGroupsViewLegacySnapshotParity(t *testing.T) {
	db := openRunnerDB(t)
	createGroupsShadow(t, db)
	now := time.Now().Unix()
	gid := "5555555555@g.us"
	if _, err := db.Exec(`INSERT INTO groups
		(jid,name,owner_jid,created_ts,is_parent,linked_parent_jid,left_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?)`,
		gid, "Legacy", "owner@s.whatsapp.net", 1690000000, 0,
		nil, 1695000000, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO group_participants
		(group_jid,user_jid,role,updated_at) VALUES(?,?,?,?)`,
		gid, "alice@s.whatsapp.net", "admin", now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BootstrapLegacySnapshots(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	runMessageViews(t, db, NewGroupsView(db, shadowSuffix))

	groupsActive := dumpRows(t, db, groupsDumpQuery(store.GroupsTable))
	groupsShadow := dumpRows(t, db, groupsDumpQuery(store.GroupsTable+shadowSuffix))
	partsActive := dumpRows(t, db, participantsDumpQuery(store.GroupParticipantsTable))
	partsShadow := dumpRows(t, db, participantsDumpQuery(store.GroupParticipantsTable+shadowSuffix))
	if len(groupsActive) != len(groupsShadow) || len(partsActive) != len(partsShadow) {
		t.Fatalf("counts groups %d/%d participants %d/%d",
			len(groupsActive), len(groupsShadow), len(partsActive), len(partsShadow))
	}
	for i := range groupsActive {
		for _, col := range groupsDumpColumns {
			if groupsActive[i][col] != groupsShadow[i][col] {
				t.Fatalf("legacy group col %s: %s vs %s",
					col, groupsActive[i][col], groupsShadow[i][col])
			}
		}
	}
	for i := range partsActive {
		for _, col := range []string{"group_jid", "user_jid", "role", "updated_at"} {
			if partsActive[i][col] != partsShadow[i][col] {
				t.Fatalf("legacy participant col %s: %s vs %s",
					col, partsActive[i][col], partsShadow[i][col])
			}
		}
	}
}

func TestGroupsViewReplayDeterminism(t *testing.T) {
	db := openRunnerDB(t)
	createGroupsShadow(t, db)
	gid := "6666666666@g.us"
	appendGroupLedgerEvent(t, db, ledger.CanonicalGroup{
		JID: gid, Name: "Replay", CreatedTS: 1680000000,
		Participants: []ledger.CanonicalGroupParticipant{
			{UserJID: "alice@s.whatsapp.net", Role: "admin"},
			{UserJID: "bob@s.whatsapp.net", Role: "member"},
		},
	})

	view := NewGroupsView(db, shadowSuffix)
	runMessageViews(t, db, view)
	first := dumpRows(t, db, groupsDumpQuery(store.GroupsTable+shadowSuffix))
	firstParts := dumpRows(t, db, participantsDumpQuery(store.GroupParticipantsTable+shadowSuffix))

	if err := db.SetCheckpoint(view.Name(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM groups_shadow`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM group_participants_shadow`); err != nil {
		t.Fatal(err)
	}
	runMessageViews(t, db, view)
	again := dumpRows(t, db, groupsDumpQuery(store.GroupsTable+shadowSuffix))
	againParts := dumpRows(t, db, participantsDumpQuery(store.GroupParticipantsTable+shadowSuffix))

	if b1, b2 := mustJSON(first), mustJSON(again); b1 != b2 {
		t.Fatalf("group replay nondeterministic:\nbefore=%s\nafter=%s", b1, b2)
	}
	if b1, b2 := mustJSON(firstParts), mustJSON(againParts); b1 != b2 {
		t.Fatalf("roster replay nondeterministic:\nbefore=%s\nafter=%s", b1, b2)
	}
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
