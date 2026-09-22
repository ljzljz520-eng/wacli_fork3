package projector

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
)

func appendContactLedgerEvent(t *testing.T, db *store.DB, source string, c ledger.CanonicalContact) bool {
	t.Helper()
	rawBytes, rawHash, err := ledger.HashCanonical(c)
	if err != nil {
		t.Fatalf("hash contact: %v", err)
	}
	dk := ledger.DedupKey("contact-event", source, c.JID,
		c.Phone, c.PushName, c.FullName, c.FirstName, c.BusinessName, c.SystemName)
	batchID, _ := db.StartBatch(source)
	now := time.Now().Unix()
	_, inserted, err := db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        source,
		EventType:     ledger.EventContact,
		ChatJID:       c.JID,
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
		t.Fatalf("AppendEvent contact: %v", err)
	}
	return inserted
}

func appendSystemNamesClearEvent(t *testing.T, db *store.DB) {
	t.Helper()
	now := time.Now().Unix()
	payload := struct {
		Type string `json:"type"`
		TS   int64  `json:"ts"`
	}{Type: ledger.EventSystemNamesClear, TS: now}
	rawBytes, rawHash, err := ledger.HashCanonical(payload)
	if err != nil {
		t.Fatalf("hash clear: %v", err)
	}
	dk := ledger.DedupKey("clear_system_names_test", fmt.Sprintf("%d", now))
	batchID, _ := db.StartBatch(ledger.SourceSystemContacts)
	_, _, err = db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceSystemContacts,
		EventType:     ledger.EventSystemNamesClear,
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
		t.Fatalf("AppendEvent clear: %v", err)
	}
}

var contactDumpColumns = []string{
	"jid", "phone", "push_name", "full_name", "first_name",
	"business_name", "system_name", "updated_at",
}

func contactsDumpQuery(table string) string {
	cols := ""
	for i, c := range contactDumpColumns {
		if i > 0 {
			cols += ", "
		}
		cols += c
	}
	return fmt.Sprintf(`SELECT %s FROM %s ORDER BY jid`, cols, table)
}

func TestContactsViewMergeFoldAndSystemNames(t *testing.T) {
	db := openRunnerDB(t)
	createMessagesShadow(t, db) // provides contacts_shadow
	jid := "555010101@s.whatsapp.net"

	// First partial snapshot: phone + push.
	if !appendContactLedgerEvent(t, db, ledger.SourceLive, ledger.CanonicalContact{
		JID: jid, Phone: "555010101", PushName: "Pushy",
	}) {
		t.Fatal("first contact event should insert")
	}
	// Second partial snapshot: full + first.
	if !appendContactLedgerEvent(t, db, ledger.SourceLive, ledger.CanonicalContact{
		JID: jid, FullName: "Alice Full", FirstName: "Ali",
	}) {
		t.Fatal("second contact event should insert")
	}
	// Identical redelivery collapses through dedup.
	if appendContactLedgerEvent(t, db, ledger.SourceLive, ledger.CanonicalContact{
		JID: jid, FullName: "Alice Full", FirstName: "Ali",
	}) {
		t.Fatal("identical contact event must be deduplicated")
	}
	// Phone-book name from system contacts.
	if !appendContactLedgerEvent(t, db, ledger.SourceSystemContacts, ledger.CanonicalContact{
		JID: jid, SystemName: "Alice Phone",
	}) {
		t.Fatal("system name event should insert")
	}
	// Bulk clear.
	appendSystemNamesClearEvent(t, db)

	runMessageViews(t, db,
		NewContactsView(db, ""),
		NewContactsView(db, shadowSuffix))

	active := dumpRows(t, db, contactsDumpQuery(store.ContactsTable))
	shadow := dumpRows(t, db, contactsDumpQuery(store.ContactsTable+shadowSuffix))
	if len(active) != 1 || len(shadow) != 1 {
		t.Fatalf("counts active=%d shadow=%d", len(active), len(shadow))
	}
	for _, col := range contactDumpColumns {
		if active[0][col] != shadow[0][col] {
			t.Fatalf("column %s: active=%s shadow=%s", col, active[0][col], shadow[0][col])
		}
	}
	want := map[string]string{
		"phone":         "str:555010101",
		"push_name":     "str:Pushy",
		"full_name":     "str:Alice Full",
		"first_name":    "str:Ali",
		"business_name": "<null>",
		"system_name":   "<null>", // cleared
	}
	for col, v := range want {
		if active[0][col] != v {
			t.Fatalf("%s = %s, want %s", col, active[0][col], v)
		}
	}
}

func TestContactsViewLegacySnapshotParity(t *testing.T) {
	db := openRunnerDB(t)
	createMessagesShadow(t, db)
	jid := "555020202@s.whatsapp.net"
	now := time.Now().Unix()
	if _, err := db.Exec(`INSERT INTO contacts
		(jid,phone,push_name,full_name,first_name,business_name,system_name,updated_at)
		VALUES(?,?,?,?,?,?,?,?)`,
		jid, "555020202", "Pushy", "Bob Full", "Bo", "Bob Biz", "Bob Phone", now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BootstrapLegacySnapshots(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	runMessageViews(t, db, NewContactsView(db, shadowSuffix))

	active := dumpRows(t, db, contactsDumpQuery(store.ContactsTable))
	shadow := dumpRows(t, db, contactsDumpQuery(store.ContactsTable+shadowSuffix))
	if len(active) != 1 || len(shadow) != 1 {
		t.Fatalf("counts active=%d shadow=%d", len(active), len(shadow))
	}
	for _, col := range contactDumpColumns {
		if active[0][col] != shadow[0][col] {
			t.Fatalf("column %s: active=%s shadow=%s", col, active[0][col], shadow[0][col])
		}
	}
}

func TestContactsViewReplayDeterminism(t *testing.T) {
	db := openRunnerDB(t)
	createMessagesShadow(t, db)
	jid := "555030303@s.whatsapp.net"
	appendContactLedgerEvent(t, db, ledger.SourceLive, ledger.CanonicalContact{
		JID: jid, Phone: jid[:10], PushName: "Carol", FullName: "Carol Full",
	})

	view := NewContactsView(db, shadowSuffix)
	runMessageViews(t, db, view)
	first := dumpRows(t, db, contactsDumpQuery(store.ContactsTable+shadowSuffix))

	// Reset checkpoint, wipe, replay.
	if err := db.SetCheckpoint(view.Name(), 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM contacts_shadow`); err != nil {
		t.Fatal(err)
	}
	runMessageViews(t, db, view)
	again := dumpRows(t, db, contactsDumpQuery(store.ContactsTable+shadowSuffix))

	b1, _ := json.Marshal(first)
	b2, _ := json.Marshal(again)
	if string(b1) != string(b2) {
		t.Fatalf("replay nondeterministic:\nbefore=%s\nafter=%s", b1, b2)
	}
}
