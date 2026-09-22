package store

import (
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
)

// newTestEvent builds a valid appendable event. chat/msg identify the WA key;
// refs point at causes.
func newTestEvent(t *testing.T, source, chat, msg string, refs ...ledger.CausalRef) *ledger.Event {
	t.Helper()
	canonical := ledger.NewCanonicalReceipt(
		"test", chat, "", "", []string{msg}, time.UnixMilli(1700000000000+int64(len(msg))),
	)
	rawBytes, rawHash, err := ledger.HashCanonical(canonical)
	if err != nil {
		t.Fatal(err)
	}
	waKey := ledger.WAKey(chat, false, msg, "")
	dk := ledger.DedupKey("test", source, waKey)
	return &ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        source,
		EventType:     ledger.EventReceipt,
		WAKey:         waKey,
		ChatJID:       chat,
		MsgID:         msg,
		ServerTS:      1700000000,
		EventTS:       1700000000,
		ReceivedAt:    1700000001,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		CausalRefs:    refs,
		BatchID:       1,
		RawBytes:      rawBytes,
	}
}

func mustAppend(t *testing.T, db *DB, evt *ledger.Event) (int64, bool) {
	t.Helper()
	seq, inserted, err := db.AppendEvent(evt)
	if err != nil {
		t.Fatalf("AppendEvent %s: %v", evt.EventID, err)
	}
	return seq, inserted
}

func TestAppendEventIdempotent(t *testing.T) {
	db := openTestDB(t)

	evt := newTestEvent(t, ledger.SourceReceipt, "chat@c.us", "MSG-1")
	seq1, inserted1 := mustAppend(t, db, evt)
	if !inserted1 || seq1 != 1 {
		t.Fatalf("first append inserted=%v seq=%d", inserted1, seq1)
	}

	// Re-append the identical event: no-op, same sequence.
	seq2, inserted2 := mustAppend(t, db, evt)
	if inserted2 {
		t.Fatal("re-append reported as inserted")
	}
	if seq2 != seq1 {
		t.Fatalf("re-append seq=%d, want %d", seq2, seq1)
	}
	if got := countRows(t, db.sql, "SELECT COUNT(*) FROM ledger_events"); got != 1 {
		t.Fatalf("row count = %d, want 1", got)
	}

	// Same dedup key but different raw hash produces a new sequence row.
	evtOther := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-1")
	// Build genuinely different envelope bytes (extra receipt id), then force
	// the same dedup key.
	otherCanonical := ledger.NewCanonicalReceipt(
		"test", "chat@c.us", "", "", []string{"MSG-1", "MSG-2"},
		time.UnixMilli(1700000000099),
	)
	otherBytes, otherHash, err := ledger.HashCanonical(otherCanonical)
	if err != nil {
		t.Fatal(err)
	}
	evtOther.RawBytes = otherBytes
	evtOther.RawHash = otherHash
	evtOther.DedupKey = evt.DedupKey
	evtOther.EventID = ledger.DeriveEventID(evtOther.DedupKey, evtOther.RawHash)
	seq3, inserted3 := mustAppend(t, db, evtOther)
	if !inserted3 || seq3 <= seq1 {
		t.Fatalf("different hash append inserted=%v seq=%d", inserted3, seq3)
	}
	if got := countRows(t, db.sql, "SELECT COUNT(*) FROM ledger_events"); got != 2 {
		t.Fatalf("row count = %d, want 2", got)
	}
}

func TestAppendEventValidation(t *testing.T) {
	db := openTestDB(t)
	evt := newTestEvent(t, ledger.SourceReceipt, "chat@c.us", "MSG-1")
	evt.RawHash = "short"
	if _, _, err := db.AppendEvent(evt); err == nil {
		t.Fatal("expected invalid raw_hash to be rejected")
	}
}

func TestCausalLinkTargetBefore(t *testing.T) {
	db := openTestDB(t)

	target := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-A")
	mustAppend(t, db, target)

	dependent := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-B",
		ledger.WAKeyRef("chat@c.us", "MSG-A", "", false))
	mustAppend(t, db, dependent)

	stored, err := db.GetEventByID(dependent.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Causes) != `["`+target.EventID+`"]` {
		t.Fatalf("causes = %s", stored.Causes)
	}

	res, err := db.ResolveCausalLinks()
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 1 || res.Dangling != 0 || res.Cyclic != 0 {
		t.Fatalf("link result = %+v", res)
	}
	dangling, err := db.EnumerateDangling()
	if err != nil {
		t.Fatal(err)
	}
	if len(dangling) != 0 {
		t.Fatalf("dangling = %+v", dangling)
	}
}

func TestCausalLinkTargetAfter(t *testing.T) {
	db := openTestDB(t)

	// Dependent arrives first: ref is dangling.
	dependent := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-B",
		ledger.WAKeyRef("chat@c.us", "MSG-A", "", false))
	mustAppend(t, db, dependent)

	stored, err := db.GetEventByID(dependent.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Flags&ledger.FlagDangling == 0 {
		t.Fatal("expected FlagDangling on unresolved event")
	}
	dangling, err := db.EnumerateDangling()
	if err != nil {
		t.Fatal(err)
	}
	if len(dangling) != 1 || dangling[0].MsgID != "MSG-A" {
		t.Fatalf("dangling = %+v", dangling)
	}

	// Target arrives, then the link pass back-resolves the edge.
	target := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-A")
	mustAppend(t, db, target)

	res, err := db.ResolveCausalLinks()
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 1 || res.Dangling != 0 || res.Cyclic != 0 {
		t.Fatalf("link result = %+v", res)
	}
	dangling, err = db.EnumerateDangling()
	if err != nil {
		t.Fatal(err)
	}
	if len(dangling) != 0 {
		t.Fatalf("expected no dangling, got %+v", dangling)
	}

	// A second pass is a no-op.
	res2, err := db.ResolveCausalLinks()
	if err != nil {
		t.Fatal(err)
	}
	if res2.Added != 0 {
		t.Fatalf("second pass added %d links", res2.Added)
	}
}

func TestCausalLinkRejectsCycle(t *testing.T) {
	db := openTestDB(t)

	// B appended first, dangling ref B -> A.
	b := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-B",
		ledger.WAKeyRef("chat@c.us", "MSG-A", "", false))
	mustAppend(t, db, b)

	// A appended later, pointing back at B (resolved forward, A -> B).
	a := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-A",
		ledger.WAKeyRef("chat@c.us", "MSG-B", "", false))
	mustAppend(t, db, a)

	// Link B -> A would close the cycle and must be rejected.
	res, err := db.ResolveCausalLinks()
	if err != nil {
		t.Fatal(err)
	}
	if res.Cyclic != 1 || res.Added != 1 {
		t.Fatalf("link result = %+v, want 1 cyclic, 1 added (A->B)", res)
	}
	dangling, err := db.EnumerateDangling()
	if err != nil {
		t.Fatal(err)
	}
	if len(dangling) != 1 || dangling[0].EventID != b.EventID {
		t.Fatalf("cyclic edge should remain dangling: %+v", dangling)
	}
}

func TestAuditLedgerCleanAndTampered(t *testing.T) {
	db := openTestDB(t)
	evt := newTestEvent(t, ledger.SourceReceipt, "chat@c.us", "MSG-1")
	mustAppend(t, db, evt)

	issues, err := db.AuditLedger()
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("clean ledger has issues: %+v", issues)
	}

	// Tamper with raw bytes: ledger_raw is mutable, audit must detect it.
	if _, err := db.sql.Exec(
		`UPDATE ledger_raw SET raw_bytes = X'00' WHERE event_id = ?`, evt.EventID,
	); err != nil {
		t.Fatal(err)
	}
	issues, err = db.AuditLedger()
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].Code != "raw_hash_mismatch" {
		t.Fatalf("expected raw_hash_mismatch, got %+v", issues)
	}

	// Removing raw bytes from a non-snapshot event is also flagged.
	if _, err := db.sql.Exec(`DELETE FROM ledger_raw WHERE event_id = ?`, evt.EventID); err != nil {
		t.Fatal(err)
	}
	issues, err = db.AuditLedger()
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].Code != "raw_missing" {
		t.Fatalf("expected raw_missing, got %+v", issues)
	}
}

func TestCheckpoints(t *testing.T) {
	db := openTestDB(t)

	cp, err := db.GetCheckpoint("messages")
	if err != nil {
		t.Fatal(err)
	}
	if cp.LastSeq != 0 {
		t.Fatalf("new checkpoint last_seq=%d", cp.LastSeq)
	}
	if err := db.SetCheckpoint("messages", 42, "projector/1.0.0"); err != nil {
		t.Fatal(err)
	}
	cp, err = db.GetCheckpoint("messages")
	if err != nil {
		t.Fatal(err)
	}
	if cp.LastSeq != 42 || cp.ProjectorVersion != "projector/1.0.0" {
		t.Fatalf("checkpoint = %+v", cp)
	}
}

func TestScanEvents(t *testing.T) {
	db := openTestDB(t)
	a := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-A")
	b := newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-B")
	mustAppend(t, db, a)
	mustAppend(t, db, b)

	var ids []string
	err := db.ScanEvents(0, 0, func(evt *StoredEvent) error {
		ids = append(ids, evt.MsgID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "MSG-A" || ids[1] != "MSG-B" {
		t.Fatalf("scanned = %v", ids)
	}

	ids = nil
	err = db.ScanEvents(1, 1, func(evt *StoredEvent) error {
		ids = append(ids, evt.MsgID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("range (1,1] scanned %v", ids)
	}
}

func TestHeadSeqAndBatch(t *testing.T) {
	db := openTestDB(t)
	head, err := db.HeadSeq()
	if err != nil {
		t.Fatal(err)
	}
	if head != 0 {
		t.Fatalf("empty head = %d", head)
	}
	mustAppend(t, db, newTestEvent(t, ledger.SourceLive, "chat@c.us", "MSG-A"))
	head, err = db.HeadSeq()
	if err != nil {
		t.Fatal(err)
	}
	if head != 1 {
		t.Fatalf("head = %d, want 1", head)
	}
	batchID, err := db.StartBatch(ledger.SourceLive)
	if err != nil {
		t.Fatal(err)
	}
	if batchID <= 0 {
		t.Fatalf("batch id = %d", batchID)
	}
}
