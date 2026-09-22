package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
)

const e2eChats = 100

func e2eChatJID(i int) string { return fmt.Sprintf("%04d@s.whatsapp.net", i) }

// e2eSnapshotEvent builds a legacy_snapshot event exactly like store bootstrap
// encoding: {"table","row"} body, hash over the body.
func e2eSnapshotEvent(chat, msg string, i int) *ledger.Event {
	row := map[string]any{
		"chat_jid": chat, "chat_name": "C" + chat[:3], "msg_id": msg,
		"sender_jid": chat, "sender_name": "S" + chat[:3], "ts": int64(i),
		"from_me": 0, "text": "hello " + msg, "display_text": "hello " + msg,
		"is_forwarded": 0, "forwarding_score": 0, "revoked": 0,
		"deleted_for_me": 0, "edited": 0, "edited_ts": 0,
	}
	body, _ := json.Marshal(struct {
		Table string         `json:"table"`
		Row   map[string]any `json:"row"`
	}{Table: "messages", Row: row})
	sum := sha256.Sum256(body)
	rh := hex.EncodeToString(sum[:])
	dk := ledger.DedupKey("legacy", "messages", chat, msg)
	return &ledger.Event{
		EventID: ledger.DeriveEventID(dk, rh), Source: ledger.SourceLegacySnapshot,
		EventType: ledger.EventSnapshotMessages, ChatJID: chat, MsgID: msg,
		SenderJID: chat, ServerTS: int64(i), EventTS: int64(i),
		ReceivedAt: 1_700_000_000 + int64(i), RawHash: rh,
		ParserVersion: ledger.ParserVersion, RulesVersion: ledger.RulesVersion,
		DedupKey: dk, BatchID: 1, Snapshot: body,
	}
}

func e2eInsertChats(t *testing.T, a *App) {
	t.Helper()
	tx, err := a.db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < e2eChats; i++ {
		jid := e2eChatJID(i)
		if _, err := tx.Exec(
			`INSERT INTO chats(jid,name,kind) VALUES(?, 'C' || ?, 'dm')`,
			jid, jid[:3]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func e2eChatSnapshotEvent(jid string) *ledger.Event {
	row := map[string]any{"jid": jid, "name": "C" + jid[:3], "kind": "dm"}
	body, _ := json.Marshal(struct {
		Table string         `json:"table"`
		Row   map[string]any `json:"row"`
	}{Table: "chats", Row: row})
	sum := sha256.Sum256(body)
	rh := hex.EncodeToString(sum[:])
	dk := ledger.DedupKey("legacy", "chats", jid)
	return &ledger.Event{
		EventID: ledger.DeriveEventID(dk, rh), Source: ledger.SourceLegacySnapshot,
		EventType: ledger.EventSnapshotChats, ChatJID: jid,
		EventTS: 1, ReceivedAt: 1_700_000_001, RawHash: rh,
		ParserVersion: ledger.ParserVersion, RulesVersion: ledger.RulesVersion,
		DedupKey: dk, BatchID: 1, Snapshot: body,
	}
}

// TestLedgerE2E10k runs the full upgrade drill on 10k synthetic events and
// records reproducible timing/memory evidence.
func TestLedgerE2E10k(t *testing.T) {
	const n = 10000
	ctx := context.Background()
	a := newTestApp(t)
	e2eInsertChats(t, a)

	// ---- Old path: direct upserts, batched 500/tx ----
	start := time.Now()
	for base := 0; base < n; base += 500 {
		tx, err := a.db.BeginTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		end := min(base+500, n)
		for i := base; i < end; i++ {
			chat := e2eChatJID(i % e2eChats)
			msg := fmt.Sprintf("m%d", i)
			if _, err := tx.Exec(`INSERT INTO messages
				(chat_jid,chat_name,msg_id,sender_jid,sender_name,ts,from_me,text,display_text)
				VALUES(?,?,?,?,?,?,0,?,?)`,
				chat, "C"+chat[:3], msg, chat, "S"+chat[:3], int64(i),
				"hello "+msg, "hello "+msg); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	tOld := time.Since(start)

	// ---- New path 1: ledger append, batched 500/tx ----
	batchID, err := a.db.StartBatch(ledger.SourceLegacySnapshot)
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	for base := 0; base < n; base += 500 {
		tx, err := a.db.BeginTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		end := min(base+500, n)
		for i := base; i < end; i++ {
			evt := e2eSnapshotEvent(e2eChatJID(i%e2eChats), fmt.Sprintf("m%d", i), i)
			evt.BatchID = batchID
			if _, _, err := a.db.AppendEventTx(tx, evt); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	tAppend := time.Since(start)

	// Parent chat events so shadow replay produces all chats (shadow tables
	// have no FK; event order does not require parents first).
	tx, err := a.db.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < e2eChats; i++ {
		if _, _, err := a.db.AppendEventTx(tx, e2eChatSnapshotEvent(e2eChatJID(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// ---- New path 2: shadow rebuild (checkpoint projector, streams) ----
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	start = time.Now()
	rep, err := a.RebuildShadow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tProject := time.Since(start)
	runtime.ReadMemStats(&m2)

	if rep.TargetSeq != n+e2eChats {
		t.Fatalf("projector target seq = %d, want %d", rep.TargetSeq, n+e2eChats)
	}

	// verify + diff + promote + verify active
	vr, err := a.VerifyShadowLedger(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if vr.HasViolations() {
		t.Fatalf("shadow violations: %+v", vr.Violations)
	}
	dr, err := a.DiffShadowLedger(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dr.HasDiffs() {
		t.Fatalf("shadow diff before promote: %+v", dr.Diffs[:3])
	}
	if _, err := a.PromoteShadow(ctx, false); err != nil {
		t.Fatal(err)
	}
	vr, err = a.VerifyLedger(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if vr.HasViolations() {
		t.Fatalf("active violations after promote: %+v", vr.Violations)
	}

	// ---- Determinism: two independent rebuilds are identical ----
	// Roll back to get shadow slot, rebuild twice, compare shadow sets.
	if _, err := a.RollbackLedger(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.RebuildShadow(ctx); err != nil {
		t.Fatal(err)
	}
	first := snapshotSet(t, a, store.ShadowSuffix)
	if _, err := a.RebuildShadow(ctx); err != nil {
		t.Fatal(err)
	}
	second := snapshotSet(t, a, store.ShadowSuffix)
	for table, rows := range first {
		if !rowsEqual(rows, second[table]) {
			t.Fatalf("non-deterministic rebuild of %s", table)
		}
	}

	// ---- Parser/rules upgrade explanation ----
	// Emulate a new projector rule that upper-cases text/display_text.
	const nextRules = "0.19-rules-next"
	if _, err := a.db.Exec(
		`UPDATE messages_shadow SET text = upper(text), display_text = upper(display_text)`); err != nil {
		t.Fatal(err)
	}
	diff2, err := a.DiffShadowLedger(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff2.Diffs) != n {
		t.Fatalf("upgrade diff rows = %d, want %d", len(diff2.Diffs), n)
	}
	// Every changed row must attribute to its source event with versions.
	for _, d := range diff2.Diffs {
		if d.Kind != "changed" || d.EventID == "" || d.Seq == 0 {
			t.Fatalf("upgrade row not fully attributed: %+v", d)
		}
		stored, err := a.db.GetEventByID(d.EventID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.ParserVersion != ledger.ParserVersion ||
			stored.RulesVersion != ledger.RulesVersion {
			t.Fatalf("event versions missing: %+v", stored)
		}
	}
	explanation := map[string]any{
		"from_rules_version": ledger.RulesVersion,
		"to_rules_version":   nextRules,
		"changed_rows":       len(diff2.Diffs),
		"sample":             diff2.Diffs[0],
	}
	if b, err := json.Marshal(explanation); err == nil {
		t.Logf("upgrade explanation: %s", b)
	}

	// ---- Timing & memory evidence ----
	overheadPct := 100 * (float64(tAppend+tProject-tOld) / float64(tOld))
	t.Log(strings.Join([]string{
		fmt.Sprintf("E2E n=%d old_direct=%v new_append=%v new_project=%v", n, tOld, tAppend, tProject),
		fmt.Sprintf("overhead=%.1f%% append_per_event=%v project_per_event=%v",
			overheadPct, tAppend/n, tProject/n),
		fmt.Sprintf("heap_before=%dKB heap_after=%dKB delta=%dKB (streaming ScanEvents, no whole-table load)",
			m1.HeapAlloc/1024, m2.HeapAlloc/1024, (m2.HeapAlloc-m1.HeapAlloc)/1024),
	}, "\n"))
}
