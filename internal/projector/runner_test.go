package projector

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
)

func seedRunnerEvents(t *testing.T, db *store.DB, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		dk := ledger.DedupKey("runner-test", fmt.Sprintf("%d", i))
		evt := &ledger.Event{
			EventID:       fmt.Sprintf("evt-runner-%03d", i),
			Source:        ledger.SourceReceipt,
			EventType:     ledger.EventReceipt,
			ReceivedAt:    int64(i),
			RawHash:       strings.Repeat("a", 64),
			ParserVersion: ledger.ParserVersion,
			RulesVersion:  ledger.RulesVersion,
			DedupKey:      dk,
			RawBytes:      []byte(fmt.Sprintf("payload-%d", i)),
		}
		if _, _, err := db.AppendEvent(evt); err != nil {
			t.Fatalf("AppendEvent %d: %v", i, err)
		}
	}
}

func countRows(t *testing.T, db *store.DB, table string) int {
	t.Helper()
	var n int
	rows, err := db.Query(
		fmt.Sprintf(`SELECT count(*) FROM %s`, table))
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	return n
}

func TestRunnerAdvancesAllViewsToFrontier(t *testing.T) {
	db := openRunnerDB(t)
	seedRunnerEvents(t, db, 10)

	runner := New(db, []View{
		newScratchView("scratch_a", 0),
		newScratchView("scratch_b", 0),
	}).WithBatchSize(3)

	report, err := runner.Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.TargetSeq != 10 {
		t.Fatalf("target = %d, want 10", report.TargetSeq)
	}
	for name, p := range report.Views {
		if p.FromSeq != 0 || p.ToSeq != 10 || p.Batches != 4 {
			t.Fatalf("%s progress = %+v, want 0->10 in 4 batches", name, p)
		}
	}
	// Both checkpoints at the shared frontier.
	for _, name := range []string{"scratch_a", "scratch_b"} {
		cp, err := db.GetCheckpoint(name)
		if err != nil {
			t.Fatalf("GetCheckpoint %s: %v", name, err)
		}
		if cp.LastSeq != 10 || cp.ProjectorVersion != "test-projector/1" {
			t.Fatalf("%s checkpoint = %+v, want seq 10", name, cp)
		}
	}
	if n := countRows(t, db, "scratch_a_rows"); n != 10 {
		t.Fatalf("scratch_a rows = %d, want 10", n)
	}
	if n := countRows(t, db, "scratch_b_rows"); n != 10 {
		t.Fatalf("scratch_b rows = %d, want 10", n)
	}

	// Re-running applies nothing and changes nothing.
	runner2 := New(db, []View{
		newScratchView("scratch_a", 0),
		newScratchView("scratch_b", 0),
	}).WithBatchSize(3)
	report2, err := runner2.Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	for name, p := range report2.Views {
		if p.Batches != 0 || p.ToSeq != 10 {
			t.Fatalf("%s second progress = %+v, want no-op", name, p)
		}
	}
}

func TestRunnerRecoversAfterFailure(t *testing.T) {
	db := openRunnerDB(t)
	seedRunnerEvents(t, db, 10)

	// First view fails once its batch reaches seq 5; the whole batch tx
	// (batch size 3: seq 4,5,6) must roll back.
	runner := New(db, []View{
		newScratchView("scratch_a", 5),
		newScratchView("scratch_b", 0),
	}).WithBatchSize(3)
	_, err := runner.Run(context.Background(), 0)
	if err == nil || !errors.Is(err, errScratchFailure) {
		t.Fatalf("Run err = %v, want errScratchFailure", err)
	}

	cpA, _ := db.GetCheckpoint("scratch_a")
	cpB, _ := db.GetCheckpoint("scratch_b")
	if cpA.LastSeq != 3 {
		t.Fatalf("scratch_a checkpoint = %d, want 3", cpA.LastSeq)
	}
	if cpB.LastSeq != 0 {
		t.Fatalf("scratch_b checkpoint = %d, want 0 (run aborts on first failure)", cpB.LastSeq)
	}
	// Rows from the failed batch (seq 4-6) must not be visible; batches 1-3
	// committed only seq 1-3.
	if n := countRows(t, db, "scratch_a_rows"); n != 3 {
		t.Fatalf("scratch_a rows after failure = %d, want 3", n)
	}

	// Repair: healthy views, resume.
	repaired := New(db, []View{
		newScratchView("scratch_a", 0),
		newScratchView("scratch_b", 0),
	}).WithBatchSize(3)
	report, err := repaired.Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("repair Run: %v", err)
	}
	for name, p := range report.Views {
		if p.ToSeq != 10 {
			t.Fatalf("%s repaired to %d, want 10", name, p.ToSeq)
		}
	}
	if n := countRows(t, db, "scratch_a_rows"); n != 10 {
		t.Fatalf("scratch_a rows after repair = %d, want 10", n)
	}
	if n := countRows(t, db, "scratch_b_rows"); n != 10 {
		t.Fatalf("scratch_b rows after repair = %d, want 10", n)
	}
}

func TestRunnerStopsAtExplicitTarget(t *testing.T) {
	db := openRunnerDB(t)
	seedRunnerEvents(t, db, 10)

	runner := New(db, []View{newScratchView("scratch_a", 0)}).WithBatchSize(4)
	report, err := runner.Run(context.Background(), 7)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.TargetSeq != 7 {
		t.Fatalf("target = %d, want 7", report.TargetSeq)
	}
	cp, _ := db.GetCheckpoint("scratch_a")
	if cp.LastSeq != 7 {
		t.Fatalf("checkpoint = %d, want 7", cp.LastSeq)
	}
	if n := countRows(t, db, "scratch_a_rows"); n != 7 {
		t.Fatalf("rows = %d, want 7", n)
	}
}

func openRunnerDB(t *testing.T) *store.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "wacli.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
