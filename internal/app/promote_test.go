package app

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
)

// TR-19.1: happy path promote, gate enforcement, second-promote block,
// rollback restores the exact previous set.
func TestPromoteShadowAndRollback(t *testing.T) {
	a := buildRebuiltStore(t)
	ctx := context.Background()

	before := snapshotSet(t, a, "")

	report, err := a.PromoteShadow(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Tables == 0 || report.HeadSeq == 0 {
		t.Fatalf("bad promote report %+v", report)
	}

	if exists, err := a.db.ShadowSetExists(ctx); err != nil || exists {
		t.Fatalf("shadow set should be gone after promote: exists=%v err=%v", exists, err)
	}
	if exists, err := a.db.BackupSetExists(ctx); err != nil || !exists {
		t.Fatalf("backup set should exist after promote: exists=%v err=%v", exists, err)
	}

	// Active must verify clean after the switch.
	vr, err := a.VerifyLedger(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if vr.HasViolations() {
		t.Fatalf("active violations after promote: %+v", vr.Violations)
	}

	// Checkpoints moved with the sets: plain keys live, backup keys retained.
	views := map[string]string{}
	rows, err := a.db.Query(`SELECT view, mode FROM ledger_view_state`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var v, m string
		if err := rows.Scan(&v, &m); err != nil {
			t.Fatal(err)
		}
		views[v] = m
	}
	rows.Close()
	if views["messages"] != "live" {
		t.Fatalf("messages view state = %q, want live; all=%v", views["messages"], views)
	}
	if _, ok := views["messages_shadow"]; ok {
		t.Fatalf("shadow view state should be gone: %v", views)
	}

	var cpSeq int64
	if err := a.db.QueryRow(`SELECT last_seq FROM projector_checkpoints WHERE view='messages'`).
		Scan(&cpSeq); err != nil {
		t.Fatal(err)
	}
	if cpSeq == 0 {
		t.Fatal("messages checkpoint missing after promote")
	}

	// FTS works against the new active set.
	if a.db.FTSEnabled() {
		var hits int
		if err := a.db.QueryRow(
			`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'hello'`).
			Scan(&hits); err != nil {
			t.Fatal(err)
		}
		if hits == 0 {
			t.Fatal("FTS returned no hits after promote")
		}
	}

	// Second promote is refused while a backup exists.
	if _, err := a.PromoteShadow(ctx, false); err == nil ||
		!strings.Contains(err.Error(), "backup view set already exists") {
		t.Fatalf("second promote not blocked: %v", err)
	}

	// Rollback restores the previous active set exactly.
	if _, err := a.RollbackLedger(ctx); err != nil {
		t.Fatal(err)
	}
	after := snapshotSet(t, a, "")
	for table, rows := range before {
		if got := after[table]; !rowsEqual(rows, got) {
			t.Fatalf("table %s changed after rollback:\nbefore=%v\nafter =%v", table, rows, got)
		}
	}
	if exists, _ := a.db.BackupSetExists(ctx); exists {
		t.Fatal("backup set should be gone after rollback")
	}
	if exists, _ := a.db.ShadowSetExists(ctx); !exists {
		t.Fatal("shadow set should exist again after rollback")
	}
	if a.db.FTSEnabled() {
		var hits int
		if err := a.db.QueryRow(
			`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'hello'`).
			Scan(&hits); err != nil || hits == 0 {
			t.Fatalf("FTS after rollback: hits=%d err=%v", hits, err)
		}
	}

	// Promote again works: the cycle is repeatable.
	if _, err := a.RebuildShadow(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PromoteShadow(ctx, false); err != nil {
		t.Fatalf("re-promote failed: %v", err)
	}
}

// TR-19.1: hard invariant gate cannot be overridden by force; diff gate can.
func TestPromoteGates(t *testing.T) {
	ctx := context.Background()

	// Hard invariant violation: shadow message without its chat.
	a := buildRebuiltStore(t)
	if _, err := a.db.Exec(`DELETE FROM chats_shadow WHERE jid='15551234567@s.whatsapp.net'`); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PromoteShadow(ctx, true); err == nil ||
		!strings.Contains(err.Error(), "invariant check failed") {
		t.Fatalf("force overrode hard invariant gate: %v", err)
	}
	// Active untouched.
	var chats int
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM chats WHERE jid='15551234567@s.whatsapp.net'`).
		Scan(&chats); err != nil || chats != 1 {
		t.Fatalf("active changed after refused promote: chats=%d err=%v", chats, err)
	}

	// Policy-only diff (changed value) refused without force, accepted with.
	a = buildRebuiltStore(t)
	if _, err := a.db.Exec(`UPDATE chats_shadow SET name='Policy Diff'
		WHERE jid='15551234567@s.whatsapp.net'`); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PromoteShadow(ctx, false); err == nil ||
		!strings.Contains(err.Error(), "differs from active") {
		t.Fatalf("diff gate not enforced: %v", err)
	}
	if _, err := a.PromoteShadow(ctx, true); err != nil {
		t.Fatalf("force should bypass diff policy: %v", err)
	}
	if _, err := a.RollbackLedger(ctx); err != nil {
		t.Fatal(err)
	}
}

// TR-19.1: failure inside the switch transaction leaves every active table
// intact and creates no partial set.
func TestPromoteAtomicFailure(t *testing.T) {
	a := buildRebuiltStore(t)
	ctx := context.Background()
	before := snapshotSet(t, a, "")

	if _, err := a.db.Exec(`DROP TABLE messages_shadow`); err != nil {
		t.Fatal(err)
	}
	_, err := a.db.PromoteShadow(ctx)
	if err == nil {
		t.Fatalf("expected rename error")
	}
	if !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("unexpected error: %v", err)
	}

	if exists, _ := a.db.BackupSetExists(ctx); exists {
		t.Fatal("backup set must not exist after failed promote")
	}
	after := snapshotSet(t, a, "")
	for table, rows := range before {
		if got := after[table]; !rowsEqual(rows, got) {
			t.Fatalf("active table %s mutated by failed promote", table)
		}
	}
}

// TR-19.2: concurrent readers always observe a self-consistent set: either
// the old world or the new world, never a mix.
func TestPromoteConcurrentReaders(t *testing.T) {
	a := buildRebuiltStore(t)
	ctx := context.Background()

	const readers = 4
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var readerErr sync.Mutex
	var firstErr error
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var orphan int
				err := a.db.QueryRow(`SELECT COUNT(*) FROM messages m
					WHERE NOT EXISTS (SELECT 1 FROM chats c WHERE c.jid=m.chat_jid)`).
					Scan(&orphan)
				if err != nil {
					readerErr.Lock()
					if firstErr == nil {
						firstErr = err
					}
					readerErr.Unlock()
					return
				}
				if orphan != 0 {
					readerErr.Lock()
					if firstErr == nil {
						firstErr = errors.New("reader observed mixed view set")
					}
					readerErr.Unlock()
					return
				}
			}
		}()
	}

	if _, err := a.PromoteShadow(ctx, false); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}

// snapshotSet dumps every promoted table (excluding rowid) as key→row pairs.
func snapshotSet(t *testing.T, a *App, suffix string) map[string]map[string]string {
	t.Helper()
	ctx := context.Background()
	out := map[string]map[string]string{}
	for _, base := range []string{
		"chats", "contacts", "groups", "group_participants", "messages",
		"message_locations", "message_payload_purges", "polls", "poll_votes",
		"status_messages", "call_events", "starred",
	} {
		cols, err := tableColumnsOf(ctx, a, base+suffix)
		if err != nil {
			t.Fatal(err)
		}
		colList := ""
		for _, c := range cols {
			if c == "rowid" {
				continue
			}
			colList += c + ","
		}
		colList = strings.TrimSuffix(colList, ",")
		rows, err := a.db.Query(`SELECT ` + colList + ` FROM ` + base + suffix)
		if err != nil {
			t.Fatal(err)
		}
		values := map[string]string{}
		for rows.Next() {
			raw, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			ptrs := make([]any, len(raw))
			buf := make([]sql.NullString, len(raw))
			for i := range ptrs {
				ptrs[i] = &buf[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			keyParts := []string{}
			for _, v := range buf {
				keyParts = append(keyParts, v.String)
			}
			values[strings.Join(keyParts, "\x00")] = strings.Join(keyParts, "|")
		}
		rows.Close()
		out[base] = values
	}
	return out
}

func tableColumnsOf(ctx context.Context, a *App, table string) ([]string, error) {
	rows, err := a.db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

func rowsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
