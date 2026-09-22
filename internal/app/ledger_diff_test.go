package app

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func buildRebuiltStore(t *testing.T) *App {
	t.Helper()
	a := newTestApp(t)
	if _, err := a.db.Exec(rebuildFixtureSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.BootstrapLegacySnapshots(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.RebuildShadow(context.Background()); err != nil {
		t.Fatal(err)
	}
	return a
}

// TR-18.1 baseline: after a clean rebuild there are no diffs and every table
// count matches.
func TestDiffShadowClean(t *testing.T) {
	a := buildRebuiltStore(t)
	report, err := a.DiffShadowLedger(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.HasDiffs() {
		t.Fatalf("clean rebuild produced diffs: %+v", report.Diffs)
	}
	for table, counts := range report.Counts {
		if counts.Active != counts.Shadow {
			t.Fatalf("table %s counts diverge: %+v", table, counts)
		}
	}

	// JSON shape.
	b, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"head_seq", "counts", "diffs"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("JSON missing key %q: %s", key, b)
		}
	}
}

// TR-18.1: injected changed/missing/extra rows are classified with fields and
// ledger attribution.
func TestDiffShadowInjected(t *testing.T) {
	a := buildRebuiltStore(t)
	ctx := context.Background()

	// changed: shadow chat name
	if _, err := a.db.Exec(`UPDATE chats_shadow SET name='Changed'
		WHERE jid='15551234567@s.whatsapp.net'`); err != nil {
		t.Fatal(err)
	}
	// missing: shadow poll deleted
	if _, err := a.db.Exec(`DELETE FROM polls_shadow
		WHERE msg_id='poll1'`); err != nil {
		t.Fatal(err)
	}
	// extra: shadow-only starred row
	if _, err := a.db.Exec(`INSERT INTO starred_shadow
		(chat_jid,msg_id,from_me,starred_at) VALUES ('x@g.us','xm',0,999)`); err != nil {
		t.Fatal(err)
	}

	report, err := a.DiffShadowLedger(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var changed, missing, extra []struct {
		key     string
		fields  []string
		eventID string
		seq     int64
	}
	for _, d := range report.Diffs {
		entry := map[string]any{}
		_ = entry
		switch d.Kind {
		case "changed":
			changed = append(changed, struct {
				key     string
				fields  []string
				eventID string
				seq     int64
			}{d.Key, d.Fields, d.EventID, d.Seq})
		case "missing":
			missing = append(missing, struct {
				key     string
				fields  []string
				eventID string
				seq     int64
			}{d.Key, nil, d.EventID, d.Seq})
		case "extra":
			extra = append(extra, struct {
				key     string
				fields  []string
				eventID string
				seq     int64
			}{d.Key, nil, d.EventID, d.Seq})
		}
	}

	if len(changed) != 1 || changed[0].key != "15551234567@s.whatsapp.net" {
		t.Fatalf("changed rows = %+v", changed)
	}
	if len(changed[0].fields) != 1 || changed[0].fields[0] != "name" {
		t.Fatalf("changed fields = %+v, want [name]", changed[0].fields)
	}
	if changed[0].eventID == "" || changed[0].seq == 0 {
		t.Fatalf("changed row has no ledger attribution: %+v", changed[0])
	}

	if len(missing) != 1 || !strings.Contains(missing[0].key, "poll1") {
		t.Fatalf("missing rows = %+v", missing)
	}
	if missing[0].eventID == "" {
		t.Fatalf("missing poll row has no ledger attribution")
	}

	if len(extra) != 1 || extra[0].key != "x@g.us\x00xm" {
		t.Fatalf("extra rows = %+v", extra)
	}

	// TR-18.2: sample report for the readability/attribution rubric.
	if sample, err := json.MarshalIndent(report, "", "  "); err == nil {
		t.Logf("sample diff report:\n%s", sample)
	}
}
