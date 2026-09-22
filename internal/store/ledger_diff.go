package store

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/openclaw/wacli/internal/ledger"
)

// Diff row kinds. "missing" rows exist only in active (shadow lacks them);
// "extra" rows exist only in shadow.
const (
	DiffMissing = "missing"
	DiffExtra   = "extra"
	DiffChanged = "changed"
)

type diffTableSpec struct {
	table   string
	keyCols []string
}

var diffTableSpecs = []diffTableSpec{
	{ChatsTable, []string{"jid"}},
	{"contacts", []string{"jid"}},
	{GroupsTable, []string{"jid"}},
	{"group_participants", []string{"group_jid", "user_jid"}},
	{MessagesTable, []string{"chat_jid", "msg_id"}},
	{MessageLocationsTable, []string{"chat_jid", "msg_id"}},
	{MessagePayloadPurges, []string{"chat_jid", "msg_id"}},
	{PollsTable, []string{"chat_jid", "msg_id"}},
	{PollVotesTable, []string{"chat_jid", "poll_msg_id", "voter_jid"}},
	{StatusMessagesTable, []string{"msg_id"}},
	{CallEventsTable, []string{"chat_jid", "call_id", "event_type", "ts"}},
	{"starred", []string{"chat_jid", "msg_id"}},
}

// ViewCount is one table's row count in both targets.
type ViewCount struct {
	Active int `json:"active"`
	Shadow int `json:"shadow"`
}

// RowDiff is one keyed discrepancy.
type RowDiff struct {
	Table   string   `json:"table"`
	Kind    string   `json:"kind"`
	Key     string   `json:"key"`
	Fields  []string `json:"changed_fields,omitempty"`
	Seq     int64    `json:"seq,omitempty"`
	EventID string   `json:"event_id,omitempty"`
}

// DiffReport summarises active vs shadow across every view.
type DiffReport struct {
	HeadSeq int64                `json:"head_seq"`
	Counts  map[string]ViewCount `json:"counts"`
	Diffs   []RowDiff            `json:"diffs"`
}

// HasDiffs reports whether any discrepancy was found.
func (r *DiffReport) HasDiffs() bool { return len(r.Diffs) > 0 }

// DiffShadow compares active and rebuilt shadow tables. Ledger tables are
// global and never compared; operational alias tables are excluded.
func (d *DB) DiffShadow(ctx context.Context) (*DiffReport, error) {
	head, err := d.HeadSeq()
	if err != nil {
		return nil, err
	}
	report := &DiffReport{HeadSeq: head, Counts: map[string]ViewCount{}}

	for _, spec := range diffTableSpecs {
		cols, err := d.tableColumns(ctx, spec.table)
		if err != nil {
			return nil, err
		}
		// rowid is an implementation artifact and is assigned independently
		// by shadow replay; FTS rowid-set consistency is covered by verify.
		cols = removeString(cols, "rowid")
		active, err := d.readKeyedRows(ctx, spec.table, cols, spec.keyCols)
		if err != nil {
			return nil, err
		}
		shadow, err := d.readKeyedRows(ctx, spec.table+ShadowSuffix, cols, spec.keyCols)
		if err != nil {
			return nil, err
		}
		report.Counts[spec.table] = ViewCount{Active: len(active), Shadow: len(shadow)}
		d.diffKeyedTable(ctx, spec, cols, active, shadow, report)
	}

	if d.ftsTargetExists(ctx, "") && d.ftsTargetExists(ctx, ShadowSuffix) {
		if err := d.diffFTS(ctx, report); err != nil {
			return nil, err
		}
	}
	return report, nil
}

func (d *DB) diffKeyedTable(
	ctx context.Context,
	spec diffTableSpec,
	cols []string,
	active, shadow map[string]keyedRow,
	report *DiffReport,
) {
	for key, arow := range active {
		brow, ok := shadow[key]
		if !ok {
			seq, eventID := d.attributeKey(ctx, spec, arow)
			report.Diffs = append(report.Diffs, RowDiff{
				Table: spec.table, Kind: DiffMissing, Key: key,
				Seq: seq, EventID: eventID,
			})
			continue
		}
		var changed []string
		for _, col := range cols {
			if !valuesEqual(arow.values[col], brow.values[col]) {
				changed = append(changed, col)
			}
		}
		if len(changed) > 0 {
			seq, eventID := d.attributeKey(ctx, spec, arow)
			report.Diffs = append(report.Diffs, RowDiff{
				Table: spec.table, Kind: DiffChanged, Key: key, Fields: changed,
				Seq: seq, EventID: eventID,
			})
		}
	}
	for key, brow := range shadow {
		if _, ok := active[key]; ok {
			continue
		}
		seq, eventID := d.attributeKey(ctx, spec, brow)
		report.Diffs = append(report.Diffs, RowDiff{
			Table: spec.table, Kind: DiffExtra, Key: key,
			Seq: seq, EventID: eventID,
		})
	}
}

// attributeKey maps a row key to the responsible ledger event. Legacy
// snapshot dks are exact; rows carrying chat/message keys fall back to the
// most recent event for the pair.
func (d *DB) attributeKey(ctx context.Context, spec diffTableSpec, row keyedRow) (int64, string) {
	keyParts := make([]string, len(spec.keyCols))
	for i, keyCol := range spec.keyCols {
		keyParts[i] = stringValue(row.values[keyCol])
	}
	dk := dedupKeyForLegacy(spec.table, keyParts)
	seq, eventID, err := d.LookupByDedupKey(ctx, dk)
	if err == nil && eventID != "" {
		return seq, eventID
	}

	chat, hasChat := row.values["chat_jid"]
	msg, hasMsg := row.values["msg_id"]
	if hasChat && hasMsg {
		seq, eventID, err := d.LookupByChatMsg(ctx, stringValue(chat), stringValue(msg))
		if err == nil {
			return seq, eventID
		}
	}
	return 0, ""
}

func dedupKeyForLegacy(table string, keyParts []string) string {
	parts := append([]string{table}, keyParts...)
	return ledger.DedupKey("legacy", parts...)
}

// keyedRow is one row addressed by an encoded key.
type keyedRow struct {
	key    string
	values map[string]any
}

func (d *DB) tableColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := d.sql.QueryContext(ctx,
		`PRAGMA table_info(`+table+`)`)
	if err != nil {
		return nil, fmt.Errorf("inspect columns of %s: %w", table, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

func (d *DB) readKeyedRows(
	ctx context.Context,
	table string,
	cols, keyCols []string,
) (map[string]keyedRow, error) {
	query := `SELECT ` + strings.Join(cols, ",") + ` FROM ` + table
	rows, err := d.sql.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", table, err)
	}
	defer rows.Close()

	out := map[string]keyedRow{}
	for rows.Next() {
		scans := make([]any, len(cols))
		values := make(map[string]any, len(cols))
		for i := range scans {
			scans[i] = &scans[i]
		}
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}
		for i, col := range cols {
			values[col] = scans[i]
		}
		keyParts := make([]string, len(keyCols))
		for i, keyCol := range keyCols {
			keyParts[i] = stringValue(values[keyCol])
		}
		key := strings.Join(keyParts, "\x00")
		out[key] = keyedRow{key: key, values: values}
	}
	return out, rows.Err()
}

// diffFTS compares the FTS content set of active and shadow. Rowids differ
// between the two independent virtual tables, so only content rows match.
func (d *DB) diffFTS(ctx context.Context, report *DiffReport) error {
	active, err := d.readFTSContent(ctx, "messages_fts")
	if err != nil {
		return err
	}
	shadow, err := d.readFTSContent(ctx, "messages_fts"+ShadowSuffix)
	if err != nil {
		return err
	}
	report.Counts["messages_fts"] = ViewCount{Active: len(active), Shadow: len(shadow)}

	for content, refs := range active {
		want := refs
		if got := shadow[content]; got > want {
			continue
		} else if got < want {
			for i := int64(0); i < want-got; i++ {
				seq, eventID := d.attributeFTSContent(ctx, content)
				report.Diffs = append(report.Diffs, RowDiff{
					Table: "messages_fts", Kind: DiffMissing, Key: content,
					Seq: seq, EventID: eventID,
				})
			}
		}
	}
	for content, refs := range shadow {
		want := refs
		got := active[content]
		if got >= want {
			continue
		}
		for i := int64(0); i < want-got; i++ {
			report.Diffs = append(report.Diffs, RowDiff{
				Table: "messages_fts", Kind: DiffExtra, Key: content,
			})
		}
	}
	return nil
}

func (d *DB) readFTSContent(ctx context.Context, table string) (map[string]int64, error) {
	rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(`
		SELECT text, media_caption, filename, chat_name, sender_name, display_text
		FROM %s`, table))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", table, err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var text, caption, filename, chatName, senderName, display string
		if err := rows.Scan(&text, &caption, &filename, &chatName, &senderName, &display); err != nil {
			return nil, err
		}
		out[strings.Join([]string{text, caption, filename, chatName, senderName, display}, "\x00")]++
	}
	return out, rows.Err()
}

// attributeFTSContent maps an FTS content row to its message's ledger event
// via one of the live messages with that content.
func (d *DB) attributeFTSContent(ctx context.Context, content string) (int64, string) {
	parts := strings.Split(content, "\x00")
	if len(parts) != 6 {
		return 0, ""
	}
	var rowid int64
	err := d.sql.QueryRowContext(ctx, `
		SELECT rowid FROM messages
		WHERE deleted_at IS NULL
			AND COALESCE(text,'')=? AND COALESCE(media_caption,'')=?
			AND COALESCE(filename,'')=? AND COALESCE(chat_name,'')=?
			AND COALESCE(sender_name,'')=? AND COALESCE(display_text,'')=?
		LIMIT 1`, parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]).Scan(&rowid)
	if err != nil {
		return 0, ""
	}
	var chatJID, msgID string
	if err := d.sql.QueryRowContext(ctx,
		`SELECT chat_jid, msg_id FROM messages WHERE rowid=?`, rowid).
		Scan(&chatJID, &msgID); err != nil {
		return 0, ""
	}
	seq, eventID, err := d.LookupByChatMsg(ctx, chatJID, msgID)
	if err != nil {
		return 0, ""
	}
	return seq, eventID
}

// valuesEqual compares scanned driver values, treating numeric kinds
// numerically and blobs byte-wise.
func valuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if an, ok := numericValue(a); ok {
		if bn, ok := numericValue(b); ok {
			return an == bn
		}
		return false
	}
	if ab, ok := a.([]byte); ok {
		bb, ok := b.([]byte)
		return ok && bytes.Equal(ab, bb)
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func numericValue(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}

func removeString(items []string, want string) []string {
	out := items[:0]
	for _, item := range items {
		if item != want {
			out = append(out, item)
		}
	}
	return out
}

func stringValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}
