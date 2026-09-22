package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Violation check identifiers.
const (
	checkMessageChat     = "message_without_chat"
	checkFTSMissing      = "fts_missing_row"
	checkFTSExtra        = "fts_extra_row"
	checkRosterGroup     = "participant_without_group"
	checkCheckpointAhead = "checkpoint_ahead_of_head"
	checkCheckpointLag   = "checkpoint_lag"
	checkCheckpointFork  = "checkpoint_frontier_fork"
	checkRawHash         = "raw_hash_mismatch"
	checkDangling        = "dangling_causal_ref"
	checkCycle           = "causal_cycle"
	checkScrubWithRaw    = "scrub_with_raw"
	checkMissingRaw      = "event_without_raw_or_scrub"
	checkUnread          = "unread_inconsistent"
)

// Violation is one invariant failure.
type Violation struct {
	Check   string `json:"check"`
	EventID string `json:"event_id,omitempty"`
	Seq     int64  `json:"seq,omitempty"`
	Key     string `json:"key,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

// VerifyReport summarises one verification pass.
type VerifyReport struct {
	HeadSeq    int64       `json:"head_seq"`
	Violations []Violation `json:"violations"`
}

// HasViolations reports whether any violation was recorded.
func (r *VerifyReport) HasViolations() bool { return len(r.Violations) > 0 }

// VerifyLedger checks cross-view invariants for one target set. Pass suffix=""
// for active tables or ShadowSuffix for the rebuilt shadow. Ledger tables
// (events, raw, links, checkpoints) are always global.
func (d *DB) VerifyLedger(ctx context.Context, suffix string) (*VerifyReport, error) {
	head, err := d.HeadSeq()
	if err != nil {
		return nil, err
	}
	report := &VerifyReport{HeadSeq: head}
	add := func(check, key string, seq int64, eventID, detail string) {
		report.Violations = append(report.Violations, Violation{
			Check: check, Key: key, Seq: seq, EventID: eventID, Detail: detail,
		})
	}

	// message → chat
	if err := d.checkMessageChat(ctx, suffix, add); err != nil {
		return nil, err
	}
	// FTS rowid set
	if d.ftsTargetExists(ctx, suffix) {
		if err := d.checkFTS(ctx, suffix, add); err != nil {
			return nil, err
		}
	}
	// participant → group
	if err := d.checkRosterGroup(ctx, suffix, add); err != nil {
		return nil, err
	}
	// checkpoints
	if err := d.checkCheckpoints(ctx, suffix, head, add); err != nil {
		return nil, err
	}
	// raw hash recompute
	if err := d.checkRawHashes(ctx, add); err != nil {
		return nil, err
	}
	// dangling causal refs
	if err := d.checkDangling(ctx, add); err != nil {
		return nil, err
	}
	// causal graph acyclic
	if err := d.checkAcyclic(ctx, add); err != nil {
		return nil, err
	}
	// raw/scrub accounting
	if err := d.checkRawAccounting(ctx, add); err != nil {
		return nil, err
	}
	// unread consistency
	if err := d.checkUnread(ctx, suffix, add); err != nil {
		return nil, err
	}
	return report, nil
}

func (d *DB) checkMessageChat(ctx context.Context, suffix string, add func(string, string, int64, string, string)) error {
	rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(`
		SELECT m.chat_jid, m.msg_id
		FROM %s m LEFT JOIN %s c ON c.jid = m.chat_jid
		WHERE c.jid IS NULL`,
		MessagesTable+suffix, ChatsTable+suffix))
	if err != nil {
		return fmt.Errorf("message→chat: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var chat, msg string
		if err := rows.Scan(&chat, &msg); err != nil {
			return err
		}
		add(checkMessageChat, chat+"/"+msg, 0, "", "message row has no chats row")
	}
	return rows.Err()
}

func (d *DB) ftsTargetExists(ctx context.Context, suffix string) bool {
	if suffix == "" && !d.ftsEnabled {
		return false
	}
	var count int
	if err := d.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master
			WHERE type IN ('table','view') AND name = ?`,
		"messages_fts"+suffix).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func (d *DB) checkFTS(ctx context.Context, suffix string, add func(string, string, int64, string, string)) error {
	msgTable := MessagesTable + suffix
	ftsTable := "messages_fts" + suffix

	missing, err := d.sql.QueryContext(ctx, fmt.Sprintf(`
		SELECT m.rowid FROM %s m
		LEFT JOIN %s f ON f.rowid = m.rowid
		WHERE m.deleted_at IS NULL AND f.rowid IS NULL`, msgTable, ftsTable))
	if err != nil {
		return fmt.Errorf("fts missing: %w", err)
	}
	for missing.Next() {
		var rowid int64
		if err := missing.Scan(&rowid); err != nil {
			missing.Close()
			return err
		}
		add(checkFTSMissing, fmt.Sprintf("rowid:%d", rowid), 0, "", "live message missing FTS row")
	}
	missing.Close()

	extra, err := d.sql.QueryContext(ctx, fmt.Sprintf(`
		SELECT f.rowid FROM %s f
		LEFT JOIN %s m ON m.rowid = f.rowid
		WHERE m.rowid IS NULL OR m.deleted_at IS NOT NULL`, ftsTable, msgTable))
	if err != nil {
		return fmt.Errorf("fts extra: %w", err)
	}
	defer extra.Close()
	for extra.Next() {
		var rowid int64
		if err := extra.Scan(&rowid); err != nil {
			return err
		}
		add(checkFTSExtra, fmt.Sprintf("rowid:%d", rowid), 0, "", "FTS row without a live message")
	}
	return extra.Err()
}

func (d *DB) checkRosterGroup(ctx context.Context, suffix string, add func(string, string, int64, string, string)) error {
	rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(`
		SELECT p.group_jid, p.user_jid
		FROM %s p LEFT JOIN %s g ON g.jid = p.group_jid
		WHERE g.jid IS NULL`,
		"group_participants"+suffix, GroupsTable+suffix))
	if err != nil {
		return fmt.Errorf("participant→group: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var group, user string
		if err := rows.Scan(&group, &user); err != nil {
			return err
		}
		add(checkRosterGroup, group+"/"+user, 0, "", "participant row has no groups row")
	}
	return rows.Err()
}

func (d *DB) checkCheckpoints(ctx context.Context, suffix string, head int64, add func(string, string, int64, string, string)) error {
	pattern := "%" + suffix
	rows, err := d.sql.QueryContext(ctx, `
		SELECT view, last_seq, projector_version FROM projector_checkpoints
		WHERE view LIKE ? ESCAPE '\' ORDER BY view`, strings.ReplaceAll(pattern, "_", "\\_"))
	if err != nil {
		return fmt.Errorf("checkpoints: %w", err)
	}
	type cpRow struct {
		name, version string
		seq           int64
	}
	var cps []cpRow
	for rows.Next() {
		var cp cpRow
		if err := rows.Scan(&cp.name, &cp.seq, &cp.version); err != nil {
			rows.Close()
			return err
		}
		cps = append(cps, cp)
	}
	rows.Close()

	var frontier int64 = -1
	for _, cp := range cps {
		switch {
		case cp.seq > head:
			add(checkCheckpointAhead, cp.name, cp.seq, "",
				fmt.Sprintf("checkpoint seq %d exceeds ledger head %d", cp.seq, head))
		case cp.seq < head:
			add(checkCheckpointLag, cp.name, cp.seq, "",
				fmt.Sprintf("checkpoint seq %d lags ledger head %d", cp.seq, head))
		}
		if frontier == -1 {
			frontier = cp.seq
		} else if cp.seq != frontier {
			add(checkCheckpointFork, cp.name, cp.seq, "",
				fmt.Sprintf("view frontier %d differs from first view frontier %d", cp.seq, frontier))
		}
	}
	return nil
}

func (d *DB) checkRawHashes(ctx context.Context, add func(string, string, int64, string, string)) error {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT e.seq, e.event_id, e.raw_hash, r.raw_bytes
		FROM ledger_events e JOIN ledger_raw r ON r.event_id = e.event_id`)
	if err != nil {
		return fmt.Errorf("raw hash: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		var eventID, wantHash string
		var raw []byte
		if err := rows.Scan(&seq, &eventID, &wantHash, &raw); err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != wantHash {
			add(checkRawHash, "", seq, eventID,
				fmt.Sprintf("stored raw_hash %s, recomputed %s", wantHash, got))
		}
	}
	return rows.Err()
}

func (d *DB) checkDangling(ctx context.Context, add func(string, string, int64, string, string)) error {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT l.event_id, l.ref_index, l.target_id
		FROM ledger_causal_links l
		LEFT JOIN ledger_events e ON e.event_id = l.target_id
		WHERE e.event_id IS NULL`)
	if err != nil {
		return fmt.Errorf("dangling refs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID, targetID string
		var refIndex int64
		if err := rows.Scan(&eventID, &refIndex, &targetID); err != nil {
			return err
		}
		add(checkDangling, eventID+fmt.Sprintf("#%d", refIndex), 0, eventID,
			"causal target missing: "+targetID)
	}
	return rows.Err()
}

func (d *DB) checkAcyclic(ctx context.Context, add func(string, string, int64, string, string)) error {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT event_id, target_id FROM ledger_causal_links`)
	if err != nil {
		return fmt.Errorf("causal graph: %w", err)
	}
	graph := map[string][]string{}
	for rows.Next() {
		var from, to string
		if err := rows.Scan(&from, &to); err != nil {
			rows.Close()
			return err
		}
		graph[from] = append(graph[from], to)
	}
	rows.Close()

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var dfs func(node string, stack []string) bool
	dfs = func(node string, stack []string) bool {
		color[node] = gray
		for _, next := range graph[node] {
			switch color[next] {
			case gray:
				add(checkCycle, next, 0, next,
					"cycle: "+strings.Join(append(stack, node, next), "→"))
				return false
			case white:
				if !dfs(next, append(stack, node)) {
					return false
				}
			}
		}
		color[node] = black
		return true
	}
	for node := range graph {
		if color[node] == white && !dfs(node, nil) {
			return nil
		}
	}
	return nil
}

func (d *DB) checkRawAccounting(ctx context.Context, add func(string, string, int64, string, string)) error {
	// Scrub events must not retain raw bytes.
	scrubRows, err := d.sql.QueryContext(ctx, `
		SELECT e.seq, e.event_id FROM ledger_events e
		JOIN ledger_raw r ON r.event_id = e.event_id
		WHERE e.event_type = 'scrub'`)
	if err != nil {
		return fmt.Errorf("raw/scrub accounting: %w", err)
	}
	for scrubRows.Next() {
		var seq int64
		var eventID string
		if err := scrubRows.Scan(&seq, &eventID); err != nil {
			scrubRows.Close()
			return err
		}
		add(checkScrubWithRaw, "", seq, eventID, "scrub event must not carry raw bytes")
	}
	scrubRows.Close()

	// Non-snapshot, non-scrub events need raw bytes.
	missingRows, err := d.sql.QueryContext(ctx, `
		SELECT e.seq, e.event_id FROM ledger_events e
		LEFT JOIN ledger_raw r ON r.event_id = e.event_id
		WHERE e.source != 'legacy_snapshot'
			AND e.event_type != 'scrub'
			AND r.event_id IS NULL`)
	if err != nil {
		return fmt.Errorf("raw accounting: %w", err)
	}
	defer missingRows.Close()
	for missingRows.Next() {
		var seq int64
		var eventID string
		if err := missingRows.Scan(&seq, &eventID); err != nil {
			return err
		}
		add(checkMissingRaw, "", seq, eventID,
			"non-snapshot event has neither raw bytes nor a scrub marker")
	}
	return missingRows.Err()
}

func (d *DB) checkUnread(ctx context.Context, suffix string, add func(string, string, int64, string, string)) error {
	rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(`
		SELECT jid FROM %s
		WHERE unread < 0 OR unread_count < 0
			OR (unread = 0 AND unread_count > 0)`, ChatsTable+suffix))
	if err != nil {
		return fmt.Errorf("unread consistency: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var jid string
		if err := rows.Scan(&jid); err != nil {
			return err
		}
		add(checkUnread, jid, 0, "",
			"unread marker/count inconsistent (count>0 requires unread marker)")
	}
	return rows.Err()
}
