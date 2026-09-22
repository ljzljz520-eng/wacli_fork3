package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
)

// pendingLinkQuery lists every causal reference that has no link row yet and
// resolves its target. The on-the-fly WAKey construction must match
// ledger.WAKey exactly: key order chat/from_me/id/participant, participant
// omitted when empty, booleans as JSON true/false.
const pendingLinkQuery = `
SELECT
	e.event_id,
	CAST(je.key AS INTEGER) AS ref_index,
	target.event_id AS target_id,
	target.seq AS target_seq
FROM ledger_events e, json_each(e.causal_refs) je
LEFT JOIN ledger_events target ON (
	(
		json_extract(je.value, '$.kind') = 'event_id'
		AND target.event_id = COALESCE(json_extract(je.value, '$.event_id'), '')
	)
	OR
	(
		json_extract(je.value, '$.kind') = 'wa_key'
		AND target.wa_key = CASE
			WHEN COALESCE(json_extract(je.value, '$.sender'), '') = ''
			THEN json_object(
				'chat', COALESCE(json_extract(je.value, '$.chat_jid'), ''),
				'from_me', CASE WHEN COALESCE(json_extract(je.value, '$.from_me'), 0) = 1
					THEN json('true') ELSE json('false') END,
				'id', COALESCE(json_extract(je.value, '$.msg_id'), '')
			)
			ELSE json_object(
				'chat', COALESCE(json_extract(je.value, '$.chat_jid'), ''),
				'from_me', CASE WHEN COALESCE(json_extract(je.value, '$.from_me'), 0) = 1
					THEN json('true') ELSE json('false') END,
				'id', COALESCE(json_extract(je.value, '$.msg_id'), ''),
				'participant', COALESCE(json_extract(je.value, '$.sender'), '')
			)
		END
	)
)
WHERE NOT EXISTS (
	SELECT 1 FROM ledger_causal_links lk
	WHERE lk.event_id = e.event_id AND lk.ref_index = CAST(je.key AS INTEGER)
)
`

// candidateLink is one resolved but not yet recorded causal edge.
type candidateLink struct {
	eventID  string
	refIndex int
	targetID string
	seq      int64
}

// LinkResult summarises a causal link resolution pass.
type LinkResult struct {
	Added      int
	Dangling   int
	Cyclic     int
	Duplicates int
}

// reachesQuery checks whether walking causal edges (causes + links) from start
// reaches want. Link rows inserted earlier in the same transaction are
// included.
const reachesQuery = `
WITH RECURSIVE edges(parent, child) AS (
	SELECT le.event_id, te.event_id
	FROM ledger_events le, json_each(le.causes) AS je
	JOIN ledger_events te ON te.event_id = je.value
	UNION
	SELECT event_id, target_id FROM ledger_causal_links
),
reach(node) AS (
	SELECT ?
	UNION
	SELECT child FROM edges JOIN reach ON edges.parent = reach.node
)
SELECT 1 FROM reach WHERE node = ? LIMIT 1
`

// ResolveCausalLinks resolves all pending causal references, inserts link rows
// for resolved targets and rejects edges that would create a cycle.
func (d *DB) ResolveCausalLinks() (LinkResult, error) {
	var result LinkResult

	rows, err := d.sql.Query(pendingLinkQuery)
	if err != nil {
		return result, fmt.Errorf("query pending links: %w", err)
	}
	candidates := map[string]candidateLink{}
	var order []string
	for rows.Next() {
		var c candidateLink
		var targetID sql.NullString
		var targetSeq sql.NullInt64
		if err := rows.Scan(&c.eventID, &c.refIndex, &targetID, &targetSeq); err != nil {
			_ = rows.Close()
			return result, err
		}
		key := linkKey(c.eventID, c.refIndex)
		if existing, ok := candidates[key]; ok {
			result.Duplicates++
			// A wa_key may match multiple events (same key, re-encoded);
			// keep the earliest target.
			if targetSeq.Valid && (existing.targetID == "" || targetSeq.Int64 < existing.seq) {
				c.targetID = targetID.String
				c.seq = targetSeq.Int64
				candidates[key] = c
			}
			continue
		}
		if !targetID.Valid {
			c.seq = 0
			candidates[key] = c
			order = append(order, key)
			result.Dangling++
			continue
		}
		c.targetID = targetID.String
		c.seq = targetSeq.Int64
		candidates[key] = c
		order = append(order, key)
	}
	if err := rows.Close(); err != nil {
		return result, err
	}
	if err := rows.Err(); err != nil {
		return result, err
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return result, err
	}
	defer rollbackIfActive(tx)

	for _, key := range order {
		c := candidates[key]
		if c.targetID == "" {
			continue // dangling
		}
		var hit int
		err := tx.QueryRow(reachesQuery, c.targetID, c.eventID).Scan(&hit)
		if errors.Is(err, sql.ErrNoRows) {
			// No cycle.
		} else if err != nil {
			return result, fmt.Errorf("cycle check: %w", err)
		} else {
			result.Cyclic++
			continue
		}
		if _, err := tx.Exec(
			`INSERT INTO ledger_causal_links(event_id, ref_index, target_id) VALUES (?, ?, ?)`,
			c.eventID, c.refIndex, c.targetID,
		); err != nil {
			return result, fmt.Errorf("insert causal link: %w", err)
		}
		result.Added++
	}

	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

func linkKey(eventID string, refIndex int) string {
	return fmt.Sprintf("%s#%d", eventID, refIndex)
}

// RefStatus describes one causal reference that has no recorded link row.
type RefStatus struct {
	EventID  string `json:"event_id"`
	RefIndex int    `json:"ref_index"`
	// State is "dangling" (no target event), "cyclic" (target exists but the
	// edge would close a cycle) or "pending" (target exists; a link pass will
	// record the edge).
	State      string `json:"state"`
	Kind       string `json:"kind"`
	ChatJID    string `json:"chat_jid,omitempty"`
	MsgID      string `json:"msg_id,omitempty"`
	SenderJID  string `json:"sender_jid,omitempty"`
	FromMe     bool   `json:"from_me,omitempty"`
	RefEventID string `json:"ref_event_id,omitempty"`
}

// unresolvedRefQuery selects refs without link rows, ref metadata and the
// earliest matching target. WAKey construction matches ledger.WAKey.
const unresolvedRefQuery = `
SELECT
	e.event_id,
	CAST(je.key AS INTEGER),
	COALESCE(json_extract(je.value, '$.kind'), ''),
	COALESCE(json_extract(je.value, '$.chat_jid'), ''),
	COALESCE(json_extract(je.value, '$.msg_id'), ''),
	COALESCE(json_extract(je.value, '$.sender'), ''),
	COALESCE(json_extract(je.value, '$.from_me'), 0),
	COALESCE(json_extract(je.value, '$.event_id'), ''),
	target.event_id,
	target.seq
FROM ledger_events e, json_each(e.causal_refs) je
LEFT JOIN ledger_events target ON (
	(
		json_extract(je.value, '$.kind') = 'event_id'
		AND target.event_id = COALESCE(json_extract(je.value, '$.event_id'), '')
	)
	OR
	(
		json_extract(je.value, '$.kind') = 'wa_key'
		AND target.wa_key = CASE
			WHEN COALESCE(json_extract(je.value, '$.sender'), '') = ''
			THEN json_object(
				'chat', COALESCE(json_extract(je.value, '$.chat_jid'), ''),
				'from_me', CASE WHEN COALESCE(json_extract(je.value, '$.from_me'), 0) = 1
					THEN json('true') ELSE json('false') END,
				'id', COALESCE(json_extract(je.value, '$.msg_id'), '')
			)
			ELSE json_object(
				'chat', COALESCE(json_extract(je.value, '$.chat_jid'), ''),
				'from_me', CASE WHEN COALESCE(json_extract(je.value, '$.from_me'), 0) = 1
					THEN json('true') ELSE json('false') END,
				'id', COALESCE(json_extract(je.value, '$.msg_id'), ''),
				'participant', COALESCE(json_extract(je.value, '$.sender'), '')
			)
		END
	)
)
WHERE NOT EXISTS (
	SELECT 1 FROM ledger_causal_links lk
	WHERE lk.event_id = e.event_id AND lk.ref_index = CAST(je.key AS INTEGER)
)
ORDER BY e.event_id, je.key, target.seq
`

// EnumerateUnresolvedRefs returns all causal references without a link row,
// classified as dangling, cyclic or pending.
func (d *DB) EnumerateUnresolvedRefs() ([]RefStatus, error) {
	rows, err := d.sql.Query(unresolvedRefQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statuses []RefStatus
	seen := map[string]bool{}
	for rows.Next() {
		var rs RefStatus
		var fromMe int
		var targetID sql.NullString
		var targetSeq sql.NullInt64
		if err := rows.Scan(&rs.EventID, &rs.RefIndex, &rs.Kind, &rs.ChatJID,
			&rs.MsgID, &rs.SenderJID, &fromMe, &rs.RefEventID, &targetID, &targetSeq); err != nil {
			return nil, err
		}
		rs.FromMe = fromMe == 1
		key := linkKey(rs.EventID, rs.RefIndex)
		if seen[key] {
			// Multiple target matches: earliest target wins. Targets are not
			// ordered in this query, so leave classification to the ordered
			// candidate pass; dangling duplicates are impossible here.
			continue
		}
		seen[key] = true
		switch {
		case !targetID.Valid:
			rs.State = "dangling"
		default:
			var hit int
			err := d.sql.QueryRow(reachesQuery, targetID.String, rs.EventID).Scan(&hit)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				rs.State = "pending"
			case err != nil:
				return nil, fmt.Errorf("cycle check: %w", err)
			default:
				rs.State = "cyclic"
			}
		}
		statuses = append(statuses, rs)
	}
	return statuses, rows.Err()
}

// EnumerateDangling returns references that cannot be linked: dangling (no
// target) or cyclic (edge would close a cycle).
func (d *DB) EnumerateDangling() ([]RefStatus, error) {
	statuses, err := d.EnumerateUnresolvedRefs()
	if err != nil {
		return nil, err
	}
	var dangling []RefStatus
	for _, rs := range statuses {
		if rs.State == "dangling" || rs.State == "cyclic" {
			dangling = append(dangling, rs)
		}
	}
	return dangling, nil
}

// AuditIssue describes one integrity problem found by AuditLedger.
type AuditIssue struct {
	EventID string `json:"event_id"`
	Code    string `json:"code"`
	Detail  string `json:"detail"`
}

// AuditLedger validates required columns, ledger_raw consistency and, for
// events with retained raw bytes, recomputes the raw hash.
func (d *DB) AuditLedger() ([]AuditIssue, error) {
	var issues []AuditIssue

	rows, err := d.sql.Query(`
	SELECT e.event_id, e.source, e.raw_hash, e.parser_version, e.rules_version,
		e.dedup_key, e.received_at, r.encoding, r.raw_bytes
	FROM ledger_events e
	LEFT JOIN ledger_raw r ON r.event_id = e.event_id
	ORDER BY e.seq
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var eventID, source, rawHash, parserVer, rulesVer, dedupKey string
		var receivedAt int64
		var encoding sql.NullString
		var rawBytes []byte
		if err := rows.Scan(&eventID, &source, &rawHash, &parserVer, &rulesVer,
			&dedupKey, &receivedAt, &encoding, &rawBytes); err != nil {
			return nil, err
		}
		add := func(code, detail string) {
			issues = append(issues, AuditIssue{EventID: eventID, Code: code, Detail: detail})
		}
		if len(rawHash) != 64 {
			add("raw_hash_length", fmt.Sprintf("length=%d", len(rawHash)))
		} else if _, err := hex.DecodeString(rawHash); err != nil {
			add("raw_hash_hex", err.Error())
		}
		if parserVer == "" {
			add("parser_version", "empty")
		}
		if rulesVer == "" {
			add("rules_version", "empty")
		}
		if dedupKey == "" {
			add("dedup_key", "empty")
		}
		if receivedAt == 0 {
			add("received_at", "zero")
		}
		if encoding.Valid {
			sum := sha256.Sum256(rawBytes)
			if got := hex.EncodeToString(sum[:]); got != rawHash {
				add("raw_hash_mismatch", fmt.Sprintf("recomputed=%s", got))
			}
		} else if source != "legacy_snapshot" && source != "scrub" {
			add("raw_missing", "envelope bytes not retained")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return issues, nil
}
