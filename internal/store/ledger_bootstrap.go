package store

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
)

// snapshotTable describes one legacy table captured during bootstrap.
type snapshotTable struct {
	table     string
	eventType string
	orderBy   string
	keyCols   []string
	// waKeyFromRow reports whether a WhatsApp key is derived from the row.
	waKeyFromRow bool
}

// snapshotTables covers every protocol-derived materialized table. Operational
// tables (locks, recovery intents, aliases) are intentionally excluded.
var snapshotTables = []snapshotTable{
	{table: "messages", eventType: ledger.EventSnapshotMessages, orderBy: "chat_jid, msg_id",
		keyCols: []string{"chat_jid", "msg_id"}, waKeyFromRow: true},
	{table: "message_locations", eventType: ledger.EventSnapshotLocations, orderBy: "chat_jid, msg_id",
		keyCols: []string{"chat_jid", "msg_id"}},
	{table: "message_payload_purges", eventType: ledger.EventSnapshotPurges, orderBy: "chat_jid, msg_id",
		keyCols: []string{"chat_jid", "msg_id"}},
	{table: "chats", eventType: ledger.EventSnapshotChats, orderBy: "jid",
		keyCols: []string{"jid"}},
	{table: "contacts", eventType: ledger.EventSnapshotContacts, orderBy: "jid",
		keyCols: []string{"jid"}},
	{table: "groups", eventType: ledger.EventSnapshotGroups, orderBy: "jid",
		keyCols: []string{"jid"}},
	{table: "group_participants", eventType: ledger.EventSnapshotRoster, orderBy: "group_jid, user_jid",
		keyCols: []string{"group_jid", "user_jid"}},
	{table: "starred", eventType: ledger.EventSnapshotStarred, orderBy: "chat_jid, msg_id",
		keyCols: []string{"chat_jid", "msg_id"}},
	{table: "call_events", eventType: ledger.EventSnapshotCalls, orderBy: "rowid",
		keyCols: []string{"chat_jid", "call_id", "event_type", "ts"}},
	{table: "status_messages", eventType: ledger.EventSnapshotStatus, orderBy: "msg_id",
		keyCols: []string{"msg_id"}},
	{table: "polls", eventType: ledger.EventSnapshotPolls, orderBy: "chat_jid, msg_id",
		keyCols: []string{"chat_jid", "msg_id"}},
	{table: "poll_votes", eventType: ledger.EventSnapshotPollVotes, orderBy: "chat_jid, poll_msg_id, voter_jid",
		keyCols: []string{"chat_jid", "poll_msg_id", "voter_jid"}},
}

// TableBootstrap summarises one table's bootstrap result.
type TableBootstrap struct {
	Appended int `json:"appended"`
	Skipped  int `json:"skipped"`
}

// BootstrapReport summarises a legacy snapshot bootstrap run.
type BootstrapReport struct {
	BatchID  int64                     `json:"batch_id"`
	PerTable map[string]TableBootstrap `json:"per_table"`
}

// BootstrapLegacySnapshots appends one legacy_snapshot event per existing row
// in every protocol-derived table. Rows are ordered by stable keys, no raw
// bytes are retained (the hash covers the canonical row encoding) and no
// projector checkpoints are written. The pass is reentrant: rerunning after an
// interruption skips events that already exist.
func (d *DB) BootstrapLegacySnapshots() (BootstrapReport, error) {
	batchID, err := d.StartBatch(ledger.SourceLegacySnapshot)
	if err != nil {
		return BootstrapReport{}, fmt.Errorf("start bootstrap batch: %w", err)
	}
	report := BootstrapReport{BatchID: batchID, PerTable: map[string]TableBootstrap{}}

	for _, cfg := range snapshotTables {
		tb, err := d.bootstrapTable(cfg, batchID)
		if err != nil {
			return report, fmt.Errorf("bootstrap %s: %w", cfg.table, err)
		}
		report.PerTable[cfg.table] = tb
	}
	return report, nil
}

func (d *DB) bootstrapTable(cfg snapshotTable, batchID int64) (TableBootstrap, error) {
	var tb TableBootstrap

	rows, err := d.sql.Query(
		"SELECT * FROM " + cfg.table + " ORDER BY " + cfg.orderBy,
	)
	if err != nil {
		return tb, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return tb, err
	}

	for rows.Next() {
		rawValues := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range rawValues {
			ptrs[i] = &rawValues[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return tb, err
		}

		rowMap := make(map[string]any, len(cols))
		for i, col := range cols {
			rowMap[col] = normalizeDriverValue(rawValues[i])
		}

		snapshotBody, err := json.Marshal(struct {
			Table string         `json:"table"`
			Row   map[string]any `json:"row"`
		}{Table: cfg.table, Row: rowMap})
		if err != nil {
			return tb, fmt.Errorf("marshal snapshot: %w", err)
		}
		sum := sha256.Sum256(snapshotBody)
		rawHash := hex.EncodeToString(sum[:])

		keyParts := make([]string, 0, len(cfg.keyCols))
		for _, keyCol := range cfg.keyCols {
			keyParts = append(keyParts, fmt.Sprintf("%v", rowMap[keyCol]))
		}
		dk := ledger.DedupKey("legacy", append([]string{cfg.table}, keyParts...)...)

		chatJID, _ := rowMap["chat_jid"].(string)
		msgID, _ := rowMap["msg_id"].(string)
		waKey := ""
		senderJID := ""
		fromMe := false
		if cfg.waKeyFromRow {
			fromMe, _ = rowMap["from_me"].(bool)
			senderJID, _ = rowMap["sender_jid"].(string)
			participant := legacyParticipant(chatJID, senderJID, fromMe)
			waKey = ledger.WAKey(chatJID, fromMe, msgID, participant)
		}

		eventTS := legacyEventTS(rowMap["ts"])
		evt := &ledger.Event{
			EventID:       ledger.DeriveEventID(dk, rawHash),
			Source:        ledger.SourceLegacySnapshot,
			EventType:     cfg.eventType,
			WAKey:         waKey,
			ChatJID:       chatJID,
			MsgID:         msgID,
			SenderJID:     senderJID,
			ServerTS:      eventTS,
			EventTS:       eventTS,
			ReceivedAt:    time.Now().Unix(),
			RawHash:       rawHash,
			ParserVersion: ledger.ParserVersion,
			RulesVersion:  ledger.RulesVersion,
			DedupKey:      dk,
			BatchID:       batchID,
			Snapshot:      snapshotBody,
		}
		_, inserted, err := d.AppendEvent(evt)
		if err != nil {
			return tb, err
		}
		if inserted {
			tb.Appended++
		} else {
			tb.Skipped++
		}
	}
	return tb, rows.Err()
}

// normalizeDriverValue renders driver values in a deterministic, JSON-safe
// form. Blobs (BLOB columns) are base64 so bytes survive JSON encoding.
func normalizeDriverValue(v any) any {
	switch val := v.(type) {
	case nil:
		return nil
	case []byte:
		return base64.StdEncoding.EncodeToString(val)
	default:
		return val
	}
}

// legacyParticipant mirrors the participant rule of live ingestion: the group
// sender is recorded only when it differs from the chat and the message is not
// from the current user.
func legacyParticipant(chatJID, senderJID string, fromMe bool) string {
	if fromMe || senderJID == "" || senderJID == chatJID {
		return ""
	}
	return senderJID
}

func legacyEventTS(ts any) int64 {
	switch val := ts.(type) {
	case int64:
		return val
	case float64:
		return int64(val)
	case string:
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}
