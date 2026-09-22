package projector

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/openclaw/wacli/internal/store"
)

// legacySnapshotPayload is the bootstrap payload shape:
// internal/store/ledger_bootstrap.go marshals {"table", "row"}.
type legacySnapshotPayload struct {
	Table string         `json:"table"`
	Row   map[string]any `json:"row"`
}

func (v *MessagesView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	var snap legacySnapshotPayload
	if err := json.Unmarshal(evt.Snapshot, &snap); err != nil {
		return fmt.Errorf("unmarshal legacy snapshot: %w", err)
	}
	switch snap.Table {
	case store.MessagesTable:
		return v.insertLegacyMessage(ctx, tx, snap.Row)
	case store.MessageLocationsTable:
		return v.insertLegacyLocation(ctx, tx, snap.Row)
	case store.MessagePayloadPurges:
		return v.insertLegacyPurge(ctx, tx, snap.Row)
	default:
		// Snapshots of other tables are projected by their owning views.
		return nil
	}
}

// legacyMessageColumns mirrors internal/store/schema.sql messages (excluding
// rowid). Order is fixed and shared by INSERT and ON CONFLICT.
var legacyMessageColumns = []string{
	"chat_jid", "chat_name", "msg_id", "sender_jid", "sender_name", "ts", "from_me",
	"text", "display_text", "quoted_msg_id", "quoted_sender_jid",
	"is_forwarded", "forwarding_score", "reaction_to_id", "reaction_emoji",
	"media_type", "media_caption", "filename", "mime_type", "direct_path",
	"media_key", "file_sha256", "file_enc_sha256", "file_length",
	"local_path", "downloaded_at", "media_unavailable_at",
	"revoked", "deleted_for_me", "deleted_at", "deletion_reason",
	"payload_purged_at", "edited", "edited_ts", "buttons",
}

// nullable/integer/blob classification for each column.
var legacyMessageTextCols = map[string]bool{
	"chat_name": true, "sender_jid": true, "sender_name": true,
	"text": true, "display_text": true, "quoted_msg_id": true,
	"quoted_sender_jid": true, "reaction_to_id": true, "reaction_emoji": true,
	"media_type": true, "media_caption": true, "filename": true,
	"mime_type": true, "direct_path": true, "local_path": true,
	"deletion_reason": true, "buttons": true,
}

var legacyMessageBlobCols = map[string]bool{
	"media_key": true, "file_sha256": true, "file_enc_sha256": true,
}

var legacyMessageIntCols = map[string]bool{
	"ts": true, "from_me": true, "is_forwarded": true, "forwarding_score": true,
	"file_length": true, "downloaded_at": true, "media_unavailable_at": true,
	"revoked": true, "deleted_for_me": true, "deleted_at": true,
	"payload_purged_at": true, "edited": true, "edited_ts": true,
}

func (v *MessagesView) insertLegacyMessage(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	args := make([]any, len(legacyMessageColumns))
	for i, col := range legacyMessageColumns {
		switch {
		case legacyMessageTextCols[col]:
			args[i] = rowNullString(row, col)
		case legacyMessageBlobCols[col]:
			args[i] = rowBlob(row, col)
		case legacyMessageIntCols[col]:
			args[i] = rowNullInt64(row, col)
		default:
			args[i] = rowString(row, col)
		}
	}

	query := `INSERT INTO ` + v.msgTable() + ` (` + joinCols(legacyMessageColumns) + `) VALUES (` + placeholders(len(legacyMessageColumns)) + `)
		ON CONFLICT(chat_jid, msg_id) DO UPDATE SET ` + legacyMessageUpsertAssignments
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert legacy message: %w", err)
	}
	if v.withFTS {
		chatJID := rowString(row, "chat_jid")
		msgID := rowString(row, "msg_id")
		return v.syncFTS(ctx, tx, chatJID, msgID)
	}
	return nil
}

// legacyMessageUpsertAssignments replaces every non-key column on conflict, so
// re-projecting a snapshot restores the exact bootstrapped final state.
const legacyMessageUpsertAssignments = `chat_name = excluded.chat_name,
		sender_jid = excluded.sender_jid, sender_name = excluded.sender_name,
		ts = excluded.ts, from_me = excluded.from_me, text = excluded.text,
		display_text = excluded.display_text, quoted_msg_id = excluded.quoted_msg_id,
		quoted_sender_jid = excluded.quoted_sender_jid,
		is_forwarded = excluded.is_forwarded, forwarding_score = excluded.forwarding_score,
		reaction_to_id = excluded.reaction_to_id, reaction_emoji = excluded.reaction_emoji,
		media_type = excluded.media_type, media_caption = excluded.media_caption,
		filename = excluded.filename, mime_type = excluded.mime_type,
		direct_path = excluded.direct_path, media_key = excluded.media_key,
		file_sha256 = excluded.file_sha256, file_enc_sha256 = excluded.file_enc_sha256,
		file_length = excluded.file_length, local_path = excluded.local_path,
		downloaded_at = excluded.downloaded_at,
		media_unavailable_at = excluded.media_unavailable_at,
		revoked = excluded.revoked, deleted_for_me = excluded.deleted_for_me,
		deleted_at = excluded.deleted_at, deletion_reason = excluded.deletion_reason,
		payload_purged_at = excluded.payload_purged_at,
		edited = excluded.edited, edited_ts = excluded.edited_ts,
		buttons = excluded.buttons`

func (v *MessagesView) insertLegacyLocation(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	cols := []string{"chat_jid", "msg_id", "latitude", "longitude", "name", "address", "is_live"}
	args := []any{
		rowString(row, "chat_jid"),
		rowString(row, "msg_id"),
		rowFloat(row, "latitude"),
		rowFloat(row, "longitude"),
		rowNullString(row, "name"),
		rowNullString(row, "address"),
		coalesceInt64(row, "is_live"),
	}
	query := `INSERT INTO ` + v.locTable() + ` (` + joinCols(cols) + `)
		VALUES (` + placeholders(len(cols)) + `)
		ON CONFLICT(chat_jid, msg_id) DO UPDATE SET
			latitude = excluded.latitude, longitude = excluded.longitude,
			name = excluded.name, address = excluded.address,
			is_live = excluded.is_live`
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert legacy location: %w", err)
	}
	return nil
}

func (v *MessagesView) insertLegacyPurge(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	cols := []string{"chat_jid", "msg_id", "purged_at", "deleted_at", "deletion_reason"}
	args := []any{
		rowString(row, "chat_jid"),
		rowString(row, "msg_id"),
		coalesceInt64(row, "purged_at"),
		coalesceInt64(row, "deleted_at"),
		rowString(row, "deletion_reason"),
	}
	table := store.MessagePayloadPurges + v.suffix
	query := `INSERT INTO ` + table + ` (` + joinCols(cols) + `)
		VALUES (` + placeholders(len(cols)) + `)
		ON CONFLICT(chat_jid, msg_id) DO UPDATE SET
			purged_at = excluded.purged_at,
			deleted_at = excluded.deleted_at,
			deletion_reason = excluded.deletion_reason`
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert legacy purge: %w", err)
	}
	return nil
}

// --- row map converters -----------------------------------------------------

func rowString(row map[string]any, col string) string {
	if s, ok := row[col].(string); ok {
		return s
	}
	return ""
}

func rowNullString(row map[string]any, col string) sql.NullString {
	if s, ok := row[col].(string); ok && s != "" {
		return sql.NullString{String: s, Valid: true}
	}
	return sql.NullString{}
}

func coalesceInt64(row map[string]any, col string) int64 {
	if v, ok := rowInt64OK(row, col); ok {
		return v
	}
	return 0
}

func rowNullInt64(row map[string]any, col string) sql.NullInt64 {
	if v, ok := rowInt64OK(row, col); ok {
		return sql.NullInt64{Int64: v, Valid: true}
	}
	return sql.NullInt64{}
}

func rowInt64OK(row map[string]any, col string) (int64, bool) {
	switch n := row[col].(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case json.Number:
		if v, err := n.Int64(); err == nil {
			return v, true
		}
	}
	return 0, false
}

func rowFloat(row map[string]any, col string) float64 {
	if n, ok := row[col].(float64); ok {
		return n
	}
	return 0
}

func rowBlob(row map[string]any, col string) []byte {
	s, ok := row[col].(string)
	if !ok || s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

func joinCols(cols []string) string {
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += c
	}
	return out
}

func placeholders(n int) string {
	out := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			out += ", "
		}
		out += "?"
	}
	return out
}
