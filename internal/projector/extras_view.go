package projector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/store/storedb"
	"github.com/openclaw/wacli/internal/wa"
)

// --- StarredView -------------------------------------------------------------

// StarredViewVersion is bumped when the star projection semantics change.
const StarredViewVersion = "starred-projector/1.0.0"

// StarredView projects star folds into the starred table.
type StarredView struct {
	db     *store.DB
	suffix string
}

// NewStarredView constructs the view.
func NewStarredView(db *store.DB, suffix string) *StarredView {
	return &StarredView{db: db, suffix: suffix}
}

// Name is the checkpoint key.
func (v *StarredView) Name() string { return store.StarredTable + v.suffix }

// Version identifies the projection semantics.
func (v *StarredView) Version() string { return StarredViewVersion }

func (v *StarredView) table() string { return store.StarredTable + v.suffix }

// Apply projects one checkpoint batch.
func (v *StarredView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventStar:
			err = v.applyStar(ctx, tx, evt)
		}
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", evt.Seq, evt.EventType, err)
		}
	}
	return nil
}

func (v *StarredView) applyStar(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read star: %w", err)
	}
	var c ledger.CanonicalStar
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("unmarshal star: %w", err)
	}
	chatJID := wa.MapJID(nil, c.ChatJID)
	senderJID := wa.MapJID(nil, c.SenderJID)
	if !c.Starred {
		query := store.RetargetTable(storedb.SetStarredDeleteSQL, store.StarredTable, v.table())
		_, err = tx.ExecContext(ctx, query, chatJID, c.MsgID)
		return err
	}
	query := store.RetargetTable(storedb.SetStarredUpsertSQL, store.StarredTable, v.table())
	_, err = tx.ExecContext(ctx, query,
		chatJID, c.MsgID,
		sql.NullString{String: senderJID, Valid: senderJID != ""},
		boolInt(c.FromMe), c.StarredTS)
	return err
}

func (v *StarredView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	cols := []string{"chat_jid", "msg_id", "sender_jid", "from_me", "starred_at"}
	row, err := legacyRowFor(evt, store.StarredTable)
	if err != nil || row == nil {
		return err
	}
	args := []any{
		rowString(row, "chat_jid"),
		rowString(row, "msg_id"),
		rowNullString(row, "sender_jid"),
		coalesceInt64(row, "from_me"),
		coalesceInt64(row, "starred_at"),
	}
	return insertLegacyRows(ctx, tx, v.table(), "chat_jid, msg_id", cols, args)
}

// --- StatusMessagesView ------------------------------------------------------

// StatusMessagesViewVersion is bumped when the status projection changes.
const StatusMessagesViewVersion = "status-messages-projector/1.0.0"

// StatusMessagesView projects status broadcast messages.
type StatusMessagesView struct {
	db     *store.DB
	suffix string
}

// NewStatusMessagesView constructs the view.
func NewStatusMessagesView(db *store.DB, suffix string) *StatusMessagesView {
	return &StatusMessagesView{db: db, suffix: suffix}
}

// Name is the checkpoint key.
func (v *StatusMessagesView) Name() string { return store.StatusMessagesTable + v.suffix }

// Version identifies the projection semantics.
func (v *StatusMessagesView) Version() string { return StatusMessagesViewVersion }

func (v *StatusMessagesView) table() string { return store.StatusMessagesTable + v.suffix }

// Apply projects one checkpoint batch.
func (v *StatusMessagesView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventStatusMessage:
			err = v.applyStatus(ctx, tx, evt)
		}
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", evt.Seq, evt.EventType, err)
		}
	}
	return nil
}

func (v *StatusMessagesView) applyStatus(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read status: %w", err)
	}
	var c ledger.CanonicalStatusMessage
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("unmarshal status: %w", err)
	}
	p := store.UpsertStatusMessageParams{
		MsgID:           c.MsgID,
		Timestamp:       time.Unix(c.TS, 0).UTC(),
		FromMe:          c.FromMe,
		SenderJID:       wa.MapJID(nil, c.SenderJID),
		SenderName:      c.SenderName,
		Text:            c.Text,
		MediaType:       c.MediaType,
		MediaCaption:    c.MediaCaption,
		Filename:        c.Filename,
		MimeType:        c.MimeType,
		DirectPath:      c.DirectPath,
		MediaKey:        c.MediaKey,
		FileSHA256:      c.FileSHA256,
		FileEncSHA256:   c.FileEncSHA256,
		FileLength:      uint64(c.FileLength),
		BackgroundColor: c.BackgroundColor,
		Font:            c.Font,
	}
	return v.db.UpsertStatusMessageTarget(ctx, tx, v.table(), p)
}

func (v *StatusMessagesView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	cols := []string{
		"msg_id", "ts", "from_me", "sender_jid", "sender_name", "text",
		"media_type", "media_caption", "filename", "mime_type", "direct_path",
		"media_key", "file_sha256", "file_enc_sha256", "file_length",
		"background_color", "font",
	}
	row, err := legacyRowFor(evt, store.StatusMessagesTable)
	if err != nil || row == nil {
		return err
	}
	args := []any{
		rowString(row, "msg_id"),
		rowNullInt64(row, "ts"),
		coalesceInt64(row, "from_me"),
		rowNullString(row, "sender_jid"),
		rowNullString(row, "sender_name"),
		rowNullString(row, "text"),
		rowNullString(row, "media_type"),
		rowNullString(row, "media_caption"),
		rowNullString(row, "filename"),
		rowNullString(row, "mime_type"),
		rowNullString(row, "direct_path"),
		rowBlob(row, "media_key"),
		rowBlob(row, "file_sha256"),
		rowBlob(row, "file_enc_sha256"),
		rowNullInt64(row, "file_length"),
		rowNullString(row, "background_color"),
		rowNullInt64(row, "font"),
	}
	return insertLegacyRows(ctx, tx, v.table(), "msg_id", cols, args)
}

// --- CallEventsView ----------------------------------------------------------

// CallEventsViewVersion is bumped when the call projection semantics change.
const CallEventsViewVersion = "call-events-projector/1.0.0"

// CallEventsView projects call events and call-log deletes.
type CallEventsView struct {
	db     *store.DB
	suffix string
}

// NewCallEventsView constructs the view.
func NewCallEventsView(db *store.DB, suffix string) *CallEventsView {
	return &CallEventsView{db: db, suffix: suffix}
}

// Name is the checkpoint key.
func (v *CallEventsView) Name() string { return store.CallEventsTable + v.suffix }

// Version identifies the projection semantics.
func (v *CallEventsView) Version() string { return CallEventsViewVersion }

func (v *CallEventsView) table() string { return store.CallEventsTable + v.suffix }

// Apply projects one checkpoint batch.
func (v *CallEventsView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventCall:
			err = v.applyCall(ctx, tx, evt)
		}
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", evt.Seq, evt.EventType, err)
		}
	}
	return nil
}

func (v *CallEventsView) applyCall(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read call: %w", err)
	}
	var c ledger.CanonicalCallEvent
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("unmarshal call: %w", err)
	}
	if c.Deleted {
		_, err = v.db.DeleteCallEventsTarget(ctx, tx, v.table(), store.DeleteCallEventsParams{
			ChatJID:   wa.MapJID(nil, c.ChatJID),
			Direction: c.Direction,
		})
		return err
	}
	participants := make([]store.CallParticipant, 0, len(c.Participants))
	for _, p := range c.Participants {
		participants = append(participants, store.CallParticipant{
			JID:     wa.MapJID(nil, p.JID),
			Outcome: p.Outcome,
		})
	}
	params := store.UpsertCallEventParams{
		ChatJID:      wa.MapJID(nil, c.ChatJID),
		ChatName:     c.ChatName,
		SenderJID:    wa.MapJID(nil, c.SenderJID),
		SenderName:   c.SenderName,
		CallID:       c.CallID,
		MsgID:        c.MsgID,
		EventType:    c.EventType,
		Direction:    c.Direction,
		Media:        c.Media,
		Outcome:      c.Outcome,
		Reason:       c.Reason,
		CallType:     c.CallType,
		DurationSecs: c.DurationSecs,
		Timestamp:    time.Unix(c.TS, 0).UTC(),
		Participants: participants,
	}
	return v.db.UpsertCallEventTarget(ctx, tx, v.table(), params)
}

func (v *CallEventsView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	cols := []string{
		"chat_jid", "chat_name", "sender_jid", "sender_name", "call_id",
		"msg_id", "event_type", "direction", "media", "outcome", "reason",
		"call_type", "duration_secs", "ts", "participants",
	}
	row, err := legacyRowFor(evt, store.CallEventsTable)
	if err != nil || row == nil {
		return err
	}
	args := []any{
		rowString(row, "chat_jid"),
		rowNullString(row, "chat_name"),
		rowNullString(row, "sender_jid"),
		rowNullString(row, "sender_name"),
		rowString(row, "call_id"),
		rowNullString(row, "msg_id"),
		rowString(row, "event_type"),
		rowNullString(row, "direction"),
		rowNullString(row, "media"),
		rowNullString(row, "outcome"),
		rowNullString(row, "reason"),
		rowNullString(row, "call_type"),
		coalesceInt64(row, "duration_secs"),
		rowNullInt64(row, "ts"),
		rowNullString(row, "participants"),
	}
	return insertLegacyRows(ctx, tx, v.table(), "chat_jid, call_id, event_type, ts", cols, args)
}

// --- shared legacy helpers ---------------------------------------------------

func legacyRowFor(evt *store.StoredEvent, table string) (map[string]any, error) {
	var snap legacySnapshotPayload
	if err := json.Unmarshal(evt.Snapshot, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal legacy snapshot: %w", err)
	}
	if snap.Table != table {
		return nil, nil
	}
	return snap.Row, nil
}

func insertLegacyRows(ctx context.Context, tx *sql.Tx, table, conflictCols string, cols []string, args []any) error {
	sets := make([]string, len(cols))
	for i, c := range cols {
		sets[i] = c + " = excluded." + c
	}
	conflict := ""
	if conflictCols != "" {
		conflict = fmt.Sprintf(`
		ON CONFLICT(%s) DO UPDATE SET %s`,
			conflictCols, strings.Join(sets, ", "))
	}
	query := `INSERT INTO ` + table + ` (` + joinCols(cols) + `)
		VALUES (` + placeholders(len(cols)) + `)` + conflict
	_, err := tx.ExecContext(ctx, query, args...)
	return err
}
