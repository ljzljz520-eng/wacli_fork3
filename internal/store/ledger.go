package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/openclaw/wacli/internal/ledger"
)

// StoredEvent is a ledger event row read back from the database.
type StoredEvent struct {
	Seq           int64
	EventID       string
	Source        string
	EventType     string
	WAKey         string
	ChatJID       string
	MsgID         string
	SenderJID     string
	ServerTS      int64
	EventTS       int64
	ReceivedAt    int64
	RawHash       string
	ParserVersion string
	RulesVersion  string
	DedupKey      string
	CausalRefs    json.RawMessage
	Causes        json.RawMessage
	BatchID       int64
	Flags         int64
	Snapshot      []byte
}

func scanStoredEvent(row interface {
	Scan(...any) error
}) (*StoredEvent, error) {
	var evt StoredEvent
	var refsStr, causesStr string
	if err := row.Scan(
		&evt.Seq, &evt.EventID, &evt.Source, &evt.EventType, &evt.WAKey,
		&evt.ChatJID, &evt.MsgID, &evt.SenderJID, &evt.ServerTS, &evt.EventTS,
		&evt.ReceivedAt, &evt.RawHash, &evt.ParserVersion, &evt.RulesVersion,
		&evt.DedupKey, &refsStr, &causesStr, &evt.BatchID, &evt.Flags,
		&evt.Snapshot,
	); err != nil {
		return nil, err
	}
	evt.CausalRefs = json.RawMessage(refsStr)
	evt.Causes = json.RawMessage(causesStr)
	return &evt, nil
}

const storedEventColumns = `seq, event_id, source, event_type, wa_key,
	chat_jid, msg_id, sender_jid, server_ts, event_ts, received_at, raw_hash,
	parser_version, rules_version, dedup_key, causal_refs, causes, batch_id,
	flags, snapshot`

// ErrLedgerValidation is returned when an event fails append-time validation.
var ErrLedgerValidation = errors.New("ledger validation failed")

// rollbackIfActive is a deferred-helper; Rollback after a successful Commit
// is a harmless no-op.
func rollbackIfActive(tx *sql.Tx) { _ = tx.Rollback() }

// AppendEvent appends an event and its raw envelope in a single transaction.
// If an identical (dedup_key, raw_hash) event already exists, it is a no-op:
// the existing sequence is returned with inserted=false. Causal references
// are resolved forward to events already present; unresolved refs remain
// dangling for the link pass.
func (d *DB) AppendEvent(evt *ledger.Event) (seq int64, inserted bool, err error) {
	if err := validateNewEvent(evt); err != nil {
		return 0, false, err
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return 0, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	seq, inserted, err = appendEventTx(tx, evt)
	if err != nil {
		return 0, false, err
	}
	if !inserted {
		if err = tx.Commit(); err != nil {
			return 0, false, err
		}
		return seq, false, nil
	}

	if err = tx.Commit(); err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

// AppendEventTx appends inside an existing transaction, allowing callers to
// batch multiple events per commit. Dedup pre-checks share the transaction.
func (d *DB) AppendEventTx(tx *sql.Tx, evt *ledger.Event) (int64, bool, error) {
	if err := validateNewEvent(evt); err != nil {
		return 0, false, err
	}
	return appendEventTx(tx, evt)
}

func appendEventTx(tx *sql.Tx, evt *ledger.Event) (seq int64, inserted bool, err error) {
	err = tx.QueryRow(
		`SELECT seq FROM ledger_events WHERE dedup_key = ? AND raw_hash = ?`,
		evt.DedupKey, evt.RawHash,
	).Scan(&seq)
	if err == nil {
		return seq, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}

	causes := evt.Causes
	if len(causes) == 0 {
		causes, err = resolveRefsForward(tx, evt.CausalRefs)
		if err != nil {
			return 0, false, err
		}
	}
	flags := evt.Flags
	if len(causes) < len(evt.CausalRefs) {
		flags |= ledger.FlagDangling
	}

	refsJSON, err := ledger.MarshalRefs(evt.CausalRefs)
	if err != nil {
		return 0, false, err
	}
	causesJSON, err := json.Marshal(causes)
	if err != nil {
		return 0, false, err
	}

	err = tx.QueryRow(`
		INSERT INTO ledger_events(
			event_id, source, event_type, wa_key, chat_jid, msg_id, sender_jid,
			server_ts, event_ts, received_at, raw_hash, parser_version,
			rules_version, dedup_key, causal_refs, causes, batch_id, flags, snapshot
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING seq`,
		evt.EventID, evt.Source, evt.EventType, evt.WAKey, evt.ChatJID,
		evt.MsgID, evt.SenderJID, evt.ServerTS, evt.EventTS, evt.ReceivedAt,
		evt.RawHash, evt.ParserVersion, evt.RulesVersion, evt.DedupKey,
		string(refsJSON), string(causesJSON), evt.BatchID, flags,
		string(evt.Snapshot),
	).Scan(&seq)
	if err != nil {
		return 0, false, err
	}

	if len(evt.RawBytes) > 0 {
		encoding := ledger.EncodingProto
		if evt.Source == ledger.SourceReceipt {
			encoding = ledger.EncodingJSON
		}
		if _, err = tx.Exec(
			`INSERT INTO ledger_raw(event_id, encoding, raw_bytes) VALUES (?, ?, ?)`,
			evt.EventID, encoding, evt.RawBytes,
		); err != nil {
			return 0, false, err
		}
	}

	return seq, true, nil
}

func validateNewEvent(evt *ledger.Event) error {
	switch {
	case evt == nil:
		return fmt.Errorf("%w: nil event", ErrLedgerValidation)
	case evt.EventID == "":
		return fmt.Errorf("%w: empty event_id", ErrLedgerValidation)
	case !ledger.IsKnownSource(evt.Source):
		return fmt.Errorf("%w: unknown source %q", ErrLedgerValidation, evt.Source)
	case evt.EventType == "":
		return fmt.Errorf("%w: empty event_type", ErrLedgerValidation)
	case evt.DedupKey == "":
		return fmt.Errorf("%w: empty dedup_key", ErrLedgerValidation)
	case len(evt.RawHash) != 64:
		return fmt.Errorf("%w: raw_hash must be 64 hex chars, got %d", ErrLedgerValidation, len(evt.RawHash))
	case evt.ParserVersion == "" || evt.RulesVersion == "":
		return fmt.Errorf("%w: parser/rules version required", ErrLedgerValidation)
	case evt.ReceivedAt == 0:
		return fmt.Errorf("%w: received_at required", ErrLedgerValidation)
	}
	if _, err := hex.DecodeString(evt.RawHash); err != nil {
		return fmt.Errorf("%w: raw_hash not hex: %v", ErrLedgerValidation, err)
	}
	return nil
}

func resolveRefsForward(tx *sql.Tx, refs []ledger.CausalRef) ([]string, error) {
	causes := make([]string, 0, len(refs))
	for _, ref := range refs {
		var target string
		var err error
		switch ref.Kind {
		case "event_id":
			err = tx.QueryRow(
				`SELECT event_id FROM ledger_events WHERE event_id = ? ORDER BY seq LIMIT 1`,
				ref.EventID,
			).Scan(&target)
		case "wa_key":
			lookup := ledger.WAKey(ref.ChatJID, ref.FromMe, ref.MsgID, ref.Sender)
			err = tx.QueryRow(
				`SELECT event_id FROM ledger_events WHERE wa_key = ? ORDER BY seq LIMIT 1`,
				lookup,
			).Scan(&target)
		default:
			return nil, fmt.Errorf("%w: unknown causal ref kind %q", ErrLedgerValidation, ref.Kind)
		}
		switch {
		case err == nil:
			causes = append(causes, target)
		case errors.Is(err, sql.ErrNoRows):
			// Dangling; resolved later by the link pass.
		default:
			return nil, err
		}
	}
	return causes, nil
}

// HeadSeq returns the highest event sequence, or 0 when the ledger is empty.
func (d *DB) HeadSeq() (int64, error) {
	var seq sql.NullInt64
	if err := d.sql.QueryRow(`SELECT MAX(seq) FROM ledger_events`).Scan(&seq); err != nil {
		return 0, err
	}
	return seq.Int64, nil
}

// EventIDByChatMsg resolves the most recent ledger event id for a chat/message
// pair. It returns "" without error when no ledger event exists for the pair
// (e.g. ledger disabled or a legacy pre-ledger row).
func (d *DB) EventIDByChatMsg(ctx context.Context, chatJID, msgID string) (string, error) {
	_, eventID, err := d.LookupByChatMsg(ctx, chatJID, msgID)
	return eventID, err
}

// LookupByChatMsg resolves the most recent ledger event (seq, id) for a
// chat/message pair. Missing pairs return (0,"",nil).
func (d *DB) LookupByChatMsg(ctx context.Context, chatJID, msgID string) (int64, string, error) {
	var seq int64
	var eventID sql.NullString
	err := d.sql.QueryRowContext(ctx,
		`SELECT seq, event_id FROM ledger_events
			WHERE chat_jid = ? AND msg_id = ?
			ORDER BY seq DESC LIMIT 1`,
		chatJID, msgID,
	).Scan(&seq, &eventID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	return seq, eventID.String, nil
}

// LookupByDedupKey resolves the ledger event (seq, id) for one dedup key.
// Missing keys return (0,"",nil).
func (d *DB) LookupByDedupKey(ctx context.Context, dedupKey string) (int64, string, error) {
	var seq int64
	var eventID sql.NullString
	err := d.sql.QueryRowContext(ctx,
		`SELECT seq, event_id FROM ledger_events WHERE dedup_key = ?
			ORDER BY seq DESC LIMIT 1`, dedupKey,
	).Scan(&seq, &eventID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	return seq, eventID.String, nil
}

// GetEventByID reads a single event by event_id.
func (d *DB) GetEventByID(eventID string) (*StoredEvent, error) {
	row := d.sql.QueryRow(
		`SELECT `+storedEventColumns+` FROM ledger_events WHERE event_id = ?`,
		eventID,
	)
	return scanStoredEvent(row)
}

// ScanEvents streams events with seq in (fromSeq, toSeq] in ascending order.
// Pass toSeq=0 to scan to the current head.
func (d *DB) ScanEvents(fromSeq, toSeq int64, fn func(*StoredEvent) error) error {
	query := `SELECT ` + storedEventColumns + ` FROM ledger_events WHERE seq > ?`
	args := []any{fromSeq}
	if toSeq > 0 {
		query += ` AND seq <= ?`
		args = append(args, toSeq)
	}
	query += ` ORDER BY seq`
	rows, err := d.sql.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		evt, err := scanStoredEvent(rows)
		if err != nil {
			return err
		}
		if err := fn(evt); err != nil {
			return err
		}
	}
	return rows.Err()
}

// StartBatch opens an ingestion batch and returns its ID.
func (d *DB) StartBatch(source string) (int64, error) {
	var batchID int64
	err := d.sql.QueryRow(
		`INSERT INTO ledger_batches(started_at, source) VALUES (strftime('%s','now'), ?) RETURNING batch_id`,
		source,
	).Scan(&batchID)
	if err != nil {
		return 0, err
	}
	return batchID, nil
}

// GetRaw reads the retained envelope bytes for an event.
func (d *DB) GetRaw(eventID string) (encoding string, raw []byte, err error) {
	err = d.sql.QueryRow(
		`SELECT encoding, raw_bytes FROM ledger_raw WHERE event_id = ?`, eventID,
	).Scan(&encoding, &raw)
	return encoding, raw, err
}

// GetRawTx reads retained envelope bytes inside an existing transaction.
func (d *DB) GetRawTx(tx *sql.Tx, eventID string) (encoding string, raw []byte, err error) {
	err = tx.QueryRow(
		`SELECT encoding, raw_bytes FROM ledger_raw WHERE event_id = ?`, eventID,
	).Scan(&encoding, &raw)
	return encoding, raw, err
}

// HasRaw reports whether envelope bytes are retained for the event.
func (d *DB) HasRaw(eventID string) (bool, error) {
	var found int
	err := d.sql.QueryRow(
		`SELECT 1 FROM ledger_raw WHERE event_id = ?`, eventID,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Checkpoint is a projector progress record.
type Checkpoint struct {
	View             string
	LastSeq          int64
	ProjectorVersion string
	UpdatedAt        int64
}

// GetCheckpoint reads a view checkpoint. A missing checkpoint returns
// LastSeq=0 and no error.
func (d *DB) GetCheckpoint(view string) (Checkpoint, error) {
	cp := Checkpoint{View: view}
	err := d.sql.QueryRow(
		`SELECT last_seq, projector_version, updated_at FROM projector_checkpoints WHERE view = ?`,
		view,
	).Scan(&cp.LastSeq, &cp.ProjectorVersion, &cp.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return cp, nil
	}
	return cp, err
}

// SetCheckpoint advances a view checkpoint.
func (d *DB) SetCheckpoint(view string, lastSeq int64, projectorVersion string) error {
	_, err := d.sql.Exec(checkpointUpsertSQL,
		view, lastSeq, projectorVersion)
	return err
}

// SetCheckpointTx advances a view checkpoint inside an existing transaction,
// so the checkpoint commit is atomic with the view's projected writes.
func (d *DB) SetCheckpointTx(tx *sql.Tx, view string, lastSeq int64, projectorVersion string) error {
	_, err := tx.Exec(checkpointUpsertSQL, view, lastSeq, projectorVersion)
	return err
}

const checkpointUpsertSQL = `
		INSERT INTO projector_checkpoints(view, last_seq, projector_version, updated_at)
		VALUES (?, ?, ?, strftime('%s','now'))
		ON CONFLICT(view) DO UPDATE SET
			last_seq = excluded.last_seq,
			projector_version = excluded.projector_version,
			updated_at = excluded.updated_at`

// GetViewMode reads the projection mode of a view (default "shadow").
func (d *DB) GetViewMode(view string) (string, error) {
	var mode string
	err := d.sql.QueryRow(
		`SELECT mode FROM ledger_view_state WHERE view = ?`, view,
	).Scan(&mode)
	if errors.Is(err, sql.ErrNoRows) {
		return "shadow", nil
	}
	return mode, err
}

// SetViewMode records the projection mode of a view.
func (d *DB) SetViewMode(view, mode string) error {
	_, err := d.sql.Exec(`
		INSERT INTO ledger_view_state(view, mode) VALUES (?, ?)
		ON CONFLICT(view) DO UPDATE SET mode = excluded.mode`, view, mode)
	return err
}
