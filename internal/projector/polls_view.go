package projector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
)

// --- PollsView ---------------------------------------------------------------

// PollsViewVersion is bumped when the poll projection semantics change.
const PollsViewVersion = "polls-projector/1.0.0"

// PollsView projects poll creations and single-option additions.
type PollsView struct {
	db       *store.DB
	suffix   string
	identity wa.IdentityMap
	idMap    *viewIdentityMap
}

// NewPollsView constructs the view.
func NewPollsView(db *store.DB, suffix string) *PollsView {
	return &PollsView{db: db, suffix: suffix}
}

// WithIdentityMap supplies a projection-time LID/PN map seed; mappings folded
// from identity_resolution events during replay take precedence.
func (v *PollsView) WithIdentityMap(m wa.IdentityMap) *PollsView {
	v.identity = m
	return v
}

// mapJID resolves a JID against the folded identity map first, then the
// external seed.
func (v *PollsView) mapJID(jid string) string {
	if canonical, ok := v.idMap.Lookup(jid); ok && canonical != "" {
		return canonical
	}
	return wa.MapJID(v.identity, jid)
}

// applyIdentity folds one identity_resolution event into poll rows. It must
// run after the messages view has folded the same event (purge ledger copy).
func (v *PollsView) applyIdentity(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read identity raw record: %w", err)
	}
	res, err := decodeIdentityResolution(raw)
	if err != nil {
		return err
	}
	v.idMap.add(res.LID, res.PN)
	return store.FoldLIDPolls(ctx, tx, v.suffix, res.LID, res.PN)
}

// Name is the checkpoint key.
func (v *PollsView) Name() string { return store.PollsTable + v.suffix }

// Version identifies the projection semantics.
func (v *PollsView) Version() string { return PollsViewVersion }

func (v *PollsView) table() string       { return store.PollsTable + v.suffix }
func (v *PollsView) purgesTable() string { return store.MessagePayloadPurges + v.suffix }

// Apply projects one checkpoint batch.
func (v *PollsView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	if v.idMap == nil {
		v.idMap = newViewIdentityMap()
	}
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventPoll:
			err = v.applyPoll(ctx, tx, evt)
		case evt.EventType == ledger.EventPollOption:
			err = v.applyPollOption(ctx, tx, evt)
		case evt.EventType == ledger.EventIdentityResolve:
			err = v.applyIdentity(ctx, tx, evt)
		}
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", evt.Seq, evt.EventType, err)
		}
	}
	return nil
}

func (v *PollsView) applyPoll(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read poll: %w", err)
	}
	var c ledger.CanonicalPoll
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("unmarshal poll: %w", err)
	}
	if c.Options == nil {
		c.Options = []string{}
	}
	p := store.Poll{
		ChatJID:         v.mapJID(c.ChatJID),
		MsgID:           c.MsgID,
		SenderJID:       v.mapJID(c.SenderJID),
		Question:        c.Question,
		Options:         c.Options,
		SelectableCount: c.SelectableCount,
		CreatedAt:       time.Unix(c.CreatedTS, 0).UTC(),
	}
	return v.db.UpsertPollTarget(ctx, tx, v.table(), v.purgesTable(), p)
}

// applyPollOption mirrors the old handlePollAddOption read/merge/write: load
// the full poll row, append the option, upsert with all existing fields.
func (v *PollsView) applyPollOption(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read poll option: %w", err)
	}
	var c ledger.CanonicalPollOptionAdd
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("unmarshal poll option: %w", err)
	}
	chatJID := v.mapJID(c.ChatJID)
	poll, err := getPollTarget(ctx, tx, v.table(), chatJID, c.PollMsgID)
	if err != nil {
		return fmt.Errorf("poll option references unknown poll %s/%s: %w",
			chatJID, c.PollMsgID, err)
	}
	option := c.Option
	already := false
	for _, existing := range poll.Options {
		if existing == option {
			already = true
			break
		}
	}
	if already {
		return nil
	}
	poll.Options = append(poll.Options, option)
	return v.db.UpsertPollTarget(ctx, tx, v.table(), v.purgesTable(), poll)
}

func (v *PollsView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	cols := []string{
		"chat_jid", "msg_id", "sender_jid", "question", "options_json",
		"selectable_count", "created_ts",
	}
	row, err := legacyRowFor(evt, store.PollsTable)
	if err != nil || row == nil {
		return err
	}
	args := []any{
		rowString(row, "chat_jid"),
		rowString(row, "msg_id"),
		rowNullString(row, "sender_jid"),
		rowNullString(row, "question"),
		rowString(row, "options_json"),
		coalesceInt64(row, "selectable_count"),
		rowNullInt64(row, "created_ts"),
	}
	return insertLegacyRows(ctx, tx, v.table(), "chat_jid, msg_id", cols, args)
}

// getPollTarget reads one full poll row from the target table.
func getPollTarget(ctx context.Context, ex store.QueryExecer, table,
	chatJID, msgID string,
) (store.Poll, error) {
	query := fmt.Sprintf(`SELECT chat_jid, msg_id, COALESCE(sender_jid,''),
		COALESCE(question,''), options_json, selectable_count, created_ts
		FROM %s WHERE chat_jid = ? AND msg_id = ?`, table)
	rows, err := ex.QueryContext(ctx, query, chatJID, msgID)
	if err != nil {
		return store.Poll{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return store.Poll{}, sql.ErrNoRows
	}
	var (
		p         store.Poll
		optsRaw   string
		createdTS int64
	)
	if err := rows.Scan(&p.ChatJID, &p.MsgID, &p.SenderJID, &p.Question,
		&optsRaw, &p.SelectableCount, &createdTS); err != nil {
		return store.Poll{}, err
	}
	p.CreatedAt = time.Unix(createdTS, 0).UTC()
	if optsRaw != "" {
		if err := json.Unmarshal([]byte(optsRaw), &p.Options); err != nil {
			return store.Poll{}, fmt.Errorf("unmarshal options: %w", err)
		}
	}
	return p, nil
}

// --- PollVotesView -----------------------------------------------------------

// PollVotesViewVersion is bumped when the poll-vote projection changes.
const PollVotesViewVersion = "poll-votes-projector/1.0.0"

// PollVotesView projects per-voter poll votes and retractions.
type PollVotesView struct {
	db       *store.DB
	suffix   string
	identity wa.IdentityMap
	idMap    *viewIdentityMap
}

// NewPollVotesView constructs the view.
func NewPollVotesView(db *store.DB, suffix string) *PollVotesView {
	return &PollVotesView{db: db, suffix: suffix}
}

// WithIdentityMap supplies a projection-time LID/PN map seed; mappings folded
// from identity_resolution events during replay take precedence.
func (v *PollVotesView) WithIdentityMap(m wa.IdentityMap) *PollVotesView {
	v.identity = m
	return v
}

// mapJID resolves a JID against the folded identity map first, then the
// external seed.
func (v *PollVotesView) mapJID(jid string) string {
	if canonical, ok := v.idMap.Lookup(jid); ok && canonical != "" {
		return canonical
	}
	return wa.MapJID(v.identity, jid)
}

// applyIdentity folds one identity_resolution event into poll votes. It must
// run after PollsView has folded the same event (purged polls remove votes).
func (v *PollVotesView) applyIdentity(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read identity raw record: %w", err)
	}
	res, err := decodeIdentityResolution(raw)
	if err != nil {
		return err
	}
	v.idMap.add(res.LID, res.PN)
	return store.FoldLIDPollVotes(ctx, tx, v.suffix, res.LID, res.PN)
}

// Name is the checkpoint key.
func (v *PollVotesView) Name() string { return store.PollVotesTable + v.suffix }

// Version identifies the projection semantics.
func (v *PollVotesView) Version() string { return PollVotesViewVersion }

func (v *PollVotesView) table() string       { return store.PollVotesTable + v.suffix }
func (v *PollVotesView) purgesTable() string { return store.MessagePayloadPurges + v.suffix }

// Apply projects one checkpoint batch.
func (v *PollVotesView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	if v.idMap == nil {
		v.idMap = newViewIdentityMap()
	}
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventPollVote:
			err = v.applyVote(ctx, tx, evt)
		case evt.EventType == ledger.EventIdentityResolve:
			err = v.applyIdentity(ctx, tx, evt)
		}
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", evt.Seq, evt.EventType, err)
		}
	}
	return nil
}

func (v *PollVotesView) applyVote(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read poll vote: %w", err)
	}
	var c ledger.CanonicalPollVote
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("unmarshal poll vote: %w", err)
	}
	chatJID := v.mapJID(c.ChatJID)
	voterJID := v.mapJID(c.VoterJID)
	if c.Deleted {
		return v.db.DeletePollVoteTarget(ctx, tx, v.table(),
			chatJID, c.PollMsgID, voterJID, time.UnixMilli(c.VotedTSMS))
	}
	vote := store.PollVote{
		ChatJID:       chatJID,
		PollMsgID:     c.PollMsgID,
		VoterJID:      voterJID,
		VoteMsgID:     c.VoteMsgID,
		Selected:      c.Selected,
		UnknownHashes: c.UnknownHashes,
		VotedAt:       time.UnixMilli(c.VotedTSMS),
	}
	return v.db.UpsertPollVoteTarget(ctx, tx, v.table(), v.purgesTable(), vote)
}

func (v *PollVotesView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	cols := []string{
		"chat_jid", "poll_msg_id", "voter_jid", "vote_msg_id",
		"selected_options_json", "ts",
	}
	row, err := legacyRowFor(evt, store.PollVotesTable)
	if err != nil || row == nil {
		return err
	}
	args := []any{
		rowString(row, "chat_jid"),
		rowString(row, "poll_msg_id"),
		rowString(row, "voter_jid"),
		rowNullString(row, "vote_msg_id"),
		rowString(row, "selected_options_json"),
		rowNullInt64(row, "ts"),
	}
	return insertLegacyRows(ctx, tx, v.table(),
		"chat_jid, poll_msg_id, voter_jid", cols, args)
}
