package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/wacli/internal/store/storedb"
)

// Poll represents a stored PollCreationMessage.
type Poll struct {
	ChatJID         string
	MsgID           string
	SenderJID       string
	Question        string
	Options         []string
	SelectableCount uint32
	CreatedAt       time.Time
}

// PollVote represents one voter's latest vote on a poll.
type PollVote struct {
	ChatJID       string
	PollMsgID     string
	VoterJID      string
	VoteMsgID     string
	Selected      []string
	UnknownHashes []string
	VotedAt       time.Time
}

// PollListFilter narrows down polls returned by ListPolls.
type PollListFilter struct {
	ChatJID  string
	ChatJIDs []string
	Limit    int
	Offset   int
}

// PollsTable is the polls table name.
const PollsTable = "polls"

// PollVotesTable is the poll votes table name.
const PollVotesTable = "poll_votes"

const pollOptionsReadSQL = `SELECT options_json FROM polls WHERE chat_jid = ? AND msg_id = ?`

const pollUpsertSQL = `
INSERT INTO polls (chat_jid, msg_id, sender_jid, question, options_json, selectable_count, created_ts)
SELECT ?, ?, ?, ?, ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM message_payload_purges p WHERE p.chat_jid = ? AND p.msg_id = ?
)
ON CONFLICT(chat_jid, msg_id) DO UPDATE SET
    sender_jid = excluded.sender_jid,
    question = excluded.question,
    options_json = excluded.options_json,
    selectable_count = excluded.selectable_count,
    created_ts = excluded.created_ts
`

const pollVoteUpsertSQL = `
INSERT INTO poll_votes (chat_jid, poll_msg_id, voter_jid, vote_msg_id, selected_options_json, ts)
SELECT ?, ?, ?, ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM message_payload_purges p WHERE p.chat_jid = ? AND p.msg_id = ?
)
  AND NOT EXISTS (
    SELECT 1 FROM message_payload_purges p WHERE p.chat_jid = ? AND p.msg_id = ?
)
ON CONFLICT(chat_jid, poll_msg_id, voter_jid) DO UPDATE SET
    vote_msg_id = excluded.vote_msg_id,
    selected_options_json = excluded.selected_options_json,
    ts = excluded.ts
WHERE excluded.ts >= poll_votes.ts
`

const pollVoteDeleteSQL = `
DELETE FROM poll_votes
WHERE chat_jid = ? AND poll_msg_id = ? AND voter_jid = ? AND ts <= ?
`

// UpsertPoll inserts or replaces a poll row keyed on (chat_jid, msg_id).
func (d *DB) UpsertPoll(p Poll) error {
	return d.UpsertPollTarget(storeCtx(), d.sql, PollsTable, MessagePayloadPurges, p)
}

// UpsertPollTarget applies the poll-creation fold to the target table
// (active or shadow) through ex, reading/guarding against purgesTable.
func (d *DB) UpsertPollTarget(ctx context.Context, ex QueryExecer, table, purgesTable string, p Poll) error {
	if strings.TrimSpace(p.ChatJID) == "" || strings.TrimSpace(p.MsgID) == "" {
		return fmt.Errorf("poll requires chat_jid and msg_id")
	}
	if p.Options == nil {
		p.Options = []string{}
	}
	options := p.Options
	if existing, err := d.pollOptionsTarget(ctx, ex, table, p.ChatJID, p.MsgID); err == nil {
		options = mergePollOptions(options, existing)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	optsJSON, err := json.Marshal(options)
	if err != nil {
		return fmt.Errorf("marshal options: %w", err)
	}
	createdTS := p.CreatedAt
	if createdTS.IsZero() {
		createdTS = nowUTC()
	}
	query := retargetPollQuery(pollUpsertSQL, PollsTable, table, purgesTable)
	_, err = ex.ExecContext(ctx, query,
		p.ChatJID, p.MsgID, nullString(p.SenderJID), p.Question, string(optsJSON),
		int64(p.SelectableCount), createdTS.UTC().Unix(),
		p.ChatJID, p.MsgID)
	if err != nil {
		return fmt.Errorf("upsert poll: %w", err)
	}
	return nil
}

// retargetPollQuery rewrites both the poll-family table and the purge guard
// table in one generated query.
func retargetPollQuery(query, oldTable, table, purgesTable string) string {
	if oldTable != table {
		query = RetargetTable(query, oldTable, table)
	}
	if purgesTable != MessagePayloadPurges {
		query = RetargetTable(query, MessagePayloadPurges, purgesTable)
	}
	return query
}

// GetPoll fetches a single poll by (chat_jid, msg_id). Returns sql.ErrNoRows
// if not found.
func (d *DB) GetPoll(chatJID, msgID string) (Poll, error) {
	if d == nil {
		return Poll{}, fmt.Errorf("nil db")
	}
	row, err := d.q.GetPoll(storeCtx(), storedb.GetPollParams{ChatJid: chatJID, MsgID: msgID})
	if err != nil {
		return Poll{}, err
	}
	return pollFromGetRow(row)
}

// FindPollByMsgID returns the most recent poll matching the given msg_id
// across any chat. Useful when the chat JID embedded in a vote event
// (often a LID) does not match the chat under which the poll was stored
// (often a phone-number JID for self-conversations). Returns sql.ErrNoRows
// if not found.
func (d *DB) FindPollByMsgID(msgID string) (Poll, error) {
	if d == nil {
		return Poll{}, fmt.Errorf("nil db")
	}
	row, err := d.q.FindPollByMsgID(storeCtx(), msgID)
	if err != nil {
		return Poll{}, err
	}
	return pollFromFindRow(row)
}

// ListPolls returns polls ordered most-recent-first.
func (d *DB) ListPolls(filter PollListFilter) ([]Poll, error) {
	if d == nil {
		return nil, fmt.Errorf("nil db")
	}
	q := `SELECT p.chat_jid, p.msg_id, COALESCE(p.sender_jid,''), p.question, p.options_json, p.selectable_count, p.created_ts
	      FROM polls p
	      LEFT JOIN messages m ON m.chat_jid = p.chat_jid AND m.msg_id = p.msg_id
	      WHERE (m.msg_id IS NULL OR m.deleted_at IS NULL)
	        AND NOT EXISTS (SELECT 1 FROM message_payload_purges x WHERE x.chat_jid = p.chat_jid AND x.msg_id = p.msg_id)`
	args := []any{}
	chatJIDs := cleanPollFilterChatJIDs(filter)
	switch {
	case len(chatJIDs) == 1:
		rows, err := d.q.ListPolls(storeCtx(), storedb.ListPollsParams{
			Column1: chatJIDs[0],
			ChatJid: chatJIDs[0],
			Limit:   int64(limitOrAll(filter.Limit)),
			Offset:  int64(filter.Offset),
		})
		if err != nil {
			return nil, fmt.Errorf("query polls: %w", err)
		}
		return pollsFromRows(rows)
	case len(chatJIDs) > 1:
		q += " AND p.chat_jid IN (?" + strings.Repeat(",?", len(chatJIDs)-1) + ")"
		for _, chatJID := range chatJIDs {
			args = append(args, chatJID)
		}
	}
	if len(chatJIDs) == 0 {
		rows, err := d.q.ListPolls(storeCtx(), storedb.ListPollsParams{
			Column1: "",
			ChatJid: "",
			Limit:   int64(limitOrAll(filter.Limit)),
			Offset:  int64(filter.Offset),
		})
		if err != nil {
			return nil, fmt.Errorf("query polls: %w", err)
		}
		return pollsFromRows(rows)
	}
	q += " ORDER BY p.created_ts DESC, p.msg_id DESC"
	if filter.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, filter.Limit)
		if filter.Offset > 0 {
			q += " OFFSET ?"
			args = append(args, filter.Offset)
		}
	}
	rows, err := d.sql.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("query polls: %w", err)
	}
	defer rows.Close()

	var out []Poll
	for rows.Next() {
		var (
			p          Poll
			optsRaw    string
			createdTS  int64
			selectable int64
		)
		if err := rows.Scan(&p.ChatJID, &p.MsgID, &p.SenderJID, &p.Question, &optsRaw, &selectable, &createdTS); err != nil {
			return nil, fmt.Errorf("scan poll: %w", err)
		}
		p.SelectableCount = uint32(selectable)
		p.CreatedAt = time.Unix(createdTS, 0).UTC()
		if optsRaw != "" {
			if err := json.Unmarshal([]byte(optsRaw), &p.Options); err != nil {
				return nil, fmt.Errorf("unmarshal options: %w", err)
			}
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func cleanPollFilterChatJIDs(filter PollListFilter) []string {
	candidates := filter.ChatJIDs
	if len(candidates) == 0 && strings.TrimSpace(filter.ChatJID) != "" {
		candidates = []string{filter.ChatJID}
	}
	out := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, chatJID := range candidates {
		chatJID = strings.TrimSpace(chatJID)
		if chatJID == "" {
			continue
		}
		if _, ok := seen[chatJID]; ok {
			continue
		}
		seen[chatJID] = struct{}{}
		out = append(out, chatJID)
	}
	return out
}

// UpsertPollVote replaces the vote row for (chat, poll, voter).
func (d *DB) UpsertPollVote(v PollVote) error {
	return d.UpsertPollVoteTarget(storeCtx(), d.sql, PollVotesTable, MessagePayloadPurges, v)
}

// UpsertPollVoteTarget applies the vote fold to the target votes table
// through ex, guarding against purgesTable.
func (d *DB) UpsertPollVoteTarget(ctx context.Context, ex QueryExecer, table, purgesTable string, v PollVote) error {
	if strings.TrimSpace(v.ChatJID) == "" || strings.TrimSpace(v.PollMsgID) == "" || strings.TrimSpace(v.VoterJID) == "" {
		return fmt.Errorf("vote requires chat_jid, poll_msg_id, voter_jid")
	}
	if v.Selected == nil {
		v.Selected = []string{}
	}
	selJSON, err := marshalPollVoteSelection(v)
	if err != nil {
		return fmt.Errorf("marshal selected: %w", err)
	}
	ts := v.VotedAt
	if ts.IsZero() {
		ts = nowUTC()
	}
	query := retargetPollQuery(pollVoteUpsertSQL, PollVotesTable, table, purgesTable)
	if _, err := ex.ExecContext(ctx, query,
		v.ChatJID, v.PollMsgID, v.VoterJID, v.VoteMsgID, string(selJSON),
		ts.UTC().UnixMilli(),
		v.ChatJID, v.PollMsgID,
		v.ChatJID, v.VoteMsgID,
	); err != nil {
		return fmt.Errorf("upsert poll vote: %w", err)
	}
	return nil
}

// DeletePollVote removes one voter's current vote if the deletion is not older
// than the stored vote row.
func (d *DB) DeletePollVote(chatJID, pollMsgID, voterJID string, votedAt time.Time) error {
	return d.DeletePollVoteTarget(storeCtx(), d.sql, PollVotesTable,
		chatJID, pollMsgID, voterJID, votedAt)
}

// DeletePollVoteTarget applies the vote retraction to the target table.
func (d *DB) DeletePollVoteTarget(ctx context.Context, ex QueryExecer, table,
	chatJID, pollMsgID, voterJID string, votedAt time.Time,
) error {
	if strings.TrimSpace(chatJID) == "" || strings.TrimSpace(pollMsgID) == "" || strings.TrimSpace(voterJID) == "" {
		return fmt.Errorf("vote requires chat_jid, poll_msg_id, voter_jid")
	}
	ts := votedAt
	if ts.IsZero() {
		ts = nowUTC()
	}
	query := RetargetTable(pollVoteDeleteSQL, PollVotesTable, table)
	if _, err := ex.ExecContext(ctx, query,
		chatJID, pollMsgID, voterJID, ts.UTC().UnixMilli()); err != nil {
		return fmt.Errorf("delete poll vote: %w", err)
	}
	return nil
}

// ListPollVotes returns the per-voter votes for a poll, ordered by ts ASC.
func (d *DB) ListPollVotes(chatJID, pollMsgID string) ([]PollVote, error) {
	if d == nil {
		return nil, fmt.Errorf("nil db")
	}
	rows, err := d.q.ListPollVotes(storeCtx(), storedb.ListPollVotesParams{ChatJid: chatJID, PollMsgID: pollMsgID})
	if err != nil {
		return nil, fmt.Errorf("query poll votes: %w", err)
	}
	return pollVotesFromRows(rows)
}

func (d *DB) pollOptionsTarget(ctx context.Context, ex QueryExecer, table,
	chatJID, msgID string,
) ([]string, error) {
	return readPollOptions(ctx, ex, table, chatJID, msgID)
}

// readPollOptions reads the stored options JSON for one poll from an arbitrary
// target table. It returns sql.ErrNoRows when the poll row is absent.
func readPollOptions(ctx context.Context, ex QueryExecer, table,
	chatJID, msgID string,
) ([]string, error) {
	query := RetargetTable(pollOptionsReadSQL, PollsTable, table)
	rows, err := ex.QueryContext(ctx, query, chatJID, msgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	var raw string
	if err := rows.Scan(&raw); err != nil {
		return nil, err
	}
	var options []string
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &options); err != nil {
			return nil, fmt.Errorf("unmarshal existing options: %w", err)
		}
	}
	return options, nil
}

func mergePollOptions(incoming, existing []string) []string {
	out := append([]string(nil), incoming...)
	seen := make(map[string]struct{}, len(out)+len(existing))
	for _, option := range out {
		seen[option] = struct{}{}
	}
	for _, option := range existing {
		if _, ok := seen[option]; ok {
			continue
		}
		out = append(out, option)
		seen[option] = struct{}{}
	}
	return out
}

// DeletePoll removes a poll and all its votes (votes are cascaded by
// foreign key, but we issue an explicit delete since the FK isn't declared
// at the table level).
func (d *DB) DeletePoll(chatJID, msgID string) error {
	if d == nil {
		return fmt.Errorf("nil db")
	}
	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	q := d.q.WithTx(tx)
	ctx := storeCtx()
	if err := q.DeletePollVotesForPoll(ctx, storedb.DeletePollVotesForPollParams{ChatJid: chatJID, PollMsgID: msgID}); err != nil {
		return fmt.Errorf("delete poll votes: %w", err)
	}
	if err := q.DeletePoll(ctx, storedb.DeletePollParams{ChatJid: chatJID, MsgID: msgID}); err != nil {
		return fmt.Errorf("delete poll: %w", err)
	}
	return tx.Commit()
}

func limitOrAll(limit int) int {
	if limit > 0 {
		return limit
	}
	return 1_000_000_000
}

func pollFromGetRow(row storedb.GetPollRow) (Poll, error) {
	return pollFromScalars(row.ChatJid, row.MsgID, row.SenderJid, row.Question, row.OptionsJson, row.SelectableCount, row.CreatedTs)
}

func pollFromFindRow(row storedb.FindPollByMsgIDRow) (Poll, error) {
	return pollFromScalars(row.ChatJid, row.MsgID, row.SenderJid, row.Question, row.OptionsJson, row.SelectableCount, row.CreatedTs)
}

func pollsFromRows(rows []storedb.ListPollsRow) ([]Poll, error) {
	out := make([]Poll, 0, len(rows))
	for _, row := range rows {
		p, err := pollFromScalars(row.ChatJid, row.MsgID, row.SenderJid, row.Question, row.OptionsJson, row.SelectableCount, row.CreatedTs)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func pollFromScalars(chatJID, msgID, senderJID, question, optsRaw string, selectable, createdTS int64) (Poll, error) {
	p := Poll{
		ChatJID:         chatJID,
		MsgID:           msgID,
		SenderJID:       senderJID,
		Question:        question,
		SelectableCount: uint32(selectable),
		CreatedAt:       time.Unix(createdTS, 0).UTC(),
	}
	if optsRaw != "" {
		if err := json.Unmarshal([]byte(optsRaw), &p.Options); err != nil {
			return Poll{}, fmt.Errorf("unmarshal options: %w", err)
		}
	}
	return p, nil
}

func pollVotesFromRows(rows []storedb.PollVote) ([]PollVote, error) {
	out := make([]PollVote, 0, len(rows))
	for _, row := range rows {
		v := PollVote{
			ChatJID:   row.ChatJid,
			PollMsgID: row.PollMsgID,
			VoterJID:  row.VoterJid,
			VoteMsgID: row.VoteMsgID,
			VotedAt:   time.UnixMilli(row.Ts).UTC(),
		}
		if row.SelectedOptionsJson != "" {
			if err := unmarshalPollVoteSelection(row.SelectedOptionsJson, &v); err != nil {
				return nil, fmt.Errorf("unmarshal selected: %w", err)
			}
		}
		out = append(out, v)
	}
	return out, nil
}

type pollVoteSelectionJSON struct {
	Selected      []string `json:"selected"`
	UnknownHashes []string `json:"unknown_hashes,omitempty"`
}

func marshalPollVoteSelection(v PollVote) (string, error) {
	if len(v.UnknownHashes) == 0 {
		raw, err := json.Marshal(v.Selected)
		return string(raw), err
	}
	raw, err := json.Marshal(pollVoteSelectionJSON{
		Selected:      v.Selected,
		UnknownHashes: v.UnknownHashes,
	})
	return string(raw), err
}

func unmarshalPollVoteSelection(raw string, v *PollVote) error {
	var selected []string
	if err := json.Unmarshal([]byte(raw), &selected); err == nil {
		v.Selected = selected
		return nil
	}
	var wrapped pollVoteSelectionJSON
	if err := json.Unmarshal([]byte(raw), &wrapped); err != nil {
		return err
	}
	v.Selected = wrapped.Selected
	v.UnknownHashes = wrapped.UnknownHashes
	if v.Selected == nil {
		v.Selected = []string{}
	}
	return nil
}
