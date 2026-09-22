package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

var ErrLIDMigrationPurgedMediaPending = errors.New("purged alias media must be removed before LID migration")

type MessageLocalMedia struct {
	ChatJID   string
	MsgID     string
	LocalPath string
}

// materializedFoldTables lists every base table name that can appear in an
// identity fold and must be rewritten when applying the fold to shadow tables.
var materializedFoldTables = []string{
	MessagesTable,
	ChatsTable,
	ContactsTable,
	GroupsTable,
	GroupParticipantsTable,
	MessageLocationsTable,
	MessagePayloadPurges,
	MessageLocalMediaAliasesTable,
	PollsTable,
	PollVotesTable,
}

// retargetFoldQuery rewrites all materialized table names for a shadow fold.
// No replacement happens for the active set (suffix == "").
func retargetFoldQuery(query, suffix string) string {
	if suffix == "" {
		return query
	}
	for _, table := range materializedFoldTables {
		query = RetargetTable(query, table, table+suffix)
	}
	return query
}

// HistoricalLIDJIDs returns distinct hidden-user JIDs stored in chat, group,
// message, and poll identity columns. The app layer resolves these through whatsmeow.
func (d *DB) HistoricalLIDJIDs() ([]string, error) {
	rows, err := d.sql.Query(`
		SELECT jid FROM chats WHERE jid GLOB '*@lid'
		UNION
		SELECT chat_jid FROM messages WHERE chat_jid GLOB '*@lid'
		UNION
		SELECT sender_jid FROM messages WHERE sender_jid GLOB '*@lid'
		UNION
		SELECT quoted_sender_jid FROM messages WHERE quoted_sender_jid GLOB '*@lid'
		UNION
		SELECT owner_jid FROM groups WHERE owner_jid GLOB '*@lid'
		UNION
		SELECT user_jid FROM group_participants WHERE user_jid GLOB '*@lid'
		UNION
		SELECT chat_jid FROM polls WHERE chat_jid GLOB '*@lid'
		UNION
		SELECT sender_jid FROM polls WHERE sender_jid GLOB '*@lid' AND chat_jid NOT GLOB '*@g.us'
		UNION
		SELECT chat_jid FROM poll_votes WHERE chat_jid GLOB '*@lid'
		UNION
		SELECT voter_jid FROM poll_votes WHERE voter_jid GLOB '*@lid'
		UNION
		SELECT chat_jid FROM message_payload_purges WHERE chat_jid GLOB '*@lid'
		ORDER BY 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var jid sql.NullString
		if err := rows.Scan(&jid); err != nil {
			return nil, err
		}
		if jid.Valid {
			if s := strings.TrimSpace(jid.String); s != "" {
				out = append(out, s)
			}
		}
	}
	return out, rows.Err()
}

// MigrateLIDToPN rewrites one historical hidden-user JID to its phone-number
// JID. It is idempotent and merges duplicate chat/message rows created by the
// old split storage behavior.
func (d *DB) MigrateLIDToPN(lidJID, pnJID string) error {
	lidJID = strings.TrimSpace(lidJID)
	pnJID = strings.TrimSpace(pnJID)
	if lidJID == "" || pnJID == "" {
		return fmt.Errorf("lid and phone-number JIDs are required")
	}
	if lidJID == pnJID {
		return nil
	}
	pendingMedia, err := d.LIDMigrationPurgedMedia(lidJID, pnJID)
	if err != nil {
		return err
	}
	if len(pendingMedia) > 0 {
		return fmt.Errorf("%w: %d file(s)", ErrLIDMigrationPurgedMediaPending, len(pendingMedia))
	}

	tx, err := d.sql.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if err := FoldLIDChats(storeCtx(), tx, "", lidJID, pnJID); err != nil {
		return err
	}
	if err := FoldLIDMessages(storeCtx(), tx, "", lidJID, pnJID, false); err != nil {
		return err
	}
	if err := FoldLIDSenders(storeCtx(), tx, "", lidJID, pnJID); err != nil {
		return err
	}
	if err := FoldLIDGroups(storeCtx(), tx, "", lidJID, pnJID); err != nil {
		return err
	}
	if err := FoldLIDLocations(storeCtx(), tx, "", lidJID, pnJID); err != nil {
		return err
	}
	if err := FoldLIDPolls(storeCtx(), tx, "", lidJID, pnJID); err != nil {
		return err
	}
	if err := FoldLIDPollVotes(storeCtx(), tx, "", lidJID, pnJID); err != nil {
		return err
	}
	if err := deleteLIDChat(storeCtx(), tx, "", lidJID); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil
	return nil
}

func (d *DB) LIDMigrationPurgedMedia(lidJID, pnJID string) ([]MessageLocalMedia, error) {
	lidJID = strings.TrimSpace(lidJID)
	pnJID = strings.TrimSpace(pnJID)
	if lidJID == "" || pnJID == "" {
		return nil, fmt.Errorf("lid and phone-number JIDs are required")
	}
	rows, err := d.sql.Query(`
		SELECT DISTINCT m.chat_jid, m.msg_id, m.local_path
		FROM messages m
		WHERE m.chat_jid IN (?, ?)
			AND COALESCE(m.local_path, '') != ''
			AND EXISTS (
				SELECT 1 FROM message_payload_purges p
				WHERE p.chat_jid IN (?, ?) AND p.msg_id = m.msg_id
			)
		UNION
		SELECT a.chat_jid, a.msg_id, a.local_path
		FROM message_local_media_aliases a
		WHERE a.chat_jid IN (?, ?)
			AND EXISTS (
				SELECT 1 FROM message_payload_purges p
				WHERE p.chat_jid IN (?, ?) AND p.msg_id = a.msg_id
			)
		ORDER BY 1, 2, 3
	`, lidJID, pnJID, lidJID, pnJID, lidJID, pnJID, lidJID, pnJID)
	if err != nil {
		return nil, fmt.Errorf("load purged alias media: %w", err)
	}
	defer rows.Close()
	var out []MessageLocalMedia
	for rows.Next() {
		var item MessageLocalMedia
		if err := rows.Scan(&item.ChatJID, &item.MsgID, &item.LocalPath); err != nil {
			return nil, fmt.Errorf("scan purged alias media: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// --- fold SQL constants ------------------------------------------------------

const lidChatMergeSQL = `
	INSERT INTO chats(jid, kind, name, last_message_ts, unread, unread_count)
	SELECT
		?,
		CASE WHEN kind = '' OR kind = 'unknown' THEN 'dm' ELSE kind END,
		name,
		last_message_ts,
		CASE WHEN COALESCE(unread, 0) != 0 THEN 1 ELSE 0 END,
		COALESCE(unread_count, 0)
	FROM chats
	WHERE jid = ?
	ON CONFLICT(jid) DO UPDATE SET
		kind = CASE
			WHEN chats.kind = '' OR chats.kind = 'unknown' OR excluded.kind = 'dm' THEN excluded.kind
			ELSE chats.kind
		END,
		name = CASE
			WHEN excluded.name IS NOT NULL
				AND excluded.name != ''
				AND (
					chats.name IS NULL
					OR chats.name = ''
					OR chats.name = chats.jid
					OR instr(chats.name, '@') > 0
				)
			THEN excluded.name
			ELSE chats.name
		END,
		last_message_ts = max(COALESCE(chats.last_message_ts, 0), COALESCE(excluded.last_message_ts, 0)),
		unread = CASE WHEN COALESCE(chats.unread, 0) != 0 OR COALESCE(excluded.unread, 0) != 0 THEN 1 ELSE 0 END,
		unread_count = COALESCE(chats.unread_count, 0) + COALESCE(excluded.unread_count, 0)
`

// lidChatFromMessagesSQL recreates a PN chat from the latest LID message. It
// is part of the active migration only; the projector has already upserted a
// chats row when projecting the message itself.
const lidChatFromMessagesSQL = `
	INSERT INTO chats(jid, kind, name, last_message_ts)
	SELECT
		?,
		'dm',
		NULLIF(chat_name, ''),
		ts
	FROM messages
	WHERE chat_jid = ?
	ORDER BY ts DESC, rowid DESC
		LIMIT 1
	ON CONFLICT(jid) DO UPDATE SET
		name = CASE
			WHEN excluded.name IS NOT NULL
				AND excluded.name != ''
				AND (
					chats.name IS NULL
					OR chats.name = ''
					OR chats.name = chats.jid
					OR instr(chats.name, '@') > 0
				)
			THEN excluded.name
			ELSE chats.name
		END,
		last_message_ts = max(COALESCE(chats.last_message_ts, 0), COALESCE(excluded.last_message_ts, 0))
`

const lidPurgesCopySQL = `
	INSERT INTO message_payload_purges(chat_jid, msg_id, purged_at, deleted_at, deletion_reason)
	SELECT ?, msg_id, purged_at, deleted_at, deletion_reason
	FROM message_payload_purges
	WHERE chat_jid = ?
	ON CONFLICT(chat_jid, msg_id) DO UPDATE SET
		purged_at = min(message_payload_purges.purged_at, excluded.purged_at),
		deleted_at = min(message_payload_purges.deleted_at, excluded.deleted_at),
		deletion_reason = COALESCE(NULLIF(message_payload_purges.deletion_reason, ''), excluded.deletion_reason)
`

const lidDisplacedMediaSQL = `
	INSERT INTO message_local_media_aliases(chat_jid, msg_id, local_path, downloaded_at)
	SELECT ?, source.msg_id, source.local_path, source.downloaded_at
	FROM messages source
	JOIN messages destination ON destination.chat_jid = ? AND destination.msg_id = source.msg_id
	WHERE source.chat_jid = ?
		AND COALESCE(source.local_path, '') != ''
		AND COALESCE(destination.local_path, '') != ''
		AND destination.local_path != source.local_path
		AND NOT EXISTS (
			SELECT 1 FROM message_payload_purges p
			WHERE p.chat_jid IN (?, ?) AND p.msg_id = source.msg_id
		)
	ON CONFLICT(chat_jid, msg_id, local_path) DO UPDATE SET
		downloaded_at = COALESCE(message_local_media_aliases.downloaded_at, excluded.downloaded_at)
`

const lidMessagesMergeSQL = `
	INSERT INTO messages(
		chat_jid, chat_name, msg_id, sender_jid, sender_name, ts, from_me, text, display_text,
		quoted_msg_id, quoted_sender_jid,
		is_forwarded, forwarding_score, reaction_to_id, reaction_emoji,
		media_type, media_caption, filename, mime_type, direct_path,
		media_key, file_sha256, file_enc_sha256, file_length, local_path, downloaded_at,
		revoked, deleted_for_me, deleted_at, deletion_reason, payload_purged_at, edited, edited_ts, buttons
	)
	SELECT
		?,
		chat_name,
		msg_id,
		CASE WHEN sender_jid = ? THEN ? ELSE sender_jid END,
		sender_name,
		ts,
		from_me,
		text,
		display_text,
		quoted_msg_id,
		CASE WHEN quoted_sender_jid = ? THEN ? ELSE quoted_sender_jid END,
		is_forwarded,
		forwarding_score,
		reaction_to_id,
		reaction_emoji,
		media_type,
		media_caption,
		filename,
		mime_type,
		direct_path,
		media_key,
		file_sha256,
		file_enc_sha256,
		file_length,
		local_path,
		downloaded_at,
		revoked,
		deleted_for_me,
		deleted_at,
		deletion_reason,
		payload_purged_at,
		edited,
		edited_ts,
		buttons
	FROM messages AS source
	WHERE chat_jid = ?
		AND (source.payload_purged_at IS NOT NULL OR NOT EXISTS (
			SELECT 1 FROM message_payload_purges p WHERE p.chat_jid = ? AND p.msg_id = source.msg_id
		))
	ON CONFLICT(chat_jid, msg_id) DO UPDATE SET
		chat_name = COALESCE(NULLIF(messages.chat_name, ''), excluded.chat_name),
		sender_jid = COALESCE(NULLIF(messages.sender_jid, ''), excluded.sender_jid),
		sender_name = COALESCE(NULLIF(messages.sender_name, ''), excluded.sender_name),
		ts = CASE WHEN messages.deleted_at IS NOT NULL OR excluded.deleted_at IS NOT NULL THEN messages.ts WHEN excluded.edited != 0 THEN messages.ts WHEN messages.edited != 0 AND excluded.edited = 0 THEN excluded.ts ELSE max(messages.ts, excluded.ts) END,
		from_me = messages.from_me,
		text = CASE WHEN messages.deleted_at IS NOT NULL OR excluded.deleted_at IS NOT NULL THEN COALESCE(NULLIF(messages.text, ''), excluded.text) WHEN excluded.edited != 0 AND (messages.edited = 0 OR excluded.edited_ts > messages.edited_ts) THEN excluded.text WHEN messages.edited != 0 AND excluded.edited = 0 THEN messages.text ELSE COALESCE(NULLIF(messages.text, ''), excluded.text) END,
		display_text = CASE WHEN messages.deleted_at IS NOT NULL OR excluded.deleted_at IS NOT NULL THEN COALESCE(NULLIF(messages.display_text, ''), excluded.display_text) WHEN excluded.edited != 0 AND (messages.edited = 0 OR excluded.edited_ts > messages.edited_ts) THEN excluded.display_text WHEN messages.edited != 0 AND excluded.edited = 0 THEN messages.display_text ELSE COALESCE(NULLIF(messages.display_text, ''), excluded.display_text) END,
		quoted_msg_id = COALESCE(NULLIF(messages.quoted_msg_id, ''), excluded.quoted_msg_id),
		quoted_sender_jid = COALESCE(NULLIF(messages.quoted_sender_jid, ''), excluded.quoted_sender_jid),
		is_forwarded = CASE WHEN messages.is_forwarded != 0 THEN messages.is_forwarded ELSE excluded.is_forwarded END,
		forwarding_score = max(messages.forwarding_score, excluded.forwarding_score),
		reaction_to_id = COALESCE(NULLIF(messages.reaction_to_id, ''), excluded.reaction_to_id),
		reaction_emoji = COALESCE(NULLIF(messages.reaction_emoji, ''), excluded.reaction_emoji),
		media_type = COALESCE(NULLIF(messages.media_type, ''), excluded.media_type),
		media_caption = COALESCE(NULLIF(messages.media_caption, ''), excluded.media_caption),
		filename = COALESCE(NULLIF(messages.filename, ''), excluded.filename),
		mime_type = COALESCE(NULLIF(messages.mime_type, ''), excluded.mime_type),
		direct_path = COALESCE(NULLIF(messages.direct_path, ''), excluded.direct_path),
		media_key = CASE WHEN messages.media_key IS NOT NULL AND length(messages.media_key) > 0 THEN messages.media_key ELSE excluded.media_key END,
		file_sha256 = CASE WHEN messages.file_sha256 IS NOT NULL AND length(messages.file_sha256) > 0 THEN messages.file_sha256 ELSE excluded.file_sha256 END,
		file_enc_sha256 = CASE WHEN messages.file_enc_sha256 IS NOT NULL AND length(messages.file_enc_sha256) > 0 THEN messages.file_enc_sha256 ELSE excluded.file_enc_sha256 END,
		file_length = CASE WHEN messages.file_length IS NOT NULL AND messages.file_length > 0 THEN messages.file_length ELSE excluded.file_length END,
		local_path = COALESCE(NULLIF(messages.local_path, ''), excluded.local_path),
		downloaded_at = CASE WHEN messages.downloaded_at IS NOT NULL AND messages.downloaded_at > 0 THEN messages.downloaded_at ELSE excluded.downloaded_at END,
		revoked = CASE WHEN messages.revoked != 0 OR excluded.revoked != 0 THEN 1 ELSE 0 END,
		deleted_for_me = CASE WHEN messages.deleted_for_me != 0 OR excluded.deleted_for_me != 0 THEN 1 ELSE 0 END,
		deleted_at = COALESCE(messages.deleted_at, excluded.deleted_at),
		deletion_reason = CASE WHEN messages.deleted_at IS NOT NULL THEN COALESCE(NULLIF(messages.deletion_reason, ''), excluded.deletion_reason) ELSE excluded.deletion_reason END,
		payload_purged_at = COALESCE(messages.payload_purged_at, excluded.payload_purged_at),
		edited = CASE WHEN messages.deleted_at IS NOT NULL OR excluded.deleted_at IS NOT NULL THEN 0 WHEN messages.edited != 0 OR excluded.edited != 0 THEN 1 ELSE 0 END,
		edited_ts = CASE WHEN messages.deleted_at IS NOT NULL OR excluded.deleted_at IS NOT NULL THEN 0 ELSE max(COALESCE(messages.edited_ts, 0), COALESCE(excluded.edited_ts, 0)) END,
		buttons = COALESCE(messages.buttons, excluded.buttons)
	WHERE messages.payload_purged_at IS NULL
`

const lidMediaAliasesMergeSQL = `
	INSERT INTO message_local_media_aliases(chat_jid, msg_id, local_path, downloaded_at)
	SELECT ?, msg_id, local_path, downloaded_at
	FROM message_local_media_aliases
	WHERE chat_jid = ?
	ON CONFLICT(chat_jid, msg_id, local_path) DO UPDATE SET
		downloaded_at = COALESCE(message_local_media_aliases.downloaded_at, excluded.downloaded_at)
`

const lidPurgesApplySQL = `
	UPDATE messages
	SET deleted_at = COALESCE(deleted_at, (
			SELECT p.deleted_at FROM message_payload_purges p
			WHERE p.chat_jid = messages.chat_jid AND p.msg_id = messages.msg_id
		)),
		deletion_reason = COALESCE(NULLIF(deletion_reason, ''), (
			SELECT p.deletion_reason FROM message_payload_purges p
			WHERE p.chat_jid = messages.chat_jid AND p.msg_id = messages.msg_id
		)),
		payload_purged_at = COALESCE(payload_purged_at, (
			SELECT p.purged_at FROM message_payload_purges p
			WHERE p.chat_jid = messages.chat_jid AND p.msg_id = messages.msg_id
		))
	WHERE chat_jid = ?
		AND EXISTS (
			SELECT 1 FROM message_payload_purges p
			WHERE p.chat_jid = messages.chat_jid AND p.msg_id = messages.msg_id
		)
`

const lidPurgedScrubSQL = `
	UPDATE messages SET
		chat_name = NULL,
		sender_jid = NULL,
		sender_name = NULL,
		text = NULL,
		display_text = NULL,
		quoted_msg_id = NULL,
		quoted_sender_jid = NULL,
		is_forwarded = 0,
		forwarding_score = 0,
		reaction_to_id = NULL,
		reaction_emoji = NULL,
		media_type = NULL,
		media_caption = NULL,
		filename = NULL,
		mime_type = NULL,
		direct_path = NULL,
		media_key = NULL,
		file_sha256 = NULL,
		file_enc_sha256 = NULL,
		file_length = NULL,
		local_path = NULL,
		downloaded_at = NULL,
		media_unavailable_at = NULL,
		edited = 0,
		edited_ts = 0,
		buttons = NULL
	WHERE chat_jid = ? AND payload_purged_at IS NOT NULL
`

const deleteLIDMessagesSQL = `DELETE FROM messages WHERE chat_jid = ?`
const deleteLIDPurgesSQL = `DELETE FROM message_payload_purges WHERE chat_jid = ?`
const deleteLIDAliasesSQL = `DELETE FROM message_local_media_aliases WHERE chat_jid = ?`

const lidSenderUpdateSQL = `UPDATE messages SET sender_jid = ? WHERE sender_jid = ?`
const lidQuotedSenderUpdateSQL = `UPDATE messages SET quoted_sender_jid = ? WHERE quoted_sender_jid = ?`

const lidGroupOwnerUpdateSQL = `UPDATE groups SET owner_jid = ? WHERE owner_jid = ?`

const lidParticipantMergeSQL = `
	INSERT INTO group_participants(group_jid, user_jid, role, updated_at)
	SELECT group_jid, ?, role, updated_at
	FROM group_participants
	WHERE user_jid = ?
	ON CONFLICT(group_jid, user_jid) DO UPDATE SET
		role = CASE
			WHEN excluded.updated_at >= group_participants.updated_at THEN excluded.role
			ELSE group_participants.role
		END,
		updated_at = max(group_participants.updated_at, excluded.updated_at)
`

const deleteLIDParticipantsSQL = `DELETE FROM group_participants WHERE user_jid = ?`

const lidLocationsMergeSQL = `
	INSERT INTO message_locations(chat_jid, msg_id, latitude, longitude, name, address, is_live)
	SELECT ?, msg_id, latitude, longitude, name, address, is_live
	FROM message_locations
	WHERE chat_jid = ?
		AND NOT EXISTS (
			SELECT 1 FROM message_payload_purges p
			WHERE p.chat_jid IN (?, ?) AND p.msg_id = message_locations.msg_id
		)
	ON CONFLICT(chat_jid, msg_id) DO UPDATE SET
		latitude = excluded.latitude,
		longitude = excluded.longitude,
		name = COALESCE(NULLIF(excluded.name, ''), message_locations.name),
		address = COALESCE(NULLIF(excluded.address, ''), message_locations.address),
		is_live = excluded.is_live
`

const deleteLIDLocationsSQL = `DELETE FROM message_locations WHERE chat_jid = ?`

const deletePurgedLocationsSQL = `
	DELETE FROM message_locations
	WHERE EXISTS (
		SELECT 1 FROM message_payload_purges p
		WHERE p.chat_jid = message_locations.chat_jid AND p.msg_id = message_locations.msg_id
	)
`

const deleteLIDPollsSQL = `
	DELETE FROM polls WHERE chat_jid = ? OR (sender_jid = ? AND chat_jid NOT GLOB '*@g.us')
`

const deletePurgedPollsSQL = `
	DELETE FROM polls
	WHERE EXISTS (
		SELECT 1 FROM message_payload_purges p
		WHERE p.chat_jid = polls.chat_jid AND p.msg_id = polls.msg_id
	)
`

const lidPollsLoadSQL = `
	SELECT chat_jid, msg_id, COALESCE(sender_jid,''), question, options_json, selectable_count, created_ts
	FROM polls
	WHERE chat_jid = ? OR (sender_jid = ? AND chat_jid NOT GLOB '*@g.us')
`

const deletePurgedPollVotesForPollSQL = `DELETE FROM poll_votes WHERE chat_jid = ? AND poll_msg_id = ?`
const deletePurgedPollForPollSQL = `DELETE FROM polls WHERE chat_jid = ? AND msg_id = ?`

const lidPollMergeSQL = `
	INSERT INTO polls(chat_jid, msg_id, sender_jid, question, options_json, selectable_count, created_ts)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(chat_jid, msg_id) DO UPDATE SET
		sender_jid = COALESCE(NULLIF(excluded.sender_jid, ''), polls.sender_jid),
		question = excluded.question,
		options_json = excluded.options_json,
		selectable_count = excluded.selectable_count,
		created_ts = max(polls.created_ts, excluded.created_ts)
`

const lidPollVotesMergeSQL = `
	INSERT INTO poll_votes(chat_jid, poll_msg_id, voter_jid, vote_msg_id, selected_options_json, ts)
	SELECT
		CASE WHEN chat_jid = ? THEN ? ELSE chat_jid END,
		poll_msg_id,
		CASE WHEN voter_jid = ? THEN ? ELSE voter_jid END,
		vote_msg_id,
		selected_options_json,
		ts
	FROM poll_votes
	WHERE chat_jid = ? OR voter_jid = ?
	ON CONFLICT(chat_jid, poll_msg_id, voter_jid) DO UPDATE SET
		vote_msg_id = excluded.vote_msg_id,
		selected_options_json = excluded.selected_options_json,
		ts = excluded.ts
	WHERE excluded.ts >= poll_votes.ts
`

const deleteLIDPollVotesSQL = `DELETE FROM poll_votes WHERE chat_jid = ? OR voter_jid = ?`

const deletePurgedPollVotesSQL = `
	DELETE FROM poll_votes
	WHERE EXISTS (
		SELECT 1 FROM message_payload_purges p
		WHERE p.chat_jid = poll_votes.chat_jid
			AND (p.msg_id = poll_votes.poll_msg_id OR p.msg_id = poll_votes.vote_msg_id)
	)
`

func deleteLIDChat(ctx context.Context, ex QueryExecer, suffix, lidJID string) error {
	return DeleteLIDChatTarget(ctx, ex, suffix, lidJID)
}

// DeleteLIDChatTarget removes the LID chats row from the active (suffix=="")
// or a shadow set after its content has been folded into the PN chat row.
func DeleteLIDChatTarget(ctx context.Context, ex QueryExecer, suffix, lidJID string) error {
	query := retargetFoldQuery(`DELETE FROM chats WHERE jid = ?`, suffix)
	if _, err := ex.ExecContext(ctx, query, lidJID); err != nil {
		return fmt.Errorf("delete migrated lid chat: %w", err)
	}
	return nil
}
