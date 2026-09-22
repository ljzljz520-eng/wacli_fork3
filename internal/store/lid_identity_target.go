package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// This file exposes the LID→PN identity fold as retargetable steps so that
// projector views can apply the same rewrite/merge semantics against shadow
// tables. The active migration (suffix == "") is unchanged; when FTS is built
// explicitly by a view (no triggers on shadow tables) the FTS steps mirror
// what the messages_ai/ad triggers would have done.

func foldExec(ctx context.Context, ex QueryExecer, suffix, query string, args ...any) error {
	if _, err := ex.ExecContext(ctx, retargetFoldQuery(query, suffix), args...); err != nil {
		return err
	}
	return nil
}

// FoldLIDChats merges the LID chat row into the PN chat row.
func FoldLIDChats(ctx context.Context, ex QueryExecer, suffix, lidJID, pnJID string) error {
	if err := foldExec(ctx, ex, suffix, lidChatMergeSQL, pnJID, lidJID); err != nil {
		return fmt.Errorf("merge lid chat into pn chat: %w", err)
	}
	return nil
}

const lidFTSDeleteSQL = `
	DELETE FROM messages_fts
	WHERE rowid IN (SELECT rowid FROM messages WHERE chat_jid = ?)
`

const lidFTSInsertMovedSQL = `
	INSERT INTO messages_fts(rowid, text, media_caption, filename, chat_name, sender_name, display_text)
	SELECT
		m.rowid,
		COALESCE(m.text, ''),
		COALESCE(m.media_caption, ''),
		COALESCE(m.filename, ''),
		COALESCE(m.chat_name, ''),
		COALESCE(m.sender_name, ''),
		COALESCE(m.display_text, '')
	FROM messages m
	WHERE m.chat_jid = ?
		AND m.deleted_at IS NULL
		AND m.msg_id IN (SELECT msg_id FROM messages WHERE chat_jid = ?)
		AND m.rowid NOT IN (SELECT rowid FROM messages_fts)
`

func foldFTSQuery(query, suffix string) string {
	query = retargetFoldQuery(query, suffix)
	if suffix != "" {
		query = strings.ReplaceAll(query, "messages_fts", "messages_fts"+suffix)
	}
	return query
}

// FoldLIDMessages executes the message-domain identity fold: purge ledger
// copy, displaced-media aliasing, row merge/upsert, purge application, scrub,
// and finally removal of the LID rows. withFTS must be set only for views
// that maintain messages_fts explicitly.
func FoldLIDMessages(ctx context.Context, ex QueryExecer, suffix, lidJID, pnJID string, withFTS bool) error {
	if withFTS {
		if _, err := ex.ExecContext(ctx, foldFTSQuery(lidFTSDeleteSQL, suffix), lidJID); err != nil {
			return fmt.Errorf("drop fts rows for lid messages: %w", err)
		}
	}

	if err := foldExec(ctx, ex, suffix, lidPurgesCopySQL, pnJID, lidJID); err != nil {
		return fmt.Errorf("copy lid purge ledger: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, lidDisplacedMediaSQL, pnJID, pnJID, lidJID, lidJID, pnJID); err != nil {
		return fmt.Errorf("preserve displaced media alias: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, lidMessagesMergeSQL,
		pnJID, lidJID, pnJID, lidJID, pnJID, lidJID, pnJID); err != nil {
		return fmt.Errorf("merge lid messages into pn chat: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, lidMediaAliasesMergeSQL, pnJID, lidJID); err != nil {
		return fmt.Errorf("merge lid media aliases: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, lidPurgesApplySQL, pnJID); err != nil {
		return fmt.Errorf("apply migrated purge ledger: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, lidPurgedScrubSQL, pnJID); err != nil {
		return fmt.Errorf("scrub migrated purged messages: %w", err)
	}

	if withFTS {
		if _, err := ex.ExecContext(ctx, foldFTSQuery(lidFTSInsertMovedSQL, suffix), pnJID, lidJID); err != nil {
			return fmt.Errorf("index moved messages in fts: %w", err)
		}
	}

	if err := foldExec(ctx, ex, suffix, deleteLIDMessagesSQL, lidJID); err != nil {
		return fmt.Errorf("delete migrated lid messages: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, deleteLIDPurgesSQL, lidJID); err != nil {
		return fmt.Errorf("delete migrated lid purge ledger: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, deleteLIDAliasesSQL, lidJID); err != nil {
		return fmt.Errorf("delete migrated lid media aliases: %w", err)
	}
	return nil
}

// FoldLIDSenders rewrites sender and quoted-sender references in the message
// domain for messages that were never stored under the LID chat key.
func FoldLIDSenders(ctx context.Context, ex QueryExecer, suffix, lidJID, pnJID string) error {
	if err := foldExec(ctx, ex, suffix, lidSenderUpdateSQL, pnJID, lidJID); err != nil {
		return fmt.Errorf("rewrite lid message senders: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, lidQuotedSenderUpdateSQL, pnJID, lidJID); err != nil {
		return fmt.Errorf("rewrite lid quoted senders: %w", err)
	}
	return nil
}

// FoldLIDGroups rewrites group owners and merges/removes participant rows.
func FoldLIDGroups(ctx context.Context, ex QueryExecer, suffix, lidJID, pnJID string) error {
	if err := foldExec(ctx, ex, suffix, lidGroupOwnerUpdateSQL, pnJID, lidJID); err != nil {
		return fmt.Errorf("rewrite lid group owners: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, lidParticipantMergeSQL, pnJID, lidJID); err != nil {
		return fmt.Errorf("merge lid group participants: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, deleteLIDParticipantsSQL, lidJID); err != nil {
		return fmt.Errorf("delete lid group participants: %w", err)
	}
	return nil
}

// FoldLIDLocations merges message location rows and removes LID and purged rows.
func FoldLIDLocations(ctx context.Context, ex QueryExecer, suffix, lidJID, pnJID string) error {
	if err := foldExec(ctx, ex, suffix, lidLocationsMergeSQL, pnJID, lidJID, lidJID, pnJID); err != nil {
		return fmt.Errorf("migrate lid message locations: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, deleteLIDLocationsSQL, lidJID); err != nil {
		return fmt.Errorf("delete migrated lid message locations: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, deletePurgedLocationsSQL); err != nil {
		return fmt.Errorf("suppress destination-only purged locations: %w", err)
	}
	return nil
}

// FoldLIDPolls migrates poll rows: purged polls are dropped together with
// their votes, remaining rows are options-merged and upserted under PN keys.
func FoldLIDPolls(ctx context.Context, ex QueryExecer, suffix, lidJID, pnJID string) error {
	query := retargetFoldQuery(lidPollsLoadSQL, suffix)
	rows, err := ex.QueryContext(ctx, query, lidJID, lidJID)
	if err != nil {
		return fmt.Errorf("load lid polls: %w", err)
	}
	type pollRow struct {
		chatJID, msgID, senderJID, question, optionsJSON string
		selectableCount, createdTS                       int64
	}
	var polls []pollRow
	for rows.Next() {
		var p pollRow
		if err := rows.Scan(&p.chatJID, &p.msgID, &p.senderJID, &p.question,
			&p.optionsJSON, &p.selectableCount, &p.createdTS); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan lid poll: %w", err)
		}
		polls = append(polls, p)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	pollsTable := PollsTable + suffix
	for _, p := range polls {
		destChat := p.chatJID
		if destChat == lidJID {
			destChat = pnJID
		}
		destSender := p.senderJID
		if destSender == lidJID && !strings.HasSuffix(p.chatJID, "@g.us") {
			destSender = pnJID
		}

		var payloadPurged int
		purgeQuery := retargetFoldQuery(
			`SELECT EXISTS(SELECT 1 FROM message_payload_purges WHERE chat_jid = ? AND msg_id = ?)`,
			suffix)
		if err := ex.QueryRowContext(ctx, purgeQuery, destChat, p.msgID).Scan(&payloadPurged); err != nil {
			return fmt.Errorf("check migrated poll purge ledger: %w", err)
		}
		if payloadPurged != 0 {
			if err := foldExec(ctx, ex, suffix, deletePurgedPollVotesForPollSQL, destChat, p.msgID); err != nil {
				return fmt.Errorf("delete purged migrated poll votes: %w", err)
			}
			if err := foldExec(ctx, ex, suffix, deletePurgedPollForPollSQL, destChat, p.msgID); err != nil {
				return fmt.Errorf("delete purged migrated poll: %w", err)
			}
			continue
		}

		var incoming []string
		if err := json.Unmarshal([]byte(p.optionsJSON), &incoming); err != nil {
			return fmt.Errorf("decode lid poll options: %w", err)
		}
		existing, err := readPollOptions(ctx, ex, pollsTable, destChat, p.msgID)
		if err != nil {
			return err
		}
		merged, err := json.Marshal(mergePollOptions(incoming, existing))
		if err != nil {
			return fmt.Errorf("encode merged poll options: %w", err)
		}
		if err := foldExec(ctx, ex, suffix, lidPollMergeSQL,
			destChat, p.msgID, destSender, p.question, string(merged),
			p.selectableCount, p.createdTS); err != nil {
			return fmt.Errorf("merge lid poll into pn row: %w", err)
		}
	}

	if err := foldExec(ctx, ex, suffix, deleteLIDPollsSQL, lidJID, lidJID); err != nil {
		return fmt.Errorf("delete migrated lid polls: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, deletePurgedPollsSQL); err != nil {
		return fmt.Errorf("suppress destination-only purged polls: %w", err)
	}
	return nil
}

// FoldLIDPollVotes merges poll vote rows and removes LID and purged rows.
func FoldLIDPollVotes(ctx context.Context, ex QueryExecer, suffix, lidJID, pnJID string) error {
	if err := foldExec(ctx, ex, suffix, lidPollVotesMergeSQL,
		lidJID, pnJID, lidJID, pnJID, lidJID, lidJID); err != nil {
		return fmt.Errorf("merge lid poll votes into pn rows: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, deleteLIDPollVotesSQL, lidJID, lidJID); err != nil {
		return fmt.Errorf("delete migrated lid poll votes: %w", err)
	}
	if err := foldExec(ctx, ex, suffix, deletePurgedPollVotesSQL); err != nil {
		return fmt.Errorf("suppress migrated purged poll votes: %w", err)
	}
	return nil
}
