package store

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"sync"

	"github.com/openclaw/wacli/internal/store/storedb"
)

// Target table names understood by the tx projection methods.
const (
	MessagesTable                 = "messages"
	MessageLocationsTable         = "message_locations"
	MessagePayloadPurges          = "message_payload_purges"
	MessageLocalMediaAliasesTable = "message_local_media_aliases"
	ChatsTable                    = "chats"
	ContactsTable                 = "contacts"
	GroupsTable                   = "groups"
)

var (
	retargetCache sync.Map // map[string]string keyed by query|target

	wordMessages = regexp.MustCompile(`\bmessages\b`)
	wordPurges   = regexp.MustCompile(`\bmessage_payload_purges\b`)
)

// StoredbMarkMessageDeletedForMeSQL re-exports the generated mark statement
// for use by views in other packages.
const StoredbMarkMessageDeletedForMeSQL = storedb.MarkMessageDeletedForMeSQL

// RetargetQuery rewrites whole-word table references in a generated query for
// a shadow target. target="messages" returns the query unchanged.
func RetargetQuery(query, target string) string {
	return retargetQuery(query, target)
}

// retargetQuery rewrites whole-word table references in a generated query for
// a shadow target. target="messages" returns the query unchanged.
func retargetQuery(query, target string) string {
	if target == "" || target == MessagesTable {
		return query
	}
	key := query + "|" + target
	if v, ok := retargetCache.Load(key); ok {
		return v.(string)
	}
	purgeTarget := MessagePayloadPurges + strings.TrimPrefix(target, MessagesTable)
	out := wordMessages.ReplaceAllString(query, target)
	out = wordPurges.ReplaceAllString(out, purgeTarget)
	retargetCache.Store(key, out)
	return out
}

// RetargetTable rewrites whole-word references to oldTable in a query to
// newTable. newTable="" or equal to oldTable returns the query unchanged.
// It is the generic variant of RetargetQuery for non-message queries.
func RetargetTable(query, oldTable, newTable string) string {
	if newTable == "" || newTable == oldTable {
		return query
	}
	key := query + "|" + oldTable + "|" + newTable
	if v, ok := retargetCache.Load(key); ok {
		return v.(string)
	}
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(oldTable) + `\b`)
	out := re.ReplaceAllString(query, newTable)
	retargetCache.Store(key, out)
	return out
}

// UpsertMessageTx runs the message merge inside an existing transaction,
// optionally against a shadow table. It is the tx-safe twin of UpsertMessage.
func (d *DB) UpsertMessageTx(ctx context.Context, tx *sql.Tx, target string, p UpsertMessageParams) error {
	sqlp := buildStoredbUpsertParams(p)
	query := retargetQuery(storedb.UpsertMessageSQL, target)
	_, err := tx.ExecContext(ctx, query,
		sqlp.ChatJid, sqlp.ChatName, sqlp.MsgID, sqlp.SenderJid, sqlp.SenderName,
		sqlp.Ts, sqlp.FromMe, sqlp.Text, sqlp.DisplayText, sqlp.QuotedMsgID,
		sqlp.QuotedSenderJid, sqlp.IsForwarded, sqlp.ForwardingScore,
		sqlp.ReactionToID, sqlp.ReactionEmoji, sqlp.MediaType, sqlp.MediaCaption,
		sqlp.Filename, sqlp.MimeType, sqlp.DirectPath, sqlp.MediaKey,
		sqlp.FileSha256, sqlp.FileEncSha256, sqlp.FileLength, sqlp.Revoked,
		sqlp.DeletedForMe, sqlp.DeletedAt, sqlp.DeletionReason, sqlp.Edited,
		sqlp.EditedTs, sqlp.Buttons, sqlp.ChatJid_2, sqlp.MsgID_2,
	)
	return err
}

// UpsertMessageLocationTx upserts a location inside an existing transaction.
func (d *DB) UpsertMessageLocationTx(ctx context.Context, tx *sql.Tx, target string,
	chatJID, msgID string, latitude, longitude float64, name, address string, isLive bool) error {
	sqlp := struct {
		Name    sql.NullString
		Address sql.NullString
		IsLive  int64
	}{
		Name:    nullString(name),
		Address: nullString(address),
		IsLive:  boolToInt64(isLive),
	}
	query := retargetQuery(storedb.UpsertMessageLocationSQL, locationTarget(target))
	_, err := tx.ExecContext(ctx, query,
		chatJID, msgID, latitude, longitude, sqlp.Name, sqlp.Address, sqlp.IsLive,
		strings.TrimSpace(chatJID), strings.TrimSpace(msgID),
	)
	return err
}

func locationTarget(messagesTarget string) string {
	if messagesTarget == "" || messagesTarget == MessagesTable {
		return MessageLocationsTable
	}
	return MessageLocationsTable + strings.TrimPrefix(messagesTarget, MessagesTable)
}

// MessageRowidTx returns the rowid of a message in the target table.
func (d *DB) MessageRowidTx(ctx context.Context, tx *sql.Tx, target, chatJID, msgID string) (int64, bool, error) {
	table := MessagesTable
	if target != "" {
		table = target
	}
	var rowid int64
	err := tx.QueryRowContext(ctx,
		`SELECT rowid FROM `+table+` WHERE chat_jid = ? AND msg_id = ?`,
		strings.TrimSpace(chatJID), strings.TrimSpace(msgID),
	).Scan(&rowid)
	if errorsIsNoRows(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return rowid, true, nil
}

// MessageDisplayTx reads display_text/text/media_type for a message in the
// target table inside tx. Projectors use it to compose reaction/quoted display
// text without leaving the projection transaction.
func (d *DB) MessageDisplayTx(ctx context.Context, tx *sql.Tx, target, chatJID, msgID string) (display, text, mediaType string, err error) {
	table := MessagesTable
	if target != "" {
		table = target
	}
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(display_text,''), COALESCE(text,''), COALESCE(media_type,'')
		 FROM `+table+` WHERE chat_jid = ? AND msg_id = ?`,
		strings.TrimSpace(chatJID), strings.TrimSpace(msgID),
	).Scan(&display, &text, &mediaType)
	if errorsIsNoRows(err) {
		return "", "", "", nil
	}
	return display, text, mediaType, err
}

func errorsIsNoRows(err error) bool { return err == sql.ErrNoRows }
