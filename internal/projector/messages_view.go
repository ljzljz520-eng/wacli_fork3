package projector

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
)

// MessagesViewVersion is bumped whenever the messages projection semantics
// change; stored alongside checkpoints so rebuilds after a version change are
// detectable.
const MessagesViewVersion = "messages-projector/1.0.0"

// MessagesView projects ledger events into messages, message_locations and,
// when FTS is available, messages_fts. The view maintains FTS explicitly
// (never via triggers), so the result is fully replayable.
//
// suffix="" targets the active tables; suffix="_shadow" targets independent
// shadow tables. The same view code drives both, which is what makes shadow
// rebuild and atomic promote trustworthy.
type MessagesView struct {
	db       *store.DB
	suffix   string
	withFTS  bool
	identity wa.IdentityMap
	// idMap accumulates identity_resolution events folded by this view; it
	// takes precedence over the externally supplied identity map.
	idMap *viewIdentityMap
}

// NewMessagesView constructs the view.
func NewMessagesView(db *store.DB, suffix string, withFTS bool) *MessagesView {
	return &MessagesView{db: db, suffix: suffix, withFTS: withFTS}
}

// WithIdentityMap supplies a projection-time LID/PN map seed. Mappings folded
// from identity_resolution events during replay take precedence; nil leaves
// unmapped JIDs unchanged.
func (v *MessagesView) WithIdentityMap(m wa.IdentityMap) *MessagesView {
	v.identity = m
	return v
}

// mapJID resolves a JID against the folded identity map first, then the
// external seed.
func (v *MessagesView) mapJID(jid string) string {
	if canonical, ok := v.idMap.Lookup(jid); ok && canonical != "" {
		return canonical
	}
	return wa.MapJID(v.identity, jid)
}

// applyIdentity folds one identity_resolution event: record the mapping, then
// run the message-domain folds (rows, senders, locations; FTS when enabled).
func (v *MessagesView) applyIdentity(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read identity raw record: %w", err)
	}
	res, err := decodeIdentityResolution(raw)
	if err != nil {
		return err
	}
	v.idMap.add(res.LID, res.PN)
	if err := store.FoldLIDMessages(ctx, tx, v.suffix, res.LID, res.PN, v.withFTS); err != nil {
		return err
	}
	if err := store.FoldLIDSenders(ctx, tx, v.suffix, res.LID, res.PN); err != nil {
		return err
	}
	if err := store.FoldLIDLocations(ctx, tx, v.suffix, res.LID, res.PN); err != nil {
		return err
	}
	return nil
}

// Name is the checkpoint key; distinct per suffix so shadow runs never move
// the active checkpoint.
func (v *MessagesView) Name() string { return store.MessagesTable + v.suffix }

// Version identifies the projection semantics.
func (v *MessagesView) Version() string { return MessagesViewVersion }

func (v *MessagesView) msgTable() string     { return store.MessagesTable + v.suffix }
func (v *MessagesView) locTable() string     { return store.MessageLocationsTable + v.suffix }
func (v *MessagesView) ftsTable() string     { return "messages_fts" + v.suffix }
func (v *MessagesView) chatTable() string    { return "chats" + v.suffix }
func (v *MessagesView) contactTable() string { return "contacts" + v.suffix }

// Apply projects one checkpoint batch.
func (v *MessagesView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	if v.idMap == nil {
		v.idMap = newViewIdentityMap()
	}
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceScrub:
			err = v.applyScrub(ctx, tx, evt)
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventMessage:
			err = v.applyMessage(ctx, tx, evt)
		case evt.EventType == ledger.EventDeleteForMe:
			err = v.applyDeleteForMe(ctx, tx, evt)
		case evt.EventType == ledger.EventIdentityResolve:
			err = v.applyIdentity(ctx, tx, evt)
		default:
			// Other event types (star, receipt, chat state, status messages,
			// polls, ...) are projected by their owning views.
		}
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", evt.Seq, evt.EventType, err)
		}
	}
	return nil
}

func (v *MessagesView) applyMessage(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read raw envelope: %w", err)
	}
	chatJID := v.mapJID(evt.ChatJID)
	senderJID := v.mapJID(evt.SenderJID)
	fromMe := evt.Flags&ledger.FlagFromMe != 0

	pm, err := wa.DecodeMessage(wa.DecodeMessageInput{
		Source:    evt.Source,
		Raw:       raw,
		ChatJID:   chatJID,
		SenderJID: senderJID,
		MsgID:     evt.MsgID,
		Timestamp: time.Unix(evt.EventTS, 0).UTC(),
		FromMe:    fromMe,
	})
	if err != nil {
		return err
	}

	chatName := v.lookupChatName(ctx, tx, chatJID)
	senderName := v.resolveSenderName(ctx, tx, senderJID, fromMe, pm.PushName)
	displayText := buildDisplayText(ctx, tx, v.msgTable(), pm)
	if pm.Revoked {
		displayText = store.DeletedMessageDisplayText
	}

	params := store.UpsertMessageParams{
		ChatJID:         chatJID,
		ChatName:        chatName,
		MsgID:           evt.MsgID,
		SenderJID:       senderJID,
		SenderName:      senderName,
		Timestamp:       time.Unix(evt.EventTS, 0).UTC(),
		FromMe:          fromMe,
		Text:            pm.Text,
		DisplayText:     displayText,
		QuotedMsgID:     pm.ReplyToID,
		QuotedSenderJID: pm.ReplyToSenderJID,
		Buttons:         waButtonsToProjectorButtons(pm.Buttons),
		IsForwarded:     pm.IsForwarded,
		ForwardingScore: pm.ForwardingScore,
		ReactionToID:    pm.ReactionToID,
		ReactionEmoji:   pm.ReactionEmoji,
		Edited:          pm.Edited,
		Revoked:         pm.Revoked,
	}
	if pm.Media != nil {
		params.MediaType = pm.Media.Type
		params.MediaCaption = pm.Media.Caption
		params.Filename = pm.Media.Filename
		params.MimeType = pm.Media.MimeType
		params.DirectPath = pm.Media.DirectPath
		params.MediaKey = pm.Media.MediaKey
		params.FileSHA256 = pm.Media.FileSHA256
		params.FileEncSHA256 = pm.Media.FileEncSHA256
		params.FileLength = pm.Media.FileLength
	}
	if err := v.db.UpsertMessageTx(ctx, tx, v.msgTable(), params); err != nil {
		return err
	}

	if pm.Location != nil {
		if err := v.db.UpsertMessageLocationTx(ctx, tx, v.msgTable(),
			chatJID, evt.MsgID, pm.Location.Latitude, pm.Location.Longitude,
			pm.Location.Name, pm.Location.Address, pm.Location.IsLive); err != nil {
			return err
		}
	}

	if v.withFTS {
		return v.syncFTS(ctx, tx, chatJID, evt.MsgID)
	}
	return nil
}

func (v *MessagesView) applyDeleteForMe(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read state event: %w", err)
	}
	state, err := wa.DecodeStateEvent(raw)
	if err != nil {
		return err
	}
	chatJID := v.mapJID(state.Chat)
	if chatJID == "" {
		chatJID = evt.ChatJID
	}
	senderJID := v.mapJID(state.Sender)
	deletedAt := time.UnixMilli(state.TimestampMS)
	if deletedAt.IsZero() {
		deletedAt = time.Unix(evt.EventTS, 0)
	}

	res, err := tx.ExecContext(ctx,
		store.RetargetQuery(store.StoredbMarkMessageDeletedForMeSQL, v.msgTable()),
		sql.NullInt64{Int64: deletedAt.Unix(), Valid: true},
		sql.NullString{String: store.MessageDeletionReasonWhatsAppDeleteForMe, Valid: true},
		strings.TrimSpace(chatJID), strings.TrimSpace(evt.MsgID),
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Row did not exist yet: fall back to a delete-for-me tombstone,
		// matching store.MarkMessageDeletedForMe.
		if err := v.db.UpsertMessageTx(ctx, tx, v.msgTable(), store.UpsertMessageParams{
			ChatJID:      chatJID,
			MsgID:        evt.MsgID,
			SenderJID:    senderJID,
			Timestamp:    deletedAt,
			FromMe:       state.FromMe,
			DeletedForMe: true,
			DeletedAt:    deletedAt,
		}); err != nil {
			return err
		}
	}
	if v.withFTS {
		return v.syncFTS(ctx, tx, chatJID, evt.MsgID)
	}
	return nil
}

func (v *MessagesView) applyScrub(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	now := time.Now().Unix()
	if _, err := tx.ExecContext(ctx, `
		UPDATE `+v.msgTable()+` SET
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
			buttons = NULL,
			deleted_at = COALESCE(deleted_at, ?),
			payload_purged_at = COALESCE(payload_purged_at, ?)
		WHERE chat_jid = ? AND msg_id = ?`,
		now, now, strings.TrimSpace(evt.ChatJID), strings.TrimSpace(evt.MsgID)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM `+v.locTable()+` WHERE chat_jid = ? AND msg_id = ?`,
		strings.TrimSpace(evt.ChatJID), strings.TrimSpace(evt.MsgID)); err != nil {
		return err
	}
	if v.withFTS {
		_, err := tx.ExecContext(ctx,
			`DELETE FROM `+v.ftsTable()+` WHERE rowid = (
				SELECT rowid FROM `+v.msgTable()+` WHERE chat_jid = ? AND msg_id = ?)`,
			strings.TrimSpace(evt.ChatJID), strings.TrimSpace(evt.MsgID))
		return err
	}
	return nil
}

// syncFTS maintains the FTS row for one message explicitly: delete then
// re-insert only when the message is not tombstoned. This replaces the
// messages_ai/ad/au triggers during projection, so FTS is reproducible.
func (v *MessagesView) syncFTS(ctx context.Context, tx *sql.Tx, chatJID, msgID string) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM `+v.ftsTable()+` WHERE rowid = (
			SELECT rowid FROM `+v.msgTable()+` WHERE chat_jid = ? AND msg_id = ?)`,
		strings.TrimSpace(chatJID), strings.TrimSpace(msgID)); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO `+v.ftsTable()+`(rowid, text, media_caption, filename, chat_name, sender_name, display_text)
		SELECT rowid,
			COALESCE(text, ''),
			COALESCE(media_caption, ''),
			COALESCE(filename, ''),
			COALESCE(chat_name, ''),
			COALESCE(sender_name, ''),
			COALESCE(display_text, '')
		FROM `+v.msgTable()+`
		WHERE chat_jid = ? AND msg_id = ? AND deleted_at IS NULL`,
		strings.TrimSpace(chatJID), strings.TrimSpace(msgID))
	return err
}

func (v *MessagesView) lookupChatName(ctx context.Context, tx *sql.Tx, chatJID string) string {
	var name sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT name FROM `+v.chatTable()+` WHERE jid = ?`,
		strings.TrimSpace(chatJID)).Scan(&name)
	if err != nil {
		return ""
	}
	return name.String
}

// resolveSenderName mirrors storeParsedMessage: "me" for own messages,
// otherwise the best contacts-table name with a push-name fallback.
func (v *MessagesView) resolveSenderName(ctx context.Context, tx *sql.Tx, senderJID string, fromMe bool, pushName string) string {
	if fromMe {
		return "me"
	}
	if strings.TrimSpace(senderJID) != "" {
		var push, full, first, business sql.NullString
		err := tx.QueryRowContext(ctx, `
			SELECT push_name, full_name, first_name, business_name
			FROM `+v.contactTable()+` WHERE jid = ?`,
			strings.TrimSpace(senderJID),
		).Scan(&push, &full, &first, &business)
		if err == nil {
			for _, n := range []sql.NullString{full, first, business} {
				if s := strings.TrimSpace(n.String); s != "" {
					return s
				}
			}
			if s := strings.TrimSpace(push.String); s != "" && s != "-" {
				return s
			}
		}
	}
	if s := strings.TrimSpace(pushName); s != "" && s != "-" {
		return s
	}
	return ""
}

func waButtonsToProjectorButtons(buttons []wa.Button) []store.Button {
	if len(buttons) == 0 {
		return nil
	}
	out := make([]store.Button, len(buttons))
	for i, b := range buttons {
		out[i] = store.Button{
			Type:         b.Type,
			DisplayText:  b.DisplayText,
			ID:           b.ID,
			URL:          b.URL,
			PhoneNumber:  b.PhoneNumber,
			Description:  b.Description,
			ResponseType: b.ResponseType,
			Index:        b.Index,
		}
	}
	return out
}
