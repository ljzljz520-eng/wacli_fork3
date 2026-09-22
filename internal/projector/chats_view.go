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
	"go.mau.fi/whatsmeow/types"
)

// ChatsViewVersion is bumped when the chats projection semantics change.
const ChatsViewVersion = "chats-projector/1.0.0"

// ChatsView projects ledger events into chats. It owns the whole row:
// kind/name/last_message_ts come from message events; archived/pinned/
// muted_until from app-state events; unread/unread_count from a seq-ordered
// fold of explicit history snapshots, mark-read events, read-self receipts
// and live incoming messages.
//
// suffix="" targets the active table; suffix="_shadow" targets the shadow.
type ChatsView struct {
	db       *store.DB
	suffix   string
	identity wa.IdentityMap
	idMap    *viewIdentityMap
}

// NewChatsView constructs the view.
func NewChatsView(db *store.DB, suffix string) *ChatsView {
	return &ChatsView{db: db, suffix: suffix}
}

// WithIdentityMap supplies a projection-time LID/PN map seed; mappings folded
// from identity_resolution events during replay take precedence.
func (v *ChatsView) WithIdentityMap(m wa.IdentityMap) *ChatsView {
	v.identity = m
	return v
}

// mapJID resolves a JID against the folded identity map first, then the
// external seed.
func (v *ChatsView) mapJID(jid string) string {
	if canonical, ok := v.idMap.Lookup(jid); ok && canonical != "" {
		return canonical
	}
	return wa.MapJID(v.identity, jid)
}

// applyIdentity folds one identity_resolution event into chats: merge the LID
// chat row into the PN row, then remove the LID row. The "create PN chat from
// latest lid message" step of the active migration is unnecessary here
// because the projector upserted the chats row while projecting the message.
func (v *ChatsView) applyIdentity(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read identity raw record: %w", err)
	}
	res, err := decodeIdentityResolution(raw)
	if err != nil {
		return err
	}
	v.idMap.add(res.LID, res.PN)
	if err := store.FoldLIDChats(ctx, tx, v.suffix, res.LID, res.PN); err != nil {
		return err
	}
	return store.DeleteLIDChatTarget(ctx, tx, v.suffix, res.LID)
}

// Name is the checkpoint key; distinct per suffix.
func (v *ChatsView) Name() string { return store.ChatsTable + v.suffix }

// Version identifies the projection semantics.
func (v *ChatsView) Version() string { return ChatsViewVersion }

func (v *ChatsView) table() string        { return store.ChatsTable + v.suffix }
func (v *ChatsView) contactTable() string { return store.ContactsTable + v.suffix }
func (v *ChatsView) groupTable() string   { return store.GroupsTable + v.suffix }

// Apply projects one checkpoint batch.
func (v *ChatsView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	if v.idMap == nil {
		v.idMap = newViewIdentityMap()
	}
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventMessage:
			err = v.applyMessage(ctx, tx, evt)
		case evt.EventType == ledger.EventMarkRead:
			err = v.applyMarkRead(ctx, tx, evt)
		case evt.EventType == ledger.EventReceipt:
			err = v.applyReceipt(ctx, tx, evt)
		case evt.EventType == ledger.EventCall:
			err = v.applyCallChat(ctx, tx, evt)
		case evt.EventType == ledger.EventArchive,
			evt.EventType == ledger.EventPin,
			evt.EventType == ledger.EventMute:
			err = v.applyChatState(ctx, tx, evt)
		case evt.EventType == ledger.EventIdentityResolve:
			err = v.applyIdentity(ctx, tx, evt)
		default:
			// Other event types are projected by their owning views.
		}
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", evt.Seq, evt.EventType, err)
		}
	}
	return nil
}

// applyMessage merges kind/name/last_message_ts exactly like the old
// storeParsedMessage path, then applies the live unread increment rule.
func (v *ChatsView) applyMessage(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	chatJID := v.mapJID(evt.ChatJID)
	chat, err := types.ParseJID(chatJID)
	if err != nil || chat.IsEmpty() {
		return fmt.Errorf("invalid chat jid %q", chatJID)
	}
	// Status broadcast messages never create a chats row.
	if chat == types.StatusBroadcastJID {
		return nil
	}

	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read raw envelope: %w", err)
	}
	fromMe := evt.Flags&ledger.FlagFromMe != 0
	pm, err := wa.DecodeMessage(wa.DecodeMessageInput{
		Source:    evt.Source,
		Raw:       raw,
		ChatJID:   chatJID,
		SenderJID: v.mapJID(evt.SenderJID),
		MsgID:     evt.MsgID,
		Timestamp: time.Unix(evt.EventTS, 0).UTC(),
		FromMe:    fromMe,
	})
	if err != nil {
		return err
	}
	name := v.resolveChatName(ctx, tx, chat, pm.PushName)
	query := store.RetargetTable(storedb.UpsertChatSQL, store.ChatsTable, v.table())
	if _, err := tx.ExecContext(ctx, query,
		strings.TrimSpace(chatJID),
		projectorChatKind(chat),
		sql.NullString{String: name, Valid: name != ""},
		sql.NullInt64{Int64: evt.EventTS, Valid: evt.EventTS > 0},
	); err != nil {
		return fmt.Errorf("upsert chat: %w", err)
	}

	if shouldIncrementUnread(evt.Source, fromMe, chat) {
		return v.incrementUnread(ctx, tx, chatJID, projectorChatKind(chat))
	}
	return nil
}

// applyMarkRead folds history unread snapshots and local/remote mark-read
// app-state events.
func (v *ChatsView) applyMarkRead(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
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
	chat, err := types.ParseJID(chatJID)
	if err != nil || chat.IsEmpty() {
		return fmt.Errorf("invalid chat jid %q", chatJID)
	}
	switch state.Type {
	case ledger.StateUnreadCount:
		count := state.Count
		if count < 0 {
			count = 0
		}
		return v.setUnread(ctx, tx, chatJID, projectorChatKind(chat), 1, count)
	case ledger.StateMarkRead:
		if state.State {
			// Read: marker and count both clear.
			return v.setUnread(ctx, tx, chatJID, projectorChatKind(chat), 0, 0)
		}
		// Explicit mark-unread keeps a count when one was already known, but
		// never presents marker=1 with count=0.
		count := v.currentUnreadCount(ctx, tx, chatJID)
		if count < 1 {
			count = 1
		}
		return v.setUnread(ctx, tx, chatJID, projectorChatKind(chat), 1, count)
	default:
		return fmt.Errorf("unknown mark_read state type %q", state.Type)
	}
}

// applyReceipt clears unread on read-self receipts, mirroring the old
// handleReceiptEvent path.
func (v *ChatsView) applyReceipt(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read receipt: %w", err)
	}
	receipt, err := wa.DecodeReceipt(raw)
	if err != nil {
		return err
	}
	if receipt.Type != string(types.ReceiptTypeReadSelf) {
		return nil
	}
	chatJID := v.mapJID(receipt.Chat)
	chat, err := types.ParseJID(chatJID)
	if err != nil || chat.IsEmpty() {
		return fmt.Errorf("invalid chat jid %q", chatJID)
	}
	return v.setUnread(ctx, tx, chatJID, projectorChatKind(chat), 0, 0)
}

// applyCallChat creates/updates the chats row for a call event, mirroring
// the UpsertChat in the old storeParsedCallEvent path. Delete markers are
// skipped; call_events rows are removed separately.
func (v *ChatsView) applyCallChat(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read call chat: %w", err)
	}
	var c struct {
		ChatJID  string `json:"chat_jid"`
		ChatName string `json:"chat_name,omitempty"`
		TS       int64  `json:"ts,omitempty"`
		Deleted  bool   `json:"deleted,omitempty"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("unmarshal call chat: %w", err)
	}
	if c.Deleted || c.ChatJID == "" {
		return nil
	}
	chatJID := v.mapJID(c.ChatJID)
	chat, err := types.ParseJID(chatJID)
	if err != nil || chat.IsEmpty() {
		return fmt.Errorf("invalid call chat jid %q", chatJID)
	}
	query := store.RetargetTable(storedb.UpsertChatSQL, store.ChatsTable, v.table())
	_, err = tx.ExecContext(ctx, query,
		chatJID,
		projectorChatKind(chat),
		sql.NullString{String: c.ChatName, Valid: strings.TrimSpace(c.ChatName) != ""},
		sql.NullInt64{Int64: c.TS, Valid: c.TS > 0},
	)
	return err
}

// applyChatState sets archive/pin/mute columns from app-state events.
func (v *ChatsView) applyChatState(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read chat state: %w", err)
	}
	state, err := wa.DecodeStateEvent(raw)
	if err != nil {
		return err
	}
	chatJID := v.mapJID(state.Chat)
	if chatJID == "" {
		chatJID = evt.ChatJID
	}
	chat, err := types.ParseJID(chatJID)
	if err != nil || chat.IsEmpty() {
		return fmt.Errorf("invalid chat jid %q", chatJID)
	}
	kind := projectorChatKind(chat)
	var col string
	var val int64
	switch evt.EventType {
	case ledger.EventArchive:
		col, val = "archived", boolInt(state.State)
	case ledger.EventPin:
		col, val = "pinned", boolInt(state.State)
	case ledger.EventMute:
		col, val = "muted_until", state.EndTSMS
	}
	query := `INSERT INTO ` + v.table() + `(jid, kind, ` + col + `)
		VALUES(?, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET ` + col + ` = excluded.` + col
	_, err = tx.ExecContext(ctx, query, strings.TrimSpace(chatJID), kind, val)
	return err
}

func (v *ChatsView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	var snap legacySnapshotPayload
	if err := json.Unmarshal(evt.Snapshot, &snap); err != nil {
		return fmt.Errorf("unmarshal legacy snapshot: %w", err)
	}
	if snap.Table != store.ChatsTable {
		return nil
	}
	row := snap.Row
	marker := coalesceInt64(row, "unread")
	count := coalesceInt64(row, "unread_count")
	if marker != 0 {
		marker = 1
		if count < 1 {
			count = 1
		}
	} else {
		count = 0
	}
	cols := []string{
		"jid", "kind", "name", "last_message_ts",
		"archived", "pinned", "muted_until", "unread", "unread_count",
	}
	args := []any{
		rowString(row, "jid"),
		rowString(row, "kind"),
		rowNullString(row, "name"),
		rowNullInt64(row, "last_message_ts"),
		coalesceInt64(row, "archived"),
		coalesceInt64(row, "pinned"),
		coalesceInt64(row, "muted_until"),
		marker,
		count,
	}
	query := `INSERT INTO ` + v.table() + ` (` + joinCols(cols) + `)
		VALUES (` + placeholders(len(cols)) + `)
		ON CONFLICT(jid) DO UPDATE SET
			kind = excluded.kind,
			name = excluded.name,
			last_message_ts = excluded.last_message_ts,
			archived = excluded.archived,
			pinned = excluded.pinned,
			muted_until = excluded.muted_until,
			unread = excluded.unread,
			unread_count = excluded.unread_count`
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert legacy chat: %w", err)
	}
	return nil
}

// incrementUnread mirrors store.IncrementChatUnread.
func (v *ChatsView) incrementUnread(ctx context.Context, tx *sql.Tx, chatJID, kind string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO `+v.table()+`(jid, kind, unread, unread_count)
		VALUES(?, ?, 1, 1)
		ON CONFLICT(jid) DO UPDATE SET
			unread = 1,
			unread_count = COALESCE(unread_count, 0) + 1`,
		strings.TrimSpace(chatJID), kind)
	return err
}

// setUnread writes the explicit marker/count pair, inserting a minimal row
// when needed (history snapshots can precede message events).
func (v *ChatsView) setUnread(ctx context.Context, tx *sql.Tx, chatJID, kind string, marker, count int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO `+v.table()+`(jid, kind, unread, unread_count)
		VALUES(?, ?, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET
			unread = excluded.unread,
			unread_count = excluded.unread_count`,
		strings.TrimSpace(chatJID), kind, marker, count)
	return err
}

func (v *ChatsView) currentUnreadCount(ctx context.Context, tx *sql.Tx, chatJID string) int64 {
	var count sql.NullInt64
	err := tx.QueryRowContext(ctx,
		`SELECT unread_count FROM `+v.table()+` WHERE jid = ?`,
		strings.TrimSpace(chatJID)).Scan(&count)
	if err != nil {
		return 0
	}
	return count.Int64
}

// resolveChatName mirrors wa.ResolveChatName against the projected tables:
// group/broadcast subject, contact best name, message push name, and finally
// the JID itself (the online fallback).
func (v *ChatsView) resolveChatName(ctx context.Context, tx *sql.Tx, chat types.JID, pushName string) string {
	switch {
	case chat.Server == types.GroupServer || chat.IsBroadcastList():
		var name sql.NullString
		err := tx.QueryRowContext(ctx,
			`SELECT name FROM `+v.groupTable()+` WHERE jid = ?`,
			chat.String()).Scan(&name)
		if err == nil && strings.TrimSpace(name.String) != "" {
			return strings.TrimSpace(name.String)
		}
	case chat.Server == types.DefaultUserServer:
		var push, full, first, business sql.NullString
		err := tx.QueryRowContext(ctx, `
			SELECT push_name, full_name, first_name, business_name
			FROM `+v.contactTable()+` WHERE jid = ?`,
			chat.ToNonAD().String()).Scan(&push, &full, &first, &business)
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
	return chat.String()
}

// shouldIncrementUnread mirrors the live unread rule: a new live, non-own
// message outside the status broadcast list. Duplicate live deliveries of the
// same WhatsApp message collapse at append time; history replays carry a
// different source and never increment (the explicit history snapshot sets
// the count instead).
func shouldIncrementUnread(source string, fromMe bool, chat types.JID) bool {
	if source != ledger.SourceLive || fromMe {
		return false
	}
	return !(chat.Server == types.BroadcastServer && chat.User == types.StatusBroadcastJID.User)
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// projectorChatKind mirrors internal/app.chatKind without importing app.
func projectorChatKind(chat types.JID) string {
	switch {
	case chat.Server == types.NewsletterServer:
		return "newsletter"
	case chat.Server == types.GroupServer:
		return "group"
	case chat.IsBroadcastList():
		return "broadcast"
	case chat.Server == types.DefaultUserServer:
		return "dm"
	default:
		return "unknown"
	}
}
