package app

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// ledgerEnabled reports whether ledger ingestion is active. WACLI_LEDGER is on
// by default; set it to off/0/false to restore legacy-only writes.
func ledgerEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("WACLI_LEDGER"))) {
	case "off", "0", "false", "no":
		return false
	default:
		return true
	}
}

// ledgerBatch returns the process batch ID for a source.
func (a *App) ledgerBatch(source string) (int64, error) {
	if v, ok := a.ledgerBatches.Load(source); ok {
		return v.(int64), nil
	}
	id, err := a.db.StartBatch(source)
	if err != nil {
		return 0, err
	}
	actual, _ := a.ledgerBatches.LoadOrStore(source, id)
	return actual.(int64), nil
}

// ingestMessageEvent appends the ledger event for one parsed message. Called
// from storeParsedMessage after chat/sender canonicalization and before any
// materialized write, so a ledger failure prevents the view write.
func (a *App) ingestMessageEvent(ctx context.Context, chatJID, senderJID string, pm wa.ParsedMessage) error {
	if !ledgerEnabled() || pm.RawEnvelope == nil {
		return nil
	}
	envelope, ok := pm.RawEnvelope.(proto.Message)
	if !ok {
		return nil
	}
	rawBytes, rawHash, err := ledger.HashProto(envelope)
	if err != nil {
		return fmt.Errorf("hash message envelope: %w", err)
	}

	participant := ""
	if pm.Chat.Server == types.GroupServer && !pm.FromMe {
		participant = senderJID
	}
	waKey := ledger.WAKey(chatJID, pm.FromMe, pm.ID, participant)
	dk := ledger.DedupKey("message", waKey)

	var refs []ledger.CausalRef
	if pm.ReplyToID != "" {
		quotedSender := a.canonicalStoreJIDString(ctx, pm.ReplyToSenderJID)
		refs = append(refs, ledger.WAKeyRef(chatJID, pm.ReplyToID, quotedSender, false))
	}
	if pm.ReactionToID != "" && pm.ReactionToID != pm.ReplyToID {
		refs = append(refs, ledger.WAKeyRef(chatJID, pm.ReactionToID, "", false))
	}

	eventType := ledger.EventMessage
	if pm.Chat == types.StatusBroadcastJID {
		eventType = ledger.EventStatusMessage
	}
	source := pm.IngestSource
	if !ledger.IsKnownSource(source) {
		source = ledger.SourceLive
	}
	batchID, err := a.ledgerBatch(source)
	if err != nil {
		return err
	}

	flags := int64(0)
	if pm.FromMe {
		flags |= ledger.FlagFromMe
	}
	if pm.FromFullSync {
		flags |= ledger.FlagFromFullSync
	}
	serverTS := pm.Timestamp.Unix()
	evt := &ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        source,
		EventType:     eventType,
		WAKey:         waKey,
		ChatJID:       chatJID,
		MsgID:         pm.ID,
		SenderJID:     senderJID,
		ServerTS:      serverTS,
		EventTS:       serverTS,
		ReceivedAt:    nowUTC().Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		CausalRefs:    refs,
		BatchID:       batchID,
		Flags:         flags,
		RawBytes:      rawBytes,
	}
	_, _, err = a.db.AppendEvent(evt)
	return err
}

// ingestReceiptEvent appends the ledger event for one delivery receipt.
func (a *App) ingestReceiptEvent(ctx context.Context, receipt *events.Receipt) error {
	if !ledgerEnabled() || receipt == nil {
		return nil
	}
	chat := a.canonicalStoreJID(ctx, receipt.Chat)
	canonical := ledger.NewCanonicalReceipt(
		string(receipt.Type), chat.String(), receipt.Sender.String(),
		receipt.MessageSender.String(), receipt.MessageIDs, receipt.Timestamp,
	)
	rawBytes, rawHash, err := ledger.HashCanonical(canonical)
	if err != nil {
		return fmt.Errorf("hash receipt: %w", err)
	}
	ids := append([]string(nil), receipt.MessageIDs...)
	sortedIDs := append([]string(nil), ids...)
	sortStrings(sortedIDs)
	dk := ledger.DedupKey("receipt", append([]string{
		string(receipt.Type), chat.String(), receipt.Sender.String(),
		receipt.MessageSender.String(), strconv.FormatInt(receipt.Timestamp.UnixMilli(), 10),
	}, sortedIDs...)...)

	refs := make([]ledger.CausalRef, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, ledger.WAKeyRef(chat.String(), id, receipt.MessageSender.String(), false))
	}
	batchID, err := a.ledgerBatch(ledger.SourceReceipt)
	if err != nil {
		return err
	}
	evt := &ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceReceipt,
		EventType:     ledger.EventReceipt,
		ChatJID:       chat.String(),
		ServerTS:      receipt.Timestamp.Unix(),
		EventTS:       receipt.Timestamp.Unix(),
		ReceivedAt:    nowUTC().Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		CausalRefs:    refs,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	}
	_, _, err = a.db.AppendEvent(evt)
	return err
}

// ingestUnreadStateEvent appends the conversation-level unread state carried
// by a history sync: an explicit count snapshot, an explicit mark-unread, or
// an explicit read. Called before the conversation's messages are projected,
// matching the old execution order.
func (a *App) ingestUnreadStateEvent(ctx context.Context, chatJID string, count int, markedUnread bool) error {
	// The conversation snapshot carries no authoritative timestamp; zero keeps
	// repeated snapshots (full syncs/recovery overlaps) byte-identical so they
	// collapse through dedup.
	state := ledger.StateEvent{Chat: chatJID}
	switch {
	case count > 0:
		state.Type = ledger.StateUnreadCount
		state.Count = int64(count)
	case markedUnread:
		state.Type = ledger.StateMarkRead
		state.State = false
	default:
		state.Type = ledger.StateMarkRead
		state.State = true
	}
	rawBytes, rawHash, err := ledger.HashCanonical(state)
	if err != nil {
		return fmt.Errorf("hash unread state: %w", err)
	}
	dk := ledger.DedupKey("unread", chatJID,
		strconv.FormatInt(int64(count), 10), strconv.FormatBool(markedUnread))
	batchID, err := a.ledgerBatch(ledger.SourceHistory)
	if err != nil {
		return err
	}
	_, _, err = a.db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceHistory,
		EventType:     ledger.EventMarkRead,
		ChatJID:       chatJID,
		ReceivedAt:    nowUTC().Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	return err
}

// ingestContactEvent appends an observed contact snapshot before a direct
// UpsertContact write (live message side effects, explicit refresh).
func (a *App) ingestContactEvent(ctx context.Context, jid, phone, pushName, fullName, firstName, businessName string) error {
	c := ledger.CanonicalContact{
		JID: strings.TrimSpace(jid), Phone: phone, PushName: pushName,
		FullName: fullName, FirstName: firstName, BusinessName: businessName,
	}
	rawBytes, rawHash, err := ledger.HashCanonical(c)
	if err != nil {
		return fmt.Errorf("hash contact: %w", err)
	}
	dk := ledger.DedupKey("contact", c.JID, c.Phone, c.PushName,
		c.FullName, c.FirstName, c.BusinessName)
	batchID, err := a.ledgerBatch(ledger.SourceLive)
	if err != nil {
		return err
	}
	now := nowUTC().Unix()
	_, _, err = a.db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceLive,
		EventType:     ledger.EventContact,
		ChatJID:       c.JID,
		ServerTS:      now,
		EventTS:       now,
		ReceivedAt:    now,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	return err
}

// ingestSystemNameEvent appends a phone-book name assignment before the
// direct SetSystemName write. System names ride a contact event; the
// projector applies them as explicit (overwriting) assignments.
func (a *App) ingestSystemNameEvent(ctx context.Context, jid, systemName string) error {
	c := ledger.CanonicalContact{JID: strings.TrimSpace(jid), SystemName: systemName}
	rawBytes, rawHash, err := ledger.HashCanonical(c)
	if err != nil {
		return fmt.Errorf("hash system name: %w", err)
	}
	dk := ledger.DedupKey("system_name", c.JID, c.SystemName)
	batchID, err := a.ledgerBatch(ledger.SourceSystemContacts)
	if err != nil {
		return err
	}
	now := nowUTC().Unix()
	_, _, err = a.db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceSystemContacts,
		EventType:     ledger.EventContact,
		ChatJID:       c.JID,
		ServerTS:      now,
		EventTS:       now,
		ReceivedAt:    now,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	return err
}

// ingestSystemNamesClearEvent appends the bulk clear of phone-book names.
func (a *App) ingestSystemNamesClearEvent(ctx context.Context) error {
	now := nowUTC().Unix()
	payload := struct {
		Type string `json:"type"`
		TS   int64  `json:"ts"`
	}{Type: ledger.EventSystemNamesClear, TS: now}
	rawBytes, rawHash, err := ledger.HashCanonical(payload)
	if err != nil {
		return fmt.Errorf("hash system names clear: %w", err)
	}
	dk := ledger.DedupKey("clear_system_names", strconv.FormatInt(now, 10))
	batchID, err := a.ledgerBatch(ledger.SourceSystemContacts)
	if err != nil {
		return err
	}
	_, _, err = a.db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceSystemContacts,
		EventType:     ledger.EventSystemNamesClear,
		ServerTS:      now,
		EventTS:       now,
		ReceivedAt:    now,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	return err
}

// UpsertContactWithLedger ingests the snapshot then performs the direct
// best-effort merge; used from message and refresh paths.
func (a *App) UpsertContactWithLedger(ctx context.Context, jid, phone, pushName, fullName, firstName, businessName string) {
	if ledgerEnabled() {
		if err := a.ingestContactEvent(ctx, jid, phone, pushName, fullName, firstName, businessName); err != nil {
			a.emitWarning("contact_ledger_failed",
				fmt.Sprintf("warning: failed to append contact event for %s: %v", jid, err),
				map[string]any{"jid": jid, "error": err.Error()})
		}
	}
	_ = a.db.UpsertContact(jid, phone, pushName, fullName, firstName, businessName)
}

// SetSystemNameWithLedger appends the phone-book assignment then writes it
// directly. Returns "contact not found" like SetSystemName.
func (a *App) SetSystemNameWithLedger(ctx context.Context, jid, systemName string) error {
	if err := a.ingestSystemNameEvent(ctx, jid, systemName); err != nil {
		return err
	}
	return a.db.SetSystemName(jid, systemName)
}

// ClearSystemNamesWithLedger appends the bulk-clear event then applies it.
func (a *App) ClearSystemNamesWithLedger(ctx context.Context) (int64, error) {
	if err := a.ingestSystemNamesClearEvent(ctx); err != nil {
		return 0, err
	}
	return a.db.ClearAllSystemNames()
}

func sortStrings(s []string) {
	// Avoid importing sort solely for one call site.
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// ingestAppStateEvent appends the ledger event for one app-state persistence
// event. Repeated calls (immediate persist + ordered replay) collapse via the
// deterministic dedup key.
func (a *App) ingestAppStateEvent(ctx context.Context, evt any) error {
	if !ledgerEnabled() {
		return nil
	}

	var (
		canonical ledger.StateEvent
		dk        string
		eventType string
		causalRef *ledger.CausalRef
	)
	tsMS := int64(0)

	switch v := evt.(type) {
	case *events.Star:
		if v == nil || v.Action == nil {
			return nil
		}
		chat := a.canonicalStoreJID(ctx, v.ChatJID).String()
		sender := ""
		if !v.SenderJID.IsEmpty() {
			sender = a.canonicalStoreJID(ctx, v.SenderJID).String()
		}
		tsMS = v.Timestamp.UnixMilli()
		canonical = ledger.StateEvent{
			Type: ledger.StateStar, Chat: chat, Sender: sender, FromMe: v.IsFromMe,
			MsgID: v.MessageID, State: v.Action.GetStarred(), TimestampMS: tsMS,
		}
		dk = ledger.DedupKey("star", chat, sender, strconv.FormatBool(v.IsFromMe),
			v.MessageID, strconv.FormatInt(tsMS, 10), strconv.FormatBool(v.Action.GetStarred()))
		eventType = ledger.EventStar
		ref := ledger.WAKeyRef(chat, v.MessageID, sender, v.IsFromMe)
		causalRef = &ref

	case *events.DeleteForMe:
		if v == nil {
			return nil
		}
		chat := a.canonicalStoreJID(ctx, v.ChatJID).String()
		sender := ""
		if !v.SenderJID.IsEmpty() {
			sender = a.canonicalStoreJID(ctx, v.SenderJID).String()
		}
		tsMS = v.Timestamp.UnixMilli()
		deleteMedia := v.Action.GetDeleteMedia()
		canonical = ledger.StateEvent{
			Type: ledger.StateDeleteForMe, Chat: chat, Sender: sender, FromMe: v.IsFromMe,
			MsgID: v.MessageID, DeleteMedia: deleteMedia, TimestampMS: tsMS,
		}
		dk = ledger.DedupKey("delete_for_me", chat, sender, strconv.FormatBool(v.IsFromMe),
			v.MessageID, strconv.FormatInt(tsMS, 10), strconv.FormatBool(deleteMedia))
		eventType = ledger.EventDeleteForMe
		ref := ledger.WAKeyRef(chat, v.MessageID, sender, v.IsFromMe)
		causalRef = &ref

	case *events.Archive:
		if v == nil || v.Action == nil {
			return nil
		}
		chat := a.canonicalStoreJID(ctx, v.JID).String()
		tsMS = v.Timestamp.UnixMilli()
		rangePresent := v.Action.GetMessageRange() != nil
		canonical = ledger.StateEvent{
			Type: ledger.StateArchive, Chat: chat, State: v.Action.GetArchived(),
			Range: strconv.FormatBool(rangePresent), TimestampMS: tsMS,
		}
		dk = ledger.DedupKey("archive", chat, strconv.FormatInt(tsMS, 10),
			strconv.FormatBool(v.Action.GetArchived()), strconv.FormatBool(rangePresent))
		eventType = ledger.EventArchive

	case *events.Pin:
		if v == nil || v.Action == nil {
			return nil
		}
		chat := a.canonicalStoreJID(ctx, v.JID).String()
		tsMS = v.Timestamp.UnixMilli()
		canonical = ledger.StateEvent{
			Type: ledger.StatePin, Chat: chat, State: v.Action.GetPinned(), TimestampMS: tsMS,
		}
		dk = ledger.DedupKey("pin", chat, strconv.FormatInt(tsMS, 10),
			strconv.FormatBool(v.Action.GetPinned()))
		eventType = ledger.EventPin

	case *events.Mute:
		if v == nil || v.Action == nil {
			return nil
		}
		chat := a.canonicalStoreJID(ctx, v.JID).String()
		tsMS = v.Timestamp.UnixMilli()
		endTS := v.Action.GetMuteEndTimestamp()
		canonical = ledger.StateEvent{
			Type: ledger.StateMute, Chat: chat, State: v.Action.GetMuted(),
			EndTSMS: endTS, TimestampMS: tsMS,
		}
		dk = ledger.DedupKey("mute", chat, strconv.FormatInt(tsMS, 10),
			strconv.FormatBool(v.Action.GetMuted()), strconv.FormatInt(endTS, 10))
		eventType = ledger.EventMute

	case *events.MarkChatAsRead:
		if v == nil || v.Action == nil {
			return nil
		}
		chat := a.canonicalStoreJID(ctx, v.JID).String()
		tsMS = v.Timestamp.UnixMilli()
		canonical = ledger.StateEvent{
			Type: ledger.StateMarkRead, Chat: chat, State: v.Action.GetRead(), TimestampMS: tsMS,
		}
		dk = ledger.DedupKey("mark_read", chat, strconv.FormatInt(tsMS, 10),
			strconv.FormatBool(v.Action.GetRead()))
		eventType = ledger.EventMarkRead

	case *events.AppState:
		if v == nil || v.SyncActionValue == nil {
			return nil
		}
		rawBytes, rawHash, err := ledger.HashProto(v.SyncActionValue)
		if err != nil {
			return fmt.Errorf("hash app state action: %w", err)
		}
		tsMS = nowUTC().UnixMilli()
		dk = ledger.DedupKey("app_state", rawHash)
		batchID, err := a.ledgerBatch(ledger.SourceAppState)
		if err != nil {
			return err
		}
		stateEvt := &ledger.Event{
			EventID:       ledger.DeriveEventID(dk, rawHash),
			Source:        ledger.SourceAppState,
			EventType:     ledger.EventCall,
			ServerTS:      tsMS / 1000,
			EventTS:       tsMS / 1000,
			ReceivedAt:    nowUTC().Unix(),
			RawHash:       rawHash,
			ParserVersion: ledger.ParserVersion,
			RulesVersion:  ledger.RulesVersion,
			DedupKey:      dk,
			BatchID:       batchID,
			RawBytes:      rawBytes,
		}
		_, _, err = a.db.AppendEvent(stateEvt)
		return err

	default:
		return nil
	}

	rawBytes, rawHash, err := ledger.HashCanonical(canonical)
	if err != nil {
		return fmt.Errorf("hash app state event: %w", err)
	}
	var refs []ledger.CausalRef
	if causalRef != nil {
		refs = []ledger.CausalRef{*causalRef}
	}
	batchID, err := a.ledgerBatch(ledger.SourceAppState)
	if err != nil {
		return err
	}
	stateEvt := &ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        ledger.SourceAppState,
		EventType:     eventType,
		ChatJID:       canonical.Chat,
		MsgID:         canonical.MsgID,
		SenderJID:     canonical.Sender,
		ServerTS:      tsMS / 1000,
		EventTS:       tsMS / 1000,
		ReceivedAt:    nowUTC().Unix(),
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		CausalRefs:    refs,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	}
	_, _, err = a.db.AppendEvent(stateEvt)
	return err
}
