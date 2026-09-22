package app

import (
	"context"
	"strconv"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"go.mau.fi/whatsmeow/types"
)

// --- star --------------------------------------------------------------------

// SetStarredWithLedger appends the star fold before the direct write.
func (a *App) SetStarredWithLedger(ctx context.Context, source string, p store.SetStarredParams) error {
	if ledgerEnabled() {
		starredAt := p.StarredAt
		if starredAt.IsZero() {
			starredAt = nowUTC()
		}
		c := ledger.CanonicalStar{
			ChatJID:   p.ChatJID,
			MsgID:     p.MsgID,
			SenderJID: p.SenderJID,
			FromMe:    p.FromMe,
			Starred:   p.Starred,
			StarredTS: starredAt.Unix(),
		}
		dk := ledger.DedupKey("star", c.ChatJID, c.MsgID,
			strconv.FormatBool(c.Starred), strconv.FormatInt(c.StarredTS, 10))
		if err := a.appendCanonicalExtra(ctx, source, ledger.EventStar, c.ChatJID, c, c.StarredTS, dk, nil); err != nil {
			return err
		}
	}
	return a.db.SetStarred(p)
}

// --- status broadcast --------------------------------------------------------

// UpsertStatusMessageWithLedger appends the status message before direct write.
func (a *App) UpsertStatusMessageWithLedger(ctx context.Context, p store.UpsertStatusMessageParams) error {
	if ledgerEnabled() {
		ts := p.Timestamp
		if ts.IsZero() {
			ts = nowUTC()
		}
		c := ledger.CanonicalStatusMessage{
			MsgID:           p.MsgID,
			TS:              ts.Unix(),
			FromMe:          p.FromMe,
			SenderJID:       p.SenderJID,
			SenderName:      p.SenderName,
			Text:            p.Text,
			MediaType:       p.MediaType,
			MediaCaption:    p.MediaCaption,
			Filename:        p.Filename,
			MimeType:        p.MimeType,
			DirectPath:      p.DirectPath,
			MediaKey:        p.MediaKey,
			FileSHA256:      p.FileSHA256,
			FileEncSHA256:   p.FileEncSHA256,
			FileLength:      int64(p.FileLength),
			BackgroundColor: p.BackgroundColor,
			Font:            p.Font,
		}
		if err := a.appendCanonicalExtra(ctx, ledger.SourceLive, ledger.EventStatusMessage,
			types.StatusBroadcastJID.String(), c, c.TS, ledger.DedupKey("status", c.MsgID), nil); err != nil {
			return err
		}
	}
	return a.db.UpsertStatusMessage(p)
}

// --- call events -------------------------------------------------------------

// appendCallEvent appends one canonical call event (upsert or delete marker).
func (a *App) appendCallEvent(ctx context.Context, source string, c ledger.CanonicalCallEvent) error {
	dk := ledger.DedupKey("call-event", c.ChatJID, c.CallID, c.EventType,
		strconv.FormatInt(c.TS, 10), strconv.FormatBool(c.Deleted))
	return a.appendCanonicalExtra(ctx, source, ledger.EventCall, c.ChatJID, c, c.TS, dk, nil)
}

func (a *App) appendCallDelete(ctx context.Context, source, chatJID, direction string) error {
	c := ledger.CanonicalCallEvent{ChatJID: chatJID, Direction: direction, Deleted: true}
	return a.appendCallEvent(ctx, source, c)
}

// --- generic -----------------------------------------------------------------

func (a *App) appendCanonicalExtra(ctx context.Context, source, eventType, chatJID string,
	payload any, eventTS int64, dk string, refs []ledger.CausalRef,
) error {
	rawBytes, rawHash, err := ledger.HashCanonical(payload)
	if err != nil {
		return err
	}
	batchID, err := a.ledgerBatch(source)
	if err != nil {
		return err
	}
	now := nowUTC().Unix()
	if eventTS == 0 {
		eventTS = now
	}
	_, _, err = a.db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        source,
		EventType:     eventType,
		ChatJID:       chatJID,
		ServerTS:      eventTS,
		EventTS:       eventTS,
		ReceivedAt:    now,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		CausalRefs:    refs,
		RawBytes:      rawBytes,
	})
	return err
}
