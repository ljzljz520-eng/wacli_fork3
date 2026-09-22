package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/openclaw/wacli/internal/ledger"
)

// AppendIdentityResolution appends one identity_resolution ledger event.
// Identity events carry no protocol envelope; the canonical payload is itself
// the stored raw record. Callers that need the active tables folded
// immediately must also invoke db.MigrateLIDToPN (dual-write).
func (a *App) AppendIdentityResolution(ctx context.Context, source string, res ledger.CanonicalIdentityResolution) error {
	if !ledgerEnabled() {
		return nil
	}
	res.LID = strings.TrimSpace(res.LID)
	res.PN = strings.TrimSpace(res.PN)
	if res.LID == "" || res.PN == "" || res.LID == res.PN {
		return nil
	}
	dk := ledger.DedupKey("identity", res.LID, res.PN)
	return a.appendCanonicalExtra(ctx, source, ledger.EventIdentityResolve,
		res.LID, res, res.ResolvedTS, dk, nil)
}

// appendScrubEvent appends a scrub record for one message whose local media
// must be removed together with payload purge. Scrub events carry no raw
// bytes; raw_hash is sha256(dk) so the record stays verifiable.
func (a *App) appendScrubEvent(ctx context.Context, source, chatJID, msgID string, eventTS int64) error {
	if !ledgerEnabled() {
		return nil
	}
	chatJID = strings.TrimSpace(chatJID)
	msgID = strings.TrimSpace(msgID)
	if chatJID == "" || msgID == "" {
		return nil
	}
	dk := ledger.DedupKey("scrub", chatJID, msgID)
	sum := sha256.Sum256([]byte(dk))
	rawHash := hex.EncodeToString(sum[:])
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
		Source:        ledger.SourceScrub,
		EventType:     ledger.EventScrub,
		ChatJID:       chatJID,
		MsgID:         msgID,
		ServerTS:      eventTS,
		EventTS:       eventTS,
		ReceivedAt:    now,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
	})
	return err
}
