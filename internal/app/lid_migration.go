package app

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/openclaw/wacli/internal/ledger"
	"go.mau.fi/whatsmeow/types"
)

func (a *App) migrateHistoricalLIDs(ctx context.Context) error {
	if a == nil || a.db == nil || a.wa == nil {
		return nil
	}
	lids, err := a.db.HistoricalLIDJIDs()
	if err != nil {
		return fmt.Errorf("load historical LID rows: %w", err)
	}
	for _, raw := range lids {
		if err := ctx.Err(); err != nil {
			return err
		}
		lid, err := types.ParseJID(strings.TrimSpace(raw))
		if err != nil || lid.Server != types.HiddenUserServer {
			continue
		}
		pn := a.wa.ResolveLIDToPN(ctx, lid)
		if err := ctx.Err(); err != nil {
			return err
		}
		if pn.IsEmpty() || pn.Server != types.DefaultUserServer {
			continue
		}
		pnJID := canonicalJIDString(pn)
		pendingMedia, err := a.db.LIDMigrationPurgedMedia(raw, pnJID)
		if err != nil {
			return fmt.Errorf("load purged alias media for historical LID %s: %w", raw, err)
		}
		for start := 0; start < len(pendingMedia); {
			end := start + 1
			for end < len(pendingMedia) && pendingMedia[end].ChatJID == pendingMedia[start].ChatJID && pendingMedia[end].MsgID == pendingMedia[start].MsgID {
				end++
			}
			media := pendingMedia[start]
			// Record the scrub in the ledger before touching the filesystem,
			// so shadow rebuilds remove the same local media references.
			if err := a.appendScrubEvent(ctx, ledger.SourceIdentityResolution,
				media.ChatJID, media.MsgID, 0); err != nil {
				return fmt.Errorf("record purged alias media scrub for historical LID %s: %w", raw, err)
			}
			for _, item := range pendingMedia[start:end] {
				if err := os.Remove(item.LocalPath); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("remove purged alias media for historical LID %s: %w", raw, err)
				}
			}
			if err := a.db.ClearMessageLocalMedia(media.ChatJID, media.MsgID); err != nil {
				return fmt.Errorf("clear purged alias media for historical LID %s: %w", raw, err)
			}
			start = end
		}
		// Append the identity resolution event (ledger source of truth), then
		// fold the active tables immediately as the dual-write.
		if err := a.AppendIdentityResolution(ctx, ledger.SourceIdentityResolution,
			ledger.CanonicalIdentityResolution{
				LID: raw, PN: pnJID, ResolvedTS: nowUTC().Unix(),
			}); err != nil {
			return fmt.Errorf("record identity resolution for historical LID %s: %w", raw, err)
		}
		if err := a.db.MigrateLIDToPN(raw, pnJID); err != nil {
			return fmt.Errorf("migrate historical LID %s: %w", raw, err)
		}
	}
	return nil
}
