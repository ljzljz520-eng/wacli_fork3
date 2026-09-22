package app

import (
	"context"
	"fmt"

	"github.com/openclaw/wacli/internal/projector"
	"github.com/openclaw/wacli/internal/store"
)

// RebuildShadow resets the shadow tables and replays the whole ledger from
// seq=0 through every view, using the current projector logic. It operates on
// wacli.db only and never opens a WhatsApp connection.
func (a *App) RebuildShadow(ctx context.Context) (*projector.Report, error) {
	withFTS := a.db.FTSEnabled()
	if err := a.db.ResetShadowSchema(ctx, withFTS); err != nil {
		return nil, fmt.Errorf("reset shadow schema: %w", err)
	}

	suffix := store.ShadowSuffix
	views := []projector.View{
		projector.NewMessagesView(a.db, suffix, withFTS),
		projector.NewChatsView(a.db, suffix),
		projector.NewContactsView(a.db, suffix),
		projector.NewGroupsView(a.db, suffix),
		projector.NewPollsView(a.db, suffix),
		projector.NewPollVotesView(a.db, suffix),
		projector.NewStarredView(a.db, suffix),
		projector.NewStatusMessagesView(a.db, suffix),
		projector.NewCallEventsView(a.db, suffix),
	}
	return projector.New(a.db, views).Run(ctx, 0)
}

// VerifyLedger runs the invariant checker over the active materialized tables.
func (a *App) VerifyLedger(ctx context.Context) (*store.VerifyReport, error) {
	return a.db.VerifyLedger(ctx, "")
}

// VerifyShadowLedger runs the invariant checker over the rebuilt shadow tables.
func (a *App) VerifyShadowLedger(ctx context.Context) (*store.VerifyReport, error) {
	return a.db.VerifyLedger(ctx, store.ShadowSuffix)
}

// DiffShadowLedger compares active and rebuilt shadow tables with per-row
// attribution to ledger events.
func (a *App) DiffShadowLedger(ctx context.Context) (*store.DiffReport, error) {
	return a.db.DiffShadow(ctx)
}

// PromoteShadow atomically switches the rebuilt shadow set to active. The
// shadow invariant check is a hard gate; force only bypasses the diff policy,
// never the invariants.
func (a *App) PromoteShadow(ctx context.Context, force bool) (*store.PromoteReport, error) {
	if exists, err := a.db.BackupSetExists(ctx); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("a backup view set already exists: roll back or confirm before promoting again")
	}
	verifyReport, err := a.db.VerifyLedger(ctx, store.ShadowSuffix)
	if err != nil {
		return nil, fmt.Errorf("verify shadow before promote: %w", err)
	}
	if verifyReport.HasViolations() {
		return nil, fmt.Errorf("shadow invariant check failed: promote refused")
	}
	if !force {
		diffReport, err := a.db.DiffShadow(ctx)
		if err != nil {
			return nil, fmt.Errorf("diff before promote: %w", err)
		}
		if diffReport.HasDiffs() {
			return nil, fmt.Errorf("shadow differs from active: review the diff or use --force to override the policy")
		}
	}
	return a.db.PromoteShadow(ctx)
}

// RollbackLedger atomically restores the backup set.
func (a *App) RollbackLedger(ctx context.Context) (*store.PromoteReport, error) {
	return a.db.RollbackLedger(ctx)
}

// LedgerStatus returns head/checkpoint/accounting status.
func (a *App) LedgerStatus(ctx context.Context) (*store.LedgerStatusReport, error) {
	return a.db.LedgerStatus(ctx)
}
