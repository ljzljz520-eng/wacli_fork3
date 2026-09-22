package store

import (
	"context"

	"github.com/openclaw/wacli/internal/ledger"
)

// ViewStatus is one view's checkpoint and its lag behind the ledger head.
type ViewStatus struct {
	View string `json:"view"`
	Mode string `json:"mode"`
	Seq  int64  `json:"seq"`
	Lag  int64  `json:"lag"`
}

// LedgerStatusReport summarises ledger health.
type LedgerStatusReport struct {
	HeadSeq    int64        `json:"head_seq"`
	RawCount   int64        `json:"raw_count"`
	ScrubCount int64        `json:"scrub_count"`
	FTSEnabled bool         `json:"fts_enabled"`
	ShadowSet  bool         `json:"shadow_set"`
	BackupSet  bool         `json:"backup_set"`
	Views      []ViewStatus `json:"views"`
}

// LedgerStatus collects head seq, raw/scrub accounting, sets present and
// per-view checkpoint lag.
func (d *DB) LedgerStatus(ctx context.Context) (*LedgerStatusReport, error) {
	head, err := d.HeadSeq()
	if err != nil {
		return nil, err
	}
	rep := &LedgerStatusReport{
		HeadSeq:    head,
		FTSEnabled: d.ftsEnabled,
		Views:      []ViewStatus{},
	}

	if err := d.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ledger_raw`).Scan(&rep.RawCount); err != nil {
		return nil, err
	}
	if err := d.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ledger_events WHERE source = ?`, ledger.SourceScrub).
		Scan(&rep.ScrubCount); err != nil {
		return nil, err
	}
	if rep.ShadowSet, err = d.objectExists(ctx, ChatsTable+ShadowSuffix); err != nil {
		return nil, err
	}
	if rep.BackupSet, err = d.objectExists(ctx, ChatsTable+BackupSuffix); err != nil {
		return nil, err
	}

	rows, err := d.sql.QueryContext(ctx,
		`SELECT c.view, COALESCE(s.mode, ''), c.last_seq
			FROM projector_checkpoints c
			LEFT JOIN ledger_view_state s ON s.view = c.view
			ORDER BY c.view`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var vs ViewStatus
		if err := rows.Scan(&vs.View, &vs.Mode, &vs.Seq); err != nil {
			return nil, err
		}
		vs.Lag = head - vs.Seq
		rep.Views = append(rep.Views, vs)
	}
	return rep, rows.Err()
}
