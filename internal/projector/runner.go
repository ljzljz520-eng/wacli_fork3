package projector

import (
	"context"
	"fmt"

	"github.com/openclaw/wacli/internal/store"
)

// DefaultBatchSize bounds one Apply transaction.
const DefaultBatchSize = 500

// Runner advances a fixed set of views toward a shared ledger frontier.
type Runner struct {
	db        *store.DB
	views     []View
	batchSize int64
}

// New constructs a Runner with the default batch size.
func New(db *store.DB, views []View) *Runner {
	return &Runner{db: db, views: views, batchSize: DefaultBatchSize}
}

// ViewProgress summarizes one view's movement during Run.
type ViewProgress struct {
	FromSeq int64
	ToSeq   int64
	Batches int
}

// Report is the outcome of a successful Run: every view sits at TargetSeq.
type Report struct {
	TargetSeq int64
	Views     map[string]ViewProgress
}

// Run advances every view to targetSeq (the ledger head when 0). Views are
// caught up independently from their own checkpoint, so an interrupted or
// failed run leaves uneven checkpoints that a subsequent Run repairs.
func (r *Runner) Run(ctx context.Context, targetSeq int64) (*Report, error) {
	if targetSeq <= 0 {
		head, err := r.db.HeadSeq()
		if err != nil {
			return nil, err
		}
		targetSeq = head
	}

	report := &Report{TargetSeq: targetSeq, Views: make(map[string]ViewProgress, len(r.views))}
	for _, view := range r.views {
		progress, err := r.advanceView(ctx, view, targetSeq)
		report.Views[view.Name()] = progress
		if err != nil {
			return report, fmt.Errorf("projector %s: %w", view.Name(), err)
		}
	}
	return report, nil
}

func (r *Runner) advanceView(ctx context.Context, view View, targetSeq int64) (ViewProgress, error) {
	cp, err := r.db.GetCheckpoint(view.Name())
	if err != nil {
		return ViewProgress{}, err
	}
	progress := ViewProgress{FromSeq: cp.LastSeq, ToSeq: cp.LastSeq}
	if cp.LastSeq > targetSeq {
		return progress, fmt.Errorf("checkpoint %d is ahead of target %d", cp.LastSeq, targetSeq)
	}

	for progress.ToSeq < targetSeq {
		if err := ctx.Err(); err != nil {
			return progress, err
		}
		next := progress.ToSeq + r.batchSize
		if next > targetSeq {
			next = targetSeq
		}

		batch, err := r.loadBatch(progress.ToSeq, next)
		if err != nil {
			return progress, err
		}
		if len(batch) == 0 {
			return progress, fmt.Errorf("no ledger events in (%d, %d]: ledger truncated?", progress.ToSeq, next)
		}

		tx, err := r.db.BeginTx(ctx)
		if err != nil {
			return progress, err
		}
		applied := false
		defer func() {
			if !applied {
				_ = tx.Rollback()
			}
		}()

		if err := view.Apply(ctx, tx, batch); err != nil {
			return progress, err
		}
		if err := r.db.SetCheckpointTx(tx, view.Name(), next, view.Version()); err != nil {
			return progress, err
		}
		if err := tx.Commit(); err != nil {
			return progress, err
		}
		applied = true
		progress.ToSeq = next
		progress.Batches++
	}
	return progress, nil
}

func (r *Runner) loadBatch(fromSeq, toSeq int64) ([]store.StoredEvent, error) {
	var batch []store.StoredEvent
	err := r.db.ScanEvents(fromSeq, toSeq, func(evt *store.StoredEvent) error {
		batch = append(batch, *evt)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return batch, nil
}
