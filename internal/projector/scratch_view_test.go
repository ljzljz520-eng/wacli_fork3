package projector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/openclaw/wacli/internal/store"
)

var errScratchFailure = errors.New("scratch failure")

// scratchView records one row per projected event in a per-view scratch table,
// so runner tests can observe exactly which events were committed.
type scratchView struct {
	name        string
	failFromSeq int64 // when a batch contains seq >= this, Apply fails
}

func newScratchView(name string, failFromSeq int64) *scratchView {
	return &scratchView{name: name, failFromSeq: failFromSeq}
}

func (v *scratchView) Name() string    { return v.name }
func (v *scratchView) Version() string { return "test-projector/1" }

func (v *scratchView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	if _, err := tx.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s_rows (seq INTEGER PRIMARY KEY)`, v.name)); err != nil {
		return err
	}
	for _, e := range batch {
		if v.failFromSeq > 0 && e.Seq >= v.failFromSeq {
			return errScratchFailure
		}
		if _, err := tx.Exec(fmt.Sprintf(
			`INSERT INTO %s_rows(seq) VALUES (?)`, v.name), e.Seq); err != nil {
			return err
		}
	}
	return nil
}
