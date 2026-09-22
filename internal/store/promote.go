package store

import (
	"context"
	"database/sql"
	"fmt"
)

// BackupSuffix marks the previous active set retained for rollback.
const BackupSuffix = "_backup"

// PromoteReport summarises one atomic view switch.
type PromoteReport struct {
	HeadSeq int64 `json:"head_seq"`
	Tables  int   `json:"tables"`
	Rolled  bool  `json:"rolled_back"`
}

// promoteChildFirst is the rename order active→backup (or active→shadow on
// rollback): children before parents so SQLite's automatic FK rewriting keeps
// the backup set internally consistent.
var promoteChildFirst = shadowDropOrder

func (d *DB) objectExists(ctx context.Context, name string) (bool, error) {
	var count int
	if err := d.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master
			WHERE type IN ('table','view') AND name = ?`, name).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// BackupSetExists reports whether a retained backup set is present.
func (d *DB) BackupSetExists(ctx context.Context) (bool, error) {
	return d.objectExists(ctx, ChatsTable+BackupSuffix)
}

// ShadowSetExists reports whether a rebuilt shadow set is present.
func (d *DB) ShadowSetExists(ctx context.Context) (bool, error) {
	return d.objectExists(ctx, ChatsTable+ShadowSuffix)
}

// PromoteShadow atomically switches active and shadow sets inside one
// transaction: active→backup (child-first), shadow→active (parent-first),
// including the FTS virtual table; checkpoints and view state move with the
// sets. The caller must have already enforced verify/diff gates.
func (d *DB) PromoteShadow(ctx context.Context) (*PromoteReport, error) {
	if exists, err := d.BackupSetExists(ctx); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("a backup view set already exists: roll back or confirm before promoting again")
	}
	head, err := d.HeadSeq()
	if err != nil {
		return nil, err
	}

	tx, err := d.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	applied := false
	defer func() {
		if !applied {
			_ = tx.Rollback()
		}
	}()

	for _, base := range promoteChildFirst {
		if err := renameObject(ctx, tx, base, base+BackupSuffix); err != nil {
			return nil, fmt.Errorf("retire active %s: %w", base, err)
		}
	}
	for _, base := range ShadowTables {
		if err := renameObject(ctx, tx, base+ShadowSuffix, base); err != nil {
			return nil, fmt.Errorf("promote shadow %s: %w", base, err)
		}
	}
	if d.ftsEnabled {
		if err := renameObject(ctx, tx, "messages_fts", "messages_fts"+BackupSuffix); err != nil {
			return nil, err
		}
		if err := renameObject(ctx, tx, "messages_fts"+ShadowSuffix, "messages_fts"); err != nil {
			return nil, err
		}
	}

	if err := d.moveCheckpointsPromote(ctx, tx); err != nil {
		return nil, err
	}
	if err := d.setViewStateLive(ctx, tx); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	applied = true
	return &PromoteReport{HeadSeq: head, Tables: len(ShadowTables)}, nil
}

// RollbackLedger reverses a promote atomically: active→shadow, backup→active.
func (d *DB) RollbackLedger(ctx context.Context) (*PromoteReport, error) {
	if exists, err := d.BackupSetExists(ctx); err != nil {
		return nil, err
	} else if !exists {
		return nil, fmt.Errorf("no backup view set exists: nothing to roll back")
	}
	if exists, err := d.ShadowSetExists(ctx); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("a shadow set exists: rollback is only available before a new rebuild")
	}
	head, err := d.HeadSeq()
	if err != nil {
		return nil, err
	}

	tx, err := d.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	applied := false
	defer func() {
		if !applied {
			_ = tx.Rollback()
		}
	}()

	// Current active (the promoted shadow tables) returns to shadow; child
	// first avoids FK rewriting surprises.
	for _, base := range promoteChildFirst {
		if err := renameObject(ctx, tx, base, base+ShadowSuffix); err != nil {
			return nil, fmt.Errorf("retire active %s: %w", base, err)
		}
	}
	for _, base := range ShadowTables {
		if err := renameObject(ctx, tx, base+BackupSuffix, base); err != nil {
			return nil, fmt.Errorf("restore backup %s: %w", base, err)
		}
	}
	if d.ftsEnabled {
		if err := renameObject(ctx, tx, "messages_fts", "messages_fts"+ShadowSuffix); err != nil {
			return nil, err
		}
		if err := renameObject(ctx, tx, "messages_fts"+BackupSuffix, "messages_fts"); err != nil {
			return nil, err
		}
	}

	if err := d.moveCheckpointsRollback(ctx, tx); err != nil {
		return nil, err
	}
	if err := d.setViewStateShadow(ctx, tx); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	applied = true
	return &PromoteReport{HeadSeq: head, Tables: len(ShadowTables), Rolled: true}, nil
}

func renameObject(ctx context.Context, tx txExecer, oldName, newName string) error {
	_, err := tx.ExecContext(ctx,
		`ALTER TABLE `+oldName+` RENAME TO `+newName)
	return err
}

// moveCheckpointsPromote: old active keys → key_backup; shadow keys → active.
func (d *DB) moveCheckpointsPromote(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM projector_checkpoints WHERE view LIKE '%\_backup' ESCAPE '\'`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE projector_checkpoints SET view = view || '_backup'
			WHERE view NOT LIKE '%\_shadow' ESCAPE '\'
				AND view NOT LIKE '%\_backup' ESCAPE '\'`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE projector_checkpoints SET view = substr(view, 1, length(view) - 7)
			WHERE view LIKE '%\_shadow' ESCAPE '\'`); err != nil {
		return err
	}
	return nil
}

// moveCheckpointsRollback: active keys → shadow; backup keys → active.
func (d *DB) moveCheckpointsRollback(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx,
		`UPDATE projector_checkpoints SET view = view || '_shadow'
			WHERE view NOT LIKE '%\_shadow' ESCAPE '\'
				AND view NOT LIKE '%\_backup' ESCAPE '\'`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE projector_checkpoints SET view = substr(view, 1, length(view) - 7)
			WHERE view LIKE '%\_backup' ESCAPE '\'`); err != nil {
		return err
	}
	return nil
}

func (d *DB) setViewStateLive(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM ledger_view_state
			WHERE view LIKE '%\_shadow' ESCAPE '\'
				OR view LIKE '%\_backup' ESCAPE '\'`); err != nil {
		return err
	}
	for _, base := range ShadowTables {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO ledger_view_state(view, mode) VALUES (?, 'live')
				ON CONFLICT(view) DO UPDATE SET mode='live'`, base); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) setViewStateShadow(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM ledger_view_state
			WHERE view NOT LIKE '%\_shadow' ESCAPE '\'
				AND view NOT LIKE '%\_backup' ESCAPE '\'`); err != nil {
		return err
	}
	// Recreate shadow entries for the set that returns to shadow.
	for _, base := range ShadowTables {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO ledger_view_state(view, mode) VALUES (?, 'shadow')
				ON CONFLICT(view) DO UPDATE SET mode='shadow'`, base+ShadowSuffix); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE ledger_view_state SET view = substr(view, 1, length(view) - 7)
			WHERE view LIKE '%\_backup' ESCAPE '\'`); err != nil {
		return err
	}
	return nil
}

// txExecer is the minimal Exec surface used inside a switch transaction.
type txExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
