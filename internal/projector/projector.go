// Package projector runs checkpointed, replayable views over the append-only
// event ledger. Each View projects one contiguous event batch inside a single
// transaction; the runner advances the view checkpoint in the same
// transaction, so a crash or cancellation never leaves a view ahead of its
// checkpoint.
package projector

import (
	"context"
	"database/sql"

	"github.com/openclaw/wacli/internal/store"
)

// View is one materialized projection of the ledger.
type View interface {
	// Name is the stable checkpoint key (e.g. "messages").
	Name() string
	// Version identifies the projection logic. It is stamped onto the
	// checkpoint; changing it lets operators detect views built by an older
	// projector and force a rebuild.
	Version() string
	// Apply projects one contiguous batch of events inside tx. It must use tx
	// exclusively (no side transactions) so its effects commit atomically
	// with the checkpoint advance.
	Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error
}
