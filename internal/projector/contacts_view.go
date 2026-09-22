package projector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/store/storedb"
	"github.com/openclaw/wacli/internal/wa"
)

// ContactsViewVersion is bumped when the contacts projection semantics
// change.
const ContactsViewVersion = "contacts-projector/1.0.0"

// ContactsView projects ledger events into contacts: partial snapshots fold
// through the same merge as the direct UpsertContact (each provided field
// fills a previously empty column); phone-book names are explicit; the bulk
// clear nulls every phone-book name.
//
// suffix="" targets the active table; suffix="_shadow" the shadow.
type ContactsView struct {
	db       *store.DB
	suffix   string
	identity wa.IdentityMap
	idMap    *viewIdentityMap
}

// NewContactsView constructs the view.
func NewContactsView(db *store.DB, suffix string) *ContactsView {
	return &ContactsView{db: db, suffix: suffix}
}

// WithIdentityMap supplies a projection-time LID/PN map seed; mappings folded
// from identity_resolution events during replay take precedence.
func (v *ContactsView) WithIdentityMap(m wa.IdentityMap) *ContactsView {
	v.identity = m
	return v
}

// mapJID resolves a JID against the folded identity map first, then the
// external seed.
func (v *ContactsView) mapJID(jid string) string {
	if canonical, ok := v.idMap.Lookup(jid); ok && canonical != "" {
		return canonical
	}
	return wa.MapJID(v.identity, jid)
}

// applyIdentity records one identity_resolution event in this view's map.
// Contacts rows are never rewritten by the fold (a LID contact is updated by
// later contact events carrying the canonical JID).
func (v *ContactsView) applyIdentity(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read identity raw record: %w", err)
	}
	res, err := decodeIdentityResolution(raw)
	if err != nil {
		return err
	}
	v.idMap.add(res.LID, res.PN)
	return nil
}

// Name is the checkpoint key; distinct per suffix.
func (v *ContactsView) Name() string { return store.ContactsTable + v.suffix }

// Version identifies the projection semantics.
func (v *ContactsView) Version() string { return ContactsViewVersion }

func (v *ContactsView) table() string { return store.ContactsTable + v.suffix }

// Apply projects one checkpoint batch.
func (v *ContactsView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	if v.idMap == nil {
		v.idMap = newViewIdentityMap()
	}
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventContact:
			err = v.applyContact(ctx, tx, evt)
		case evt.EventType == ledger.EventSystemNamesClear:
			err = v.applyClear(ctx, tx, evt)
		case evt.EventType == ledger.EventIdentityResolve:
			err = v.applyIdentity(ctx, tx, evt)
		default:
			// Other event types are projected by their owning views.
		}
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", evt.Seq, evt.EventType, err)
		}
	}
	return nil
}

func (v *ContactsView) applyContact(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read contact snapshot: %w", err)
	}
	var c ledger.CanonicalContact
	if err := json.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("unmarshal contact snapshot: %w", err)
	}
	c.JID = v.mapJID(strings.TrimSpace(c.JID))
	if c.JID == "" {
		return fmt.Errorf("contact event without jid")
	}

	// Partial fields fold through the generated merge; updated_at is the
	// event's observation time.
	query := store.RetargetTable(storedb.UpsertContactSQL, store.ContactsTable, v.table())
	if _, err := tx.ExecContext(ctx, query,
		c.JID,
		sql.NullString{String: c.Phone, Valid: c.Phone != ""},
		sql.NullString{String: c.PushName, Valid: c.PushName != ""},
		sql.NullString{String: c.FullName, Valid: c.FullName != ""},
		sql.NullString{String: c.FirstName, Valid: c.FirstName != ""},
		sql.NullString{String: c.BusinessName, Valid: c.BusinessName != ""},
		evt.ServerTS,
	); err != nil {
		return fmt.Errorf("merge contact: %w", err)
	}

	// Phone-book name is explicit and overwrites.
	if c.SystemName != "" {
		res, err := tx.ExecContext(ctx,
			`UPDATE `+v.table()+` SET system_name = ?, updated_at = ? WHERE jid = ?`,
			c.SystemName, evt.ServerTS, c.JID)
		if err != nil {
			return fmt.Errorf("set system name: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			// Defensive: row missing despite the merge.
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO `+v.table()+`(jid, system_name, updated_at) VALUES(?, ?, ?)`,
				c.JID, c.SystemName, evt.ServerTS); err != nil {
				return fmt.Errorf("insert system name: %w", err)
			}
		}
	}
	return nil
}

func (v *ContactsView) applyClear(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE `+v.table()+` SET system_name = NULL, updated_at = ?`, evt.ServerTS)
	return err
}

func (v *ContactsView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	var snap legacySnapshotPayload
	if err := json.Unmarshal(evt.Snapshot, &snap); err != nil {
		return fmt.Errorf("unmarshal legacy snapshot: %w", err)
	}
	if snap.Table != store.ContactsTable {
		return nil
	}
	row := snap.Row
	cols := []string{
		"jid", "phone", "push_name", "full_name", "first_name",
		"business_name", "system_name", "updated_at",
	}
	args := []any{
		rowString(row, "jid"),
		rowNullString(row, "phone"),
		rowNullString(row, "push_name"),
		rowNullString(row, "full_name"),
		rowNullString(row, "first_name"),
		rowNullString(row, "business_name"),
		rowNullString(row, "system_name"),
		coalesceInt64(row, "updated_at"),
	}
	query := `INSERT INTO ` + v.table() + ` (` + joinCols(cols) + `)
		VALUES (` + placeholders(len(cols)) + `)
		ON CONFLICT(jid) DO UPDATE SET
			phone = excluded.phone,
			push_name = excluded.push_name,
			full_name = excluded.full_name,
			first_name = excluded.first_name,
			business_name = excluded.business_name,
			system_name = excluded.system_name,
			updated_at = excluded.updated_at`
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert legacy contact: %w", err)
	}
	return nil
}
