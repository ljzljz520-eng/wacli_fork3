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

// GroupsViewVersion is bumped when the groups/roster projection
// semantics change.
const GroupsViewVersion = "groups-projector/1.0.0"

// GroupsView projects ledger events into groups and group_participants:
// a full group snapshot folds metadata through the generated hierarchy
// merge and replaces the roster; a leave marker sets left_at.
//
// suffix="" targets the active tables; suffix="_shadow" the shadows.
type GroupsView struct {
	db       *store.DB
	suffix   string
	identity wa.IdentityMap
	idMap    *viewIdentityMap
}

// NewGroupsView constructs the view.
func NewGroupsView(db *store.DB, suffix string) *GroupsView {
	return &GroupsView{db: db, suffix: suffix}
}

// WithIdentityMap supplies a projection-time LID/PN map seed; mappings folded
// from identity_resolution events during replay take precedence.
func (v *GroupsView) WithIdentityMap(m wa.IdentityMap) *GroupsView {
	v.identity = m
	return v
}

// mapJID resolves a JID against the folded identity map first, then the
// external seed.
func (v *GroupsView) mapJID(jid string) string {
	if canonical, ok := v.idMap.Lookup(jid); ok && canonical != "" {
		return canonical
	}
	return wa.MapJID(v.identity, jid)
}

// applyIdentity folds one identity_resolution event into groups: owner rewrite
// and participant merge/removal.
func (v *GroupsView) applyIdentity(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read identity raw record: %w", err)
	}
	res, err := decodeIdentityResolution(raw)
	if err != nil {
		return err
	}
	v.idMap.add(res.LID, res.PN)
	return store.FoldLIDGroups(ctx, tx, v.suffix, res.LID, res.PN)
}

// Name is the checkpoint key; distinct per suffix.
func (v *GroupsView) Name() string { return store.GroupsTable + v.suffix }

// Version identifies the projection semantics.
func (v *GroupsView) Version() string { return GroupsViewVersion }

func (v *GroupsView) table() string { return store.GroupsTable + v.suffix }
func (v *GroupsView) participantsTable() string {
	return store.GroupParticipantsTable + v.suffix
}

// Apply projects one checkpoint batch.
func (v *GroupsView) Apply(ctx context.Context, tx *sql.Tx, batch []store.StoredEvent) error {
	if v.idMap == nil {
		v.idMap = newViewIdentityMap()
	}
	for i := range batch {
		evt := &batch[i]
		var err error
		switch {
		case evt.Source == ledger.SourceLegacySnapshot:
			err = v.applyLegacySnapshot(ctx, tx, evt)
		case evt.EventType == ledger.EventGroup:
			err = v.applyGroup(ctx, tx, evt)
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

func (v *GroupsView) applyGroup(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	_, raw, err := v.db.GetRawTx(tx, evt.EventID)
	if err != nil {
		return fmt.Errorf("read group snapshot: %w", err)
	}
	var g ledger.CanonicalGroup
	if err := json.Unmarshal(raw, &g); err != nil {
		return fmt.Errorf("unmarshal group snapshot: %w", err)
	}
	g.JID = strings.TrimSpace(g.JID)
	if g.JID == "" {
		return fmt.Errorf("group event without jid")
	}
	if g.LeftAt > 0 && len(g.Participants) == 0 {
		return v.applyLeave(ctx, tx, g, evt.ServerTS)
	}
	return v.applySnapshot(ctx, tx, g, evt.ServerTS)
}

// applyLeave mirrors store.MarkGroupLeft.
func (v *GroupsView) applyLeave(ctx context.Context, tx *sql.Tx, g ledger.CanonicalGroup, updatedAt int64) error {
	query := store.RetargetTable(storedb.MarkGroupLeftSQL, store.GroupsTable, v.table())
	_, err := tx.ExecContext(ctx, query,
		sql.NullInt64{Int64: g.LeftAt, Valid: g.LeftAt > 0},
		updatedAt,
		g.JID,
	)
	if err != nil {
		return fmt.Errorf("mark group left: %w", err)
	}
	return nil
}

// applySnapshot mirrors UpsertGroupWithHierarchy + ReplaceGroupParticipants.
func (v *GroupsView) applySnapshot(ctx context.Context, tx *sql.Tx, g ledger.CanonicalGroup, updatedAt int64) error {
	linkedParent := strings.TrimSpace(g.LinkedParentJID)
	if g.IsParent {
		linkedParent = ""
	}
	ownerJID := v.mapJID(g.OwnerJID)
	query := store.RetargetTable(storedb.UpsertGroupWithHierarchySQL, store.GroupsTable, v.table())
	if _, err := tx.ExecContext(ctx, query,
		g.JID,
		sql.NullString{String: g.Name, Valid: g.Name != ""},
		sql.NullString{String: ownerJID, Valid: ownerJID != ""},
		sql.NullInt64{Int64: g.CreatedTS, Valid: g.CreatedTS != 0},
		boolInt(g.IsParent),
		sql.NullString{String: linkedParent, Valid: linkedParent != ""},
		updatedAt,
	); err != nil {
		return fmt.Errorf("upsert group: %w", err)
	}

	// Full roster replace within the same transaction.
	deleteQ := store.RetargetTable(storedb.DeleteGroupParticipantsSQL, store.GroupParticipantsTable, v.participantsTable())
	if _, err := tx.ExecContext(ctx, deleteQ, g.JID); err != nil {
		return fmt.Errorf("clear roster: %w", err)
	}
	insertQ := store.RetargetTable(storedb.InsertGroupParticipantSQL, store.GroupParticipantsTable, v.participantsTable())
	for _, p := range g.Participants {
		userJID := v.mapJID(p.UserJID)
		role := strings.TrimSpace(p.Role)
		if role == "" {
			role = "member"
		}
		if _, err := tx.ExecContext(ctx, insertQ, g.JID, userJID,
			sql.NullString{String: role, Valid: true}, updatedAt); err != nil {
			return fmt.Errorf("insert roster entry: %w", err)
		}
	}
	return nil
}

func (v *GroupsView) applyLegacySnapshot(ctx context.Context, tx *sql.Tx, evt *store.StoredEvent) error {
	var snap legacySnapshotPayload
	if err := json.Unmarshal(evt.Snapshot, &snap); err != nil {
		return fmt.Errorf("unmarshal legacy snapshot: %w", err)
	}
	switch snap.Table {
	case store.GroupsTable:
		return v.applyLegacyGroup(ctx, tx, snap.Row)
	case store.GroupParticipantsTable:
		return v.applyLegacyParticipant(ctx, tx, snap.Row)
	default:
		return nil
	}
}

func (v *GroupsView) applyLegacyGroup(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	cols := []string{
		"jid", "name", "owner_jid", "created_ts", "is_parent",
		"linked_parent_jid", "left_at", "updated_at",
	}
	args := []any{
		rowString(row, "jid"),
		rowNullString(row, "name"),
		rowNullString(row, "owner_jid"),
		rowNullInt64(row, "created_ts"),
		coalesceInt64(row, "is_parent"),
		rowNullString(row, "linked_parent_jid"),
		rowNullInt64(row, "left_at"),
		coalesceInt64(row, "updated_at"),
	}
	query := `INSERT INTO ` + v.table() + ` (` + joinCols(cols) + `)
		VALUES (` + placeholders(len(cols)) + `)
		ON CONFLICT(jid) DO UPDATE SET
			name = excluded.name,
			owner_jid = excluded.owner_jid,
			created_ts = excluded.created_ts,
			is_parent = excluded.is_parent,
			linked_parent_jid = excluded.linked_parent_jid,
			left_at = excluded.left_at,
			updated_at = excluded.updated_at`
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert legacy group: %w", err)
	}
	return nil
}

func (v *GroupsView) applyLegacyParticipant(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	role := rowString(row, "role")
	if strings.TrimSpace(role) == "" {
		role = "member"
	}
	query := `INSERT INTO ` + v.participantsTable() + `
		(group_jid, user_jid, role, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(group_jid, user_jid) DO UPDATE SET
			role = excluded.role,
			updated_at = excluded.updated_at`
	_, err := tx.ExecContext(ctx, query,
		rowString(row, "group_jid"),
		rowString(row, "user_jid"),
		role,
		coalesceInt64(row, "updated_at"),
	)
	if err != nil {
		return fmt.Errorf("insert legacy participant: %w", err)
	}
	return nil
}
