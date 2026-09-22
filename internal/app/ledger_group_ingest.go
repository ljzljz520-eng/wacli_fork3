package app

import (
	"context"
	"sort"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"go.mau.fi/whatsmeow/types"
)

// buildCanonicalGroup converts a whatsmeow GroupInfo into the deterministic
// snapshot: JIDs canonicalized, roles normalized, roster sorted so repeated
// snapshots of the same state collapse through dedup.
func (a *App) buildCanonicalGroup(ctx context.Context, info *types.GroupInfo) ledger.CanonicalGroup {
	g := ledger.CanonicalGroup{
		JID:             info.JID.String(),
		Name:            info.GroupName.Name,
		OwnerJID:        a.canonicalStoreJID(ctx, info.OwnerJID).String(),
		CreatedTS:       info.GroupCreated.Unix(),
		IsParent:        info.IsParent,
		LinkedParentJID: info.LinkedParentJID.String(),
	}
	if g.IsParent {
		g.LinkedParentJID = ""
	}
	participants := make([]ledger.CanonicalGroupParticipant, 0, len(info.Participants))
	for _, p := range info.Participants {
		role := "member"
		if p.IsSuperAdmin {
			role = "superadmin"
		} else if p.IsAdmin {
			role = "admin"
		}
		participants = append(participants, ledger.CanonicalGroupParticipant{
			UserJID: a.canonicalStoreJID(ctx, p.JID).String(),
			Role:    role,
		})
	}
	sort.Slice(participants, func(i, j int) bool { return participants[i].UserJID < participants[j].UserJID })
	g.Participants = participants
	return g
}

// ingestGroupSnapshotEvent appends one full group+roster snapshot.
func (a *App) ingestGroupSnapshotEvent(ctx context.Context, info *types.GroupInfo) error {
	g := a.buildCanonicalGroup(ctx, info)
	return a.appendCanonicalGroup(ctx, ledger.SourceLive, g)
}

// ingestGroupLeftEvent appends a leave marker for one group.
func (a *App) ingestGroupLeftEvent(ctx context.Context, jid string, leftAt time.Time) error {
	if leftAt.IsZero() {
		leftAt = nowUTC()
	}
	g := ledger.CanonicalGroup{JID: jid, LeftAt: leftAt.Unix()}
	return a.appendCanonicalGroup(ctx, ledger.SourceLive, g)
}

func (a *App) appendCanonicalGroup(ctx context.Context, source string, g ledger.CanonicalGroup) error {
	rawBytes, rawHash, err := ledger.HashCanonical(g)
	if err != nil {
		return err
	}
	dk := ledger.DedupKey("group", g.JID, rawHash)
	batchID, err := a.ledgerBatch(source)
	if err != nil {
		return err
	}
	now := nowUTC().Unix()
	_, _, err = a.db.AppendEvent(&ledger.Event{
		EventID:       ledger.DeriveEventID(dk, rawHash),
		Source:        source,
		EventType:     ledger.EventGroup,
		ChatJID:       g.JID,
		ServerTS:      now,
		EventTS:       now,
		ReceivedAt:    now,
		RawHash:       rawHash,
		ParserVersion: ledger.ParserVersion,
		RulesVersion:  ledger.RulesVersion,
		DedupKey:      dk,
		BatchID:       batchID,
		RawBytes:      rawBytes,
	})
	return err
}

// StoreGroupSnapshot is the ingestion entry point used by cmd-side group
// persistence (persistGroupInfo).
func (a *App) StoreGroupSnapshot(ctx context.Context, info *types.GroupInfo) error {
	if !ledgerEnabled() {
		return nil
	}
	return a.ingestGroupSnapshotEvent(ctx, info)
}

// StoreGroupInfoWithLedger appends the snapshot then applies the direct
// writes (groups upsert + full roster replace).
func (a *App) StoreGroupInfoWithLedger(ctx context.Context, info *types.GroupInfo) error {
	if err := a.ingestGroupSnapshotEvent(ctx, info); err != nil {
		return err
	}
	return a.applyGroupInfoDirect(ctx, info)
}

// applyGroupInfoDirect mirrors the old storeGroupInfo without ingestion.
func (a *App) applyGroupInfoDirect(ctx context.Context, info *types.GroupInfo) error {
	ownerJID := a.canonicalStoreJID(ctx, info.OwnerJID).String()
	if err := a.db.UpsertGroupWithHierarchy(
		info.JID.String(), info.GroupName.Name, ownerJID,
		info.GroupCreated, info.IsParent, info.LinkedParentJID.String(),
	); err != nil {
		return err
	}
	participants := make([]store.GroupParticipant, 0, len(info.Participants))
	for _, p := range info.Participants {
		role := "member"
		if p.IsSuperAdmin {
			role = "superadmin"
		} else if p.IsAdmin {
			role = "admin"
		}
		participants = append(participants, store.GroupParticipant{
			GroupJID: info.JID.String(),
			UserJID:  a.canonicalStoreJID(ctx, p.JID).String(),
			Role:     role,
		})
	}
	return a.db.ReplaceGroupParticipants(info.JID.String(), participants)
}

// MarkGroupLeftWithLedger appends the leave marker before the direct write.
func (a *App) MarkGroupLeftWithLedger(ctx context.Context, jid string, leftAt time.Time) error {
	if leftAt.IsZero() {
		leftAt = nowUTC()
	}
	if err := a.ingestGroupLeftEvent(ctx, jid, leftAt); err != nil {
		return err
	}
	return a.db.MarkGroupLeft(jid, leftAt)
}

// MarkGroupsMissingFromLedger is the ledgerized twin of MarkGroupsMissingFrom:
// appends a leave event for every joined group absent from the joined set,
// then applies the direct marks.
func (a *App) MarkGroupsMissingFromLedger(ctx context.Context, joined map[string]bool, leftAt time.Time) error {
	if leftAt.IsZero() {
		leftAt = nowUTC()
	}
	joinedJIDs, err := a.db.ListJoinedGroupJIDs()
	if err != nil {
		return err
	}
	for _, jid := range joinedJIDs {
		if joined[jid] {
			continue
		}
		if err := a.ingestGroupLeftEvent(ctx, jid, leftAt); err != nil {
			return err
		}
		if err := a.db.MarkGroupLeft(jid, leftAt); err != nil {
			return err
		}
	}
	return nil
}
