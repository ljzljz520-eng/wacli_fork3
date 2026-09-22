package main

import (
	"context"

	"github.com/openclaw/wacli/internal/store"
	"go.mau.fi/whatsmeow/types"
)

func canonicalCLIJID(jid types.JID) types.JID {
	if jid.Server == types.DefaultUserServer {
		return jid.ToNonAD()
	}
	return jid
}

type groupJIDResolver interface {
	ResolveLIDToPN(context.Context, types.JID) types.JID
}

// groupPersistTarget is the subset of *app.App needed to persist a group
// snapshot: ingest the canonical event, then perform direct writes.
type groupPersistTarget interface {
	groupJIDResolver
	DB() *store.DB
	StoreGroupSnapshot(context.Context, *types.GroupInfo) error
}

func persistGroupInfo(ctx context.Context, target groupPersistTarget, info *types.GroupInfo) error {
	if info == nil {
		return nil
	}
	if err := target.StoreGroupSnapshot(ctx, info); err != nil {
		return err
	}
	db := target.DB()
	ownerJID := canonicalCLIJID(target.ResolveLIDToPN(ctx, info.OwnerJID)).String()
	if err := db.UpsertGroupWithHierarchy(
		info.JID.String(),
		info.GroupName.Name,
		ownerJID,
		info.GroupCreated,
		info.IsParent,
		info.LinkedParentJID.String(),
	); err != nil {
		return err
	}
	var ps []store.GroupParticipant
	for _, p := range info.Participants {
		role := "member"
		if p.IsSuperAdmin {
			role = "superadmin"
		} else if p.IsAdmin {
			role = "admin"
		}
		ps = append(ps, store.GroupParticipant{
			GroupJID: info.JID.String(),
			UserJID:  canonicalCLIJID(target.ResolveLIDToPN(ctx, p.JID)).String(),
			Role:     role,
		})
	}
	return db.ReplaceGroupParticipants(info.JID.String(), ps)
}

func groupKindLabel(isParent bool, linkedParentJID string) string {
	if isParent {
		return "community"
	}
	if linkedParentJID != "" {
		return "subgroup"
	}
	return "group"
}
