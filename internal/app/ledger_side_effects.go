package app

import (
	"context"
	"sync"
)

// SideEffectKind identifies a post-projection side effect.
type SideEffectKind string

const (
	// SideEffectWebhook is a sync webhook delivery.
	SideEffectWebhook SideEffectKind = "webhook"
	// SideEffectMediaEnqueue is an automatic media-download enqueue.
	SideEffectMediaEnqueue SideEffectKind = "media_enqueue"
)

// AttributedSideEffect records one side effect together with the ledger event
// that caused it. EventID may be empty when the corresponding row predates the
// ledger (legacy bootstrap); newly ingested events always attribute.
type AttributedSideEffect struct {
	EventID string         `json:"event_id"`
	Kind    SideEffectKind `json:"kind"`
	ChatJID string         `json:"chat_jid"`
	MsgID   string         `json:"msg_id,omitempty"`
}

type sideEffectLog struct {
	mu      sync.Mutex
	entries []AttributedSideEffect
}

func (l *sideEffectLog) record(e AttributedSideEffect) {
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
}

// Snapshot returns a copy of the recorded side effects.
func (l *sideEffectLog) Snapshot() []AttributedSideEffect {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]AttributedSideEffect, len(l.entries))
	copy(out, l.entries)
	return out
}

func (l *sideEffectLog) reset() {
	l.mu.Lock()
	l.entries = nil
	l.mu.Unlock()
}

func (a *App) traceSideEffect(ctx context.Context, kind SideEffectKind, chatJID, msgID string) {
	eventID := ""
	if ledgerEnabled() {
		id, err := a.db.EventIDByChatMsg(ctx, chatJID, msgID)
		if err != nil {
			a.emitWarning(
				"side_effect_attribute_failed",
				"warning: failed to attribute side effect to ledger event",
				map[string]any{"error": err.Error(), "kind": string(kind)},
			)
		} else {
			eventID = id
		}
	}
	a.sideEffects.record(AttributedSideEffect{
		EventID: eventID,
		Kind:    kind,
		ChatJID: chatJID,
		MsgID:   msgID,
	})
}

// SideEffects returns the side effects recorded on this app instance. It is
// used by tests and diagnostics; the log is in-memory and not part of the gate.
func (a *App) SideEffects() []AttributedSideEffect {
	return a.sideEffects.Snapshot()
}

// wrapMediaEnqueuer wraps a media-download enqueuer so every enqueue is
// recorded and attributed to its ledger event before the raw enqueuer runs.
func (a *App) wrapMediaEnqueuer(ctx context.Context, raw func(chatJID, msgID string)) func(chatJID, msgID string) {
	return func(chatJID, msgID string) {
		a.traceSideEffect(ctx, SideEffectMediaEnqueue, chatJID, msgID)
		raw(chatJID, msgID)
	}
}

// wrapWebhookEnqueuer wraps a sync-webhook enqueuer so each delivery is
// recorded (once per chat/message pair) and attributed before the raw enqueuer
// runs.
func (a *App) wrapWebhookEnqueuer(ctx context.Context, raw func(syncWebhookEvent)) func(syncWebhookEvent) {
	return func(evt syncWebhookEvent) {
		for _, target := range evt.sideEffectTargets() {
			a.traceSideEffect(ctx, target.Kind, target.ChatJID, target.MsgID)
		}
		raw(evt)
	}
}
