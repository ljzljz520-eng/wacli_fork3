package app

import (
	"context"
	"fmt"
	"testing"

	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/types"
)

func mustParseJID(t *testing.T, s string) types.JID {
	t.Helper()
	jid, err := types.ParseJID(s)
	if err != nil {
		t.Fatalf("parse jid %q: %v", s, err)
	}
	return jid
}

// insertLedgerMessageRow inserts a minimal ledger_events row and returns the
// event id, so side-effect attribution can resolve the pair.
func insertLedgerMessageRow(t *testing.T, a *App, chatJID, msgID string) string {
	t.Helper()
	eventID := fmt.Sprintf("evt-%s-%s", chatJID, msgID)
	_, err := a.db.Exec(`INSERT INTO ledger_events
		(event_id, source, event_type, chat_jid, msg_id, received_at,
			raw_hash, parser_version, rules_version, dedup_key)
		VALUES (?, 'live', 'message', ?, ?, 1, 'h', 'pv', 'rv', 'dk')`,
		eventID, chatJID, msgID)
	if err != nil {
		t.Fatal(err)
	}
	return eventID
}

// TR-15.1: wrapped enqueuers fire the raw enqueuer exactly as often with the
// same arguments (payload/count parity), while each firing is recorded with a
// ledger event id when one exists.
func TestWrappedEnqueuersAttributeSideEffects(t *testing.T) {
	t.Setenv("WACLI_LEDGER", "1")
	a := newTestApp(t)
	ctx := context.Background()

	chat1 := "15551234567@s.whatsapp.net"
	chat2 := "15557654321@s.whatsapp.net"
	id1 := insertLedgerMessageRow(t, a, chat1, "m1")
	id2 := insertLedgerMessageRow(t, a, chat2, "m2")

	// Media enqueue parity + attribution.
	type mediaCall struct{ chat, msg string }
	var mediaCalls []mediaCall
	media := a.wrapMediaEnqueuer(ctx, func(chatJID, msgID string) {
		mediaCalls = append(mediaCalls, mediaCall{chatJID, msgID})
	})
	media(chat1, "m1")
	media(chat2, "m2")
	media(chat1, "legacy") // no ledger pair: unattributed
	if len(mediaCalls) != 3 ||
		mediaCalls[0] != (mediaCall{chat1, "m1"}) ||
		mediaCalls[2] != (mediaCall{chat1, "legacy"}) {
		t.Fatalf("raw media enqueue calls = %+v, want unchanged order/payload", mediaCalls)
	}

	// Webhook parity: message, receipt (two ids), presence.
	var webhookRawCalls []SyncWebhookEventKind
	webhook := a.wrapWebhookEnqueuer(ctx, func(evt syncWebhookEvent) {
		webhookRawCalls = append(webhookRawCalls, evt.Kind)
	})
	webhook(syncWebhookEvent{
		Kind: SyncWebhookEventMessage,
		Message: wa.ParsedMessage{
			Chat: mustParseJID(t, chat1),
			ID:   "m1",
		},
	})
	webhook(syncWebhookEvent{
		Kind: SyncWebhookEventReceipt,
		Receipt: syncWebhookReceipt{
			Chat:       mustParseJID(t, chat1),
			MessageIDs: []string{"m1"},
		},
	})
	webhook(syncWebhookEvent{
		Kind: SyncWebhookEventChatPresence,
		Presence: syncWebhookChatPresence{
			Chat: mustParseJID(t, chat2),
		},
	})
	if len(webhookRawCalls) != 3 ||
		webhookRawCalls[0] != SyncWebhookEventMessage ||
		webhookRawCalls[1] != SyncWebhookEventReceipt ||
		webhookRawCalls[2] != SyncWebhookEventChatPresence {
		t.Fatalf("raw webhook calls = %+v, want one per event in order", webhookRawCalls)
	}

	entries := a.SideEffects()
	want := []AttributedSideEffect{
		{EventID: id1, Kind: SideEffectMediaEnqueue, ChatJID: chat1, MsgID: "m1"},
		{EventID: id2, Kind: SideEffectMediaEnqueue, ChatJID: chat2, MsgID: "m2"},
		{EventID: "", Kind: SideEffectMediaEnqueue, ChatJID: chat1, MsgID: "legacy"},
		{EventID: id1, Kind: SideEffectWebhook, ChatJID: chat1, MsgID: "m1"},
		{EventID: id1, Kind: SideEffectWebhook, ChatJID: chat1, MsgID: "m1"},
		{EventID: "", Kind: SideEffectWebhook, ChatJID: chat2},
	}
	if len(entries) != len(want) {
		t.Fatalf("side effect entries = %d, want %d: %+v", len(entries), len(want), entries)
	}
	for i, got := range entries {
		if got != want[i] {
			t.Fatalf("side effect[%d] = %+v, want %+v", i, got, want[i])
		}
	}
}
