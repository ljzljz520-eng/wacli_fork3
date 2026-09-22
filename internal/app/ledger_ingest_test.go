package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"github.com/openclaw/wacli/internal/wa"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func scanLedger(t *testing.T, a *App) []store.StoredEvent {
	t.Helper()
	var got []store.StoredEvent
	err := a.db.ScanEvents(0, 0, func(evt *store.StoredEvent) error {
		got = append(got, *evt)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanEvents: %v", err)
	}
	return got
}

func TestLedgerIngestLiveMessageAndDedup(t *testing.T) {
	a := newTestApp(t)
	a.wa = newFakeWA()

	chat := types.JID{User: "15551112222", Server: types.DefaultUserServer}
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rawMsg := &waE2E.Message{Conversation: protoString("hello live")}
	pm := wa.ParseLiveMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "live-1",
			Timestamp:     ts,
		},
		Message:    rawMsg,
		RawMessage: rawMsg,
	})

	for i := 0; i < 2; i++ {
		if err := a.storeParsedMessage(context.Background(), pm); err != nil {
			t.Fatalf("storeParsedMessage #%d: %v", i, err)
		}
	}

	evts := scanLedger(t, a)
	// The DM side effect also appends a contact event; the message itself must
	// still dedup to exactly one row.
	var msgEvt *store.StoredEvent
	for i := range evts {
		if evts[i].EventType == ledger.EventMessage {
			msgEvt = &evts[i]
		}
	}
	if msgEvt == nil {
		t.Fatalf("ledger has no message event (events=%d)", len(evts))
	}
	var contactEvents int
	for _, e := range evts {
		if e.EventType == ledger.EventContact {
			contactEvents++
		}
	}
	if contactEvents != 1 {
		t.Fatalf("contact events = %d, want 1", contactEvents)
	}
	evt := *msgEvt
	if evt.Source != ledger.SourceLive {
		t.Fatalf("source = %s, want live", evt.Source)
	}
	wantKey := ledger.WAKey(chat.String(), false, "live-1", "")
	if evt.WAKey != wantKey || evt.ChatJID != chat.String() || evt.MsgID != "live-1" {
		t.Fatalf("unexpected key fields: %+v", evt)
	}
	if evt.ParserVersion != ledger.ParserVersion || evt.RulesVersion != ledger.RulesVersion {
		t.Fatalf("missing version tags: %+v", evt)
	}
	if evt.Flags&ledger.FlagFromMe != 0 {
		t.Fatalf("incoming message flagged from-me")
	}
	// Raw envelope is retrievable and hashes back to the stored hash.
	_, raw, err := a.db.GetRaw(evt.EventID)
	if err != nil {
		t.Fatalf("GetRaw: %v", err)
	}
	if _, hash, err := ledger.HashProto(rawMsg); err != nil || hash != evt.RawHash {
		t.Fatalf("raw hash mismatch: %v %s", err, evt.RawHash)
	}
	if len(raw) == 0 {
		t.Fatalf("raw bytes not stored")
	}
	issues, err := a.db.AuditLedger()
	if err != nil {
		t.Fatalf("AuditLedger: %v", err)
	}
	if len(issues) != 0 {
		t.Fatalf("audit issues: %+v", issues)
	}
}

func TestLedgerIngestReactionAndQuoteRefs(t *testing.T) {
	a := newTestApp(t)
	a.wa = newFakeWA()

	chat := types.JID{User: "15551112222", Server: types.DefaultUserServer}
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rawMsg := &waE2E.Message{Conversation: protoString("react")}
	pm := wa.ParseLiveMessage(&events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            "react-1", Timestamp: ts,
		},
		Message: rawMsg, RawMessage: rawMsg,
	})
	pm.ReactionToID = "target-1"

	if err := a.storeParsedMessage(context.Background(), pm); err != nil {
		t.Fatalf("storeParsedMessage: %v", err)
	}
	evts := scanLedger(t, a)
	var msgEvt *store.StoredEvent
	for i := range evts {
		if evts[i].EventType == ledger.EventMessage {
			msgEvt = &evts[i]
		}
	}
	if msgEvt == nil {
		t.Fatalf("no message event (events=%d)", len(evts))
	}
	var refs []ledger.CausalRef
	if err := json.Unmarshal(msgEvt.CausalRefs, &refs); err != nil {
		t.Fatalf("refs: %v", err)
	}
	if len(refs) != 1 || refs[0].MsgID != "target-1" || refs[0].ChatJID == "" {
		t.Fatalf("unexpected causal refs: %+v", refs)
	}
}

func TestLedgerIngestOffSwitch(t *testing.T) {
	t.Setenv("WACLI_LEDGER", "off")
	a := newTestApp(t)
	a.wa = newFakeWA()

	chat := types.JID{User: "15551112222", Server: types.DefaultUserServer}
	rawMsg := &waE2E.Message{Conversation: protoString("hello")}
	pm := wa.ParsedMessage{
		Chat: chat, ID: "m-off", Timestamp: time.Now(),
		RawEnvelope: rawMsg, IngestSource: ledger.SourceLive,
	}
	if err := a.storeParsedMessage(context.Background(), pm); err != nil {
		t.Fatalf("storeParsedMessage: %v", err)
	}
	if head, err := a.db.HeadSeq(); err != nil || head != 0 {
		t.Fatalf("ledger head = %d (err=%v), want 0 with WACLI_LEDGER=off", head, err)
	}
}

func TestLedgerIngestOnDemandHistorySource(t *testing.T) {
	a := newTestApp(t)
	a.wa = newFakeWA()

	chat := types.JID{User: "15551112222", Server: types.DefaultUserServer}
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	webMsg := &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID: protoString(chat.String()),
			FromMe:    protoBool(false),
			ID:        protoString("hist-1"),
		},
		MessageTimestamp: protoUint64(uint64(ts.Unix())),
		Message:          &waE2E.Message{Conversation: protoString("history")},
	}
	pm := wa.ParseHistoryMessage(chat.String(), webMsg)
	pm.IngestSource = ledger.SourceOnDemandHistory
	if err := a.storeParsedMessage(context.Background(), pm); err != nil {
		t.Fatalf("storeParsedMessage: %v", err)
	}
	evts := scanLedger(t, a)
	var onDemand *store.StoredEvent
	for i := range evts {
		if evts[i].Source == ledger.SourceOnDemandHistory {
			onDemand = &evts[i]
		}
	}
	if onDemand == nil {
		t.Fatalf("no on_demand_history event (events=%d)", len(evts))
	}
	if onDemand.EventType != ledger.EventMessage {
		t.Fatalf("event type = %s, want message", onDemand.EventType)
	}
}

func TestLedgerIngestReceipt(t *testing.T) {
	a := newTestApp(t)
	a.wa = newFakeWA()

	chat := types.JID{User: "15551112222", Server: types.DefaultUserServer}
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	receipt := &events.Receipt{
		MessageSource: types.MessageSource{Chat: chat},
		MessageIDs:    []string{"r-2", "r-1"},
		Timestamp:     ts,
		Type:          types.ReceiptTypeRead,
	}
	for i := 0; i < 2; i++ {
		if err := a.ingestReceiptEvent(context.Background(), receipt); err != nil {
			t.Fatalf("ingestReceiptEvent #%d: %v", i, err)
		}
	}
	evts := scanLedger(t, a)
	if len(evts) != 1 {
		t.Fatalf("receipt events = %d, want 1", len(evts))
	}
	evt := evts[0]
	if evt.Source != ledger.SourceReceipt || evt.EventType != ledger.EventReceipt || evt.ChatJID != chat.String() {
		t.Fatalf("unexpected receipt event: %+v", evt)
	}
	var refs []ledger.CausalRef
	if err := json.Unmarshal(evt.CausalRefs, &refs); err != nil {
		t.Fatalf("refs: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("receipt refs = %d, want 2", len(refs))
	}
	// Raw is the canonical JSON receipt and round-trips to the stored hash.
	_, raw, err := a.db.GetRaw(evt.EventID)
	if err != nil {
		t.Fatalf("GetRaw: %v", err)
	}
	canonical := ledger.NewCanonicalReceipt(string(types.ReceiptTypeRead), chat.String(), "", "", []string{"r-2", "r-1"}, ts)
	if _, hash, err := ledger.HashCanonical(canonical); err != nil || hash != evt.RawHash {
		t.Fatalf("receipt hash mismatch: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("receipt raw missing")
	}
}

func TestLedgerIngestAppStateEvents(t *testing.T) {
	a := newTestApp(t)
	a.wa = newFakeWA()
	ctx := context.Background()
	chat := types.JID{User: "15551112222", Server: types.DefaultUserServer}
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		evt   any
		etype string
	}{
		{"star", &events.Star{
			ChatJID: chat, MessageID: "star-1", Timestamp: ts,
			Action: &waSyncAction.StarAction{Starred: protoBool(true)},
		}, ledger.EventStar},
		{"delete_for_me", &events.DeleteForMe{
			ChatJID: chat, MessageID: "del-1", Timestamp: ts,
			Action: &waSyncAction.DeleteMessageForMeAction{DeleteMedia: protoBool(false)},
		}, ledger.EventDeleteForMe},
		{"archive", &events.Archive{
			JID: chat, Timestamp: ts,
			Action: &waSyncAction.ArchiveChatAction{Archived: protoBool(true)},
		}, ledger.EventArchive},
		{"pin", &events.Pin{
			JID: chat, Timestamp: ts,
			Action: &waSyncAction.PinAction{Pinned: protoBool(true)},
		}, ledger.EventPin},
		{"mute", &events.Mute{
			JID: chat, Timestamp: ts,
			Action: &waSyncAction.MuteAction{Muted: protoBool(true), MuteEndTimestamp: protoInt64(0)},
		}, ledger.EventMute},
		{"mark_read", &events.MarkChatAsRead{
			JID: chat, Timestamp: ts,
			Action: &waSyncAction.MarkChatAsReadAction{Read: protoBool(true)},
		}, ledger.EventMarkRead},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Replay the same event twice: must collapse to one ledger row.
			for i := 0; i < 2; i++ {
				if err := a.ingestAppStateEvent(ctx, tc.evt); err != nil {
					t.Fatalf("ingestAppStateEvent #%d: %v", i, err)
				}
			}
		})
	}

	evts := scanLedger(t, a)
	if len(evts) != len(cases) {
		t.Fatalf("app-state events = %d, want %d", len(evts), len(cases))
	}
	types := map[string]store.StoredEvent{}
	for _, e := range evts {
		types[e.EventType] = e
	}
	for _, tc := range cases {
		e, ok := types[tc.etype]
		if !ok {
			t.Fatalf("missing event type %s", tc.etype)
		}
		if e.Source != ledger.SourceAppState || e.ChatJID != chat.String() {
			t.Fatalf("%s: unexpected fields %+v", tc.etype, e)
		}
	}
	// Star carries a causal ref to the starred message.
	var starRefs []ledger.CausalRef
	if err := json.Unmarshal(types[ledger.EventStar].CausalRefs, &starRefs); err != nil {
		t.Fatalf("star refs: %v", err)
	}
	if len(starRefs) != 1 || starRefs[0].MsgID != "star-1" {
		t.Fatalf("unexpected star refs: %+v", starRefs)
	}
	issues, err := a.db.AuditLedger()
	if err != nil {
		t.Fatalf("AuditLedger: %v", err)
	}
	if len(issues) != 0 {
		t.Fatalf("audit issues: %+v", issues)
	}
}

func protoString(s string) *string { return &s }
func protoBool(b bool) *bool       { return &b }
func protoInt64(v int64) *int64    { return &v }
func protoUint64(v uint64) *uint64 { return &v }
