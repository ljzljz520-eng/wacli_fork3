package wa

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// DecodeMessageInput carries a stored raw envelope and the ledger-row metadata
// required to reconstruct a ParsedMessage without a WhatsApp connection.
type DecodeMessageInput struct {
	Source    string
	Raw       []byte
	ChatJID   string
	SenderJID string
	MsgID     string
	Timestamp time.Time
	FromMe    bool
}

// DecodeMessage decodes one ledger message event offline. History-sourced
// events unmarshal the WebMessageInfo envelope; live-sourced events unmarshal
// the waE2E.Message and rebuild the minimal event context ParseLiveMessage
// needs. The result is field-identical to the online parse of the same input.
func DecodeMessage(in DecodeMessageInput) (ParsedMessage, error) {
	if len(in.Raw) == 0 {
		return ParsedMessage{}, fmt.Errorf("cannot decode message event: raw bytes missing (scrubbed or legacy snapshot)")
	}
	switch in.Source {
	case ledger.SourceHistory, ledger.SourceOnDemandHistory:
		info := &waWeb.WebMessageInfo{}
		if err := proto.Unmarshal(in.Raw, info); err != nil {
			return ParsedMessage{}, fmt.Errorf("unmarshal WebMessageInfo: %w", err)
		}
		return ParseHistoryMessage(in.ChatJID, info), nil
	default:
		msg := &waE2E.Message{}
		if err := proto.Unmarshal(in.Raw, msg); err != nil {
			return ParsedMessage{}, fmt.Errorf("unmarshal waE2E.Message: %w", err)
		}
		var chat types.JID
		if parsed, err := types.ParseJID(in.ChatJID); err == nil {
			chat = parsed
		}
		source := types.MessageSource{Chat: chat, IsFromMe: in.FromMe}
		if in.SenderJID != "" {
			if parsed, err := types.ParseJID(in.SenderJID); err == nil {
				source.Sender = parsed
			}
		}
		evt := &events.Message{
			Info: types.MessageInfo{
				MessageSource: source,
				ID:            types.MessageID(in.MsgID),
				Timestamp:     in.Timestamp,
			},
			Message:    msg,
			RawMessage: msg,
		}
		return ParseLiveMessage(evt), nil
	}
}

// DecodeReceipt decodes the canonical JSON receipt stored for receipt events.
func DecodeReceipt(raw []byte) (ledger.CanonicalReceipt, error) {
	var c ledger.CanonicalReceipt
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("unmarshal canonical receipt: %w", err)
	}
	return c, nil
}

// DecodeStateEvent decodes the canonical JSON form of an app-state event.
func DecodeStateEvent(raw []byte) (ledger.StateEvent, error) {
	var s ledger.StateEvent
	if err := json.Unmarshal(raw, &s); err != nil {
		return s, fmt.Errorf("unmarshal state event: %w", err)
	}
	return s, nil
}

// IdentityMap maps JIDs (typically LIDs) to the canonical store JID during
// offline projection. Implementations accumulate mappings exclusively from
// identity_resolution ledger events, never from live resolution.
type IdentityMap interface {
	Lookup(jid string) (canonical string, ok bool)
}

// MapJID returns the mapped JID, or jid unchanged when the map is nil or has
// no entry. Unmapped JIDs are preserved verbatim.
func MapJID(m IdentityMap, jid string) string {
	if m == nil || jid == "" {
		return jid
	}
	if canonical, ok := m.Lookup(jid); ok && canonical != "" {
		return canonical
	}
	return jid
}
