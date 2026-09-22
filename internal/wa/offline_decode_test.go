package wa

import (
	"reflect"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestDecodeLiveMessageMatchesOnlineParse(t *testing.T) {
	chat := types.JID{User: "15551112222", Server: types.DefaultUserServer}
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		msg  *waE2E.Message
	}{
		{
			name: "text",
			msg:  &waE2E.Message{Conversation: proto.String("hello")},
		},
		{
			name: "image_media",
			msg: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String("a caption"),
				Mimetype:      proto.String("image/jpeg"),
				DirectPath:    proto.String("/v/t"),
				MediaKey:      []byte{1, 2, 3},
				FileSHA256:    []byte{4, 5},
				FileEncSHA256: []byte{6},
				FileLength:    proto.Uint64(42),
			}},
		},
		{
			name: "location",
			msg: &waE2E.Message{LocationMessage: &waE2E.LocationMessage{
				DegreesLatitude:  proto.Float64(37.5),
				DegreesLongitude: proto.Float64(-122.1),
			}},
		},
		{
			name: "reaction",
			msg: &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
				Key:  &waCommon.MessageKey{ID: proto.String("orig-1"), FromMe: proto.Bool(false)},
				Text: proto.String("👍"),
			}},
		},
		{
			name: "poll",
			msg: &waE2E.Message{PollCreationMessageV3: &waE2E.PollCreationMessage{
				Name: proto.String("pick one"),
				Options: []*waE2E.PollCreationMessage_Option{
					{OptionName: proto.String("a")},
					{OptionName: proto.String("b")},
				},
			}},
		},
		{
			name: "business_buttons",
			msg: &waE2E.Message{ButtonsMessage: &waE2E.ButtonsMessage{
				ContentText: proto.String("Choose"),
				Buttons: []*waE2E.ButtonsMessage_Button{
					{
						ButtonID: proto.String("btn-1"),
						ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{
							DisplayText: proto.String("OK"),
						},
					},
				},
			}},
		},
		{
			name: "call_log",
			msg: &waE2E.Message{CallLogMesssage: &waE2E.CallLogMessage{
				IsVideo:      proto.Bool(true),
				DurationSecs: proto.Int64(12),
			}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			onlineEvt := &events.Message{
				Info: types.MessageInfo{
					MessageSource: types.MessageSource{Chat: chat, Sender: chat},
					ID:            "m-decode", Timestamp: ts,
				},
				Message:    tc.msg,
				RawMessage: tc.msg,
			}
			golden := ParseLiveMessage(onlineEvt)

			raw, _, err := ledger.HashProto(tc.msg)
			if err != nil {
				t.Fatalf("HashProto: %v", err)
			}
			decoded, err := DecodeMessage(DecodeMessageInput{
				Source:    ledger.SourceLive,
				Raw:       raw,
				ChatJID:   chat.String(),
				SenderJID: chat.String(),
				MsgID:     "m-decode",
				Timestamp: ts,
			})
			if err != nil {
				t.Fatalf("DecodeMessage: %v", err)
			}
			if !parsedMessagesEqual(decoded, golden) {
				t.Fatalf("offline decode diverged from online parse\ngolden: %+v\ndecoded: %+v", golden, decoded)
			}
		})
	}
}

func TestDecodeHistoryMessageMatchesOnlineParse(t *testing.T) {
	chat := types.JID{User: "15551112222", Server: types.DefaultUserServer}
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	info := &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID: proto.String(chat.String()),
			FromMe:    proto.Bool(false),
			ID:        proto.String("hist-decode"),
		},
		MessageTimestamp: proto.Uint64(uint64(ts.Unix())),
		Message:          &waE2E.Message{Conversation: proto.String("from history")},
	}
	golden := ParseHistoryMessage(chat.String(), info)

	raw, _, err := ledger.HashProto(info)
	if err != nil {
		t.Fatalf("HashProto: %v", err)
	}
	decoded, err := DecodeMessage(DecodeMessageInput{
		Source:  ledger.SourceHistory,
		Raw:     raw,
		ChatJID: chat.String(),
	})
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if !parsedMessagesEqual(decoded, golden) {
		t.Fatalf("history decode diverged\ngolden: %+v\ndecoded: %+v", golden, decoded)
	}
}

// parsedMessagesEqual compares two ParsedMessages field by field. RawEnvelope
// values are compared with proto.Equal: unmarshalling regenerates protobuf
// internal caches that reflect.DeepEqual would flag despite identical content.
func parsedMessagesEqual(a, b ParsedMessage) bool {
	aEnv, aOK := a.RawEnvelope.(proto.Message)
	bEnv, bOK := b.RawEnvelope.(proto.Message)
	a.RawEnvelope = nil
	b.RawEnvelope = nil
	if !reflect.DeepEqual(a, b) {
		return false
	}
	if aOK != bOK {
		return false
	}
	if aOK && !proto.Equal(aEnv, bEnv) {
		return false
	}
	return true
}

func TestDecodeReceiptAndState(t *testing.T) {
	receipt := ledger.NewCanonicalReceipt("read", "15551112222@s.whatsapp.net", "", "",
		[]string{"a", "b"}, time.UnixMilli(1700000000123))
	raw, _, err := ledger.HashCanonical(receipt)
	if err != nil {
		t.Fatalf("HashCanonical: %v", err)
	}
	decoded, err := DecodeReceipt(raw)
	if err != nil || !reflect.DeepEqual(decoded, receipt) {
		t.Fatalf("DecodeReceipt: %v %+v", err, decoded)
	}

	state := ledger.StateEvent{
		Type: ledger.StateArchive, Chat: "15551112222@s.whatsapp.net",
		State: true, TimestampMS: 1700000000123,
	}
	sraw, _, err := ledger.HashCanonical(state)
	if err != nil {
		t.Fatalf("HashCanonical state: %v", err)
	}
	sdecoded, err := DecodeStateEvent(sraw)
	if err != nil || !reflect.DeepEqual(sdecoded, state) {
		t.Fatalf("DecodeStateEvent: %v %+v", err, sdecoded)
	}
}

func TestDecodeMissingRawFails(t *testing.T) {
	if _, err := DecodeMessage(DecodeMessageInput{Source: ledger.SourceLive}); err == nil {
		t.Fatalf("expected error decoding event without raw bytes")
	}
}

type fakeIdentityMap map[string]string

func (m fakeIdentityMap) Lookup(jid string) (string, bool) {
	v, ok := m[jid]
	return v, ok
}

func TestMapJID(t *testing.T) {
	m := fakeIdentityMap{"123@lid": "15551112222@s.whatsapp.net"}
	if got := MapJID(m, "123@lid"); got != "15551112222@s.whatsapp.net" {
		t.Fatalf("mapped jid = %s", got)
	}
	// Unmapped JIDs are preserved verbatim.
	if got := MapJID(m, "999@lid"); got != "999@lid" {
		t.Fatalf("unmapped jid = %s, want unchanged", got)
	}
	// Nil map is safe.
	if got := MapJID(nil, "x"); got != "x" {
		t.Fatalf("nil map changed jid: %s", got)
	}
}
