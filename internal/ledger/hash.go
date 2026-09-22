package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Version constants stamped on every event. Bump ParserVersion whenever the
// parsing rules in internal/wa change, and RulesVersion whenever the LID /
// identity rules change; projectors use them to decide whether a replay is
// required.
const (
	ParserVersion = "wa-parser/1.0.0"
	RulesVersion  = "lid-rules/1.0.0"
)

// Encoding values stored in ledger_raw.encoding.
const (
	EncodingProto = "protobuf"
	EncodingJSON  = "canonical-json"
)

// waKeyBody is the canonical serialization of a WhatsApp message key.
type waKeyBody struct {
	Chat        string `json:"chat"`
	FromMe      bool   `json:"from_me"`
	ID          string `json:"id"`
	Participant string `json:"participant,omitempty"`
}

// WAKey renders the canonical WhatsApp key string for a message: chat,
// from-me flag, message ID and group participant (sender).
func WAKey(chatJID string, fromMe bool, msgID, participantJID string) string {
	b, err := json.Marshal(waKeyBody{Chat: chatJID, FromMe: fromMe, ID: msgID, Participant: participantJID})
	if err != nil {
		// Marshal of this fixed struct cannot fail.
		panic(fmt.Errorf("encode wa key: %w", err))
	}
	return string(b)
}

// dedupBody is the canonical serialization of a dedup key.
type dedupBody struct {
	Scope string   `json:"scope"`
	Parts []string `json:"parts"`
}

// DedupKey builds a deterministic de-duplication key for an event scope and
// identity parts. Equivalent protocol events from different paths (live vs
// history vs replay) must carry the same scope and parts.
func DedupKey(scope string, parts ...string) string {
	b, err := json.Marshal(dedupBody{Scope: scope, Parts: parts})
	if err != nil {
		panic(fmt.Errorf("encode dedup key: %w", err))
	}
	return "dd:" + string(b)
}

// DeriveEventID derives the globally unique event identifier from the dedup
// key and the raw-envelope hash. Equivalent inputs produce the same event ID;
// the same dedup key with different raw bytes produces a different event ID.
func DeriveEventID(dedupKey, rawHash string) string {
	h := sha256.New()
	h.Write([]byte(dedupKey))
	h.Write([]byte{0x0a})
	h.Write([]byte(rawHash))
	return "evt_" + hex.EncodeToString(h.Sum(nil)[:16])
}

// HashProto deterministically serializes a protobuf envelope and returns the
// encoded bytes and their SHA-256 hex digest. Deterministic marshalling sorts
// map fields so structurally equal messages hash equally.
func HashProto(m proto.Message) ([]byte, string, error) {
	opts := proto.MarshalOptions{Deterministic: true}
	encoded, err := opts.Marshal(m)
	if err != nil {
		return nil, "", fmt.Errorf("deterministic proto marshal: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(sum[:]), nil
}

// HashCanonical serializes a value with canonical JSON and returns the bytes
// and SHA-256 hex digest. Values should be fixed-order structs.
func HashCanonical(v any) ([]byte, string, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil, "", fmt.Errorf("canonical json marshal: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(sum[:]), nil
}

// MarshalRefs serializes causal references for storage, producing "[]" when
// there are no refs.
func MarshalRefs(refs []CausalRef) ([]byte, error) {
	if len(refs) == 0 {
		return []byte("[]"), nil
	}
	encoded, err := json.Marshal(refs)
	if err != nil {
		return nil, fmt.Errorf("marshal causal refs: %w", err)
	}
	return encoded, nil
}

// LookupKey returns the key under which a causal reference resolves in the
// ledger: the canonical WhatsApp key for wa_key refs, or "eid:"+id.
func (r CausalRef) LookupKey() string {
	switch r.Kind {
	case "wa_key":
		return WAKey(r.ChatJID, r.FromMe, r.MsgID, r.Sender)
	case "event_id":
		return "eid:" + r.EventID
	default:
		return ""
	}
}
