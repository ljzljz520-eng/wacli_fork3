package ledger

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestHashProtoDeterministic(t *testing.T) {
	msg := &waE2E.Message{
		Conversation: proto.String("hello"),
	}

	encoded1, hash1, err := HashProto(msg)
	if err != nil {
		t.Fatal(err)
	}
	encoded2, hash2, err := HashProto(msg)
	if err != nil {
		t.Fatal(err)
	}
	if hash1 != hash2 {
		t.Fatalf("hash not deterministic: %s vs %s", hash1, hash2)
	}
	if string(encoded1) != string(encoded2) {
		t.Fatal("encoded bytes not deterministic")
	}
	if len(hash1) != 64 {
		t.Fatalf("hash length = %d, want 64", len(hash1))
	}
	if _, err := hex.DecodeString(hash1); err != nil {
		t.Fatalf("hash not hex: %v", err)
	}

	// Same content rebuilt from scratch must hash identically; proto field
	// declaration order never affects the wire encoding.
	rebuilt := &waE2E.Message{Conversation: proto.String("hello")}
	_, hashRebuilt, err := HashProto(rebuilt)
	if err != nil {
		t.Fatal(err)
	}
	if hashRebuilt != hash1 {
		t.Fatalf("equivalent proto hashed differently: %s vs %s", hashRebuilt, hash1)
	}

	// A different wrapping (content moved into another field) must hash
	// differently even though the visible text is the same.
	wrapped := &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("hello")},
	}
	_, hashWrapped, err := HashProto(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if hashWrapped == hash1 {
		t.Fatal("expected different wrapping to produce different hash")
	}
}

func TestCanonicalReceiptGoldenVector(t *testing.T) {
	canonical := NewCanonicalReceipt(
		"read",
		"15551112222@s.whatsapp.net",
		"",
		"",
		[]string{"MSG-2", "MSG-1"},
		time.UnixMilli(1700000000123),
	)
	encoded, hash, err := HashCanonical(canonical)
	if err != nil {
		t.Fatal(err)
	}
	const wantJSON = `{"type":"read","chat":"15551112222@s.whatsapp.net","message_ids":["MSG-1","MSG-2"],"timestamp_ms":1700000000123}`
	if string(encoded) != wantJSON {
		t.Fatalf("canonical json = %s, want %s", encoded, wantJSON)
	}
	const wantHash = "a4322b5ada7272f6e330a199c9ea9a8d4a637ca62808768d8062468dc5d1088e"
	if hash != wantHash {
		t.Fatalf("golden hash = %s, want %s", hash, wantHash)
	}

	// Same receipt built with IDs in a different order must hash identically.
	reordered := NewCanonicalReceipt(
		"read", "15551112222@s.whatsapp.net", "", "",
		[]string{"MSG-1", "MSG-2"}, time.UnixMilli(1700000000123),
	)
	_, hashReordered, err := HashCanonical(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if hashReordered != wantHash {
		t.Fatalf("reordered receipt hash = %s, want %s", hashReordered, wantHash)
	}
}

func TestDedupKeyAndEventIDEquivalence(t *testing.T) {
	key1 := WAKey("chat@c.us", false, "ID-1", "")
	key2 := WAKey("chat@c.us", false, "ID-1", "")
	if key1 != key2 {
		t.Fatal("WAKey not deterministic")
	}
	if !strings.Contains(key1, `"chat":"chat@c.us"`) || !strings.Contains(key1, `"id":"ID-1"`) {
		t.Fatalf("WAKey missing components: %s", key1)
	}

	dk1 := DedupKey("message", "live", key1)
	dk2 := DedupKey("message", "live", key2)
	if dk1 != dk2 {
		t.Fatal("equivalent events produced different dedup keys")
	}
	dkOther := DedupKey("message", "history", key1)
	if dkOther == dk1 {
		t.Fatal("different scopes produced the same dedup key")
	}

	// Same dedup key + same raw hash => same event id.
	id1 := DeriveEventID(dk1, "raw-hash-a")
	id2 := DeriveEventID(dk1, "raw-hash-a")
	if id1 != id2 {
		t.Fatal("event id not deterministic")
	}
	// Same dedup key + different raw hash => different event id.
	id3 := DeriveEventID(dk1, "raw-hash-b")
	if id3 == id1 {
		t.Fatal("different raw bytes produced same event id")
	}
	// Different dedup key => different event id.
	id4 := DeriveEventID(dkOther, "raw-hash-a")
	if id4 == id1 {
		t.Fatal("different dedup keys produced same event id")
	}
}

func TestCausalRefLookupKey(t *testing.T) {
	ref := WAKeyRef("chat@c.us", "ID-1", "sender@c.us", false)
	want := WAKey("chat@c.us", false, "ID-1", "sender@c.us")
	if got := ref.LookupKey(); got != want {
		t.Fatalf("wa ref lookup = %q, want %q", got, want)
	}

	eid := CausalRef{Kind: "event_id", EventID: "evt-abc"}
	if got := eid.LookupKey(); got != "eid:evt-abc" {
		t.Fatalf("event id ref lookup = %q", got)
	}
}

func TestMarshalRefsEmpty(t *testing.T) {
	got, err := MarshalRefs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "[]" {
		t.Fatalf("empty refs = %q, want []", got)
	}
}

func TestIsKnownSource(t *testing.T) {
	if !IsKnownSource(SourceLive) || !IsKnownSource(SourceLegacySnapshot) {
		t.Fatal("known source rejected")
	}
	if IsKnownSource("bogus") {
		t.Fatal("unknown source accepted")
	}
}
