package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/ledger"
	"github.com/openclaw/wacli/internal/store"
	"go.mau.fi/whatsmeow/types"
)

type cancelingLIDResolver struct {
	WAClient
	cancel context.CancelFunc
}

func (f cancelingLIDResolver) ResolveLIDToPN(ctx context.Context, jid types.JID) types.JID {
	f.cancel()
	return types.JID{User: "15550000001", Server: types.DefaultUserServer}
}

func TestEnsureAuthedStopsHistoricalMigrationOnCancellation(t *testing.T) {
	a := newTestApp(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	a.wa = cancelingLIDResolver{WAClient: newFakeWA(), cancel: cancel}
	if err := a.db.UpsertChat("123@lid", "dm", "Synthetic", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := a.EnsureAuthed(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureAuthed = %v, want context.Canceled", err)
	}
	lids, err := a.db.HistoricalLIDJIDs()
	if err != nil || len(lids) != 1 || lids[0] != "123@lid" {
		t.Fatalf("canceled migration changed historical identities: %v, %v", lids, err)
	}
}

func TestEnsureAuthedMigratesHistoricalLIDs(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	lid := types.JID{User: "999123456789", Device: 42, Server: types.HiddenUserServer}
	lidNonAD := lid.ToNonAD()
	pn := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	f.lids[lidNonAD] = pn

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(lid.String(), "unknown", lid.String(), base); err != nil {
		t.Fatalf("UpsertChat lid: %v", err)
	}
	if err := a.db.UpsertMessage(store.UpsertMessageParams{
		ChatJID:   lid.String(),
		MsgID:     "m-lid",
		SenderJID: lid.String(),
		Timestamp: base,
		Text:      "historical",
	}); err != nil {
		t.Fatalf("UpsertMessage lid: %v", err)
	}

	if err := a.EnsureAuthed(t.Context()); err != nil {
		t.Fatalf("EnsureAuthed: %v", err)
	}

	msg, err := a.db.GetMessage(pn.String(), "m-lid")
	if err != nil {
		t.Fatalf("GetMessage pn: %v", err)
	}
	if msg.ChatJID != pn.String() {
		t.Fatalf("ChatJID = %q, want %q", msg.ChatJID, pn.String())
	}
	if msg.SenderJID != pn.String() {
		t.Fatalf("SenderJID = %q, want %q", msg.SenderJID, pn.String())
	}
	lids, err := a.db.HistoricalLIDJIDs()
	if err != nil {
		t.Fatalf("HistoricalLIDJIDs: %v", err)
	}
	if len(lids) != 0 {
		t.Fatalf("HistoricalLIDJIDs = %#v, want none", lids)
	}
}

func TestEnsureAuthedRemovesPurgedAliasMediaBeforeLIDMigration(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	lid := types.JID{User: "999123456789", Server: types.HiddenUserServer}
	pn := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	f.lids[lid.ToNonAD()] = pn
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, chat := range []string{lid.String(), pn.String()} {
		if err := a.db.UpsertChat(chat, "dm", "Alice", base); err != nil {
			t.Fatal(err)
		}
		if err := a.db.UpsertMessage(store.UpsertMessageParams{ChatJID: chat, MsgID: "mid", Timestamp: base, Text: "payload"}); err != nil {
			t.Fatal(err)
		}
	}
	aliasPath := filepath.Join(t.TempDir(), "alias-media.bin")
	if err := os.WriteFile(aliasPath, []byte("alias media"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := a.db.MarkMediaDownloaded(pn.String(), "mid", aliasPath, base); err != nil {
		t.Fatal(err)
	}
	if err := a.db.MarkMessageRevoked(lid.String(), "mid"); err != nil {
		t.Fatal(err)
	}
	if err := a.db.PurgeMessage(lid.String(), "mid"); err != nil {
		t.Fatal(err)
	}

	if err := a.EnsureAuthed(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(aliasPath); !os.IsNotExist(err) {
		t.Fatalf("purged alias media still exists: %v", err)
	}
	msg, err := a.db.GetMessage(pn.String(), "mid")
	if err != nil {
		t.Fatal(err)
	}
	if msg.PayloadPurgedAt == nil || msg.LocalPath != "" || msg.Text != "" {
		t.Fatalf("migrated purge state = %+v", msg)
	}
}

func TestEnsureAuthedRecordsScrubAndIdentityEventsForPurgedMedia(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	lid := types.JID{User: "999123456789", Server: types.HiddenUserServer}
	pn := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	f.lids[lid.ToNonAD()] = pn
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, chat := range []string{lid.String(), pn.String()} {
		if err := a.db.UpsertChat(chat, "dm", "Alice", base); err != nil {
			t.Fatal(err)
		}
		if err := a.db.UpsertMessage(store.UpsertMessageParams{
			ChatJID: chat, MsgID: "mid", Timestamp: base, Text: "payload",
		}); err != nil {
			t.Fatal(err)
		}
	}
	mediaDir := t.TempDir()
	aliasPath := filepath.Join(mediaDir, "alias-media.bin")
	if err := os.WriteFile(aliasPath, []byte("alias media"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Downloaded media referenced from the PN message row and from an alias row.
	if err := a.db.MarkMediaDownloaded(pn.String(), "mid", aliasPath, base); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.Exec(`INSERT INTO message_local_media_aliases
		(chat_jid, msg_id, local_path, downloaded_at) VALUES (?, ?, ?, ?)`,
		pn.String(), "mid", aliasPath, base.Unix()); err != nil {
		t.Fatal(err)
	}
	if err := a.db.MarkMessageRevoked(lid.String(), "mid"); err != nil {
		t.Fatal(err)
	}
	if err := a.db.PurgeMessage(lid.String(), "mid"); err != nil {
		t.Fatal(err)
	}

	if err := a.EnsureAuthed(t.Context()); err != nil {
		t.Fatalf("EnsureAuthed: %v", err)
	}

	// TR-14.2: exactly one scrub event for the purged message, with a
	// recomputable raw hash.
	dk := ledger.DedupKey("scrub", pn.String(), "mid")
	scrubCount := queryCount(t, a,
		`SELECT COUNT(*) FROM ledger_events WHERE event_type='scrub'
			AND chat_jid=? AND msg_id='mid' AND raw_hash=?`,
		pn.String(), sha256Hex(dk))
	if scrubCount != 1 {
		t.Fatalf("scrub ledger rows = %d, want 1", scrubCount)
	}

	// Identity resolution event recorded for the pair.
	identityCount := queryCount(t, a,
		`SELECT COUNT(*) FROM ledger_events WHERE event_type='identity_resolution'
			AND chat_jid=?`, lid.String())
	if identityCount != 1 {
		t.Fatalf("identity_resolution ledger rows = %d, want 1", identityCount)
	}

	// Raw message/alias rows no longer carry local media references.
	msgMedia := queryCount(t, a,
		`SELECT COUNT(*) FROM messages WHERE chat_jid IN (?, ?)
			AND COALESCE(local_path,'') != ''`, lid.String(), pn.String())
	if msgMedia != 0 {
		t.Fatalf("message rows still referencing local media = %d", msgMedia)
	}
	aliasMedia := queryCount(t, a,
		`SELECT COUNT(*) FROM message_local_media_aliases
			WHERE chat_jid=? AND COALESCE(local_path,'') != ''`, pn.String())
	if aliasMedia != 0 {
		t.Fatalf("alias rows still referencing local media = %d", aliasMedia)
	}
	if _, err := os.Stat(aliasPath); !os.IsNotExist(err) {
		t.Fatalf("purged alias media still exists: %v", err)
	}
}

func queryCount(t *testing.T, a *App, query string, args ...any) int {
	t.Helper()
	var count int
	if err := a.db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v\n%s", err, query)
	}
	return count
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestEnsureAuthedPreservesDuplicateAliasMediaForLaterPurge(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	lid := types.JID{User: "999123456789", Server: types.HiddenUserServer}
	pn := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	f.lids[lid.ToNonAD()] = pn
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, chat := range []string{lid.String(), pn.String()} {
		if err := a.db.UpsertChat(chat, "dm", "Alice", base); err != nil {
			t.Fatal(err)
		}
		if err := a.db.UpsertMessage(store.UpsertMessageParams{ChatJID: chat, MsgID: "mid", Timestamp: base, Text: "payload"}); err != nil {
			t.Fatal(err)
		}
	}
	mediaDir := t.TempDir()
	lidPath := filepath.Join(mediaDir, "lid-media.bin")
	pnPath := filepath.Join(mediaDir, "pn-media.bin")
	for path, body := range map[string]string{lidPath: "lid media", pnPath: "pn media"} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.db.MarkMediaDownloaded(lid.String(), "mid", lidPath, base); err != nil {
		t.Fatal(err)
	}
	if err := a.db.MarkMediaDownloaded(pn.String(), "mid", pnPath, base); err != nil {
		t.Fatal(err)
	}

	if err := a.EnsureAuthed(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(lidPath); err != nil {
		t.Fatalf("alias media missing: %v", err)
	}
	if _, err := os.Stat(pnPath); err != nil {
		t.Fatalf("surviving destination media missing: %v", err)
	}
	msg, err := a.db.GetMessage(pn.String(), "mid")
	if err != nil {
		t.Fatal(err)
	}
	if msg.LocalPath != pnPath {
		t.Fatalf("migrated local path = %q, want %q", msg.LocalPath, pnPath)
	}
	paths, err := a.db.MessageLocalMediaPaths(pn.String(), "mid")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("migrated media paths = %#v, want both copies", paths)
	}
}

func TestEnsureAuthedPreservesAliasMediaWhenDestinationPathIsStale(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	lid := types.JID{User: "999123456789", Server: types.HiddenUserServer}
	pn := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	f.lids[lid.ToNonAD()] = pn
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, chat := range []string{lid.String(), pn.String()} {
		if err := a.db.UpsertChat(chat, "dm", "Alice", base); err != nil {
			t.Fatal(err)
		}
		if err := a.db.UpsertMessage(store.UpsertMessageParams{ChatJID: chat, MsgID: "mid", Timestamp: base, Text: "payload"}); err != nil {
			t.Fatal(err)
		}
	}
	mediaDir := t.TempDir()
	lidPath := filepath.Join(mediaDir, "lid-media.bin")
	stalePNPath := filepath.Join(mediaDir, "missing-pn-media.bin")
	if err := os.WriteFile(lidPath, []byte("lid media"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := a.db.MarkMediaDownloaded(lid.String(), "mid", lidPath, base); err != nil {
		t.Fatal(err)
	}
	if err := a.db.MarkMediaDownloaded(pn.String(), "mid", stalePNPath, base); err != nil {
		t.Fatal(err)
	}

	if err := a.EnsureAuthed(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(lidPath); err != nil {
		t.Fatalf("source alias media missing: %v", err)
	}
	paths, err := a.db.MessageLocalMediaPaths(pn.String(), "mid")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("migrated media paths = %#v, want retained alias and destination metadata", paths)
	}
}

func TestEnsureAuthedKeepsMediaWhenDuplicatePathsIdentifySameFile(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	lid := types.JID{User: "999123456789", Server: types.HiddenUserServer}
	pn := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	f.lids[lid.ToNonAD()] = pn
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, chat := range []string{lid.String(), pn.String()} {
		if err := a.db.UpsertChat(chat, "dm", "Alice", base); err != nil {
			t.Fatal(err)
		}
		if err := a.db.UpsertMessage(store.UpsertMessageParams{ChatJID: chat, MsgID: "mid", Timestamp: base, Text: "payload"}); err != nil {
			t.Fatal(err)
		}
	}
	mediaDir := t.TempDir()
	mediaPath := filepath.Join(mediaDir, "media.bin")
	aliasPath := filepath.Join(mediaDir, "nested", "..", "media.bin")
	if err := os.Mkdir(filepath.Join(mediaDir, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mediaPath, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := a.db.MarkMediaDownloaded(lid.String(), "mid", aliasPath, base); err != nil {
		t.Fatal(err)
	}
	if err := a.db.MarkMediaDownloaded(pn.String(), "mid", mediaPath, base); err != nil {
		t.Fatal(err)
	}

	if err := a.EnsureAuthed(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(mediaPath); err != nil {
		t.Fatalf("shared media missing: %v", err)
	}
	msg, err := a.db.GetMessage(pn.String(), "mid")
	if err != nil {
		t.Fatal(err)
	}
	if msg.LocalPath != mediaPath {
		t.Fatalf("migrated local path = %q, want %q", msg.LocalPath, mediaPath)
	}
}

func TestEnsureAuthedLeavesUnresolvedHistoricalLIDs(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f

	lid := types.JID{User: "999123456789", Server: types.HiddenUserServer}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := a.db.UpsertChat(lid.String(), "unknown", lid.String(), base); err != nil {
		t.Fatalf("UpsertChat lid: %v", err)
	}

	if err := a.EnsureAuthed(t.Context()); err != nil {
		t.Fatalf("EnsureAuthed: %v", err)
	}
	lids, err := a.db.HistoricalLIDJIDs()
	if err != nil {
		t.Fatalf("HistoricalLIDJIDs: %v", err)
	}
	if len(lids) != 1 || lids[0] != lid.String() {
		t.Fatalf("HistoricalLIDJIDs = %#v, want %q", lids, lid.String())
	}
}

func TestEnsureAuthedSkipsHistoricalLIDMigrationReadOnly(t *testing.T) {
	storeDir := t.TempDir()
	writer, err := New(Options{StoreDir: storeDir})
	if err != nil {
		t.Fatalf("New writer: %v", err)
	}

	lid := types.JID{User: "999123456789", Device: 42, Server: types.HiddenUserServer}
	pn := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := writer.db.UpsertChat(lid.String(), "unknown", lid.String(), base); err != nil {
		t.Fatalf("UpsertChat lid: %v", err)
	}
	writer.Close()

	reader, err := New(Options{StoreDir: storeDir, ReadOnly: true})
	if err != nil {
		t.Fatalf("New read-only: %v", err)
	}
	defer reader.Close()
	f := newFakeWA()
	f.lids[lid.ToNonAD()] = pn
	reader.wa = f

	if err := reader.EnsureAuthed(t.Context()); err != nil {
		t.Fatalf("EnsureAuthed read-only: %v", err)
	}
	lids, err := reader.db.HistoricalLIDJIDs()
	if err != nil {
		t.Fatalf("HistoricalLIDJIDs: %v", err)
	}
	if len(lids) != 1 || lids[0] != lid.String() {
		t.Fatalf("HistoricalLIDJIDs = %#v, want %q", lids, lid.String())
	}
}
