// Package ledger defines the append-only protocol-event ledger: the controlled
// event vocabulary, deterministic envelope encoders, identity rules and the
// event records appended before any materialized view is written.
package ledger

// Source identifies the ingestion path of an event. Controlled vocabulary.
const (
	SourceLive               = "live"
	SourceHistory            = "history"
	SourceOnDemandHistory    = "on_demand_history"
	SourceAppState           = "app_state"
	SourceAppStateRecovery   = "app_state_recovery"
	SourceReceipt            = "receipt"
	SourceIdentityResolution = "identity_resolution"
	SourceScrub              = "scrub"
	SourceLegacySnapshot     = "legacy_snapshot"
	SourceSystemContacts     = "system_contacts"
)

// EventType identifies the logical protocol operation. Controlled vocabulary.
// Legacy snapshot events use one type per source table.
const (
	EventMessage          = "message"
	EventStatusMessage    = "status_message"
	EventReceipt          = "receipt"
	EventStar             = "star"
	EventRevoke           = "revoke"
	EventDeleteForMe      = "delete_for_me"
	EventArchive          = "archive"
	EventPin              = "pin"
	EventMute             = "mute"
	EventMarkRead         = "mark_read"
	EventClearChat        = "clear_chat"
	EventDeleteChat       = "delete_chat"
	EventContact          = "contact"
	EventPushName         = "push_name"
	EventGroup            = "group"
	EventRoster           = "roster"
	EventCall             = "call"
	EventPoll             = "poll"
	EventPollOption       = "poll_option"
	EventPollVote         = "poll_vote"
	EventIdentityResolve  = "identity_resolution"
	EventScrub            = "scrub"
	EventSystemNamesClear = "clear_system_names"

	// Snapshot event types, one per source materialized table.
	EventSnapshotMessages  = "snapshot_messages"
	EventSnapshotChats     = "snapshot_chats"
	EventSnapshotContacts  = "snapshot_contacts"
	EventSnapshotGroups    = "snapshot_groups"
	EventSnapshotRoster    = "snapshot_roster"
	EventSnapshotStarred   = "snapshot_starred"
	EventSnapshotCalls     = "snapshot_calls"
	EventSnapshotStatus    = "snapshot_status"
	EventSnapshotPolls     = "snapshot_polls"
	EventSnapshotPollVotes = "snapshot_poll_votes"
	EventSnapshotLocations = "snapshot_locations"
	EventSnapshotPurges    = "snapshot_purges"
)

// Flags bitmask carried on every event.
const (
	// FlagFromMe marks events produced by the current user's own devices.
	FlagFromMe int64 = 1 << iota
	// FlagFromFullSync marks app-state events emitted during full sync replay.
	FlagFromFullSync
	// FlagDangling marks events whose causal target was not present at append
	// time (informational; the authoritative dangling set comes from the
	// linker queries).
	FlagDangling
)

// CausalRef points at the event's cause, either by WhatsApp message key or by
// an explicit ledger event_id. JSON tags define the stable on-wire encoding.
type CausalRef struct {
	// Kind is "wa_key" or "event_id".
	Kind    string `json:"kind"`
	ChatJID string `json:"chat_jid,omitempty"`
	MsgID   string `json:"msg_id,omitempty"`
	Sender  string `json:"sender,omitempty"`
	FromMe  bool   `json:"from_me,omitempty"`
	EventID string `json:"event_id,omitempty"`
}

// WAKeyRef builds a WhatsApp-key causal reference.
func WAKeyRef(chatJID, msgID, senderJID string, fromMe bool) CausalRef {
	return CausalRef{Kind: "wa_key", ChatJID: chatJID, MsgID: msgID, Sender: senderJID, FromMe: fromMe}
}

// Event is a ready-to-append ledger record. Raw bytes are kept out of the
// record and written to the separate ledger_raw table.
type Event struct {
	EventID       string
	Source        string
	EventType     string
	WAKey         string
	ChatJID       string
	MsgID         string
	SenderJID     string
	ServerTS      int64
	EventTS       int64
	ReceivedAt    int64
	RawHash       string
	ParserVersion string
	RulesVersion  string
	DedupKey      string
	CausalRefs    []CausalRef
	Causes        []string
	BatchID       int64
	Flags         int64
	// Snapshot is the JSON payload for snapshot and scrub events.
	Snapshot []byte
	// RawBytes, when present, is written to ledger_raw; empty for events that
	// do not retain an envelope (snapshots, scrubs).
	RawBytes []byte
}

// IsKnownSource reports whether s is a valid source value.
func IsKnownSource(s string) bool {
	switch s {
	case SourceLive, SourceHistory, SourceOnDemandHistory, SourceAppState,
		SourceAppStateRecovery, SourceReceipt, SourceIdentityResolution,
		SourceScrub, SourceLegacySnapshot, SourceSystemContacts:
		return true
	}
	return false
}
