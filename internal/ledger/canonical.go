package ledger

import (
	"sort"
	"time"
)

// CanonicalReceipt is the deterministic encoding of a delivery/read receipt.
type CanonicalReceipt struct {
	Type          string   `json:"type"`
	Chat          string   `json:"chat"`
	Sender        string   `json:"sender,omitempty"`
	MessageSender string   `json:"message_sender,omitempty"`
	MessageIDs    []string `json:"message_ids"`
	TimestampMS   int64    `json:"timestamp_ms"`
}

// NewCanonicalReceipt builds the canonical receipt encoding. Message IDs are
// sorted so reordering on the wire does not produce a different event.
func NewCanonicalReceipt(receiptType, chatJID, senderJID, messageSenderJID string,
	messageIDs []string, timestamp time.Time) CanonicalReceipt {
	ids := append([]string(nil), messageIDs...)
	sort.Strings(ids)
	return CanonicalReceipt{
		Type:          receiptType,
		Chat:          chatJID,
		Sender:        senderJID,
		MessageSender: messageSenderJID,
		MessageIDs:    ids,
		TimestampMS:   timestamp.UnixMilli(),
	}
}

// StateEvent is the canonical encoding of an app-state persistence event.
type StateEvent struct {
	Type        string `json:"type"`
	Chat        string `json:"chat,omitempty"`
	Sender      string `json:"sender,omitempty"`
	FromMe      bool   `json:"from_me,omitempty"`
	MsgID       string `json:"msg_id,omitempty"`
	State       bool   `json:"state,omitempty"`
	EndTSMS     int64  `json:"end_ts_ms,omitempty"`
	Range       string `json:"range,omitempty"`
	DeleteMedia bool   `json:"delete_media,omitempty"`
	// Count is the explicit unread count for StateUnreadCount snapshots.
	Count       int64 `json:"count,omitempty"`
	TimestampMS int64 `json:"timestamp_ms"`
}

// App-state canonical type labels.
const (
	StateStar        = "star"
	StateDeleteForMe = "delete_for_me"
	StateArchive     = "archive"
	StatePin         = "pin"
	StateMute        = "mute"
	StateMarkRead    = "mark_read"
	StateUnreadCount = "unread_count"
)
