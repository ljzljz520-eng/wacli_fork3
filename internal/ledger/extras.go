package ledger

// CanonicalStar expresses one star/unstar fold for a message.
type CanonicalStar struct {
	ChatJID   string `json:"chat_jid"`
	MsgID     string `json:"msg_id"`
	SenderJID string `json:"sender_jid,omitempty"`
	FromMe    bool   `json:"from_me,omitempty"`
	Starred   bool   `json:"starred"`
	StarredTS int64  `json:"starred_ts,omitempty"`
}

// CanonicalStatusMessage expresses one status broadcast message.
type CanonicalStatusMessage struct {
	MsgID           string `json:"msg_id"`
	TS              int64  `json:"ts"`
	FromMe          bool   `json:"from_me,omitempty"`
	SenderJID       string `json:"sender_jid,omitempty"`
	SenderName      string `json:"sender_name,omitempty"`
	Text            string `json:"text,omitempty"`
	MediaType       string `json:"media_type,omitempty"`
	MediaCaption    string `json:"media_caption,omitempty"`
	Filename        string `json:"filename,omitempty"`
	MimeType        string `json:"mime_type,omitempty"`
	DirectPath      string `json:"direct_path,omitempty"`
	MediaKey        []byte `json:"media_key,omitempty"`
	FileSHA256      []byte `json:"file_sha256,omitempty"`
	FileEncSHA256   []byte `json:"file_enc_sha256,omitempty"`
	FileLength      int64  `json:"file_length,omitempty"`
	BackgroundColor string `json:"background_color,omitempty"`
	Font            int32  `json:"font,omitempty"`
}

// CanonicalCallParticipant is one participant entry inside a call event.
type CanonicalCallParticipant struct {
	JID     string `json:"jid"`
	Outcome string `json:"outcome,omitempty"`
}

// CanonicalCallEvent expresses one call event. Deleted=true turns it into a
// call-log delete marker (only ChatJID/Direction apply).
type CanonicalCallEvent struct {
	ChatJID      string                     `json:"chat_jid"`
	ChatName     string                     `json:"chat_name,omitempty"`
	SenderJID    string                     `json:"sender_jid,omitempty"`
	SenderName   string                     `json:"sender_name,omitempty"`
	CallID       string                     `json:"call_id,omitempty"`
	MsgID        string                     `json:"msg_id,omitempty"`
	EventType    string                     `json:"event_type,omitempty"`
	Direction    string                     `json:"direction,omitempty"`
	Media        string                     `json:"media,omitempty"`
	Outcome      string                     `json:"outcome,omitempty"`
	Reason       string                     `json:"reason,omitempty"`
	CallType     string                     `json:"call_type,omitempty"`
	DurationSecs int64                      `json:"duration_secs,omitempty"`
	TS           int64                      `json:"ts,omitempty"`
	Participants []CanonicalCallParticipant `json:"participants,omitempty"`

	Deleted bool `json:"deleted,omitempty"`
}
