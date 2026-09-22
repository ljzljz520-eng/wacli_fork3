package ledger

// CanonicalPoll expresses one poll creation snapshot. Options are the full
// option list at creation time; later single-option additions are expressed
// as separate CanonicalPollOptionAdd events.
type CanonicalPoll struct {
	ChatJID         string   `json:"chat_jid"`
	MsgID           string   `json:"msg_id"`
	SenderJID       string   `json:"sender_jid,omitempty"`
	Question        string   `json:"question,omitempty"`
	Options         []string `json:"options"`
	SelectableCount uint32   `json:"selectable_count,omitempty"`
	CreatedTS       int64    `json:"created_ts,omitempty"`
}

// CanonicalPollOptionAdd expresses one poll-update message appending a
// single option to an existing poll.
type CanonicalPollOptionAdd struct {
	ChatJID   string `json:"chat_jid"`
	PollMsgID string `json:"poll_msg_id"`
	Option    string `json:"option"`
}

// CanonicalPollVote expresses one voter's latest vote on a poll. Deleted=true
// (empty selection) turns it into a vote retraction marker.
type CanonicalPollVote struct {
	ChatJID       string   `json:"chat_jid"`
	PollMsgID     string   `json:"poll_msg_id"`
	VoterJID      string   `json:"voter_jid"`
	VoteMsgID     string   `json:"vote_msg_id,omitempty"`
	Selected      []string `json:"selected"`
	UnknownHashes []string `json:"unknown_hashes,omitempty"`
	VotedTSMS     int64    `json:"voted_ts_ms"`

	Deleted bool `json:"deleted,omitempty"`
}
