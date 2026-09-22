package ledger

// CanonicalGroup is the deterministic encoding of one observed group
// snapshot: metadata plus the full roster (ordered by user JID by the
// builder, so repeated snapshots of the same state are byte-identical).
//
// A snapshot with LeftAt>0 and no participants expresses a leave event.
type CanonicalGroup struct {
	JID             string                      `json:"jid"`
	Name            string                      `json:"name,omitempty"`
	OwnerJID        string                      `json:"owner_jid,omitempty"`
	CreatedTS       int64                       `json:"created_ts,omitempty"`
	IsParent        bool                        `json:"is_parent,omitempty"`
	LinkedParentJID string                      `json:"linked_parent_jid,omitempty"`
	LeftAt          int64                       `json:"left_at,omitempty"`
	Participants    []CanonicalGroupParticipant `json:"participants,omitempty"`
}

// CanonicalGroupParticipant is one roster entry in a group snapshot.
type CanonicalGroupParticipant struct {
	UserJID string `json:"user_jid"`
	Role    string `json:"role"`
}
