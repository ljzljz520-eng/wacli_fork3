package ledger

// CanonicalContact is the deterministic encoding of an observed contact
// snapshot. Empty fields are omitted so re-delivering partial snapshots
// stays byte-identical when nothing changed. It projects to the contacts
// merge (each provided field fills a previously empty column); SystemName is
// explicit and overwrites.
type CanonicalContact struct {
	JID          string `json:"jid"`
	Phone        string `json:"phone,omitempty"`
	PushName     string `json:"push_name,omitempty"`
	FullName     string `json:"full_name,omitempty"`
	FirstName    string `json:"first_name,omitempty"`
	BusinessName string `json:"business_name,omitempty"`
	SystemName   string `json:"system_name,omitempty"`
}
