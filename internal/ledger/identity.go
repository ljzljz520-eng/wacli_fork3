package ledger

// CanonicalIdentityResolution records one mapping learned from the LID
// resolver after connect/EnsureAuthed. It carries no protocol envelope; the
// projector folds it into JID rewrites and row merges across all views.
type CanonicalIdentityResolution struct {
	// LID is the hidden-user JID (e.g. 12345@lid).
	LID string `json:"lid"`
	// PN is the canonical phone-number user JID (e.g. 12345@s.whatsapp.net).
	PN string `json:"pn"`
	// ResolvedTS is the unix second at which the resolver returned the map.
	ResolvedTS int64 `json:"resolved_ts,omitempty"`
}
