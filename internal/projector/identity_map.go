package projector

// viewIdentityMap accumulates LID→PN mappings exclusively from
// identity_resolution ledger events folded by one view instance. It is the
// projector-owned IdentityMap used during replay: each view rebuilds its own
// map deterministically from the event stream, independent of other views.
type viewIdentityMap struct {
	mappings map[string]string
}

func newViewIdentityMap() *viewIdentityMap {
	return &viewIdentityMap{mappings: make(map[string]string)}
}

func (m *viewIdentityMap) Lookup(jid string) (string, bool) {
	if m == nil {
		return "", false
	}
	canonical, ok := m.mappings[jid]
	return canonical, ok
}

// add records one mapping. Empty keys are ignored.
func (m *viewIdentityMap) add(from, to string) {
	if from == "" || to == "" {
		return
	}
	m.mappings[from] = to
}
