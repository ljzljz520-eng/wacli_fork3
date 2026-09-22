package projector

import (
	"encoding/json"
	"fmt"

	"github.com/openclaw/wacli/internal/ledger"
)

// decodeIdentityResolution extracts the canonical payload of an
// identity_resolution event from its stored raw bytes.
func decodeIdentityResolution(raw []byte) (ledger.CanonicalIdentityResolution, error) {
	var res ledger.CanonicalIdentityResolution
	if err := json.Unmarshal(raw, &res); err != nil {
		return res, fmt.Errorf("decode identity_resolution payload: %w", err)
	}
	return res, nil
}
