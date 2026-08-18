package lineage

import (
	"fmt"
	"sort"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
)

// BuildManifest produces the unique immutable finalization manifest from the
// finalized edges and per-child verifications. Items are ordered stably by child
// tube ID and assigned a monotonic ordinal (domain rule 10). It fails rather
// than emit a partial manifest if any finalized child lacks a verification.
func BuildManifest(m Manifest, edges []Edge, verifications map[catalog.TubeID]Verification) (Manifest, []ManifestItem, error) {
	items := make([]ManifestItem, 0, len(edges))
	for _, e := range edges {
		v, ok := verifications[e.ChildTubeID]
		if !ok {
			return Manifest{}, nil, fmt.Errorf("child %q is missing a refreeze verification", e.ChildTubeID)
		}
		items = append(items, ManifestItem{
			ChildTubeID:  e.ChildTubeID,
			AllocationUL: e.AllocationUL,
			FreezeRunID:  v.FreezeRunID,
		})
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].ChildTubeID < items[j].ChildTubeID })
	for i := range items {
		items[i].Ordinal = i
	}
	return m, items, nil
}

// ManifestFromGraph builds a manifest header from the session snapshot and the
// lineage graph's finalized edges and verifications.
func ManifestFromGraph(session catalog.SessionID, parent catalog.TubeID, sample catalog.SampleID, batch catalog.BatchID, revision catalog.Revision, frozen int64, totalLoss int64, g *Graph) (Manifest, []ManifestItem, error) {
	header := Manifest{
		SessionID:      session,
		MotherTubeID:   parent,
		SampleID:       sample,
		BatchID:        batch,
		Revision:       revision,
		FrozenVolumeUL: frozen,
		TotalLossUL:    totalLoss,
	}
	return BuildManifest(header, g.Edges, g.Verifications)
}
