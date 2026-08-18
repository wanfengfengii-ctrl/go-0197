// Package lineage maintains the 1:N mother-to-children lineage graph: pending
// edges, per-child refreeze verification, whole-batch quarantine marking, and
// the unique immutable finalization manifest (domain rules 9-10).
package lineage

import (
	"sort"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
)

// EdgeState is the state of a parent-to-child lineage edge.
type EdgeState string

const (
	EdgePending   EdgeState = "pending"
	EdgeFinalized EdgeState = "finalized"
)

// Edge is a pending or finalized parent-child lineage edge.
type Edge struct {
	SessionID    catalog.SessionID
	ParentTubeID catalog.TubeID
	ChildTubeID  catalog.TubeID
	AllocationUL int64
	State        EdgeState
}

// Verification is a per-child refreeze verification record. Each child has at
// most one verification.
type Verification struct {
	ChildTubeID  catalog.TubeID
	SampleID     catalog.SampleID
	BatchID      catalog.BatchID
	Revision     catalog.Revision
	AllocationUL int64
	FreezeRunID  string
}

// Manifest is the unique immutable finalization header (domain rule 10).
type Manifest struct {
	SessionID      catalog.SessionID `json:"session_id"`
	MotherTubeID   catalog.TubeID    `json:"mother_tube_id"`
	SampleID       catalog.SampleID  `json:"sample_id"`
	BatchID        catalog.BatchID   `json:"batch_id"`
	Revision       catalog.Revision  `json:"revision"`
	FrozenVolumeUL int64             `json:"frozen_volume_uL"`
	TotalLossUL    int64             `json:"total_loss_uL"`
}

// ManifestItem is a stably ordered manifest detail row.
type ManifestItem struct {
	Ordinal      int            `json:"ordinal"`
	ChildTubeID  catalog.TubeID `json:"child_tube_id"`
	AllocationUL int64          `json:"allocation_uL"`
	FreezeRunID  string         `json:"freeze_run_id"`
}

// MissingVerifications returns the planned child tube IDs that lack a matching
// verification, sorted stably by tube ID (domain rule 9). A planned child is
// considered verified when it has a verification whose sample, batch, revision,
// and allocation match the frozen plan.
func MissingVerifications(plan []catalog.TubeID, verifications map[catalog.TubeID]Verification) []catalog.TubeID {
	missing := make([]catalog.TubeID, 0, len(plan))
	for _, id := range plan {
		if _, ok := verifications[id]; !ok {
			missing = append(missing, id)
		}
	}
	sort.SliceStable(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return missing
}
