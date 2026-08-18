package aliquot

import "github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"

// ChildPlan is a single ordered planned child tube. The ordinal, child tube
// number, and planned integer microliter allocation are frozen at reservation.
type ChildPlan struct {
	Ordinal         int
	ChildTubeID     catalog.TubeID
	PlannedVolumeUL int64
}

// Snapshot is the immutable reservation snapshot (domain rule 2). Sample,
// batch, revision, mother tube, plan order, child tube numbers, and each
// allocation are frozen and must not change for the life of the session.
type Snapshot struct {
	SessionID      catalog.SessionID
	MotherTubeID   catalog.TubeID
	SampleID       catalog.SampleID
	BatchID        catalog.BatchID
	Revision       catalog.Revision
	LockedVolumeUL int64
	Children       []ChildPlan
}

// PlannedVolume returns the total planned allocation across all children.
func (s Snapshot) PlannedVolume() int64 {
	var total int64
	for _, c := range s.Children {
		total += c.PlannedVolumeUL
	}
	return total
}
