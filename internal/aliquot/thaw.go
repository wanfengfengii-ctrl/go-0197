package aliquot

import "github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"

// ThawEvidence is the operator's physically scanned thaw report: the mother
// tube, specimen, and batch revision observed when the tube was pulled from
// frozen storage (business flow 3).
type ThawEvidence struct {
	MotherTubeID catalog.TubeID
	SampleID     catalog.SampleID
	BatchID      catalog.BatchID
	Revision     catalog.Revision
}

// Matches reports whether the thaw evidence matches the frozen reservation
// snapshot exactly. Any mismatch at the thaw step quarantines the whole batch.
func (e ThawEvidence) Matches(s Snapshot) bool {
	return e.MotherTubeID == s.MotherTubeID &&
		e.SampleID == s.SampleID &&
		e.BatchID == s.BatchID &&
		e.Revision == s.Revision
}
