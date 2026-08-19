package catalog

import "fmt"

// TubeSet tracks globally claimed tube numbers across the container catalog and
// every session declaration. A child tube number is permanently claimed at
// reservation time and can never be reused (domain rule 4).
type TubeSet map[TubeID]bool

// Claim marks a tube number as claimed.
func (s TubeSet) Claim(id TubeID) {
	if s != nil {
		s[id] = true
	}
}

// Has reports whether a tube number is already claimed.
func (s TubeSet) Has(id TubeID) bool {
	return s != nil && s[id]
}

// FindDuplicate returns the first tube ID that appears more than once in ids.
func FindDuplicate(ids []TubeID) (TubeID, bool) {
	seen := make(map[TubeID]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			return id, true
		}
		seen[id] = true
	}
	return "", false
}

// EnsureTubeNumbersAvailable validates that none of the requested tube numbers
// is already claimed and that the request contains no duplicates. It returns a
// descriptive error naming the first conflicting tube number so callers can map
// it to the stable CHILD_NUMBER_CONFLICT code (failure boundary 1).
func EnsureTubeNumbersAvailable(requested []TubeID, claimed TubeSet) error {
	if dup, ok := FindDuplicate(requested); ok {
		return fmt.Errorf("duplicate child tube number %q within plan", dup)
	}
	for _, id := range requested {
		if claimed.Has(id) {
			return fmt.Errorf("child tube number %q already claimed", id)
		}
	}
	return nil
}

// ValidateFacts verifies the internal consistency of a bootstrap facts set
// before the first catalog write (failure boundary 1). Every batch revision
// must reference a registered specimen, and every mother tube must reference
// both a registered specimen and a registered batch revision that belongs to
// its own sample. A dangling or cross-sample reference is a stable business
// rejection that must occur before any sample, batch, mother, or operation
// record is written.
//
// Mothers are passed as Container values carrying only their identity
// references (ID, SampleID, BatchID, Revision); the remaining container fields
// are ignored so callers can validate a bootstrap request before any tube is
// persisted.
func ValidateFacts(specimens []Specimen, batches []BatchRevision, mothers []Container) error {
	specimenSet := make(map[SampleID]bool, len(specimens))
	for _, s := range specimens {
		specimenSet[s.ID] = true
	}
	batchSet := make(map[BatchRevision]bool, len(batches))
	for _, b := range batches {
		if !specimenSet[b.SampleID] {
			return fmt.Errorf("batch revision %q/%q/%q references unknown specimen %q",
				b.SampleID, b.BatchID, b.Revision, b.SampleID)
		}
		batchSet[b] = true
	}
	for _, m := range mothers {
		if !specimenSet[m.SampleID] {
			return fmt.Errorf("mother %q references unknown specimen %q", m.ID, m.SampleID)
		}
		key := BatchRevision{SampleID: m.SampleID, BatchID: m.BatchID, Revision: m.Revision}
		if !batchSet[key] {
			return fmt.Errorf("mother %q references unknown batch revision %q/%q/%q",
				m.ID, m.SampleID, m.BatchID, m.Revision)
		}
	}
	return nil
}
