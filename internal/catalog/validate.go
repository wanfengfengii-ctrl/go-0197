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
