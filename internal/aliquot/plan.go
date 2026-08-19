package aliquot

import (
	"fmt"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
)

// ValidatePlan checks the frozen child plan before reservation: every child
// must carry a non-empty unique tube number, a positive integer allocation that
// does not individually exceed the locked volume, and a total allocation that
// does not exceed the locked volume (domain rules 2 and 5). It is pure and runs
// before any write, so a rejected plan never reserves the mother.
func ValidatePlan(locked int64, plan []ChildPlan) error {
	seen := make(map[catalog.TubeID]bool, len(plan))
	var total int64
	for i, c := range plan {
		if c.ChildTubeID == "" {
			return fmt.Errorf("child %d has an empty tube number", i)
		}
		if seen[c.ChildTubeID] {
			return fmt.Errorf("duplicate child tube number %q", c.ChildTubeID)
		}
		seen[c.ChildTubeID] = true
		if c.PlannedVolumeUL <= 0 {
			return fmt.Errorf("child %q has non-positive allocation %d", c.ChildTubeID, c.PlannedVolumeUL)
		}
		if c.PlannedVolumeUL > locked {
			return fmt.Errorf("child %q allocation %d exceeds locked volume %d", c.ChildTubeID, c.PlannedVolumeUL, locked)
		}
		if c.PlannedVolumeUL > locked-total {
			return fmt.Errorf("plan total exceeds locked volume %d", locked)
		}
		total += c.PlannedVolumeUL
	}
	if total > locked {
		return fmt.Errorf("plan total %d exceeds locked volume %d", total, locked)
	}
	return nil
}

// SortPlan returns a copy of the plan ordered by ordinal, so callers can rely on
// a stable execution order regardless of how the plan was supplied.
func SortPlan(plan []ChildPlan) []ChildPlan {
	out := make([]ChildPlan, len(plan))
	copy(out, plan)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Ordinal < out[j-1].Ordinal; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
