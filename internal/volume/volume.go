// Package volume implements the integer-microliter reservation ledger. All
// quantities are non-negative int64 microliters. It records the frozen start
// volume, per-child allocations, losses, and the remaining volume, and it
// validates the conservation identity after every commit (domain rule 5).
package volume

import (
	"fmt"
)

// EntryType classifies a ledger entry.
type EntryType string

const (
	// EntryChildAllocation records the exact planned allocation of a created child.
	EntryChildAllocation EntryType = "child_allocation"
	// EntryRecordedLoss records a non-negative integer loss in microliters.
	EntryRecordedLoss EntryType = "recorded_loss"
)

// Entry is a single append-only ledger line used to recompute conservation.
type Entry struct {
	Seq         int
	Type        EntryType
	QuantityUL  int64
	ChildTubeID string
	OperationID string
}

// Summary is the derived conservation view of a session.
type Summary struct {
	LockedVolumeUL    int64 // frozen aliquotable volume
	AllocatedUL       int64 // sum of created child allocations
	LossUL            int64 // sum of recorded losses
	RemainingUL       int64 // mother remaining volume
	OutstandingPlanUL int64 // planned-but-not-created child volume
}

// CheckConservation validates the integer-microliter conservation identity and
// the outstanding-plan coverage rule (domain rule 5):
//
//	locked = allocated + loss + remaining, and remaining >= outstanding plan.
//
// It returns a descriptive error for the first violated invariant.
func CheckConservation(locked, allocated, loss, remaining, outstanding int64) error {
	if locked < 0 || allocated < 0 || loss < 0 || remaining < 0 || outstanding < 0 {
		return fmt.Errorf("negative volume: locked=%d allocated=%d loss=%d remaining=%d outstanding=%d",
			locked, allocated, loss, remaining, outstanding)
	}
	if locked != allocated+loss+remaining {
		return fmt.Errorf("conservation violated: locked %d != allocated %d + loss %d + remaining %d",
			locked, allocated, loss, remaining)
	}
	if remaining < outstanding {
		return fmt.Errorf("outstanding plan %d exceeds remaining %d", outstanding, remaining)
	}
	return nil
}
