package volume

// Ledger accumulates append-only entries for a session and derives the
// conservation summary from the frozen locked volume (domain rule 5). It is a
// pure, in-memory projection of the persisted volume_entries table.
type Ledger struct {
	LockedVolumeUL int64
	Entries        []Entry
}

// NewLedger constructs a ledger from the frozen locked volume and the ordered
// persisted entries.
func NewLedger(locked int64, entries []Entry) *Ledger {
	return &Ledger{LockedVolumeUL: locked, Entries: entries}
}

// Allocated returns the sum of child allocation entries.
func (l *Ledger) Allocated() int64 {
	var total int64
	for _, e := range l.Entries {
		if e.Type == EntryChildAllocation {
			total += e.QuantityUL
		}
	}
	return total
}

// Loss returns the sum of recorded loss entries.
func (l *Ledger) Loss() int64 {
	var total int64
	for _, e := range l.Entries {
		if e.Type == EntryRecordedLoss {
			total += e.QuantityUL
		}
	}
	return total
}

// Remaining returns the mother remaining volume implied by the conservation
// identity: locked - allocated - loss.
func (l *Ledger) Remaining() int64 {
	return l.LockedVolumeUL - l.Allocated() - l.Loss()
}

// Summary derives the full conservation view including the outstanding planned
// (not yet created) child volume.
func (l *Ledger) Summary(outstanding int64) Summary {
	return Summary{
		LockedVolumeUL:    l.LockedVolumeUL,
		AllocatedUL:       l.Allocated(),
		LossUL:            l.Loss(),
		RemainingUL:       l.Remaining(),
		OutstandingPlanUL: outstanding,
	}
}

// Validate asserts the conservation identity and the outstanding-plan coverage
// rule for the current ledger state.
func (l *Ledger) Validate(outstanding int64) error {
	s := l.Summary(outstanding)
	return CheckConservation(s.LockedVolumeUL, s.AllocatedUL, s.LossUL, s.RemainingUL, s.OutstandingPlanUL)
}

// OutstandingPlan computes the sum of planned volumes for children that have not
// yet been created.
func OutstandingPlan(plan []PlanItem) int64 {
	var total int64
	for _, p := range plan {
		if !p.Created {
			total += p.PlannedVolumeUL
		}
	}
	return total
}

// PlanItem is the minimal planned-child projection needed to compute the
// outstanding plan volume without importing the aliquot session model.
type PlanItem struct {
	ChildTubeID     string
	PlannedVolumeUL int64
	Created         bool
}
