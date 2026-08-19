package aggregate_test

import (
	"context"
	"math"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
)

// maxVolumeFacts builds a catalog whose single mother carries the given volume,
// used to exercise the extreme integer boundary at the system upper limit.
func maxVolumeFacts(volume int64) store.Facts {
	return store.Facts{
		Specimens: []catalog.Specimen{{ID: "S1", Description: "sample one"}},
		BatchRevisions: []catalog.BatchRevision{
			{SampleID: "S1", BatchID: "B1", Revision: "r1"},
		},
		Mothers: []store.MotherFact{
			{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: volume},
		},
	}
}

func bootstrapVolume(t *testing.T, svc aggregate.Service, volume int64) {
	t.Helper()
	if err := svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
		OperationID: "op-boot",
		Facts:       maxVolumeFacts(volume),
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
}

// TestModel_ReservationRejectsOverflowPlanLeavesNoState is the end-to-end
// regression for the integer-overflow capacity bug. When the mother lock is at
// the int64 upper limit and the child plan's true total exceeds it (two
// max-int64 allocations whose naive sum overflows and wraps negative), the
// reservation must reject immediately with OVER_ALLOCATION and leave no mother
// occupation, no session, and no child tube number declaration. The rejection
// is persisted so a same-content retry replays it (domain rule 12, failure
// boundary 1).
func TestModel_ReservationRejectsOverflowPlanLeavesNoState(t *testing.T) {
	svc, st := newService(t)
	bootstrapVolume(t, svc, math.MaxInt64)

	const max = int64(math.MaxInt64)
	overflowChildren := []aliquot.ChildPlan{
		{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: max},
		{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: max},
	}
	reserve := func(opID string) error {
		_, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
			Operation:    aggregate.Operation{OperationID: opID},
			MotherTubeID: "M1",
			SampleID:     "S1",
			BatchID:      "B1",
			Revision:     "r1",
			LockedVolume: max,
			Children:     overflowChildren,
		})
		return err
	}

	if err := reserve("op-overflow"); err == nil {
		t.Fatalf("overflow reservation unexpectedly succeeded; expected OVER_ALLOCATION before any write")
	} else if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeOverAllocation {
		t.Fatalf("expected OVER_ALLOCATION, got %v", err)
	}

	// Mother must remain available and unbound: no occupation, no session.
	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if !mother.IsAvailable() {
		t.Fatalf("mother should remain available, got status %q session %q", mother.Status, mother.SessionID)
	}
	if mother.SessionID != "" {
		t.Fatalf("mother should have no session, got %q", mother.SessionID)
	}
	if mother.RemainingUL != max {
		t.Fatalf("mother remaining = %d, want %d (unchanged)", mother.RemainingUL, max)
	}

	// No child tube numbers may be declared by the rejected plan.
	claimed, err := st.ClaimedTubeNumbers(context.Background(), []catalog.TubeID{"C1", "C2"})
	if err != nil {
		t.Fatalf("claimed tube numbers: %v", err)
	}
	for _, id := range []catalog.TubeID{"C1", "C2"} {
		if claimed.Has(id) {
			t.Fatalf("child tube number %q must not be declared after a rejected reservation", id)
		}
	}

	// A same-content retry must replay the same stable rejection (domain rule 12).
	if err := reserve("op-overflow"); err == nil {
		t.Fatalf("retry unexpectedly succeeded; expected stable OVER_ALLOCATION")
	} else if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeOverAllocation {
		t.Fatalf("retry expected OVER_ALLOCATION, got %v", err)
	}

	// The rejected mother must still be freely reservable with a valid plan,
	// proving no occupation or session lingered. Reusing C1 also proves the
	// number was never declared.
	if _, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "op-valid-after-reject"},
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		LockedVolume: max,
		Children:     []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: max}},
	}); err != nil {
		t.Fatalf("valid reservation after rejection should succeed, got %v", err)
	}
}

// TestModel_ReservationAcceptsLargeBoundaryPlan verifies that the fix preserves
// the normal large-volume plan behavior: a plan whose total exactly equals the
// locked volume at the int64 extreme is still accepted, the mother is reserved,
// and the frozen snapshot and persisted state carry the full volume (acceptance 1).
func TestModel_ReservationAcceptsLargeBoundaryPlan(t *testing.T) {
	svc, st := newService(t)
	bootstrapVolume(t, svc, math.MaxInt64)

	const max = int64(math.MaxInt64)
	// Single child consuming the entire max volume: total == locked, accepted.
	view, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "op-large"},
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		LockedVolume: max,
		Children:     []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: max}},
	})
	if err != nil {
		t.Fatalf("expected large boundary reservation to succeed, got %v", err)
	}
	if view.LockedVolumeUL != max {
		t.Fatalf("locked volume = %d, want %d", view.LockedVolumeUL, max)
	}
	if view.Status != aliquot.StatusReserved {
		t.Fatalf("status = %s, want reserved", view.Status)
	}
	if view.RemainingUL != max {
		t.Fatalf("remaining = %d, want %d", view.RemainingUL, max)
	}
	if len(view.Children) != 1 || view.Children[0].PlannedVolumeUL != max || view.Children[0].Created {
		t.Fatalf("child plan = %+v, want single uncreated child at %d", view.Children, max)
	}

	// Persisted state must reflect the reservation independently of the view.
	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if mother.Status != catalog.ContainerStatusReserved {
		t.Fatalf("mother status = %q, want reserved", mother.Status)
	}
	if mother.SessionID != view.SessionID {
		t.Fatalf("mother session = %q, want %q", mother.SessionID, view.SessionID)
	}
	claimed, err := st.ClaimedTubeNumbers(context.Background(), []catalog.TubeID{"C1"})
	if err != nil {
		t.Fatalf("claimed tube numbers: %v", err)
	}
	if !claimed.Has("C1") {
		t.Fatalf("child tube number C1 should be declared after a successful reservation")
	}
}

// TestModel_ReservationRejectsOverLimitSumRepresentable covers the over-limit
// case that does not involve overflow: a plan total that stays representable
// but exceeds the locked volume must also be rejected at reservation time with
// no state side effects, mirroring the overflow path.
func TestModel_ReservationRejectsOverLimitSumRepresentable(t *testing.T) {
	svc, st := newService(t)
	bootstrapVolume(t, svc, 1000)

	_, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "op-over"},
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		LockedVolume: 1000,
		Children: []aliquot.ChildPlan{
			{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 600},
			{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 600},
		},
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeOverAllocation {
		t.Fatalf("expected OVER_ALLOCATION, got %v", err)
	}

	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if !mother.IsAvailable() || mother.SessionID != "" || mother.RemainingUL != 1000 {
		t.Fatalf("mother should be unchanged available/1000, got status=%q session=%q remaining=%d",
			mother.Status, mother.SessionID, mother.RemainingUL)
	}
	claimed, err := st.ClaimedTubeNumbers(context.Background(), []catalog.TubeID{"C1", "C2"})
	if err != nil {
		t.Fatalf("claimed tube numbers: %v", err)
	}
	for _, id := range []catalog.TubeID{"C1", "C2"} {
		if claimed.Has(id) {
			t.Fatalf("child tube number %q must not be declared after a rejected reservation", id)
		}
	}
}
