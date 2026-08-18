package aggregate_test

import (
	"context"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
)

// TestReservationFreezesSnapshot verifies that reservation freezes the sample,
// batch revision, mother tube, integer volume, and ordered child plan as an
// immutable snapshot (acceptance 1).
func TestReservationFreezesSnapshot(t *testing.T) {
	svc, _ := newService(t)
	bootstrap(t, svc, "op-boot")

	view := reserveRes(t, svc, "op-res")

	if view.SessionID == "" {
		t.Fatalf("expected a non-empty session id")
	}
	if view.MotherTubeID != "M1" || view.SampleID != "S1" || view.BatchID != "B1" || view.Revision != "r1" {
		t.Fatalf("snapshot catalog mismatch: %+v", view)
	}
	if view.LockedVolumeUL != 1200 {
		t.Fatalf("locked volume = %d, want 1200", view.LockedVolumeUL)
	}
	if view.Status != aliquot.StatusReserved {
		t.Fatalf("status = %s, want reserved", view.Status)
	}
	if view.RemainingUL != 1200 {
		t.Fatalf("remaining = %d, want 1200", view.RemainingUL)
	}
	if len(view.Children) != 3 {
		t.Fatalf("children = %d, want 3", len(view.Children))
	}
	want := []struct {
		id  catalog.TubeID
		vol int64
	}{
		{"C1", 500}, {"C2", 400}, {"C3", 200},
	}
	for i, w := range want {
		c := view.Children[i]
		if c.ChildTubeID != w.id || c.PlannedVolumeUL != w.vol || c.Created {
			t.Fatalf("child %d = %+v, want id=%s vol=%d created=false", i, c, w.id, w.vol)
		}
	}

	// Re-reading the session returns the same frozen snapshot.
	again, err := svc.GetSession(context.Background(), view.SessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if again.Revision != "r1" || again.LockedVolumeUL != 1200 || len(again.Children) != 3 {
		t.Fatalf("snapshot changed between reads: %+v", again)
	}
}

// TestReservationRejectsSampleOrBatchMismatch verifies that a mismatched
// reservation rejects without reserving the mother (acceptance 1).
func TestReservationRejectsSampleOrBatchMismatch(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")

	_, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "op-bad-sample"},
		MotherTubeID: "M1", SampleID: "S2", BatchID: "B1", Revision: "r1",
		LockedVolume: 1200,
		Children:     []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500}},
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeSampleMismatch {
		t.Fatalf("expected SAMPLE_MISMATCH, got %v", err)
	}

	_, err = svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "op-bad-batch"},
		MotherTubeID: "M1", SampleID: "S1", BatchID: "B2", Revision: "r1",
		LockedVolume: 1200,
		Children:     []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500}},
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeBatchMismatch {
		t.Fatalf("expected BATCH_MISMATCH, got %v", err)
	}

	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if !mother.IsAvailable() {
		t.Fatalf("mother should remain available, got status %q", mother.Status)
	}
	if mother.SessionID != "" {
		t.Fatalf("mother should have no session, got %q", mother.SessionID)
	}
}

// TestConcurrentReservationHasOneWinner verifies that two concurrent
// reservations of the same mother produce exactly one winner and one stable
// MOTHER_ALREADY_RESERVED rejection (acceptance 2).
func TestConcurrentReservationHasOneWinner(t *testing.T) {
	barrier := newTestBarrier()
	hooks := &store.Hooks{Barrier: barrier.hook}
	svc, _ := openService(t, tempDBPath(t), hooks)
	bootstrap(t, svc, "op-boot")

	type result struct {
		view aggregate.SessionView
		err  error
	}
	results := make(chan result, 2)
	run := func(opID string) {
		v, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
			Operation:    aggregate.Operation{OperationID: opID},
			MotherTubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1",
			LockedVolume: 1200,
			Children: []aliquot.ChildPlan{
				{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
				{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
				{Ordinal: 2, ChildTubeID: "C3", PlannedVolumeUL: 200},
			},
		})
		results <- result{view: v, err: err}
	}

	go run("op-a")
	go run("op-b")

	barrier.waitFor(2)
	barrier.releaseByIndex(0)
	first := <-results
	barrier.releaseByIndex(1)
	second := <-results

	successes := 0
	conflicts := 0
	for _, r := range []result{first, second} {
		if r.err == nil {
			successes++
			continue
		}
		if e, ok := r.err.(*aliquot.Error); ok && e.Code == aliquot.CodeMotherAlreadyReserved {
			conflicts++
			continue
		}
		t.Fatalf("unexpected error: %v", r.err)
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d, want exactly one each", successes, conflicts)
	}
}

// TestChildNumberConflictAtReservation verifies that a reservation whose plan
// reuses a tube number already declared by another session rolls back entirely
// (domain rule 4, acceptance 4).
func TestChildNumberConflictAtReservation(t *testing.T) {
	svc, st := newService(t)
	if err := svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
		OperationID: "op-boot",
		Facts: store.Facts{
			Specimens: []catalog.Specimen{{ID: "S1", Description: "sample one"}},
			BatchRevisions: []catalog.BatchRevision{
				{SampleID: "S1", BatchID: "B1", Revision: "r1"},
			},
			Mothers: []store.MotherFact{
				{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200},
				{TubeID: "M2", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 600},
			},
		},
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// Reserve M2 first, declaring C1 and C2.
	_, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "op-m2"},
		MotherTubeID: "M2", SampleID: "S1", BatchID: "B1", Revision: "r1",
		LockedVolume: 600,
		Children: []aliquot.ChildPlan{
			{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 300},
			{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 300},
		},
	})
	if err != nil {
		t.Fatalf("reserve M2: %v", err)
	}

	// M1's plan reuses C1, which is already declared by M2's session.
	_, err = svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "op-m1"},
		MotherTubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1",
		LockedVolume: 1200,
		Children: []aliquot.ChildPlan{
			{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
			{Ordinal: 1, ChildTubeID: "C3", PlannedVolumeUL: 700},
		},
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeChildNumberConflict {
		t.Fatalf("expected CHILD_NUMBER_CONFLICT, got %v", err)
	}

	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if !mother.IsAvailable() {
		t.Fatalf("M1 should remain available after failed reservation, got %q", mother.Status)
	}
}
