package aggregate_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
)

// standardReserveCommand builds the canonical reservation used by the reorder
// idempotency tests: a 1200 uL mother and a 500/400/200 three-child plan.
func standardReserveCommand(opID string, plan []aliquot.ChildPlan) aggregate.ReserveMotherCommand {
	return aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: opID},
		MotherTubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1",
		LockedVolume: 1200,
		Children:     plan,
	}
}

func threeChildPlan() []aliquot.ChildPlan {
	return []aliquot.ChildPlan{
		{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
		{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
		{Ordinal: 2, ChildTubeID: "C3", PlannedVolumeUL: 200},
	}
}

// TestModel_ReserveReorderRetryReplaysOriginal covers the reorder-retry
// boundary: after a first submission carrying an ordered multi-child plan, a
// retry that only rearranges the children array (keeping each ordinal, tube
// number, allocation, and mother info unchanged) must replay the original
// session and revision instead of returning OPERATION_CONFLICT.
func TestModel_ReserveReorderRetryReplaysOriginal(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")

	plan := threeChildPlan()
	first, err := svc.ReserveMother(context.Background(), standardReserveCommand("op-res", plan))
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	origSession := first.SessionID
	if first.RevisionNum != 0 {
		t.Fatalf("first revision = %d, want 0", first.RevisionNum)
	}

	// Every rearrangement of the children array is semantically identical and
	// must replay the first result without re-executing.
	perms := [][]aliquot.ChildPlan{
		{plan[0], plan[1], plan[2]}, // identical order
		{plan[2], plan[0], plan[1]},
		{plan[1], plan[2], plan[0]},
		{plan[2], plan[1], plan[0]}, // fully reversed
		{plan[1], plan[0], plan[2]},
		{plan[0], plan[2], plan[1]},
	}
	for i, rp := range perms {
		retry, err := svc.ReserveMother(context.Background(), standardReserveCommand("op-res", rp))
		if err != nil {
			t.Fatalf("perm %d reordered retry should replay original, got %v", i, err)
		}
		if retry.SessionID != origSession {
			t.Fatalf("perm %d session = %s, want original %s", i, retry.SessionID, origSession)
		}
		if retry.RevisionNum != 0 {
			t.Fatalf("perm %d revision = %d, want original 0", i, retry.RevisionNum)
		}
	}

	// A retry carrying the identical plan in its original order also replays.
	retry, err := svc.ReserveMother(context.Background(), standardReserveCommand("op-res", plan))
	if err != nil {
		t.Fatalf("identical retry should replay original, got %v", err)
	}
	if retry.SessionID != origSession || retry.RevisionNum != 0 {
		t.Fatalf("identical retry = session %s rev %d, want %s/0", retry.SessionID, retry.RevisionNum, origSession)
	}

	// Replays never wrote: the operation result is still the single original ok.
	opRes, err := st.LoadOperation(context.Background(), "op-res")
	if err != nil {
		t.Fatalf("load operation: %v", err)
	}
	if opRes.Code != "ok" || opRes.Revision != 0 || opRes.SessionID != origSession {
		t.Fatalf("operation result after replays = %+v, want ok/0/%s", opRes, origSession)
	}
}

// TestModel_ReserveRealDifferenceConflictsNoWrite covers the real-difference
// conflict boundary: a retry that reuses an operation ID with genuinely
// different content must return OPERATION_CONFLICT and perform no business
// write. The canonicalization used for reorder tolerance must not blur any
// genuine content change.
func TestModel_ReserveRealDifferenceConflictsNoWrite(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(c aggregate.ReserveMotherCommand) aggregate.ReserveMotherCommand
	}{
		{"different_allocation", func(c aggregate.ReserveMotherCommand) aggregate.ReserveMotherCommand {
			cp := append([]aliquot.ChildPlan(nil), c.Children...)
			cp[0].PlannedVolumeUL = 600
			c.Children = cp
			return c
		}},
		{"swapped_ordinals", func(c aggregate.ReserveMotherCommand) aggregate.ReserveMotherCommand {
			cp := append([]aliquot.ChildPlan(nil), c.Children...)
			cp[0].Ordinal, cp[1].Ordinal = 1, 0
			c.Children = cp
			return c
		}},
		{"different_child_tube", func(c aggregate.ReserveMotherCommand) aggregate.ReserveMotherCommand {
			cp := append([]aliquot.ChildPlan(nil), c.Children...)
			cp[1].ChildTubeID = "CX"
			c.Children = cp
			return c
		}},
		{"different_locked_volume", func(c aggregate.ReserveMotherCommand) aggregate.ReserveMotherCommand {
			c.LockedVolume = 1199
			return c
		}},
		{"different_sample", func(c aggregate.ReserveMotherCommand) aggregate.ReserveMotherCommand {
			c.SampleID = "S2"
			return c
		}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			svc, st := newService(t)
			bootstrap(t, svc, "op-boot")

			plan := []aliquot.ChildPlan{
				{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
				{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
			}
			base := standardReserveCommand("op-res", plan)
			first, err := svc.ReserveMother(context.Background(), base)
			if err != nil {
				t.Fatalf("first reserve: %v", err)
			}
			origSession := first.SessionID

			// Capture the original persisted snapshot before the conflicting retry.
			origMother, err := st.LoadMother(context.Background(), "M1")
			if err != nil {
				t.Fatalf("load mother: %v", err)
			}
			origOp, err := st.LoadOperation(context.Background(), "op-res")
			if err != nil {
				t.Fatalf("load operation: %v", err)
			}

			_, err = svc.ReserveMother(context.Background(), tc.mutate(base))
			if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeOperationConflict {
				t.Fatalf("expected OPERATION_CONFLICT, got %v", err)
			}

			// No new write: the mother is still bound to the original session.
			mother, err := st.LoadMother(context.Background(), "M1")
			if err != nil {
				t.Fatalf("load mother after conflict: %v", err)
			}
			if mother.Status != catalog.ContainerStatusReserved {
				t.Fatalf("mother status = %q, want reserved", mother.Status)
			}
			if mother.SessionID != origSession {
				t.Fatalf("mother session = %q, want original %q (no new session)", mother.SessionID, origSession)
			}
			if mother.RemainingUL != origMother.RemainingUL {
				t.Fatalf("mother remaining = %d, want %d", mother.RemainingUL, origMother.RemainingUL)
			}

			// No new write: the persisted plan is unchanged in ordinal order.
			sess, err := st.LoadSession(context.Background(), origSession)
			if err != nil {
				t.Fatalf("load session after conflict: %v", err)
			}
			wantChildren := []store.PlannedChild{
				{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
				{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
			}
			if !reflect.DeepEqual(sess.Children, wantChildren) {
				t.Fatalf("persisted children changed after conflict: %+v, want %+v", sess.Children, wantChildren)
			}
			if sess.RevisionNum != 0 {
				t.Fatalf("session revision = %d, want 0", sess.RevisionNum)
			}

			// No new write: the operation result is still the original fingerprint.
			opRes, err := st.LoadOperation(context.Background(), "op-res")
			if err != nil {
				t.Fatalf("load operation after conflict: %v", err)
			}
			if opRes.Fingerprint != origOp.Fingerprint {
				t.Fatalf("operation fingerprint changed: %q vs %q", opRes.Fingerprint, origOp.Fingerprint)
			}
			if opRes.Code != "ok" || opRes.SessionID != origSession {
				t.Fatalf("operation result changed after conflict: %+v", opRes)
			}
		})
	}
}

// TestModel_ReserveReplayStateInvariance covers state invariance: a reordered
// retry that replays the original result must leave every persisted invariant
// untouched — mother status/binding/remaining, the ordinal-ordered frozen plan,
// the session revision, and the single original operation result.
func TestModel_ReserveReplayStateInvariance(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")

	plan := threeChildPlan()
	first, err := svc.ReserveMother(context.Background(), standardReserveCommand("op-res", plan))
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	origSession := first.SessionID

	origMother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	origOp, err := st.LoadOperation(context.Background(), "op-res")
	if err != nil {
		t.Fatalf("load operation: %v", err)
	}
	origSess, err := st.LoadSession(context.Background(), origSession)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}

	// Retry with the children array fully reversed; each ordinal, tube number,
	// allocation, and the mother info are unchanged.
	retry, err := svc.ReserveMother(context.Background(), standardReserveCommand("op-res",
		[]aliquot.ChildPlan{plan[2], plan[1], plan[0]}))
	if err != nil {
		t.Fatalf("reordered retry should replay original, got %v", err)
	}
	if retry.SessionID != origSession {
		t.Fatalf("retry session = %s, want original %s", retry.SessionID, origSession)
	}
	if retry.RevisionNum != 0 {
		t.Fatalf("retry revision = %d, want original 0", retry.RevisionNum)
	}
	if !reflect.DeepEqual(retry.Children, first.Children) {
		t.Fatalf("retry view children = %+v, want %+v", retry.Children, first.Children)
	}

	// Invariant 1: the mother tube is unchanged.
	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother after replay: %v", err)
	}
	if mother.Status != origMother.Status ||
		mother.SessionID != origMother.SessionID ||
		mother.RemainingUL != origMother.RemainingUL {
		t.Fatalf("mother changed after replay: %+v, want %+v", mother, origMother)
	}
	if mother.Status != catalog.ContainerStatusReserved {
		t.Fatalf("mother status = %q, want reserved", mother.Status)
	}

	// Invariant 2: the frozen plan is unchanged, still in ordinal order.
	sess, err := st.LoadSession(context.Background(), origSession)
	if err != nil {
		t.Fatalf("load session after replay: %v", err)
	}
	if !reflect.DeepEqual(sess.Children, origSess.Children) {
		t.Fatalf("persisted children changed after replay: %+v, want %+v", sess.Children, origSess.Children)
	}
	wantChildren := []store.PlannedChild{
		{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
		{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
		{Ordinal: 2, ChildTubeID: "C3", PlannedVolumeUL: 200},
	}
	if !reflect.DeepEqual(sess.Children, wantChildren) {
		t.Fatalf("persisted children = %+v, want %+v", sess.Children, wantChildren)
	}
	if sess.RevisionNum != 0 {
		t.Fatalf("session revision = %d, want 0", sess.RevisionNum)
	}
	if sess.Status != aliquot.StatusReserved {
		t.Fatalf("session status = %s, want reserved", sess.Status)
	}

	// Invariant 3: the operation result is the single original ok outcome.
	opRes, err := st.LoadOperation(context.Background(), "op-res")
	if err != nil {
		t.Fatalf("load operation after replay: %v", err)
	}
	if opRes.Fingerprint != origOp.Fingerprint {
		t.Fatalf("operation fingerprint changed: %q vs %q", opRes.Fingerprint, origOp.Fingerprint)
	}
	if opRes.Code != "ok" || opRes.Revision != 0 || opRes.SessionID != origSession {
		t.Fatalf("operation result changed after replay: %+v, want ok/0/%s", opRes, origSession)
	}
}
