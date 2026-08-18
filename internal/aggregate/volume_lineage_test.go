package aggregate_test

import (
	"context"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/volume"
)

func assertConservation(t *testing.T, v aggregate.SessionView) {
	t.Helper()
	if v.LockedVolumeUL != v.AllocatedUL+v.LossUL+v.RemainingUL {
		t.Fatalf("conservation violated: locked=%d allocated=%d loss=%d remaining=%d",
			v.LockedVolumeUL, v.AllocatedUL, v.LossUL, v.RemainingUL)
	}
}

// TestIntegerVolumeConservation drives the 1200 uL mother through 500/400/200
// child allocations and a 100 uL loss, recomputing the conservation identity at
// every committed version (acceptance 3).
func TestIntegerVolumeConservation(t *testing.T) {
	svc, _ := newService(t)
	bootstrap(t, svc, "op-boot")

	view := reserveRes(t, svc, "op-res")
	assertConservation(t, view)
	if view.RemainingUL != 1200 {
		t.Fatalf("remaining after reserve = %d, want 1200", view.RemainingUL)
	}

	rev := thawRes(t, svc, "op-thaw", view.SessionID, 0).RevisionNum

	view = createChildRes(t, svc, "op-c1", view.SessionID, rev, "C1")
	rev = view.RevisionNum
	assertConservation(t, view)
	if view.AllocatedUL != 500 || view.RemainingUL != 700 {
		t.Fatalf("after C1: allocated=%d remaining=%d", view.AllocatedUL, view.RemainingUL)
	}

	view = createChildRes(t, svc, "op-c2", view.SessionID, rev, "C2")
	rev = view.RevisionNum
	assertConservation(t, view)
	if view.AllocatedUL != 900 || view.RemainingUL != 300 {
		t.Fatalf("after C2: allocated=%d remaining=%d", view.AllocatedUL, view.RemainingUL)
	}

	view = createChildRes(t, svc, "op-c3", view.SessionID, rev, "C3")
	rev = view.RevisionNum
	assertConservation(t, view)
	if view.AllocatedUL != 1100 || view.RemainingUL != 100 {
		t.Fatalf("after C3: allocated=%d remaining=%d", view.AllocatedUL, view.RemainingUL)
	}

	loss, err := svc.RecordLoss(context.Background(), aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: "op-loss", ExpectedRevision: rev},
		SessionID: view.SessionID,
		LossUL:    100,
	})
	if err != nil {
		t.Fatalf("record loss: %v", err)
	}
	assertConservation(t, loss)
	if loss.AllocatedUL != 1100 || loss.LossUL != 100 || loss.RemainingUL != 0 {
		t.Fatalf("final ledger: allocated=%d loss=%d remaining=%d", loss.AllocatedUL, loss.LossUL, loss.RemainingUL)
	}
	if loss.Status != aliquot.StatusDepleted {
		t.Fatalf("status = %s, want depleted", loss.Status)
	}
}

// TestOverAllocationLeavesStateUnchanged verifies that a rejected child creation
// writes no child, ledger entry, or lineage edge (acceptance 3).
func TestOverAllocationLeavesStateUnchanged(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")

	view := reserveRes(t, svc, "op-res")
	thawRes(t, svc, "op-thaw", view.SessionID, 0)

	_, err := svc.CreateChild(context.Background(), aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "op-bad", ExpectedRevision: 1},
		SessionID:   view.SessionID,
		ChildTubeID: "C9", // not in the plan
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeInvalidState {
		t.Fatalf("expected INVALID_STATE, got %v", err)
	}

	sess, err := st.LoadSession(context.Background(), view.SessionID)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	for _, c := range sess.Children {
		if c.Created {
			t.Fatalf("no child should be created, but %q is marked created", c.ChildTubeID)
		}
	}
	if len(sess.Entries) != 0 {
		t.Fatalf("expected no ledger entries, got %d", len(sess.Entries))
	}
	if len(sess.Edges) != 0 {
		t.Fatalf("expected no lineage edges, got %d", len(sess.Edges))
	}
	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if mother.RemainingUL != 1200 {
		t.Fatalf("mother remaining = %d, want 1200", mother.RemainingUL)
	}
}

// TestLossCannotConsumeOutstandingPlan verifies that a loss exceeding the
// non-outstanding volume is rejected without changing the balance (domain rule 8).
func TestLossCannotConsumeOutstandingPlan(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")

	view := reserveRes(t, svc, "op-res")
	rev := thawRes(t, svc, "op-thaw", view.SessionID, 0).RevisionNum
	view = createChildRes(t, svc, "op-c1", view.SessionID, rev, "C1")
	// outstanding plan is now C2(400) + C3(200) = 600, remaining = 700.

	_, err := svc.RecordLoss(context.Background(), aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: "op-loss", ExpectedRevision: view.RevisionNum},
		SessionID: view.SessionID,
		LossUL:    200,
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeLossExceedsOutstanding {
		t.Fatalf("expected LOSS_EXCEEDS_OUTSTANDING, got %v", err)
	}

	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if mother.RemainingUL != 700 {
		t.Fatalf("remaining = %d, want 700 (unchanged)", mother.RemainingUL)
	}
}

// TestChildCreationWritesAllArtifacts verifies that creating a child atomically
// writes the child container, the mother debit, the ledger entry, the plan
// completion mark, and the pending lineage edge (acceptance 5).
func TestChildCreationWritesAllArtifacts(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")

	view := reserveRes(t, svc, "op-res")
	thawRes(t, svc, "op-thaw", view.SessionID, 0)
	createChildRes(t, svc, "op-c1", view.SessionID, 1, "C1")

	child, err := st.LoadMother(context.Background(), "C1")
	if err != nil {
		t.Fatalf("load child: %v", err)
	}
	if child == nil || !child.IsChild() {
		t.Fatalf("expected child container C1 to exist")
	}
	if child.RemainingUL != 500 {
		t.Fatalf("child remaining = %d, want 500", child.RemainingUL)
	}

	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if mother.RemainingUL != 700 {
		t.Fatalf("mother remaining = %d, want 700", mother.RemainingUL)
	}

	sess, err := st.LoadSession(context.Background(), view.SessionID)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	if !sess.Children[0].Created {
		t.Fatalf("planned child C1 should be marked created")
	}
	if len(sess.Entries) != 1 {
		t.Fatalf("expected 1 ledger entry, got %d", len(sess.Entries))
	}
	if sess.Entries[0].Type != volume.EntryChildAllocation || sess.Entries[0].QuantityUL != 500 {
		t.Fatalf("ledger entry = %+v, want child_allocation 500", sess.Entries[0])
	}
	if len(sess.Edges) != 1 {
		t.Fatalf("expected 1 lineage edge, got %d", len(sess.Edges))
	}
	edge := sess.Edges[0]
	if edge.ParentTubeID != "M1" || edge.ChildTubeID != "C1" || edge.AllocationUL != 500 || edge.State != lineage.EdgePending {
		t.Fatalf("edge = %+v", edge)
	}
}
