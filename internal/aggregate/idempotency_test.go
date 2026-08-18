package aggregate_test

import (
	"context"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
)

// TestIdenticalOperationReturnsOriginalSuccess verifies that retrying a
// successful operation with the same ID and content returns the original
// revision and result without re-executing (acceptance 4).
func TestIdenticalOperationReturnsOriginalSuccess(t *testing.T) {
	svc, _ := newService(t)
	bootstrap(t, svc, "op-boot")

	view := reserveRes(t, svc, "op-res")
	sid := view.SessionID
	thawRes(t, svc, "op-thaw", sid, 0)

	first := createChildRes(t, svc, "op-c1", sid, 1, "C1")
	if first.RevisionNum != 2 {
		t.Fatalf("first create revision = %d, want 2", first.RevisionNum)
	}
	// Advance state with an unrelated operation.
	createChildRes(t, svc, "op-c2", sid, 2, "C2")

	// Retry the original create with identical operation ID and content.
	retry, err := svc.CreateChild(context.Background(), aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "op-c1", ExpectedRevision: 1},
		SessionID:   sid,
		ChildTubeID: "C1",
	})
	if err != nil {
		t.Fatalf("retry should succeed, got %v", err)
	}
	if retry.RevisionNum != 2 {
		t.Fatalf("retry revision = %d, want original 2", retry.RevisionNum)
	}
}

// TestOperationContentConflict verifies that reusing an operation ID with
// different content yields OPERATION_CONFLICT and performs no business write.
func TestOperationContentConflict(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")

	view := reserveRes(t, svc, "op-res")
	sid := view.SessionID
	thawRes(t, svc, "op-thaw", sid, 0)
	createChildRes(t, svc, "op-c1", sid, 1, "C1")

	_, err := svc.CreateChild(context.Background(), aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "op-c1", ExpectedRevision: 2},
		SessionID:   sid,
		ChildTubeID: "C2", // different target than the original
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeOperationConflict {
		t.Fatalf("expected OPERATION_CONFLICT, got %v", err)
	}

	sess, err := st.LoadSession(context.Background(), sid)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	for _, c := range sess.Children {
		if c.ChildTubeID == "C2" && c.Created {
			t.Fatalf("C2 should not be created on conflict")
		}
	}
}

// TestRejectedResultIsStable verifies that a persisted business rejection is
// returned on retry even after the underlying state changes (acceptance 4).
func TestRejectedResultIsStable(t *testing.T) {
	svc, _ := newService(t)
	bootstrap(t, svc, "op-boot")

	view := reserveRes(t, svc, "op-res")
	sid := view.SessionID
	rev := fullyAliquot(t, svc, sid)

	// Finalize is rejected because no child has a verification yet.
	_, err := svc.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
		Operation: aggregate.Operation{OperationID: "op-fin", ExpectedRevision: rev},
		SessionID: sid,
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeMissingVerification {
		t.Fatalf("expected MISSING_VERIFICATION, got %v", err)
	}

	// Complete all verifications via other operations.
	rev = verifyRes(t, svc, "op-v1", sid, rev, "C1", 500).RevisionNum
	rev = verifyRes(t, svc, "op-v2", sid, rev, "C2", 400).RevisionNum
	rev = verifyRes(t, svc, "op-v3", sid, rev, "C3", 200).RevisionNum

	// Retrying the original finalize still returns the original rejection.
	_, err = svc.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
		Operation: aggregate.Operation{OperationID: "op-fin", ExpectedRevision: rev - 3},
		SessionID: sid,
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeMissingVerification {
		t.Fatalf("expected stable MISSING_VERIFICATION, got %v", err)
	}
}
