package aggregate_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store/sqlite"
)

type graderB5_fd0f3ebc_createResult struct {
	view aggregate.SessionView
	err  error
}

var graderB5_fd0f3ebc_interruptErr = errors.New("grader checkpoint interrupt")

func graderB5_fd0f3ebc_facts() store.Facts {
	return store.Facts{
		Specimens:      []catalog.Specimen{{ID: "S1", Description: "sample one"}},
		BatchRevisions: []catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
		Mothers: []store.MotherFact{
			{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200},
			{TubeID: "M2", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 600},
		},
	}
}

func graderB5_fd0f3ebc_reserve(t *testing.T, svc aggregate.Service, opID, mother string, children []aliquot.ChildPlan) aggregate.SessionView {
	t.Helper()
	view, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: opID},
		MotherTubeID: catalog.TubeID(mother),
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		LockedVolume: func() int64 {
			if mother == "M2" {
				return 600
			}
			return 1200
		}(),
		Children: children,
	})
	if err != nil {
		t.Fatalf("reserve %s: %v", mother, err)
	}
	return view
}

func graderB5_fd0f3ebc_assertState(t *testing.T, svc aggregate.Service, st *sqlite.Store, sid catalog.SessionID, want aliquot.MotherStatus, remaining int64) {
	t.Helper()
	view, err := svc.GetSession(context.Background(), sid)
	if err != nil {
		t.Fatalf("get session %s: %v", sid, err)
	}
	if view.Status != want || view.RemainingUL != remaining {
		t.Fatalf("session state = status %q remaining %d, want %q %d", view.Status, view.RemainingUL, want, remaining)
	}
	mother, err := st.LoadMother(context.Background(), view.MotherTubeID)
	if err != nil {
		t.Fatalf("load mother %s: %v", view.MotherTubeID, err)
	}
	if mother == nil || mother.Status != string(want) || mother.RemainingUL != remaining {
		t.Fatalf("mother state = %+v, want status %q remaining %d", mother, want, remaining)
	}
}

func graderB5_fd0f3ebc_open(t *testing.T, path string, hooks *store.Hooks) (*sqlite.Store, aggregate.Service) {
	t.Helper()
	st, err := sqlite.Open(path, hooks)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	return st, aggregate.New(st, hooks)
}

func TestGoldB5_fd0f3ebc_statusPersistence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "status.db")
	st, svc := graderB5_fd0f3ebc_open(t, path, nil)
	if err := svc.BootstrapCatalog(ctx, aggregate.BootstrapCommand{OperationID: "b5-boot", Facts: graderB5_fd0f3ebc_facts()}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	reserved := graderB5_fd0f3ebc_reserve(t, svc, "b5-reserve-m1", "M1", []aliquot.ChildPlan{
		{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
		{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 500},
	})
	sid := reserved.SessionID
	graderB5_fd0f3ebc_assertState(t, svc, st, sid, aliquot.StatusReserved, 1200)

	thawed, err := svc.ConfirmThaw(ctx, aggregate.ConfirmThawCommand{
		Operation:    aggregate.Operation{OperationID: "b5-thaw", ExpectedRevision: 0},
		SessionID:    sid,
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
	})
	if err != nil || thawed.Status != aliquot.StatusThawed {
		t.Fatalf("thaw: view=%+v err=%v", thawed, err)
	}
	graderB5_fd0f3ebc_assertState(t, svc, st, sid, aliquot.StatusThawed, 1200)

	if err := st.Close(); err != nil {
		t.Fatalf("close after thaw: %v", err)
	}
	st, svc = graderB5_fd0f3ebc_open(t, path, nil)
	graderB5_fd0f3ebc_assertState(t, svc, st, sid, aliquot.StatusThawed, 1200)

	results := make(chan graderB5_fd0f3ebc_createResult, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			view, err := svc.CreateChild(ctx, aggregate.CreateChildCommand{
				Operation:   aggregate.Operation{OperationID: "b5-child-c1", ExpectedRevision: 1},
				SessionID:   sid,
				ChildTubeID: "C1",
			})
			results <- graderB5_fd0f3ebc_createResult{view: view, err: err}
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil || result.view.RevisionNum != 2 {
			t.Fatalf("concurrent idempotent C1 = view=%+v err=%v", result.view, result.err)
		}
	}
	graderB5_fd0f3ebc_assertState(t, svc, st, sid, aliquot.StatusAliquoting, 700)

	second, err := svc.CreateChild(ctx, aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "b5-child-c2", ExpectedRevision: 2},
		SessionID:   sid,
		ChildTubeID: "C2",
	})
	if err != nil || second.Status != aliquot.StatusAliquoting {
		t.Fatalf("last planned child: view=%+v err=%v", second, err)
	}
	graderB5_fd0f3ebc_assertState(t, svc, st, sid, aliquot.StatusAliquoting, 200)

	replayed, err := svc.CreateChild(ctx, aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "b5-child-c2", ExpectedRevision: 2},
		SessionID:   sid,
		ChildTubeID: "C2",
	})
	if err != nil || replayed.RevisionNum != second.RevisionNum {
		t.Fatalf("idempotent C2 replay: view=%+v err=%v", replayed, err)
	}

	depleted, err := svc.RecordLoss(ctx, aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: "b5-loss", ExpectedRevision: 3},
		SessionID: sid,
		LossUL:    200,
	})
	if err != nil || depleted.Status != aliquot.StatusDepleted {
		t.Fatalf("loss depletion: view=%+v err=%v", depleted, err)
	}
	graderB5_fd0f3ebc_assertState(t, svc, st, sid, aliquot.StatusDepleted, 0)

	if err := st.Close(); err != nil {
		t.Fatalf("close after depletion: %v", err)
	}
	st, svc = graderB5_fd0f3ebc_open(t, path, nil)
	graderB5_fd0f3ebc_assertState(t, svc, st, sid, aliquot.StatusDepleted, 0)

	verified, err := svc.VerifyRefreeze(ctx, aggregate.VerifyRefreezeCommand{
		Operation:    aggregate.Operation{OperationID: "b5-verify-c1", ExpectedRevision: 4},
		SessionID:    sid,
		ChildTubeID:  "C1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		AllocationUL: 500,
		FreezeRunID:  "FR-C1",
	})
	if err != nil {
		t.Fatalf("verify C1: %v", err)
	}
	verified, err = svc.VerifyRefreeze(ctx, aggregate.VerifyRefreezeCommand{
		Operation:    aggregate.Operation{OperationID: "b5-verify-c2", ExpectedRevision: verified.RevisionNum},
		SessionID:    sid,
		ChildTubeID:  "C2",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		AllocationUL: 500,
		FreezeRunID:  "FR-C2",
	})
	if err != nil {
		t.Fatalf("verify C2: %v", err)
	}
	finalized, err := svc.FinalizeLineage(ctx, aggregate.FinalizeLineageCommand{
		Operation: aggregate.Operation{OperationID: "b5-finalize", ExpectedRevision: verified.RevisionNum},
		SessionID: sid,
	})
	if err != nil || finalized.Status != aliquot.StatusFinalized {
		t.Fatalf("finalize: view=%+v err=%v", finalized, err)
	}
	graderB5_fd0f3ebc_assertState(t, svc, st, sid, aliquot.StatusFinalized, 0)

	singleSession := graderB5_fd0f3ebc_reserve(t, svc, "b5-reserve-m2", "M2", []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "C3", PlannedVolumeUL: 600}}).SessionID
	graderB5_fd0f3ebc_assertState(t, svc, st, singleSession, aliquot.StatusReserved, 600)
	if _, err := svc.ConfirmThaw(ctx, aggregate.ConfirmThawCommand{
		Operation:    aggregate.Operation{OperationID: "b5-thaw-m2", ExpectedRevision: 0},
		SessionID:    singleSession,
		MotherTubeID: "M2",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
	}); err != nil {
		t.Fatalf("single-child thaw: %v", err)
	}
	singleDepleted, err := svc.CreateChild(ctx, aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "b5-child-c3", ExpectedRevision: 1},
		SessionID:   singleSession,
		ChildTubeID: "C3",
	})
	if err != nil || singleDepleted.Status != aliquot.StatusDepleted {
		t.Fatalf("single-child depletion: view=%+v err=%v", singleDepleted, err)
	}
	graderB5_fd0f3ebc_assertState(t, svc, st, singleSession, aliquot.StatusDepleted, 0)
	quarantined, err := svc.QuarantineBatch(ctx, aggregate.QuarantineBatchCommand{
		Operation: aggregate.Operation{OperationID: "b5-quarantine", ExpectedRevision: 2},
		SessionID: singleSession,
		Reason:    "grader quarantine",
	})
	if err != nil || quarantined.Status != aliquot.StatusQuarantined {
		t.Fatalf("quarantine: view=%+v err=%v", quarantined, err)
	}
	graderB5_fd0f3ebc_assertState(t, svc, st, singleSession, aliquot.StatusQuarantined, 0)
	_ = st.Close()

	rollbackPath := filepath.Join(t.TempDir(), "rollback.db")
	hooks := &store.Hooks{Interrupt: func(checkpoint store.Checkpoint) error {
		if checkpoint == store.CheckpointBeforeCommit {
			return graderB5_fd0f3ebc_interruptErr
		}
		return nil
	}}
	rollbackStore, rollbackSvc := graderB5_fd0f3ebc_open(t, rollbackPath, hooks)
	if err := rollbackSvc.BootstrapCatalog(ctx, aggregate.BootstrapCommand{OperationID: "b5-rb-boot", Facts: graderB5_fd0f3ebc_facts()}); err != nil {
		t.Fatalf("rollback bootstrap: %v", err)
	}
	rollbackSession := graderB5_fd0f3ebc_reserve(t, rollbackSvc, "b5-rb-reserve", "M1", []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "RC1", PlannedVolumeUL: 500}}).SessionID
	if _, err := rollbackSvc.ConfirmThaw(ctx, aggregate.ConfirmThawCommand{
		Operation:    aggregate.Operation{OperationID: "b5-rb-thaw", ExpectedRevision: 0},
		SessionID:    rollbackSession,
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
	}); err != nil {
		t.Fatalf("rollback thaw: %v", err)
	}
	if _, err := rollbackSvc.CreateChild(ctx, aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "b5-rb-child", ExpectedRevision: 1},
		SessionID:   rollbackSession,
		ChildTubeID: "RC1",
	}); !errors.Is(err, graderB5_fd0f3ebc_interruptErr) {
		t.Fatalf("expected checkpoint rollback error, got %v", err)
	}
	if err := rollbackStore.Close(); err != nil {
		t.Fatalf("close rollback store: %v", err)
	}
	rollbackStore, rollbackSvc = graderB5_fd0f3ebc_open(t, rollbackPath, nil)
	graderB5_fd0f3ebc_assertState(t, rollbackSvc, rollbackStore, rollbackSession, aliquot.StatusThawed, 1200)
	if child, err := rollbackStore.LoadMother(ctx, "RC1"); err != nil || child != nil {
		t.Fatalf("rolled back child = %+v err=%v", child, err)
	}
	if operation, err := rollbackStore.LoadOperation(ctx, "b5-rb-child"); err != nil || operation != nil {
		t.Fatalf("rolled back operation = %+v err=%v", operation, err)
	}
	if err := rollbackStore.Recover(ctx); err != nil {
		t.Fatalf("recovery after rollback: %v", err)
	}
	_ = rollbackStore.Close()
}
