package aggregate_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store/sqlite"
)

func goldB6_0c563441MustView(t *testing.T, view aggregate.SessionView, err error) aggregate.SessionView {
	t.Helper()
	if err != nil {
		t.Fatalf("session mutation failed: %v", err)
	}
	return view
}

func goldB6_0c563441AssertReplay(t *testing.T, name string, want aggregate.SessionView, got aggregate.SessionView, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s replay failed: %v", name, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s replay returned a different snapshot\n got: %+v\nwant: %+v", name, got, want)
	}
}

func TestGoldB6_0c563441_IdempotentSuccessReplaysCompleteOriginalSnapshots(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "idempotent-snapshots.db")
	st1, err := sqlite.Open(path, nil)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	svc1 := aggregate.New(st1, nil)

	facts := store.Facts{
		Specimens: []catalog.Specimen{{ID: "S1", Description: "sample one"}},
		BatchRevisions: []catalog.BatchRevision{
			{SampleID: "S1", BatchID: "B1", Revision: "r1"},
		},
		Mothers: []store.MotherFact{
			{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200},
			{TubeID: "M2", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 300},
		},
	}
	if err := svc1.BootstrapCatalog(ctx, aggregate.BootstrapCommand{OperationID: "gold-b6-boot", Facts: facts}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	plan1 := []aliquot.ChildPlan{
		{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
		{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
	}
	reserve1Command := aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "gold-b6-reserve-1", ExpectedRevision: 0},
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		LockedVolume: 1200,
		Children:     plan1,
	}
	result, err := svc1.ReserveMother(ctx, reserve1Command)
	reserve1 := goldB6_0c563441MustView(t, result, err)
	sid1 := reserve1.SessionID

	thaw1Command := aggregate.ConfirmThawCommand{
		Operation:    aggregate.Operation{OperationID: "gold-b6-thaw-1", ExpectedRevision: 0},
		SessionID:    sid1,
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
	}
	result, err = svc1.ConfirmThaw(ctx, thaw1Command)
	thaw1 := goldB6_0c563441MustView(t, result, err)

	create1Command := aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "gold-b6-create-1", ExpectedRevision: 1},
		SessionID:   sid1,
		ChildTubeID: "C1",
	}
	result, err = svc1.CreateChild(ctx, create1Command)
	create1 := goldB6_0c563441MustView(t, result, err)
	if create1.RevisionNum != 2 || create1.Status != aliquot.StatusAliquoting ||
		create1.AllocatedUL != 500 || create1.LossUL != 0 || create1.RemainingUL != 700 ||
		len(create1.Children) != 2 || !create1.Children[0].Created || create1.Children[0].Verified ||
		create1.Children[1].Created || create1.Children[1].Verified ||
		!reflect.DeepEqual(create1.MissingVerify, []catalog.TubeID{"C1", "C2"}) || create1.Terminal != nil {
		t.Fatalf("first C1 result is not the required revision-2 snapshot: %+v", create1)
	}

	loss1Command := aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: "gold-b6-loss-1", ExpectedRevision: 2},
		SessionID: sid1,
		LossUL:    100,
	}
	result, err = svc1.RecordLoss(ctx, loss1Command)
	loss1 := goldB6_0c563441MustView(t, result, err)

	verify1Command := aggregate.VerifyRefreezeCommand{
		Operation:    aggregate.Operation{OperationID: "gold-b6-verify-1", ExpectedRevision: 3},
		SessionID:    sid1,
		ChildTubeID:  "C1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		AllocationUL: 500,
		FreezeRunID:  "FR-C1",
	}
	result, err = svc1.VerifyRefreeze(ctx, verify1Command)
	verify1 := goldB6_0c563441MustView(t, result, err)

	create2Command := aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "gold-b6-create-2", ExpectedRevision: 4},
		SessionID:   sid1,
		ChildTubeID: "C2",
	}
	result, err = svc1.CreateChild(ctx, create2Command)
	_ = goldB6_0c563441MustView(t, result, err)
	verify2Command := aggregate.VerifyRefreezeCommand{
		Operation:    aggregate.Operation{OperationID: "gold-b6-verify-2", ExpectedRevision: 5},
		SessionID:    sid1,
		ChildTubeID:  "C2",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		AllocationUL: 400,
		FreezeRunID:  "FR-C2",
	}
	result, err = svc1.VerifyRefreeze(ctx, verify2Command)
	_ = goldB6_0c563441MustView(t, result, err)
	loss2Command := aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: "gold-b6-loss-2", ExpectedRevision: 6},
		SessionID: sid1,
		LossUL:    200,
	}
	result, err = svc1.RecordLoss(ctx, loss2Command)
	_ = goldB6_0c563441MustView(t, result, err)
	finalizeCommand := aggregate.FinalizeLineageCommand{
		Operation: aggregate.Operation{OperationID: "gold-b6-finalize", ExpectedRevision: 7},
		SessionID: sid1,
	}
	result, err = svc1.FinalizeLineage(ctx, finalizeCommand)
	finalized := goldB6_0c563441MustView(t, result, err)
	if finalized.RevisionNum != 8 || finalized.Status != aliquot.StatusFinalized ||
		finalized.AllocatedUL != 900 || finalized.LossUL != 300 || finalized.RemainingUL != 0 ||
		len(finalized.MissingVerify) != 0 || finalized.Terminal == nil ||
		finalized.Terminal.Kind != aggregate.TerminalFinalized || finalized.Terminal.Revision != 8 {
		t.Fatalf("finalized result is incomplete: %+v", finalized)
	}

	plan2 := []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "C3", PlannedVolumeUL: 300}}
	reserve2Command := aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "gold-b6-reserve-2", ExpectedRevision: 0},
		MotherTubeID: "M2",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		LockedVolume: 300,
		Children:     plan2,
	}
	result, err = svc1.ReserveMother(ctx, reserve2Command)
	reserve2 := goldB6_0c563441MustView(t, result, err)
	sid2 := reserve2.SessionID
	thaw2Command := aggregate.ConfirmThawCommand{
		Operation:    aggregate.Operation{OperationID: "gold-b6-thaw-2", ExpectedRevision: 0},
		SessionID:    sid2,
		MotherTubeID: "M2",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
	}
	result, err = svc1.ConfirmThaw(ctx, thaw2Command)
	_ = goldB6_0c563441MustView(t, result, err)
	quarantineCommand := aggregate.QuarantineBatchCommand{
		Operation: aggregate.Operation{OperationID: "gold-b6-quarantine", ExpectedRevision: 1},
		SessionID: sid2,
		Reason:    "temperature excursion",
	}
	result, err = svc1.QuarantineBatch(ctx, quarantineCommand)
	quarantined := goldB6_0c563441MustView(t, result, err)
	if quarantined.Status != aliquot.StatusQuarantined || quarantined.Terminal == nil ||
		quarantined.Terminal.Kind != aggregate.TerminalQuarantined ||
		quarantined.Terminal.Reason != "temperature excursion" || quarantined.Terminal.Revision != 2 {
		t.Fatalf("quarantine result is incomplete: %+v", quarantined)
	}

	if err := st1.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}
	st2, err := sqlite.Open(path, nil)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st2.Close()
	svc2 := aggregate.New(st2, nil)

	before1, err := st2.LoadSession(ctx, sid1)
	if err != nil {
		t.Fatalf("load finalized session before retries: %v", err)
	}
	before2, err := st2.LoadSession(ctx, sid2)
	if err != nil {
		t.Fatalf("load quarantined session before retries: %v", err)
	}

	result, err = svc2.ReserveMother(ctx, reserve1Command)
	goldB6_0c563441AssertReplay(t, "reserve", reserve1, result, err)
	result, err = svc2.ConfirmThaw(ctx, thaw1Command)
	goldB6_0c563441AssertReplay(t, "thaw", thaw1, result, err)
	result, err = svc2.CreateChild(ctx, create1Command)
	goldB6_0c563441AssertReplay(t, "create child", create1, result, err)
	result, err = svc2.RecordLoss(ctx, loss1Command)
	goldB6_0c563441AssertReplay(t, "record loss", loss1, result, err)
	result, err = svc2.VerifyRefreeze(ctx, verify1Command)
	goldB6_0c563441AssertReplay(t, "verify refreeze", verify1, result, err)
	result, err = svc2.FinalizeLineage(ctx, finalizeCommand)
	goldB6_0c563441AssertReplay(t, "finalize", finalized, result, err)
	result, err = svc2.QuarantineBatch(ctx, quarantineCommand)
	goldB6_0c563441AssertReplay(t, "quarantine", quarantined, result, err)

	conflictingCreate := create1Command
	conflictingCreate.ChildTubeID = "C2"
	_, err = svc2.CreateChild(ctx, conflictingCreate)
	if domainErr, ok := err.(*aliquot.Error); !ok || domainErr.Code != aliquot.CodeOperationConflict {
		t.Fatalf("changed content with a consumed operation ID: got %v, want OPERATION_CONFLICT", err)
	}

	after1, err := st2.LoadSession(ctx, sid1)
	if err != nil {
		t.Fatalf("load finalized session after retries: %v", err)
	}
	after2, err := st2.LoadSession(ctx, sid2)
	if err != nil {
		t.Fatalf("load quarantined session after retries: %v", err)
	}
	if !reflect.DeepEqual(after1, before1) || !reflect.DeepEqual(after2, before2) {
		t.Fatalf("idempotent replay or operation conflict changed persisted session state")
	}
}
