package aggregate_test

import (
	"context"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
)

// setupDepletedVerified builds a depleted, fully-verified session ready for a
// terminal transition.
func setupDepletedVerified(t *testing.T, svc aggregate.Service) catalog.SessionID {
	t.Helper()
	bootstrap(t, svc, "op-boot")
	view := reserveRes(t, svc, "op-res")
	sid := view.SessionID
	rev := fullyAliquot(t, svc, sid)
	rev = verifyRes(t, svc, "op-v1", sid, rev, "C1", 500).RevisionNum
	rev = verifyRes(t, svc, "op-v2", sid, rev, "C2", 400).RevisionNum
	verifyRes(t, svc, "op-v3", sid, rev, "C3", 200)
	return sid
}

// TestFinalizeRequiresEveryVerification verifies that finalization rejects with
// a stably sorted missing list and produces no manifest until every child is
// verified (acceptance 6).
func TestFinalizeRequiresEveryVerification(t *testing.T) {
	svc, _ := newService(t)
	bootstrap(t, svc, "op-boot")
	view := reserveRes(t, svc, "op-res")
	sid := view.SessionID
	rev := fullyAliquot(t, svc, sid)

	// Verify only C1.
	verifyRes(t, svc, "op-v1", sid, rev, "C1", 500)

	_, err := svc.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
		Operation: aggregate.Operation{OperationID: "op-fin", ExpectedRevision: rev + 1},
		SessionID: sid,
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeMissingVerification {
		t.Fatalf("expected MISSING_VERIFICATION, got %v", err)
	}

	got, err := svc.GetSession(context.Background(), sid)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if len(got.MissingVerify) != 2 || got.MissingVerify[0] != "C2" || got.MissingVerify[1] != "C3" {
		t.Fatalf("missing verify = %v, want [C2 C3]", got.MissingVerify)
	}
	if _, _, err := svc.GetManifest(context.Background(), sid); err == nil {
		t.Fatalf("expected no manifest before finalization")
	}
}

// TestFinalizeProducesSingleStableManifest verifies that a single immutable
// manifest is produced with items sorted by child tube ID (acceptance 6).
func TestFinalizeProducesSingleStableManifest(t *testing.T) {
	svc, _ := newService(t)
	sid := setupDepletedVerified(t, svc)

	_, err := svc.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
		Operation: aggregate.Operation{OperationID: "op-fin1", ExpectedRevision: 8},
		SessionID: sid,
	})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}

	// A second finalize with a different operation ID must not override.
	_, err = svc.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
		Operation: aggregate.Operation{OperationID: "op-fin2", ExpectedRevision: 8},
		SessionID: sid,
	})
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeTerminalConflict {
		t.Fatalf("expected TERMINAL_CONFLICT, got %v", err)
	}

	manifest, items, err := svc.GetManifest(context.Background(), sid)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if manifest.MotherTubeID != "M1" || manifest.FrozenVolumeUL != 1200 || manifest.TotalLossUL != 100 {
		t.Fatalf("manifest header = %+v", manifest)
	}
	if len(items) != 3 {
		t.Fatalf("manifest items = %d, want 3", len(items))
	}
	for i, want := range []string{"C1", "C2", "C3"} {
		if items[i].ChildTubeID != catalog.TubeID(want) {
			t.Fatalf("item %d = %s, want %s (stable sort)", i, items[i].ChildTubeID, want)
		}
	}
}

// raceTerminal runs finalize and quarantine concurrently under a barrier and
// returns their errors, releasing the named winner first.
func raceTerminal(t *testing.T, svc aggregate.Service, hooks *store.Hooks, sid catalog.SessionID, winner string) (finErr, qErr error) {
	t.Helper()
	barrier := newTestBarrier()
	hooks.Barrier = barrier.hook

	finCh := make(chan error, 1)
	qCh := make(chan error, 1)
	go func() {
		_, err := svc.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
			Operation: aggregate.Operation{OperationID: "op-fin", ExpectedRevision: 8},
			SessionID: sid,
		})
		finCh <- err
	}()
	go func() {
		_, err := svc.QuarantineBatch(context.Background(), aggregate.QuarantineBatchCommand{
			Operation: aggregate.Operation{OperationID: "op-quar", ExpectedRevision: 8},
			SessionID: sid,
			Reason:    "race",
		})
		qCh <- err
	}()

	barrier.waitFor(2)
	if winner == "finalize" {
		barrier.releaseByName("finalize:" + string(sid))
		finErr = <-finCh
		barrier.releaseByName("quarantine:" + string(sid))
		qErr = <-qCh
	} else {
		barrier.releaseByName("quarantine:" + string(sid))
		qErr = <-qCh
		barrier.releaseByName("finalize:" + string(sid))
		finErr = <-finCh
	}
	hooks.Barrier = nil
	return finErr, qErr
}

// TestQuarantineWinsBarrierRace verifies that when quarantine commits first,
// finalize loses, no manifest is produced, and the tubes are quarantined
// (acceptance 7).
func TestQuarantineWinsBarrierRace(t *testing.T) {
	hooks := &store.Hooks{}
	svc, st := openService(t, tempDBPath(t), hooks)
	sid := setupDepletedVerified(t, svc)

	finErr, qErr := raceTerminal(t, svc, hooks, sid, "quarantine")

	if qErr != nil {
		t.Fatalf("quarantine should win, got %v", qErr)
	}
	if e, ok := finErr.(*aliquot.Error); !ok || e.Code != aliquot.CodeTerminalConflict {
		t.Fatalf("finalize should lose with TERMINAL_CONFLICT, got %v", finErr)
	}
	if _, _, err := svc.GetManifest(context.Background(), sid); err == nil {
		t.Fatalf("expected no manifest when quarantine wins")
	}
	mother, _ := st.LoadMother(context.Background(), "M1")
	if mother.Status != "quarantined" {
		t.Fatalf("mother status = %q, want quarantined", mother.Status)
	}
	child, _ := st.LoadMother(context.Background(), "C1")
	if child.Status != "quarantined" {
		t.Fatalf("child status = %q, want quarantined", child.Status)
	}
}

// TestFinalizeWinsBarrierRace verifies that when finalize commits first,
// quarantine cannot override the finalized outcome (acceptance 7).
func TestFinalizeWinsBarrierRace(t *testing.T) {
	hooks := &store.Hooks{}
	svc, st := openService(t, tempDBPath(t), hooks)
	sid := setupDepletedVerified(t, svc)

	finErr, qErr := raceTerminal(t, svc, hooks, sid, "finalize")

	if finErr != nil {
		t.Fatalf("finalize should win, got %v", finErr)
	}
	if e, ok := qErr.(*aliquot.Error); !ok || e.Code != aliquot.CodeTerminalConflict {
		t.Fatalf("quarantine should lose with TERMINAL_CONFLICT, got %v", qErr)
	}
	mother, _ := st.LoadMother(context.Background(), "M1")
	if mother.Status != "finalized" {
		t.Fatalf("mother status = %q, want finalized", mother.Status)
	}
	if _, _, err := svc.GetManifest(context.Background(), sid); err != nil {
		t.Fatalf("expected manifest when finalize wins: %v", err)
	}
}
