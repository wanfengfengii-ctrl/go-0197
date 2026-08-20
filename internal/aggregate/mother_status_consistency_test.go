package aggregate_test

import (
	"context"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store/sqlite"
)

// This file holds the focused regression coverage for the mother tube
// persistence-consistency bugfix: every non-terminal commit boundary (thaw,
// child creation, loss) must mirror the session lifecycle state onto the
// mother container row in the same transaction, so the container directory
// (read via LoadMother) and a restart never expose a stale "reserved" status
// after the session has already advanced. Terminal finalized/quarantined
// semantics, concurrency, idempotency, and rollback must remain unaffected.

// assertMotherContainerStatus loads the container row from the container
// directory and asserts its persisted status equals want.
func assertMotherContainerStatus(t *testing.T, st *sqlite.Store, id catalog.TubeID, want string) {
	t.Helper()
	mother, err := st.LoadMother(context.Background(), id)
	if err != nil {
		t.Fatalf("load mother %s: %v", id, err)
	}
	if mother == nil {
		t.Fatalf("mother %s not found", id)
	}
	if mother.Status != want {
		t.Fatalf("mother %s container status = %q, want %q", id, mother.Status, want)
	}
}

// assertMotherRemaining asserts the persisted mother remaining volume.
func assertMotherRemaining(t *testing.T, st *sqlite.Store, id catalog.TubeID, want int64) {
	t.Helper()
	mother, err := st.LoadMother(context.Background(), id)
	if err != nil || mother == nil {
		t.Fatalf("load mother %s: %v", id, err)
	}
	if mother.RemainingUL != want {
		t.Fatalf("mother %s remaining = %d, want %d", id, mother.RemainingUL, want)
	}
}

// assertSessionContainerConsistent asserts that the session view status and the
// persisted mother container status agree. This is the core invariant fixed by
// the bugfix: the container directory must not lag behind the session view.
func assertSessionContainerConsistent(t *testing.T, svc aggregate.Service, st *sqlite.Store, sid catalog.SessionID, motherID catalog.TubeID) {
	t.Helper()
	view, err := svc.GetSession(context.Background(), sid)
	if err != nil {
		t.Fatalf("get session %s: %v", sid, err)
	}
	mother, err := st.LoadMother(context.Background(), motherID)
	if err != nil || mother == nil {
		t.Fatalf("load mother %s: %v", motherID, err)
	}
	if string(view.Status) != mother.Status {
		t.Fatalf("session %s status %q disagrees with container status %q", sid, view.Status, mother.Status)
	}
}

// TestModel_ThawMirrorsMotherContainerStatus verifies that confirming the thaw
// writes the thawed lifecycle onto the mother container row in the same commit
// boundary as the session, instead of leaving it stale at reserved.
func TestModel_ThawMirrorsMotherContainerStatus(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")
	view := reserveRes(t, svc, "op-res")
	sid := view.SessionID

	// After reservation the mother container is reserved, in sync with the session.
	assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusReserved))
	assertSessionContainerConsistent(t, svc, st, sid, "M1")

	thawRes(t, svc, "op-thaw", sid, 0)

	// The thaw commit boundary must mirror thawed onto the container directory.
	assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusThawed))
	assertSessionContainerConsistent(t, svc, st, sid, "M1")
}

// TestModel_CreateChildMirrorsMotherContainerStatus verifies that creating a
// planned child mirrors the new lifecycle (aliquoting for a non-final child,
// depleted for the final child that drains the remaining volume) onto the
// mother container at every child-creation commit boundary.
func TestModel_CreateChildMirrorsMotherContainerStatus(t *testing.T) {
	t.Run("single_child_aliquoting", func(t *testing.T) {
		svc, st := newService(t)
		bootstrap(t, svc, "op-boot")
		view := reserveRes(t, svc, "op-res") // 500/400/200 of 1200
		sid := view.SessionID
		thawRes(t, svc, "op-thaw", sid, 0)

		v := createChildRes(t, svc, "op-c1", sid, 1, "C1")
		if v.Status != aliquot.StatusAliquoting {
			t.Fatalf("session status = %s, want aliquoting", v.Status)
		}
		// First (non-final) child: container must move to aliquoting, not stay
		// stale at thawed/reserved.
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusAliquoting))
		assertMotherRemaining(t, st, "M1", 700)
		assertSessionContainerConsistent(t, svc, st, sid, "M1")
	})

	t.Run("last_child_depleted", func(t *testing.T) {
		svc, st := newService(t)
		bootstrap(t, svc, "op-boot")
		// A plan that exactly sums to the locked volume: the last child drains
		// the mother to zero and must drive the container to depleted.
		view := reserve(t, svc, "op-res", "M1", "S1", "B1", "r1", 1200, []aliquot.ChildPlan{
			{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
			{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
			{Ordinal: 2, ChildTubeID: "C3", PlannedVolumeUL: 300},
		})
		sid := view.SessionID
		rev := thawRes(t, svc, "op-thaw", sid, 0).RevisionNum

		rev = createChildRes(t, svc, "op-c1", sid, rev, "C1").RevisionNum
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusAliquoting))
		assertMotherRemaining(t, st, "M1", 700)

		rev = createChildRes(t, svc, "op-c2", sid, rev, "C2").RevisionNum
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusAliquoting))
		assertMotherRemaining(t, st, "M1", 300)

		v := createChildRes(t, svc, "op-c3", sid, rev, "C3")
		if v.Status != aliquot.StatusDepleted {
			t.Fatalf("session status = %s, want depleted", v.Status)
		}
		// Final child draining the mother: container must reach depleted at this
		// commit boundary, not remain stale at aliquoting/reserved.
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusDepleted))
		assertMotherRemaining(t, st, "M1", 0)
		assertSessionContainerConsistent(t, svc, st, sid, "M1")
	})
}

// TestModel_RecordLossDepletionMirrorsMotherContainerStatus verifies that a
// recorded loss which drains the remaining volume drives both the session and
// the mother container to depleted in the same commit boundary, while the
// pre-depletion loss keeps the container mirrored at aliquoting.
func TestModel_RecordLossDepletionMirrorsMotherContainerStatus(t *testing.T) {
	svc, st := newService(t)
	bootstrap(t, svc, "op-boot")
	view := reserveRes(t, svc, "op-res") // 500/400/200 of 1200
	sid := view.SessionID
	rev := thawRes(t, svc, "op-thaw", sid, 0).RevisionNum
	rev = createChildRes(t, svc, "op-c1", sid, rev, "C1").RevisionNum
	rev = createChildRes(t, svc, "op-c2", sid, rev, "C2").RevisionNum
	rev = createChildRes(t, svc, "op-c3", sid, rev, "C3").RevisionNum
	// All three children created; remaining = 100, not yet depleted.
	assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusAliquoting))
	assertMotherRemaining(t, st, "M1", 100)

	v, err := svc.RecordLoss(context.Background(), aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: "op-loss", ExpectedRevision: rev},
		SessionID: sid,
		LossUL:    100,
	})
	if err != nil {
		t.Fatalf("record loss: %v", err)
	}
	if v.Status != aliquot.StatusDepleted {
		t.Fatalf("session status = %s, want depleted", v.Status)
	}
	// The draining loss must move the container to depleted, not leave it stale.
	assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusDepleted))
	assertMotherRemaining(t, st, "M1", 0)
	assertSessionContainerConsistent(t, svc, st, sid, "M1")
}

// TestModel_RestartReadsMirroredMotherContainerStatus verifies that after an
// unexpected restart, loading the mother container and the session view exposes
// the real lifecycle state (here depleted) and never the stale reserved value
// that the bug persisted across restarts.
func TestModel_RestartReadsMirroredMotherContainerStatus(t *testing.T) {
	path := tempDBPath(t)
	st1, err := sqlite.Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	svc1 := aggregate.New(st1, nil)
	bootstrap(t, svc1, "op-boot")
	view := reserveRes(t, svc1, "op-res")
	sid := view.SessionID
	rev := thawRes(t, svc1, "op-thaw", sid, 0).RevisionNum
	rev = createChildRes(t, svc1, "op-c1", sid, rev, "C1").RevisionNum
	rev = createChildRes(t, svc1, "op-c2", sid, rev, "C2").RevisionNum
	rev = createChildRes(t, svc1, "op-c3", sid, rev, "C3").RevisionNum
	if _, err := svc1.RecordLoss(context.Background(), aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: "op-loss", ExpectedRevision: rev},
		SessionID: sid,
		LossUL:    100,
	}); err != nil {
		t.Fatalf("record loss: %v", err)
	}
	// Confirm the in-process container directory is depleted before restart.
	assertMotherContainerStatus(t, st1, "M1", string(aliquot.StatusDepleted))
	if err := st1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen from the same file; nothing is re-derived in memory.
	st2, err := sqlite.Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	svc2 := aggregate.New(st2, nil)

	// The persisted container status must survive restart: depleted, not the
	// stale reserved the bug used to leave behind.
	assertMotherContainerStatus(t, st2, "M1", string(aliquot.StatusDepleted))
	assertMotherRemaining(t, st2, "M1", 0)
	assertSessionContainerConsistent(t, svc2, st2, sid, "M1")

	restored, err := svc2.GetSession(context.Background(), sid)
	if err != nil {
		t.Fatalf("get session after restart: %v", err)
	}
	if restored.Status != aliquot.StatusDepleted {
		t.Fatalf("restored session status = %s, want depleted", restored.Status)
	}
	if restored.RemainingUL != 0 || restored.AllocatedUL != 1100 || restored.LossUL != 100 {
		t.Fatalf("restored ledger = allocated=%d loss=%d remaining=%d", restored.AllocatedUL, restored.LossUL, restored.RemainingUL)
	}
}

// TestModel_RollbackKeepsMotherContainerStatusConsistent verifies that an
// interrupted or rejected child/loss transaction rolls back the mother
// container status together with the session status, so the container never
// advances ahead of the session (and never stays stale behind it).
func TestModel_RollbackKeepsMotherContainerStatusConsistent(t *testing.T) {
	// Each named child-transaction checkpoint interrupts before commit; the
	// whole transaction, including the mirrored mother status update, must
	// roll back so the container stays at thawed (consistent with session rev 1).
	checkpoints := []store.Checkpoint{
		store.CheckpointAfterChildInsert,
		store.CheckpointAfterVolumeDebit,
		store.CheckpointAfterLineageInsert,
		store.CheckpointBeforeCommit,
	}
	for _, cp := range checkpoints {
		t.Run(string(cp), func(t *testing.T) {
			path := tempDBPath(t)
			st1, err := sqlite.Open(path, interruptOn(cp))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			svc1 := aggregate.New(st1, interruptOn(cp))
			bootstrap(t, svc1, "op-boot")
			view := reserveRes(t, svc1, "op-res")
			sid := view.SessionID
			thawRes(t, svc1, "op-thaw", sid, 0) // container -> thawed, session rev 1

			_, err = svc1.CreateChild(context.Background(), aggregate.CreateChildCommand{
				Operation:   aggregate.Operation{OperationID: "op-c1", ExpectedRevision: 1},
				SessionID:   sid,
				ChildTubeID: "C1",
			})
			if err == nil {
				t.Fatalf("expected create child to be interrupted at %s", cp)
			}
			if err := st1.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			st2, err := sqlite.Open(path, nil)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer st2.Close()
			svc2 := aggregate.New(st2, nil)

			// Rolled back: container must remain thawed (not advanced to
			// aliquoting, not reverted to reserved), matching the session.
			assertMotherContainerStatus(t, st2, "M1", string(aliquot.StatusThawed))
			assertMotherRemaining(t, st2, "M1", 1200)
			assertSessionContainerConsistent(t, svc2, st2, sid, "M1")
			restored, _ := svc2.GetSession(context.Background(), sid)
			if restored.RevisionNum != 1 || restored.Status != aliquot.StatusThawed {
				t.Fatalf("session after rollback = rev %d %s, want rev 1 thawed", restored.RevisionNum, restored.Status)
			}
			if child, _ := st2.LoadMother(context.Background(), "C1"); child != nil {
				t.Fatalf("orphan child C1 survived interruption at %s", cp)
			}
		})
	}

	// A business rejection (loss exceeding the outstanding plan) happens before
	// any write; the container must stay mirrored at aliquoting, unchanged.
	t.Run("loss_rejection_unchanged", func(t *testing.T) {
		svc, st := newService(t)
		bootstrap(t, svc, "op-boot")
		view := reserveRes(t, svc, "op-res")
		sid := view.SessionID
		rev := thawRes(t, svc, "op-thaw", sid, 0).RevisionNum
		rev = createChildRes(t, svc, "op-c1", sid, rev, "C1").RevisionNum
		// outstanding plan C2(400)+C3(200)=600, remaining=700, max loss=100.
		_, err := svc.RecordLoss(context.Background(), aggregate.RecordLossCommand{
			Operation: aggregate.Operation{OperationID: "op-bad-loss", ExpectedRevision: rev},
			SessionID: sid,
			LossUL:    200,
		})
		if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeLossExceedsOutstanding {
			t.Fatalf("expected LOSS_EXCEEDS_OUTSTANDING, got %v", err)
		}
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusAliquoting))
		assertMotherRemaining(t, st, "M1", 700)
		assertSessionContainerConsistent(t, svc, st, sid, "M1")
	})
}

// TestModel_ConcurrentChildCreationKeepsMotherContainerStatusConsistent
// verifies that concurrent child creation on independent sessions keeps each
// mother container mirrored to its own session lifecycle with no
// cross-contamination, proving the bugfix does not affect concurrency.
//
// The barrier hook is armed only for the concurrent child-creation step (after
// the sequential reserve/thaw setup completes with the barrier disarmed),
// mirroring the raceTerminal pattern. Two independent mothers are used so the
// two CreateChild transactions run through the single-writer SQLite store
// without contending on the same per-session lock.
func TestModel_ConcurrentChildCreationKeepsMotherContainerStatusConsistent(t *testing.T) {
	hooks := &store.Hooks{}
	svc, st := openService(t, tempDBPath(t), hooks)

	if err := svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
		OperationID: "op-boot",
		Facts: store.Facts{
			Specimens:      []catalog.Specimen{{ID: "S1", Description: "sample one"}},
			BatchRevisions: []catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
			Mothers: []store.MotherFact{
				{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200},
				{TubeID: "M2", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200},
			},
		},
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	v1 := reserve(t, svc, "op-r1", "M1", "S1", "B1", "r1", 1200, []aliquot.ChildPlan{
		{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
		{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 700},
	})
	v2 := reserve(t, svc, "op-r2", "M2", "S1", "B1", "r1", 1200, []aliquot.ChildPlan{
		{Ordinal: 0, ChildTubeID: "D1", PlannedVolumeUL: 500},
		{Ordinal: 1, ChildTubeID: "D2", PlannedVolumeUL: 700},
	})
	thawRes(t, svc, "op-t1", v1.SessionID, 0)
	// thawRes hardcodes MotherTubeID "M1"; thaw M2 with its own evidence.
	if _, err := svc.ConfirmThaw(context.Background(), aggregate.ConfirmThawCommand{
		Operation:    aggregate.Operation{OperationID: "op-t2", ExpectedRevision: 0},
		SessionID:    v2.SessionID,
		MotherTubeID: "M2",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
	}); err != nil {
		t.Fatalf("thaw M2: %v", err)
	}

	// Arm the barrier only for the concurrent child-creation step.
	barrier := newTestBarrier()
	hooks.Barrier = barrier.hook
	defer func() { hooks.Barrier = nil }()

	type outcome struct{ err error }
	out := make(chan outcome, 2)
	run := func(opID string, sid catalog.SessionID, child catalog.TubeID) {
		_, err := svc.CreateChild(context.Background(), aggregate.CreateChildCommand{
			Operation:   aggregate.Operation{OperationID: opID},
			SessionID:   sid,
			ChildTubeID: child,
		})
		out <- outcome{err}
	}
	go run("op-c1", v1.SessionID, "C1")
	go run("op-d1", v2.SessionID, "D1")

	barrier.waitFor(2)
	barrier.releaseByIndex(0)
	first := <-out
	barrier.releaseByIndex(1)
	second := <-out

	if first.err != nil || second.err != nil {
		t.Fatalf("both concurrent creates should succeed: first=%v second=%v", first.err, second.err)
	}
	// Each mother container independently mirrors its own session's aliquoting
	// state, with no cross-contamination from the concurrent peer.
	assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusAliquoting))
	assertMotherContainerStatus(t, st, "M2", string(aliquot.StatusAliquoting))
	assertMotherRemaining(t, st, "M1", 700)
	assertMotherRemaining(t, st, "M2", 700)
	assertSessionContainerConsistent(t, svc, st, v1.SessionID, "M1")
	assertSessionContainerConsistent(t, svc, st, v2.SessionID, "M2")
}

// TestModel_TerminalTransitionsKeepContainerStatus verifies that the terminal
// transitions continue to set the mother (and child) container status with
// their existing semantics, unaffected by the intermediate-state mirroring
// fix.
func TestModel_TerminalTransitionsKeepContainerStatus(t *testing.T) {
	t.Run("finalize", func(t *testing.T) {
		svc, st := newService(t)
		sid := setupDepletedVerified(t, svc)
		// Before terminal: the fixed code leaves the mother at depleted.
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusDepleted))

		if _, err := svc.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
			Operation: aggregate.Operation{OperationID: "op-fin", ExpectedRevision: 8},
			SessionID: sid,
		}); err != nil {
			t.Fatalf("finalize: %v", err)
		}
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusFinalized))
		assertSessionContainerConsistent(t, svc, st, sid, "M1")
	})

	t.Run("quarantine", func(t *testing.T) {
		svc, st := newService(t)
		bootstrap(t, svc, "op-boot")
		view := reserveRes(t, svc, "op-res")
		sid := view.SessionID
		rev := thawRes(t, svc, "op-thaw", sid, 0).RevisionNum
		rev = createChildRes(t, svc, "op-c1", sid, rev, "C1").RevisionNum
		// Mother is aliquoting and one child exists before quarantine.
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusAliquoting))

		if _, err := svc.QuarantineBatch(context.Background(), aggregate.QuarantineBatchCommand{
			Operation: aggregate.Operation{OperationID: "op-quar", ExpectedRevision: rev},
			SessionID: sid,
			Reason:    "terminal semantics",
		}); err != nil {
			t.Fatalf("quarantine: %v", err)
		}
		assertMotherContainerStatus(t, st, "M1", string(aliquot.StatusQuarantined))
		assertMotherContainerStatus(t, st, "C1", string(aliquot.StatusQuarantined))
		assertSessionContainerConsistent(t, svc, st, sid, "M1")
	})
}
