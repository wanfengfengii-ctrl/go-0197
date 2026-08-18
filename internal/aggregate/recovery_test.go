package aggregate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store/sqlite"
)

var errInterrupted = errors.New("checkpoint interrupted")

// interruptOn returns a hook that interrupts only the named checkpoint.
func interruptOn(cp store.Checkpoint) *store.Hooks {
	return &store.Hooks{Interrupt: func(c store.Checkpoint) error {
		if c == cp {
			return errInterrupted
		}
		return nil
	}}
}

// TestRestartRestoresCommittedOpenSession verifies that a committed open session
// survives restart with its reservation, remaining volume, operation result, and
// pending lineage edge intact (acceptance 8).
func TestRestartRestoresCommittedOpenSession(t *testing.T) {
	path := tempDBPath(t)
	st1, err := sqlite.Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	svc1 := aggregate.New(st1, nil)
	bootstrap(t, svc1, "op-boot")
	view := reserveRes(t, svc1, "op-res")
	sid := view.SessionID
	thawRes(t, svc1, "op-thaw", sid, 0)
	createChildRes(t, svc1, "op-c1", sid, 1, "C1")
	if err := st1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st2, err := sqlite.Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	svc2 := aggregate.New(st2, nil)

	restored, err := svc2.GetSession(context.Background(), sid)
	if err != nil {
		t.Fatalf("get session after restart: %v", err)
	}
	if restored.Status != aliquot.StatusAliquoting || restored.RemainingUL != 700 || restored.AllocatedUL != 500 {
		t.Fatalf("restored session = %+v", restored)
	}

	// The operation result survives, so a retry is idempotent across restarts.
	retry, err := svc2.CreateChild(context.Background(), aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: "op-c1", ExpectedRevision: 1},
		SessionID:   sid,
		ChildTubeID: "C1",
	})
	if err != nil || retry.RevisionNum != 2 {
		t.Fatalf("retry after restart: rev=%d err=%v", retry.RevisionNum, err)
	}

	sess, err := st2.LoadSession(context.Background(), sid)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	if len(sess.Edges) != 1 || sess.Edges[0].State != lineage.EdgePending {
		t.Fatalf("pending edge not restored: %+v", sess.Edges)
	}
}

// TestInterruptedChildTransactionRollsBackAtEveryCheckpoint verifies that a
// child transaction interrupted at any named checkpoint leaves no orphan child,
// ledger entry, or edge, and consumes no operation result (acceptance 5, 8).
func TestInterruptedChildTransactionRollsBackAtEveryCheckpoint(t *testing.T) {
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
			thawRes(t, svc1, "op-thaw", sid, 0)

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

			child, _ := st2.LoadMother(context.Background(), "C1")
			if child != nil {
				t.Fatalf("orphan child C1 survived interruption at %s", cp)
			}
			mother, _ := st2.LoadMother(context.Background(), "M1")
			if mother.RemainingUL != 1200 {
				t.Fatalf("mother remaining = %d, want 1200 at %s", mother.RemainingUL, cp)
			}
			sess, _ := st2.LoadSession(context.Background(), sid)
			if len(sess.Edges) != 0 || len(sess.Entries) != 0 {
				t.Fatalf("partial artifacts at %s: edges=%d entries=%d", cp, len(sess.Edges), len(sess.Entries))
			}
			if op, _ := st2.LoadOperation(context.Background(), "op-c1"); op != nil {
				t.Fatalf("operation result should not be persisted after interrupt at %s", cp)
			}
		})
	}
}

// TestInterruptedTerminalTransactionRollsBackAtEveryCheckpoint verifies that a
// finalize interrupted at any named terminal checkpoint leaves no half manifest,
// no partially frozen edges, and the session can still be finalized (acceptance 8).
func TestInterruptedTerminalTransactionRollsBackAtEveryCheckpoint(t *testing.T) {
	checkpoints := []store.Checkpoint{
		store.CheckpointAfterTerminalClaim,
		store.CheckpointAfterManifestInsert,
		store.CheckpointBeforeTerminalCommit,
	}
	for _, cp := range checkpoints {
		t.Run(string(cp), func(t *testing.T) {
			path := tempDBPath(t)
			st1, err := sqlite.Open(path, interruptOn(cp))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			svc1 := aggregate.New(st1, interruptOn(cp))
			sid := setupDepletedVerified(t, svc1)

			_, err = svc1.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
				Operation: aggregate.Operation{OperationID: "op-fin", ExpectedRevision: 8},
				SessionID: sid,
			})
			if err == nil {
				t.Fatalf("expected finalize to be interrupted at %s", cp)
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

			if _, _, err := svc2.GetManifest(context.Background(), sid); err == nil {
				t.Fatalf("expected no manifest after interrupt at %s", cp)
			}
			sess, _ := st2.LoadSession(context.Background(), sid)
			for _, e := range sess.Edges {
				if e.State != lineage.EdgePending {
					t.Fatalf("edge %s should remain pending after interrupt at %s", e.ChildTubeID, cp)
				}
			}
			// The session can still be completed with the same operation content.
			if _, err := svc2.FinalizeLineage(context.Background(), aggregate.FinalizeLineageCommand{
				Operation: aggregate.Operation{OperationID: "op-fin", ExpectedRevision: 8},
				SessionID: sid,
			}); err != nil {
				t.Fatalf("finalize should succeed after recovery at %s: %v", cp, err)
			}
		})
	}
}

var _ = catalog.TubeID("")
