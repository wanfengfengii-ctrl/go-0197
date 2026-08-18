package aggregate_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store/sqlite"
)

// testBarrier is a deterministic test synchronization barrier. It records
// arrivals in order and lets tests release them one at a time or by name.
type testBarrier struct {
	mu    sync.Mutex
	names []string
	chs   []chan struct{}
}

func newTestBarrier() *testBarrier { return &testBarrier{} }

func (b *testBarrier) hook(name string) {
	ch := make(chan struct{})
	b.mu.Lock()
	b.names = append(b.names, name)
	b.chs = append(b.chs, ch)
	b.mu.Unlock()
	<-ch
}

func (b *testBarrier) waitFor(n int) {
	for {
		b.mu.Lock()
		c := len(b.names)
		b.mu.Unlock()
		if c >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (b *testBarrier) releaseByIndex(i int) {
	b.mu.Lock()
	ch := b.chs[i]
	b.chs[i] = nil
	b.mu.Unlock()
	close(ch)
}

func (b *testBarrier) releaseByName(name string) {
	b.mu.Lock()
	var ch chan struct{}
	for i, n := range b.names {
		if n == name && b.chs[i] != nil {
			ch = b.chs[i]
			b.chs[i] = nil
			break
		}
	}
	b.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// tempDBPath returns a fresh temporary SQLite path.
func tempDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "aliquotseal.db")
}

// openService opens a store plus service at path with the given hooks.
func openService(t *testing.T, path string, hooks *store.Hooks) (aggregate.Service, *sqlite.Store) {
	t.Helper()
	st, err := sqlite.Open(path, hooks)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return aggregate.New(st, hooks), st
}

// newService opens a fresh service on a temporary database with no hooks.
func newService(t *testing.T) (aggregate.Service, *sqlite.Store) {
	t.Helper()
	return openService(t, tempDBPath(t), nil)
}

// standardFacts is the canonical catalog used across most tests: one specimen,
// one batch revision, and a 1200 uL mother tube.
func standardFacts() store.Facts {
	return store.Facts{
		Specimens: []catalog.Specimen{{ID: "S1", Description: "sample one"}},
		BatchRevisions: []catalog.BatchRevision{
			{SampleID: "S1", BatchID: "B1", Revision: "r1"},
		},
		Mothers: []store.MotherFact{
			{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200},
		},
	}
}

// bootstrap standardizes the catalog bootstrap.
func bootstrap(t *testing.T, svc aggregate.Service, opID string) {
	t.Helper()
	if err := svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
		OperationID: opID, Facts: standardFacts(),
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
}

// reserveRes reserves the standard mother with the canonical 500/400/200 plan.
func reserveRes(t *testing.T, svc aggregate.Service, opID string) aggregate.SessionView {
	t.Helper()
	return reserve(t, svc, opID, "M1", "S1", "B1", "r1", 1200, []aliquot.ChildPlan{
		{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
		{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
		{Ordinal: 2, ChildTubeID: "C3", PlannedVolumeUL: 200},
	})
}

func reserve(t *testing.T, svc aggregate.Service, opID string, mother, sample, batch, revision string, locked int64, children []aliquot.ChildPlan) aggregate.SessionView {
	t.Helper()
	v, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: opID},
		MotherTubeID: catalog.TubeID(mother),
		SampleID:     catalog.SampleID(sample),
		BatchID:      catalog.BatchID(batch),
		Revision:     catalog.Revision(revision),
		LockedVolume: locked,
		Children:     children,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	return v
}

// thawRes confirms thaw on the standard session.
func thawRes(t *testing.T, svc aggregate.Service, opID string, session catalog.SessionID, rev int64) aggregate.SessionView {
	t.Helper()
	v, err := svc.ConfirmThaw(context.Background(), aggregate.ConfirmThawCommand{
		Operation:    aggregate.Operation{OperationID: opID, ExpectedRevision: rev},
		SessionID:    session,
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
	})
	if err != nil {
		t.Fatalf("thaw: %v", err)
	}
	return v
}

// createChildRes creates a planned child.
func createChildRes(t *testing.T, svc aggregate.Service, opID string, session catalog.SessionID, rev int64, child catalog.TubeID) aggregate.SessionView {
	t.Helper()
	v, err := svc.CreateChild(context.Background(), aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: opID, ExpectedRevision: rev},
		SessionID:   session,
		ChildTubeID: child,
	})
	if err != nil {
		t.Fatalf("create child %s: %v", child, err)
	}
	return v
}

// verifyRes records a refreeze verification for a child.
func verifyRes(t *testing.T, svc aggregate.Service, opID string, session catalog.SessionID, rev int64, child catalog.TubeID, alloc int64) aggregate.SessionView {
	t.Helper()
	v, err := svc.VerifyRefreeze(context.Background(), aggregate.VerifyRefreezeCommand{
		Operation:    aggregate.Operation{OperationID: opID, ExpectedRevision: rev},
		SessionID:    session,
		ChildTubeID:  child,
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		AllocationUL: alloc,
		FreezeRunID:  "FR-" + string(child),
	})
	if err != nil {
		t.Fatalf("verify %s: %v", child, err)
	}
	return v
}

// fullyAliquot drives the standard session through thaw, all three children, and
// a 100 uL loss, leaving the mother depleted with zero remaining volume.
func fullyAliquot(t *testing.T, svc aggregate.Service, session catalog.SessionID) int64 {
	t.Helper()
	rev := int64(0)
	rev = thawRes(t, svc, "op-thaw", session, rev).RevisionNum
	rev = createChildRes(t, svc, "op-c1", session, rev, "C1").RevisionNum
	rev = createChildRes(t, svc, "op-c2", session, rev, "C2").RevisionNum
	rev = createChildRes(t, svc, "op-c3", session, rev, "C3").RevisionNum
	v, err := svc.RecordLoss(context.Background(), aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: "op-loss", ExpectedRevision: rev},
		SessionID: session,
		LossUL:    100,
	})
	if err != nil {
		t.Fatalf("record loss: %v", err)
	}
	return v.RevisionNum
}
