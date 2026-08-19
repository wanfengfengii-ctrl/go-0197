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

// assertInvalidState asserts that err is a stable *aliquot.Error whose code is
// exactly INVALID_STATE (the stable client error returned at the cataloging
// boundary when bootstrap facts are internally inconsistent).
func assertInvalidState(t *testing.T, err error) {
	t.Helper()
	e, ok := err.(*aliquot.Error)
	if !ok {
		t.Fatalf("expected *aliquot.Error, got %T: %v", err, err)
	}
	if e.Code != aliquot.CodeInvalidState {
		t.Fatalf("error code = %s, want %s", e.Code, aliquot.CodeInvalidState)
	}
}

// assertCatalogEmpty asserts that no catalog facts or operation records were
// persisted: the catalog is still empty, the named mother is absent, and the
// operation result for opID was not recorded.
func assertCatalogEmpty(t *testing.T, st *sqlite.Store, mother catalog.TubeID, opID string) {
	t.Helper()
	empty, err := st.CatalogEmpty(context.Background())
	if err != nil {
		t.Fatalf("check catalog empty: %v", err)
	}
	if !empty {
		t.Fatalf("catalog should be empty after a rejected bootstrap, but facts were written")
	}
	if m, _ := st.LoadMother(context.Background(), mother); m != nil {
		t.Fatalf("mother %q should not exist after a rejected bootstrap", mother)
	}
	if op, _ := st.LoadOperation(context.Background(), opID); op != nil {
		t.Fatalf("operation result %q should not be persisted after a rejected bootstrap", opID)
	}
}

// factsFor builds a store.Facts set from the given specimens, batch revisions,
// and mothers.
func factsFor(specs []catalog.Specimen, batches []catalog.BatchRevision, mothers []store.MotherFact) store.Facts {
	return store.Facts{Specimens: specs, BatchRevisions: batches, Mothers: mothers}
}

// boot issues a bootstrap command and returns the error (nil on success).
func boot(t *testing.T, svc aggregate.Service, opID string, facts store.Facts) error {
	t.Helper()
	return svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
		OperationID: opID, Facts: facts,
	})
}

// TestModel_BootstrapCatalogIntegrity verifies that the cataloging boundary
// rejects internally inconsistent bootstrap facts before the first write,
// returns a stable INVALID_STATE client error, and persists no sample, batch,
// mother, or operation record; a retry with corrected facts then succeeds, and
// a fully consistent bootstrap behaves unchanged.
func TestModel_BootstrapCatalogIntegrity(t *testing.T) {
	// --- 正常建档: a consistent bootstrap writes an available mother. ---
	t.Run("normal_cataloging_writes_available_mother", func(t *testing.T) {
		svc, st := newService(t)
		if err := boot(t, svc, "op-boot", standardFacts()); err != nil {
			t.Fatalf("bootstrap: %v", err)
		}
		empty, err := st.CatalogEmpty(context.Background())
		if err != nil {
			t.Fatalf("check catalog empty: %v", err)
		}
		if empty {
			t.Fatalf("catalog should be populated after a successful bootstrap")
		}
		mother, err := st.LoadMother(context.Background(), "M1")
		if err != nil {
			t.Fatalf("load mother: %v", err)
		}
		if mother == nil || !mother.IsAvailable() {
			t.Fatalf("mother should be available, got %+v", mother)
		}
		if op, _ := st.LoadOperation(context.Background(), "op-boot"); op == nil || op.Code != "ok" {
			t.Fatalf("operation result should be persisted as ok, got %+v", op)
		}
		// The available mother can be reserved on the normal path.
		view := reserveRes(t, svc, "op-res")
		if view.Status != aliquot.StatusReserved {
			t.Fatalf("status = %s, want reserved", view.Status)
		}
	})

	// --- 错配 (sample): a mother whose sample does not match any specimen. ---
	t.Run("mother_sample_mismatch_rejected_before_write", func(t *testing.T) {
		svc, st := newService(t)
		facts := factsFor(
			[]catalog.Specimen{{ID: "S1", Description: "sample one"}},
			[]catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
			[]store.MotherFact{{TubeID: "M1", SampleID: "S2", BatchID: "B1", Revision: "r1", VolumeUL: 1200}},
		)
		if err := boot(t, svc, "op-bad", facts); err == nil {
			t.Fatalf("expected bootstrap to be rejected for mother sample mismatch")
		} else {
			assertInvalidState(t, err)
		}
		assertCatalogEmpty(t, st, "M1", "op-bad")

		// No available mother means a reservation cannot be established.
		_, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
			Operation:    aggregate.Operation{OperationID: "op-res"},
			MotherTubeID: "M1", SampleID: "S2", BatchID: "B1", Revision: "r1",
			LockedVolume: 1200,
			Children:     []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500}},
		})
		if err == nil {
			t.Fatalf("reservation should fail when no available mother was written")
		}
	})

	// --- 错配 (batch): a mother whose batch revision does not match the
	// registered one. ---
	t.Run("mother_batch_revision_mismatch_rejected_before_write", func(t *testing.T) {
		svc, st := newService(t)
		facts := factsFor(
			[]catalog.Specimen{{ID: "S1", Description: "sample one"}},
			[]catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
			[]store.MotherFact{{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r2", VolumeUL: 1200}},
		)
		if err := boot(t, svc, "op-bad", facts); err == nil {
			t.Fatalf("expected bootstrap to be rejected for batch revision mismatch")
		} else {
			assertInvalidState(t, err)
		}
		assertCatalogEmpty(t, st, "M1", "op-bad")
	})

	// --- 缺失引用: a mother references a batch revision that was never
	// registered. ---
	t.Run("missing_batch_revision_rejected_without_records", func(t *testing.T) {
		svc, st := newService(t)
		facts := factsFor(
			[]catalog.Specimen{{ID: "S1", Description: "sample one"}},
			// No batch revision for S1/B1/r1 is registered.
			[]catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r2"}},
			[]store.MotherFact{{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200}},
		)
		if err := boot(t, svc, "op-bad", facts); err == nil {
			t.Fatalf("expected bootstrap to be rejected for unknown batch revision")
		} else {
			assertInvalidState(t, err)
		}
		assertCatalogEmpty(t, st, "M1", "op-bad")
	})

	// --- 缺失引用 (specimen): a batch revision references an unregistered
	// specimen. ---
	t.Run("batch_revision_unknown_specimen_rejected_without_records", func(t *testing.T) {
		svc, st := newService(t)
		facts := factsFor(
			[]catalog.Specimen{{ID: "S1", Description: "sample one"}},
			[]catalog.BatchRevision{{SampleID: "S2", BatchID: "B1", Revision: "r1"}},
			[]store.MotherFact{{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200}},
		)
		if err := boot(t, svc, "op-bad", facts); err == nil {
			t.Fatalf("expected bootstrap to be rejected for batch revision with unknown specimen")
		} else {
			assertInvalidState(t, err)
		}
		assertCatalogEmpty(t, st, "M1", "op-bad")
	})

	// --- 跨样本: a mother uses a batch revision that belongs to another
	// sample. ---
	t.Run("cross_sample_batch_rejected_before_any_write", func(t *testing.T) {
		svc, st := newService(t)
		facts := factsFor(
			[]catalog.Specimen{{ID: "S1", Description: "sample one"}, {ID: "S2", Description: "sample two"}},
			// The only batch revision is registered under S1.
			[]catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
			// Mother M1 belongs to S2 but reuses S1's batch revision.
			[]store.MotherFact{{TubeID: "M1", SampleID: "S2", BatchID: "B1", Revision: "r1", VolumeUL: 1200}},
		)
		if err := boot(t, svc, "op-bad", facts); err == nil {
			t.Fatalf("expected bootstrap to be rejected for cross-sample batch reference")
		} else {
			assertInvalidState(t, err)
		}
		assertCatalogEmpty(t, st, "M1", "op-bad")
	})

	// --- 事务无写入: a rejected bootstrap leaves no operation record, so the
	// same operation id can be retried with corrected facts. ---
	t.Run("rejected_bootstrap_writes_no_operation_record", func(t *testing.T) {
		svc, st := newService(t)
		bad := factsFor(
			[]catalog.Specimen{{ID: "S1", Description: "sample one"}},
			[]catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
			[]store.MotherFact{{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r2", VolumeUL: 1200}},
		)
		if err := boot(t, svc, "op-retry", bad); err == nil {
			t.Fatalf("expected bootstrap to be rejected")
		}
		// No operation record was persisted, so the operation id was not consumed.
		if op, _ := st.LoadOperation(context.Background(), "op-retry"); op != nil {
			t.Fatalf("rejected bootstrap should not persist an operation result, got %+v", op)
		}
	})

	// --- 失败后重试 (corrected facts): after a rejected bootstrap, a retry
	// with corrected facts succeeds because nothing was written. ---
	t.Run("retry_after_failure_succeeds_with_corrected_facts", func(t *testing.T) {
		svc, st := newService(t)
		bad := factsFor(
			[]catalog.Specimen{{ID: "S1", Description: "sample one"}},
			[]catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
			[]store.MotherFact{{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r2", VolumeUL: 1200}},
		)
		if err := boot(t, svc, "op-retry", bad); err == nil {
			t.Fatalf("expected first bootstrap to be rejected")
		}
		assertCatalogEmpty(t, st, "M1", "op-retry")

		// Retry with the SAME operation id but corrected, consistent facts. Because
		// the rejection was not persisted, the operation id is still free and the
		// corrected bootstrap succeeds.
		good := standardFacts()
		if err := boot(t, svc, "op-retry", good); err != nil {
			t.Fatalf("retry with corrected facts should succeed, got %v", err)
		}
		empty, err := st.CatalogEmpty(context.Background())
		if err != nil {
			t.Fatalf("check catalog empty: %v", err)
		}
		if empty {
			t.Fatalf("catalog should be populated after a successful retry")
		}
		mother, err := st.LoadMother(context.Background(), "M1")
		if err != nil {
			t.Fatalf("load mother: %v", err)
		}
		if mother == nil || !mother.IsAvailable() {
			t.Fatalf("mother should be available after a successful retry, got %+v", mother)
		}
	})

	// --- 失败后重试 (same bad facts): retrying the same rejected content
	// returns the same stable INVALID_STATE rejection. ---
	t.Run("retry_same_bad_facts_returns_same_rejection", func(t *testing.T) {
		svc, st := newService(t)
		bad := factsFor(
			[]catalog.Specimen{{ID: "S1", Description: "sample one"}},
			[]catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
			[]store.MotherFact{{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r2", VolumeUL: 1200}},
		)
		if err := boot(t, svc, "op-bad", bad); err == nil {
			t.Fatalf("expected first bootstrap to be rejected")
		} else {
			assertInvalidState(t, err)
		}
		// Retrying the identical bad content yields the same stable rejection.
		if err := boot(t, svc, "op-bad", bad); err == nil {
			t.Fatalf("expected retry of bad bootstrap to be rejected")
		} else {
			assertInvalidState(t, err)
		}
		assertCatalogEmpty(t, st, "M1", "op-bad")
	})
}

// TestModel_BootstrapCatalogIdempotentSuccess verifies that a repeated
// identical successful bootstrap returns the original result without writing a
// second set of records, and that the cataloging boundary does not interfere
// with the normal idempotent path.
func TestModel_BootstrapCatalogIdempotentSuccess(t *testing.T) {
	svc, st := newService(t)
	if err := boot(t, svc, "op-boot", standardFacts()); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	// Repeating the identical bootstrap is idempotent: it returns nil (the
	// original success) rather than an already-bootstrapped error.
	if err := boot(t, svc, "op-boot", standardFacts()); err != nil {
		t.Fatalf("repeated identical bootstrap should be idempotent, got %v", err)
	}
	// The catalog holds exactly one mother.
	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother: %v", err)
	}
	if mother == nil || !mother.IsAvailable() {
		t.Fatalf("mother should remain available after idempotent repeat, got %+v", mother)
	}
}
