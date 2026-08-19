package goldbug2_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/httpapi"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store/sqlite"
)

func TestGoldB2_2d9acdca_BootstrapCatalogIntegrity(t *testing.T) {
	t.Run("cross_sample_batch_is_rejected_before_any_write", func(t *testing.T) {
		svc, st := goldB2_2d9acdca_newService(t)
		bad := goldB2_2d9acdca_facts("S2", "S1", true)

		err := svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
			OperationID: "op-cross-sample",
			Facts:       bad,
		})
		goldB2_2d9acdca_requireInvalidBootstrap(t, err)
		goldB2_2d9acdca_requireEmptyCatalog(t, st, "op-cross-sample")

		if err := svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
			OperationID: "op-cross-sample",
			Facts:       goldB2_2d9acdca_facts("S1", "S1", true),
		}); err != nil {
			t.Fatalf("corrected retry should bootstrap an empty catalog: %v", err)
		}
		if err := svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
			OperationID: "op-cross-sample",
			Facts:       goldB2_2d9acdca_facts("S1", "S1", true),
		}); err != nil {
			t.Fatalf("identical successful retry should be idempotent: %v", err)
		}
		mother, err := st.LoadMother(context.Background(), "M1")
		if err != nil || mother == nil || !mother.IsAvailable() {
			t.Fatalf("valid retry should persist an available mother, mother=%+v err=%v", mother, err)
		}
	})

	t.Run("missing_batch_revision_is_rejected_without_reservation", func(t *testing.T) {
		svc, st := goldB2_2d9acdca_newService(t)
		facts := goldB2_2d9acdca_facts("S1", "S1", false)

		err := svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
			OperationID: "op-missing-batch",
			Facts:       facts,
		})
		goldB2_2d9acdca_requireInvalidBootstrap(t, err)
		goldB2_2d9acdca_requireEmptyCatalog(t, st, "op-missing-batch")

		_, err = svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
			Operation:    aggregate.Operation{OperationID: "op-reserve-after-failure"},
			MotherTubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1",
			LockedVolume: 1200,
		})
		if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeInvalidState {
			t.Fatalf("reservation must not see a mother after failed bootstrap, got %v", err)
		}
	})

	t.Run("mother_sample_mismatch_is_rejected_at_http_boundary", func(t *testing.T) {
		svc, st := goldB2_2d9acdca_newService(t)
		handler := httpapi.New(svc).Handler()

		body := map[string]any{
			"operation_id": "op-http-mismatch",
			"specimens": []map[string]string{
				{"sample_id": "S1", "description": "sample one"},
				{"sample_id": "S2", "description": "sample two"},
			},
			"batch_revisions": []map[string]string{{"sample_id": "S2", "batch_id": "B1", "revision": "r1"}},
			"mothers":         []map[string]any{{"tube_id": "M1", "sample_id": "S1", "batch_id": "B1", "revision": "r1", "volume_uL": 1200}},
		}
		status, response := goldB2_2d9acdca_postJSON(t, handler, "/v1/catalog/bootstrap", body)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("mismatched bootstrap status = %d, want %d (%s)", status, http.StatusUnprocessableEntity, response)
		}
		var envelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(response), &envelope); err != nil {
			t.Fatalf("decode bootstrap error: %v", err)
		}
		if envelope.Error.Code != string(aliquot.CodeInvalidState) {
			t.Fatalf("bootstrap error code = %q, want %q", envelope.Error.Code, aliquot.CodeInvalidState)
		}
		goldB2_2d9acdca_requireEmptyCatalog(t, st, "op-http-mismatch")

		reserveBody := map[string]any{
			"operation_id":   "op-http-reserve-after-failure",
			"mother_tube_id": "M1", "sample_id": "S1", "batch_id": "B1", "revision": "r1",
			"locked_volume_uL": 1200,
		}
		reserveStatus, _ := goldB2_2d9acdca_postJSON(t, handler, "/v1/sessions", reserveBody)
		if reserveStatus != http.StatusUnprocessableEntity {
			t.Fatalf("reservation after rejected bootstrap status = %d, want %d", reserveStatus, http.StatusUnprocessableEntity)
		}

		validBody := goldB2_2d9acdca_httpFacts("op-http-mismatch")
		validStatus, validResponse := goldB2_2d9acdca_postJSON(t, handler, "/v1/catalog/bootstrap", validBody)
		if validStatus != http.StatusOK {
			t.Fatalf("corrected HTTP retry status = %d, want 200 (%s)", validStatus, validResponse)
		}
		reserveBody["operation_id"] = "op-http-reserve-valid"
		reserveStatus, reserveResponse := goldB2_2d9acdca_postJSON(t, handler, "/v1/sessions", reserveBody)
		if reserveStatus != http.StatusOK {
			t.Fatalf("valid catalog reservation status = %d, want 200 (%s)", reserveStatus, reserveResponse)
		}
	})
}

func goldB2_2d9acdca_newService(t *testing.T) (aggregate.Service, *sqlite.Store) {
	t.Helper()
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "catalog.db"), nil)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return aggregate.New(st, nil), st
}

func goldB2_2d9acdca_facts(batchSample, motherSample string, includeBatch bool) store.Facts {
	facts := store.Facts{
		Specimens: []catalog.Specimen{{ID: "S1", Description: "sample one"}, {ID: "S2", Description: "sample two"}},
		Mothers:   []store.MotherFact{{TubeID: "M1", SampleID: catalog.SampleID(motherSample), BatchID: "B1", Revision: "r1", VolumeUL: 1200}},
	}
	if includeBatch {
		facts.BatchRevisions = []catalog.BatchRevision{{SampleID: catalog.SampleID(batchSample), BatchID: "B1", Revision: "r1"}}
	}
	return facts
}

func goldB2_2d9acdca_requireInvalidBootstrap(t *testing.T, err error) {
	t.Helper()
	if e, ok := err.(*aliquot.Error); !ok || e.Code != aliquot.CodeInvalidState {
		t.Fatalf("bootstrap error = %v, want INVALID_STATE", err)
	}
}

func goldB2_2d9acdca_requireEmptyCatalog(t *testing.T, st *sqlite.Store, opID string) {
	t.Helper()
	empty, err := st.CatalogEmpty(context.Background())
	if err != nil {
		t.Fatalf("check catalog empty: %v", err)
	}
	if !empty {
		t.Fatal("failed bootstrap must not persist specimens")
	}
	mother, err := st.LoadMother(context.Background(), "M1")
	if err != nil {
		t.Fatalf("load mother after failed bootstrap: %v", err)
	}
	if mother != nil {
		t.Fatalf("failed bootstrap must not persist mother: %+v", mother)
	}
	op, err := st.LoadOperation(context.Background(), opID)
	if err != nil {
		t.Fatalf("load operation after failed bootstrap: %v", err)
	}
	if op != nil {
		t.Fatalf("failed bootstrap must not persist operation result: %+v", op)
	}
}

func goldB2_2d9acdca_httpFacts(operationID string) map[string]any {
	return map[string]any{
		"operation_id":    operationID,
		"specimens":       []map[string]string{{"sample_id": "S1", "description": "sample one"}},
		"batch_revisions": []map[string]string{{"sample_id": "S1", "batch_id": "B1", "revision": "r1"}},
		"mothers":         []map[string]any{{"tube_id": "M1", "sample_id": "S1", "batch_id": "B1", "revision": "r1", "volume_uL": 1200}},
	}
}

func goldB2_2d9acdca_postJSON(t *testing.T, handler http.Handler, path string, body map[string]any) (int, string) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode JSON: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	response, err := io.ReadAll(res.Result().Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return res.Code, string(response)
}
