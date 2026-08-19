package aggregate_test

import (
	"bytes"
	"context"
	"encoding/json"
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

func goldB1_da259917_newService(t *testing.T, volume int64) (aggregate.Service, *sqlite.Store) {
	t.Helper()
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "state.db"), nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := aggregate.New(st, nil)
	err = svc.BootstrapCatalog(context.Background(), aggregate.BootstrapCommand{
		OperationID: "op-boot",
		Facts: store.Facts{
			Specimens:      []catalog.Specimen{{ID: "S1", Description: "sample"}},
			BatchRevisions: []catalog.BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}},
			Mothers:        []store.MotherFact{{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: volume}},
		},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return svc, st
}

func goldB1_da259917_postJSON(t *testing.T, h http.Handler, path string, body any) (int, []byte) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

func TestGoldB1_da259917_ReservationRejectsInt64SumOverflow(t *testing.T) {
	const maxInt64 int64 = 1<<63 - 1
	const fiveE18 int64 = 5_000_000_000_000_000_000

	t.Run("service rejects overflow before any state change and retries idempotently", func(t *testing.T) {
		svc, st := goldB1_da259917_newService(t, maxInt64)
		cmd := aggregate.ReserveMotherCommand{
			Operation:    aggregate.Operation{OperationID: "op-overflow"},
			MotherTubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1",
			LockedVolume: maxInt64,
			Children: []aliquot.ChildPlan{
				{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: fiveE18},
				{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: fiveE18},
			},
		}

		_, err := svc.ReserveMother(context.Background(), cmd)
		domainErr, ok := err.(*aliquot.Error)
		if !ok || domainErr.Code != aliquot.CodeOverAllocation {
			t.Fatalf("overflow reservation error = %v, want OVER_ALLOCATION", err)
		}
		if domainErr.Revision != 0 {
			t.Fatalf("rejection revision = %d, want 0", domainErr.Revision)
		}

		mother, err := st.LoadMother(context.Background(), "M1")
		if err != nil {
			t.Fatalf("load mother: %v", err)
		}
		if mother == nil || !mother.IsAvailable() || mother.RemainingUL != maxInt64 || mother.SessionID != "" {
			t.Fatalf("mother changed after rejected reservation: %+v", mother)
		}
		claimed, err := st.ClaimedTubeNumbers(context.Background(), []catalog.TubeID{"C1", "C2"})
		if err != nil {
			t.Fatalf("check claimed tubes: %v", err)
		}
		if claimed.Has("C1") || claimed.Has("C2") {
			t.Fatalf("rejected plan declared child tubes: %+v", claimed)
		}
		result, err := st.LoadOperation(context.Background(), "op-overflow")
		if err != nil || result == nil {
			t.Fatalf("rejection operation result = %+v, err=%v", result, err)
		}
		if result.Code != string(aliquot.CodeOverAllocation) || result.SessionID != "" || result.Revision != 0 {
			t.Fatalf("persisted rejection = %+v", result)
		}
		if sess, err := st.LoadSession(context.Background(), result.SessionID); err != nil || sess != nil {
			t.Fatalf("rejected reservation created session: session=%+v err=%v", sess, err)
		}

		_, retryErr := svc.ReserveMother(context.Background(), cmd)
		retryDomainErr, retryOK := retryErr.(*aliquot.Error)
		if !retryOK || retryDomainErr.Code != aliquot.CodeOverAllocation || retryDomainErr.Revision != domainErr.Revision {
			t.Fatalf("retry error = %v, want stable OVER_ALLOCATION revision 0", retryErr)
		}
	})

	t.Run("service rejects ordinary non-overflow over-allocation", func(t *testing.T) {
		svc, st := goldB1_da259917_newService(t, 100)
		_, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
			Operation:    aggregate.Operation{OperationID: "op-over"},
			MotherTubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", LockedVolume: 100,
			Children: []aliquot.ChildPlan{
				{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 60},
				{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 41},
			},
		})
		if domainErr, ok := err.(*aliquot.Error); !ok || domainErr.Code != aliquot.CodeOverAllocation {
			t.Fatalf("ordinary over-allocation error = %v, want OVER_ALLOCATION", err)
		}
		mother, err := st.LoadMother(context.Background(), "M1")
		if err != nil || mother == nil || !mother.IsAvailable() || mother.RemainingUL != 100 {
			t.Fatalf("mother changed after ordinary rejection: %+v err=%v", mother, err)
		}
	})

	t.Run("exact MaxInt64 allocation remains a valid boundary", func(t *testing.T) {
		svc, st := goldB1_da259917_newService(t, maxInt64)
		view, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
			Operation:    aggregate.Operation{OperationID: "op-max"},
			MotherTubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", LockedVolume: maxInt64,
			Children: []aliquot.ChildPlan{{Ordinal: 0, ChildTubeID: "C-max", PlannedVolumeUL: maxInt64}},
		})
		if err != nil {
			t.Fatalf("exact maximum reservation: %v", err)
		}
		if view.SessionID == "" || view.Status != aliquot.StatusReserved || view.LockedVolumeUL != maxInt64 || view.RemainingUL != maxInt64 {
			t.Fatalf("maximum reservation view = %+v", view)
		}
		if len(view.Children) != 1 || view.Children[0].PlannedVolumeUL != maxInt64 || view.Children[0].Created {
			t.Fatalf("maximum reservation children = %+v", view.Children)
		}
		mother, err := st.LoadMother(context.Background(), "M1")
		if err != nil || mother == nil || mother.Status != catalog.ContainerStatusReserved || mother.SessionID != view.SessionID {
			t.Fatalf("persisted maximum reservation mother = %+v err=%v", mother, err)
		}
		sess, err := st.LoadSession(context.Background(), view.SessionID)
		if err != nil || sess == nil || sess.LockedVolumeUL != maxInt64 || len(sess.Children) != 1 {
			t.Fatalf("persisted maximum reservation session = %+v err=%v", sess, err)
		}
	})

	t.Run("normal large multi-child reservation remains successful", func(t *testing.T) {
		svc, st := goldB1_da259917_newService(t, maxInt64)
		view, err := svc.ReserveMother(context.Background(), aggregate.ReserveMotherCommand{
			Operation:    aggregate.Operation{OperationID: "op-large"},
			MotherTubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", LockedVolume: maxInt64,
			Children: []aliquot.ChildPlan{
				{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 4_000_000_000_000_000_000},
				{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 5_000_000_000_000_000_000},
			},
		})
		if err != nil {
			t.Fatalf("normal large reservation: %v", err)
		}
		if view.SessionID == "" || view.Status != aliquot.StatusReserved || len(view.Children) != 2 {
			t.Fatalf("normal large reservation view = %+v", view)
		}
		mother, err := st.LoadMother(context.Background(), "M1")
		if err != nil || mother == nil || mother.Status != catalog.ContainerStatusReserved || mother.RemainingUL != maxInt64 {
			t.Fatalf("normal large reservation mother = %+v err=%v", mother, err)
		}
	})

	t.Run("HTTP reservation returns stable rejection without persistence side effects", func(t *testing.T) {
		svc, st := goldB1_da259917_newService(t, maxInt64)
		handler := httpapi.New(svc).Handler()
		request := map[string]any{
			"operation_id":     "op-http-overflow",
			"mother_tube_id":   "M1",
			"sample_id":        "S1",
			"batch_id":         "B1",
			"revision":         "r1",
			"locked_volume_uL": maxInt64,
			"children": []map[string]any{
				{"ordinal": 0, "child_tube_id": "HC1", "planned_volume_uL": fiveE18},
				{"ordinal": 1, "child_tube_id": "HC2", "planned_volume_uL": fiveE18},
			},
		}
		status, body := goldB1_da259917_postJSON(t, handler, "/v1/sessions", request)
		var response struct {
			Error struct {
				Code     string `json:"code"`
				Revision int64  `json:"revision"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatalf("decode HTTP rejection: %v; body=%s", err, body)
		}
		if status != http.StatusUnprocessableEntity || response.Error.Code != string(aliquot.CodeOverAllocation) || response.Error.Revision != 0 {
			t.Fatalf("HTTP rejection status=%d response=%+v body=%s", status, response, body)
		}
		retryStatus, retryBody := goldB1_da259917_postJSON(t, handler, "/v1/sessions", request)
		var retryResponse struct {
			Error struct {
				Code     string `json:"code"`
				Revision int64  `json:"revision"`
			} `json:"error"`
		}
		if err := json.Unmarshal(retryBody, &retryResponse); err != nil {
			t.Fatalf("decode HTTP retry rejection: %v; body=%s", err, retryBody)
		}
		if retryStatus != status || retryResponse.Error.Code != response.Error.Code || retryResponse.Error.Revision != response.Error.Revision {
			t.Fatalf("HTTP retry status=%d response=%+v, first status=%d response=%+v", retryStatus, retryResponse, status, response)
		}

		mother, err := st.LoadMother(context.Background(), "M1")
		if err != nil || mother == nil || !mother.IsAvailable() || mother.RemainingUL != maxInt64 || mother.SessionID != "" {
			t.Fatalf("mother changed after HTTP rejection: %+v err=%v", mother, err)
		}
		claimed, err := st.ClaimedTubeNumbers(context.Background(), []catalog.TubeID{"HC1", "HC2"})
		if err != nil || claimed.Has("HC1") || claimed.Has("HC2") {
			t.Fatalf("HTTP rejected plan declared child tubes: %+v err=%v", claimed, err)
		}
		result, err := st.LoadOperation(context.Background(), "op-http-overflow")
		if err != nil || result == nil || result.Code != string(aliquot.CodeOverAllocation) || result.SessionID != "" {
			t.Fatalf("HTTP rejection operation result = %+v err=%v", result, err)
		}
	})
}
