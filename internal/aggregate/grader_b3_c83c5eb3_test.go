package aggregate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/httpapi"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store/sqlite"
)

type goldB3_c83c5eb3_fixture struct {
	handler http.Handler
}

func (f goldB3_c83c5eb3_fixture) goldB3_c83c5eb3_postReserve(
	t *testing.T,
	command aggregate.ReserveMotherCommand,
) (int, []byte) {
	t.Helper()
	type goldB3_c83c5eb3_childJSON struct {
		Ordinal         int    `json:"ordinal"`
		ChildTubeID     string `json:"child_tube_id"`
		PlannedVolumeUL int64  `json:"planned_volume_uL"`
	}
	payload := struct {
		OperationID      string                      `json:"operation_id"`
		ExpectedRevision int64                       `json:"expected_revision"`
		MotherTubeID     string                      `json:"mother_tube_id"`
		SampleID         string                      `json:"sample_id"`
		BatchID          string                      `json:"batch_id"`
		Revision         string                      `json:"revision"`
		LockedVolumeUL   int64                       `json:"locked_volume_uL"`
		Children         []goldB3_c83c5eb3_childJSON `json:"children"`
	}{
		OperationID:      command.OperationID,
		ExpectedRevision: command.ExpectedRevision,
		MotherTubeID:     string(command.MotherTubeID),
		SampleID:         string(command.SampleID),
		BatchID:          string(command.BatchID),
		Revision:         string(command.Revision),
		LockedVolumeUL:   command.LockedVolume,
	}
	for _, child := range command.Children {
		payload.Children = append(payload.Children, goldB3_c83c5eb3_childJSON{
			Ordinal:         child.Ordinal,
			ChildTubeID:     string(child.ChildTubeID),
			PlannedVolumeUL: child.PlannedVolumeUL,
		})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal reserve request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

func goldB3_c83c5eb3_cloneReserve(command aggregate.ReserveMotherCommand) aggregate.ReserveMotherCommand {
	cloned := command
	cloned.Children = append([]aliquot.ChildPlan(nil), command.Children...)
	return cloned
}

func TestGoldB3_c83c5eb3_ReserveFingerprintCanonicalizesChildOrder(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "reserve-idempotency.db")
	st, err := sqlite.Open(databasePath, nil)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc := aggregate.New(st, nil)
	if err := svc.BootstrapCatalog(ctx, aggregate.BootstrapCommand{
		OperationID: "gold-b3-bootstrap",
		Facts: store.Facts{
			Specimens: []catalog.Specimen{
				{ID: "S1", Description: "primary sample"},
				{ID: "S2", Description: "alternate sample"},
			},
			BatchRevisions: []catalog.BatchRevision{
				{SampleID: "S1", BatchID: "B1", Revision: "r1"},
				{SampleID: "S2", BatchID: "B2", Revision: "r2"},
			},
			Mothers: []store.MotherFact{
				{TubeID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1", VolumeUL: 1200},
				{TubeID: "M2", SampleID: "S2", BatchID: "B2", Revision: "r2", VolumeUL: 1200},
			},
		},
	}); err != nil {
		t.Fatalf("bootstrap catalog: %v", err)
	}

	command := aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: "gold-b3-reserve"},
		MotherTubeID: "M1",
		SampleID:     "S1",
		BatchID:      "B1",
		Revision:     "r1",
		LockedVolume: 1200,
		Children: []aliquot.ChildPlan{
			{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
			{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
			{Ordinal: 2, ChildTubeID: "C3", PlannedVolumeUL: 200},
		},
	}
	fixture := goldB3_c83c5eb3_fixture{handler: httpapi.New(svc).Handler()}
	var baseline aggregate.SessionView

	t.Run("normal reservation retains the ordered public response", func(t *testing.T) {
		status, body := fixture.goldB3_c83c5eb3_postReserve(t, command)
		if status != http.StatusOK {
			t.Fatalf("first reserve status = %d, body = %s", status, body)
		}
		if err := json.Unmarshal(body, &baseline); err != nil {
			t.Fatalf("decode first reserve response: %v", err)
		}
		if baseline.SessionID == "" {
			t.Fatal("first reserve returned an empty session id")
		}
		if baseline.RevisionNum != 0 || baseline.Status != aliquot.StatusReserved {
			t.Fatalf("first reserve revision/status = %d/%s, want 0/%s", baseline.RevisionNum, baseline.Status, aliquot.StatusReserved)
		}
		wantChildren := []aggregate.ChildView{
			{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
			{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
			{Ordinal: 2, ChildTubeID: "C3", PlannedVolumeUL: 200},
		}
		if !reflect.DeepEqual(baseline.Children, wantChildren) {
			t.Fatalf("persisted child plan = %#v, want %#v", baseline.Children, wantChildren)
		}
	})
	if baseline.SessionID == "" {
		t.FailNow()
	}

	operationBefore, err := st.LoadOperation(ctx, command.OperationID)
	if err != nil || operationBefore == nil {
		t.Fatalf("load initial operation result: result=%#v err=%v", operationBefore, err)
	}

	t.Run("identical service retry replays the original session and revision", func(t *testing.T) {
		replayed, err := svc.ReserveMother(ctx, goldB3_c83c5eb3_cloneReserve(command))
		if err != nil {
			t.Fatalf("identical retry: %v", err)
		}
		if !reflect.DeepEqual(replayed, baseline) {
			t.Fatalf("identical retry = %#v, want original %#v", replayed, baseline)
		}
	})

	reordered := goldB3_c83c5eb3_cloneReserve(command)
	reordered.Children = []aliquot.ChildPlan{
		command.Children[2],
		command.Children[0],
		command.Children[1],
	}
	t.Run("reordered HTTP retry replays the original session and revision", func(t *testing.T) {
		status, body := fixture.goldB3_c83c5eb3_postReserve(t, reordered)
		if status != http.StatusOK {
			t.Fatalf("reordered retry status = %d, body = %s", status, body)
		}
		var replayed aggregate.SessionView
		if err := json.Unmarshal(body, &replayed); err != nil {
			t.Fatalf("decode reordered retry response: %v", err)
		}
		if !reflect.DeepEqual(replayed, baseline) {
			t.Fatalf("reordered retry = %#v, want original %#v", replayed, baseline)
		}
	})

	t.Run("semantic field changes conflict without persistence writes", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(*aggregate.ReserveMotherCommand)
		}{
			{name: "child tube number", mutate: func(c *aggregate.ReserveMotherCommand) { c.Children[1].ChildTubeID = "C9" }},
			{name: "child ordinal", mutate: func(c *aggregate.ReserveMotherCommand) { c.Children[1].Ordinal = 9 }},
			{name: "child allocation", mutate: func(c *aggregate.ReserveMotherCommand) { c.Children[1].PlannedVolumeUL++ }},
			{name: "mother tube", mutate: func(c *aggregate.ReserveMotherCommand) { c.MotherTubeID = "M2" }},
			{name: "sample catalog field", mutate: func(c *aggregate.ReserveMotherCommand) { c.SampleID = "S2" }},
			{name: "batch catalog field", mutate: func(c *aggregate.ReserveMotherCommand) { c.BatchID = "B2" }},
			{name: "revision catalog field", mutate: func(c *aggregate.ReserveMotherCommand) { c.Revision = "r2" }},
			{name: "locked mother volume", mutate: func(c *aggregate.ReserveMotherCommand) { c.LockedVolume++ }},
		}
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				changed := goldB3_c83c5eb3_cloneReserve(command)
				testCase.mutate(&changed)
				_, err := svc.ReserveMother(ctx, changed)
				var domainErr *aliquot.Error
				if !errors.As(err, &domainErr) || domainErr.Code != aliquot.CodeOperationConflict {
					t.Fatalf("changed request error = %v, want %s", err, aliquot.CodeOperationConflict)
				}
				if domainErr.Revision != baseline.RevisionNum {
					t.Fatalf("conflict revision = %d, want original %d", domainErr.Revision, baseline.RevisionNum)
				}
				persisted, err := svc.GetSession(ctx, baseline.SessionID)
				if err != nil {
					t.Fatalf("get session after conflict: %v", err)
				}
				if !reflect.DeepEqual(persisted, baseline) {
					t.Fatalf("session changed after conflict: got %#v, want %#v", persisted, baseline)
				}
				operationAfter, err := st.LoadOperation(ctx, command.OperationID)
				if err != nil {
					t.Fatalf("load operation after conflict: %v", err)
				}
				if !reflect.DeepEqual(operationAfter, operationBefore) {
					t.Fatalf("operation result changed after conflict: got %#v, want %#v", operationAfter, operationBefore)
				}
			})
		}
	})

	t.Run("reordered replay and state survive store restart", func(t *testing.T) {
		if err := st.Close(); err != nil {
			t.Fatalf("close store before restart: %v", err)
		}
		st, err = sqlite.Open(databasePath, nil)
		if err != nil {
			t.Fatalf("reopen sqlite store: %v", err)
		}
		svc = aggregate.New(st, nil)
		fixture = goldB3_c83c5eb3_fixture{handler: httpapi.New(svc).Handler()}

		persisted, err := svc.GetSession(ctx, baseline.SessionID)
		if err != nil {
			t.Fatalf("get session after restart: %v", err)
		}
		if !reflect.DeepEqual(persisted, baseline) {
			t.Fatalf("session after restart = %#v, want %#v", persisted, baseline)
		}
		status, body := fixture.goldB3_c83c5eb3_postReserve(t, reordered)
		if status != http.StatusOK {
			t.Fatalf("reordered retry after restart status = %d, body = %s", status, body)
		}
		var replayed aggregate.SessionView
		if err := json.Unmarshal(body, &replayed); err != nil {
			t.Fatalf("decode retry after restart: %v", err)
		}
		if !reflect.DeepEqual(replayed, baseline) {
			t.Fatalf("retry after restart = %#v, want original %#v", replayed, baseline)
		}
		operationAfterRestart, err := st.LoadOperation(ctx, command.OperationID)
		if err != nil {
			t.Fatalf("load operation after restart retry: %v", err)
		}
		if !reflect.DeepEqual(operationAfterRestart, operationBefore) {
			t.Fatalf("operation changed after restart retry: got %#v, want %#v", operationAfterRestart, operationBefore)
		}
	})
}
