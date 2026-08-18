package httpapi

import (
	"net/http"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
)

// opEnvelope carries the shared operation_id and expected_revision fields.
type opEnvelope struct {
	OperationID      string `json:"operation_id"`
	ExpectedRevision int64  `json:"expected_revision"`
}

// --- bootstrap ---

type bootstrapRequest struct {
	OperationID    string             `json:"operation_id"`
	Specimens      []specimenDTO      `json:"specimens"`
	BatchRevisions []batchRevisionDTO `json:"batch_revisions"`
	Mothers        []motherDTO        `json:"mothers"`
}

type specimenDTO struct {
	SampleID    string `json:"sample_id"`
	Description string `json:"description"`
}

type batchRevisionDTO struct {
	SampleID string `json:"sample_id"`
	BatchID  string `json:"batch_id"`
	Revision string `json:"revision"`
}

type motherDTO struct {
	TubeID   string `json:"tube_id"`
	SampleID string `json:"sample_id"`
	BatchID  string `json:"batch_id"`
	Revision string `json:"revision"`
	VolumeUL int64  `json:"volume_uL"`
}

// --- reservation ---

type reserveRequest struct {
	opEnvelope
	MotherTubeID string     `json:"mother_tube_id"`
	SampleID     string     `json:"sample_id"`
	BatchID      string     `json:"batch_id"`
	Revision     string     `json:"revision"`
	LockedVolume int64      `json:"locked_volume_uL"`
	Children     []childDTO `json:"children"`
}

type childDTO struct {
	Ordinal         int    `json:"ordinal"`
	ChildTubeID     string `json:"child_tube_id"`
	PlannedVolumeUL int64  `json:"planned_volume_uL"`
}

// --- session commands ---

type thawRequest struct {
	opEnvelope
	MotherTubeID string `json:"mother_tube_id"`
	SampleID     string `json:"sample_id"`
	BatchID      string `json:"batch_id"`
	Revision     string `json:"revision"`
}

type createChildRequest struct {
	opEnvelope
	ChildTubeID string `json:"child_tube_id"`
}

type lossRequest struct {
	opEnvelope
	LossUL int64 `json:"loss_uL"`
}

type refreezeRequest struct {
	opEnvelope
	SampleID     string `json:"sample_id"`
	BatchID      string `json:"batch_id"`
	Revision     string `json:"revision"`
	AllocationUL int64  `json:"allocation_uL"`
	FreezeRunID  string `json:"freeze_run_id"`
}

type finalizeRequest struct {
	opEnvelope
}

type quarantineRequest struct {
	opEnvelope
	Reason string `json:"reason"`
}

// manifestResponse wraps the manifest header with its stably ordered items.
type manifestResponse struct {
	lineage.Manifest
	Items []lineage.ManifestItem `json:"items"`
}

func (s *Server) bootstrap(w http.ResponseWriter, r *http.Request) {
	var req bootstrapRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := validateStrings(req.OperationID); err != nil {
		writeError(w, err)
		return
	}
	facts := store.Facts{}
	for _, spec := range req.Specimens {
		if err := validateStrings(spec.SampleID, spec.Description); err != nil {
			writeError(w, err)
			return
		}
		facts.Specimens = append(facts.Specimens, catalog.Specimen{ID: catalog.SampleID(spec.SampleID), Description: spec.Description})
	}
	for _, br := range req.BatchRevisions {
		if err := validateStrings(br.SampleID, br.BatchID, br.Revision); err != nil {
			writeError(w, err)
			return
		}
		facts.BatchRevisions = append(facts.BatchRevisions, catalog.BatchRevision{
			SampleID: catalog.SampleID(br.SampleID), BatchID: catalog.BatchID(br.BatchID), Revision: catalog.Revision(br.Revision)})
	}
	for _, m := range req.Mothers {
		if err := validateStrings(m.TubeID, m.SampleID, m.BatchID, m.Revision); err != nil {
			writeError(w, err)
			return
		}
		facts.Mothers = append(facts.Mothers, store.MotherFact{
			TubeID: catalog.TubeID(m.TubeID), SampleID: catalog.SampleID(m.SampleID),
			BatchID: catalog.BatchID(m.BatchID), Revision: catalog.Revision(m.Revision), VolumeUL: m.VolumeUL})
	}
	if err := s.svc.BootstrapCatalog(r.Context(), aggregate.BootstrapCommand{OperationID: req.OperationID, Facts: facts}); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "bootstrapped"})
}

func (s *Server) reserve(w http.ResponseWriter, r *http.Request) {
	var req reserveRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := validateStrings(req.OperationID, req.MotherTubeID, req.SampleID, req.BatchID, req.Revision); err != nil {
		writeError(w, err)
		return
	}
	if len(req.Children) > maxPlanChildren {
		writeError(w, failf("child plan exceeds maximum of %d children", maxPlanChildren))
		return
	}
	children := make([]aliquot.ChildPlan, 0, len(req.Children))
	for _, c := range req.Children {
		if err := validateStrings(c.ChildTubeID); err != nil {
			writeError(w, err)
			return
		}
		children = append(children, aliquot.ChildPlan{
			Ordinal: c.Ordinal, ChildTubeID: catalog.TubeID(c.ChildTubeID), PlannedVolumeUL: c.PlannedVolumeUL})
	}
	view, err := s.svc.ReserveMother(r.Context(), aggregate.ReserveMotherCommand{
		Operation:    aggregate.Operation{OperationID: req.OperationID},
		MotherTubeID: catalog.TubeID(req.MotherTubeID),
		SampleID:     catalog.SampleID(req.SampleID),
		BatchID:      catalog.BatchID(req.BatchID),
		Revision:     catalog.Revision(req.Revision),
		LockedVolume: req.LockedVolume,
		Children:     children,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) thaw(w http.ResponseWriter, r *http.Request) {
	var req thawRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := validateStrings(req.OperationID, req.MotherTubeID, req.SampleID, req.BatchID, req.Revision); err != nil {
		writeError(w, err)
		return
	}
	view, err := s.svc.ConfirmThaw(r.Context(), aggregate.ConfirmThawCommand{
		Operation:    aggregate.Operation{OperationID: req.OperationID, ExpectedRevision: req.ExpectedRevision},
		SessionID:    catalog.SessionID(r.PathValue("id")),
		MotherTubeID: catalog.TubeID(req.MotherTubeID),
		SampleID:     catalog.SampleID(req.SampleID),
		BatchID:      catalog.BatchID(req.BatchID),
		Revision:     catalog.Revision(req.Revision),
	})
	s.respondView(w, view, err)
}

func (s *Server) createChild(w http.ResponseWriter, r *http.Request) {
	var req createChildRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := validateStrings(req.OperationID, req.ChildTubeID); err != nil {
		writeError(w, err)
		return
	}
	view, err := s.svc.CreateChild(r.Context(), aggregate.CreateChildCommand{
		Operation:   aggregate.Operation{OperationID: req.OperationID, ExpectedRevision: req.ExpectedRevision},
		SessionID:   catalog.SessionID(r.PathValue("id")),
		ChildTubeID: catalog.TubeID(req.ChildTubeID),
	})
	s.respondView(w, view, err)
}

func (s *Server) recordLoss(w http.ResponseWriter, r *http.Request) {
	var req lossRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := validateStrings(req.OperationID); err != nil {
		writeError(w, err)
		return
	}
	view, err := s.svc.RecordLoss(r.Context(), aggregate.RecordLossCommand{
		Operation: aggregate.Operation{OperationID: req.OperationID, ExpectedRevision: req.ExpectedRevision},
		SessionID: catalog.SessionID(r.PathValue("id")),
		LossUL:    req.LossUL,
	})
	s.respondView(w, view, err)
}

func (s *Server) refreeze(w http.ResponseWriter, r *http.Request) {
	var req refreezeRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := validateStrings(req.OperationID, req.SampleID, req.BatchID, req.Revision, req.FreezeRunID); err != nil {
		writeError(w, err)
		return
	}
	view, err := s.svc.VerifyRefreeze(r.Context(), aggregate.VerifyRefreezeCommand{
		Operation:    aggregate.Operation{OperationID: req.OperationID, ExpectedRevision: req.ExpectedRevision},
		SessionID:    catalog.SessionID(r.PathValue("id")),
		ChildTubeID:  catalog.TubeID(r.PathValue("tube")),
		SampleID:     catalog.SampleID(req.SampleID),
		BatchID:      catalog.BatchID(req.BatchID),
		Revision:     catalog.Revision(req.Revision),
		AllocationUL: req.AllocationUL,
		FreezeRunID:  req.FreezeRunID,
	})
	s.respondView(w, view, err)
}

func (s *Server) finalize(w http.ResponseWriter, r *http.Request) {
	var req finalizeRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := validateStrings(req.OperationID); err != nil {
		writeError(w, err)
		return
	}
	view, err := s.svc.FinalizeLineage(r.Context(), aggregate.FinalizeLineageCommand{
		Operation: aggregate.Operation{OperationID: req.OperationID, ExpectedRevision: req.ExpectedRevision},
		SessionID: catalog.SessionID(r.PathValue("id")),
	})
	s.respondView(w, view, err)
}

func (s *Server) quarantine(w http.ResponseWriter, r *http.Request) {
	var req quarantineRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if err := validateStrings(req.OperationID, req.Reason); err != nil {
		writeError(w, err)
		return
	}
	view, err := s.svc.QuarantineBatch(r.Context(), aggregate.QuarantineBatchCommand{
		Operation: aggregate.Operation{OperationID: req.OperationID, ExpectedRevision: req.ExpectedRevision},
		SessionID: catalog.SessionID(r.PathValue("id")),
		Reason:    req.Reason,
	})
	s.respondView(w, view, err)
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	view, err := s.svc.GetSession(r.Context(), catalog.SessionID(r.PathValue("id")))
	s.respondView(w, view, err)
}

func (s *Server) getManifest(w http.ResponseWriter, r *http.Request) {
	manifest, items, err := s.svc.GetManifest(r.Context(), catalog.SessionID(r.PathValue("id")))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, manifestResponse{Manifest: *manifest, Items: items})
}

// respondView writes a session view or a domain error.
func (s *Server) respondView(w http.ResponseWriter, view aggregate.SessionView, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}
