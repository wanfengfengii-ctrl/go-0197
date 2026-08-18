// Package httpapi adapts the aggregate domain Service to the HTTP surface
// described in public interfaces 2-7. It registers the route table, decodes
// requests with strict unknown-field rejection and limits, and maps domain
// errors to stable machine codes.
package httpapi

import (
	"net/http"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aggregate"
)

// Server holds the domain service and exposes the HTTP route table.
type Server struct {
	svc aggregate.Service
}

// New constructs a Server around the aggregate Service.
func New(svc aggregate.Service) *Server {
	return &Server{svc: svc}
}

// Handler returns the routed HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/catalog/bootstrap", s.bootstrap)
	mux.HandleFunc("POST /v1/sessions", s.reserve)
	mux.HandleFunc("POST /v1/sessions/{id}/thaw", s.thaw)
	mux.HandleFunc("POST /v1/sessions/{id}/children", s.createChild)
	mux.HandleFunc("POST /v1/sessions/{id}/losses", s.recordLoss)
	mux.HandleFunc("POST /v1/sessions/{id}/children/{tube}/refreeze", s.refreeze)
	mux.HandleFunc("POST /v1/sessions/{id}/finalize", s.finalize)
	mux.HandleFunc("POST /v1/sessions/{id}/quarantine", s.quarantine)
	mux.HandleFunc("GET /v1/sessions/{id}", s.getSession)
	mux.HandleFunc("GET /v1/sessions/{id}/manifest", s.getManifest)
	return mux
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
