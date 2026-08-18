package lineage

import (
	"sort"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
)

// Graph is the in-memory view of a session's 1:N mother-to-children lineage:
// the pending parent-child edges, the per-child refreeze verifications, and the
// derived quarantine/finalize projections. It is built from the persisted
// lineage_edges and refreeze_verifications tables.
type Graph struct {
	SessionID     catalog.SessionID
	ParentTubeID  catalog.TubeID
	Edges         []Edge
	Verifications map[catalog.TubeID]Verification
}

// NewGraph constructs an empty lineage graph for a session's mother tube.
func NewGraph(session catalog.SessionID, parent catalog.TubeID) *Graph {
	return &Graph{
		SessionID:     session,
		ParentTubeID:  parent,
		Verifications: make(map[catalog.TubeID]Verification),
	}
}

// AddEdge appends a pending parent-child edge, deduplicated by child tube ID so
// a child can never appear twice in the graph.
func (g *Graph) AddEdge(e Edge) {
	for i := range g.Edges {
		if g.Edges[i].ChildTubeID == e.ChildTubeID {
			g.Edges[i] = e
			return
		}
	}
	g.Edges = append(g.Edges, e)
}

// AddVerification records a per-child refreeze verification, keyed by child tube
// ID. Each child keeps at most one verification.
func (g *Graph) AddVerification(v Verification) {
	if g.Verifications == nil {
		g.Verifications = make(map[catalog.TubeID]Verification)
	}
	g.Verifications[v.ChildTubeID] = v
}

// CreatedChildren returns the child tube IDs that have a lineage edge, sorted
// stably by tube ID.
func (g *Graph) CreatedChildren() []catalog.TubeID {
	ids := make([]catalog.TubeID, 0, len(g.Edges))
	for _, e := range g.Edges {
		ids = append(ids, e.ChildTubeID)
	}
	sort.SliceStable(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Verified reports whether a child has a matching refreeze verification.
func (g *Graph) Verified(child catalog.TubeID) bool {
	_, ok := g.Verifications[child]
	return ok
}

// MissingForPlan returns the planned child tube IDs that lack a matching
// verification, sorted stably by tube ID (domain rule 9).
func (g *Graph) MissingForPlan(plan []catalog.TubeID) []catalog.TubeID {
	return MissingVerifications(plan, g.Verifications)
}

// AllCreated reports whether every planned child has a lineage edge.
func (g *Graph) AllCreated(plan []catalog.TubeID) bool {
	created := make(map[catalog.TubeID]bool, len(g.Edges))
	for _, e := range g.Edges {
		created[e.ChildTubeID] = true
	}
	for _, id := range plan {
		if !created[id] {
			return false
		}
	}
	return true
}
