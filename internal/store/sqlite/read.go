package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/volume"
)

// CatalogEmpty reports whether no catalog facts have been bootstrapped yet.
func (s *Store) CatalogEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM specimens`).Scan(&n); err != nil {
		return false, fmt.Errorf("count specimens: %w", err)
	}
	return n == 0, nil
}

// LoadMother loads a single container by tube ID.
func (s *Store) LoadMother(ctx context.Context, id catalog.TubeID) (*catalog.Container, error) {
	const q = `SELECT tube_id, type, sample_id, batch_id, revision, remaining_uL, status, session_id
	           FROM containers WHERE tube_id = ?`
	var c catalog.Container
	err := s.db.QueryRowContext(ctx, q, string(id)).Scan(
		&c.ID, &c.Type, &c.SampleID, &c.BatchID, &c.Revision, &c.RemainingUL, &c.Status, &c.SessionID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load mother: %w", err)
	}
	return &c, nil
}

// LoadOperation loads a persisted operation result by operation ID.
func (s *Store) LoadOperation(ctx context.Context, opID string) (*store.OperationResult, error) {
	const q = `SELECT operation_id, fingerprint, status_code, message, revision, terminal, session_id, snapshot
	           FROM operation_results WHERE operation_id = ?`
	var r store.OperationResult
	err := s.db.QueryRowContext(ctx, q, opID).Scan(
		&r.OperationID, &r.Fingerprint, &r.Code, &r.Message, &r.Revision, &r.Terminal, &r.SessionID, &r.Snapshot)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load operation: %w", err)
	}
	return &r, nil
}

// ClaimedTubeNumbers returns the subset of ids already claimed by an existing
// container or a prior session declaration (domain rule 4).
func (s *Store) ClaimedTubeNumbers(ctx context.Context, ids []catalog.TubeID) (catalog.TubeSet, error) {
	set := make(catalog.TubeSet)
	for _, id := range ids {
		var n int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM containers WHERE tube_id = ?`, string(id)).Scan(&n); err != nil {
			return nil, fmt.Errorf("check container: %w", err)
		}
		if n > 0 {
			set.Claim(id)
			continue
		}
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM planned_children WHERE child_tube_id = ?`, string(id)).Scan(&n); err != nil {
			return nil, fmt.Errorf("check planned child: %w", err)
		}
		if n > 0 {
			set.Claim(id)
		}
	}
	return set, nil
}

// LoadSession assembles the full session state from all related tables.
func (s *Store) LoadSession(ctx context.Context, id catalog.SessionID) (*store.Session, error) {
	sess := &store.Session{ID: id, Verifications: map[catalog.TubeID]lineage.Verification{}}

	const q = `SELECT session_id, mother_tube_id, sample_id, batch_id, revision,
	                  locked_volume_uL, status, revision_num, terminal_kind,
	                  terminal_reason, created_at
	           FROM aliquot_sessions WHERE session_id = ?`
	var createdAt string
	err := s.db.QueryRowContext(ctx, q, string(id)).Scan(
		&sess.ID, &sess.MotherTubeID, &sess.SampleID, &sess.BatchID, &sess.Revision,
		&sess.LockedVolumeUL, &sess.Status, &sess.RevisionNum, &sess.TerminalKind,
		&sess.TerminalReason, &createdAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load session: %w", err)
	}
	sess.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)

	if err := s.loadChildren(ctx, sess); err != nil {
		return nil, err
	}
	if err := s.loadEntries(ctx, sess); err != nil {
		return nil, err
	}
	if err := s.loadEdges(ctx, sess); err != nil {
		return nil, err
	}
	if err := s.loadVerifications(ctx, sess); err != nil {
		return nil, err
	}
	if err := s.loadManifest(ctx, sess); err != nil {
		return nil, err
	}
	return sess, nil
}

func (s *Store) loadChildren(ctx context.Context, sess *store.Session) error {
	const q = `SELECT ordinal, child_tube_id, planned_volume_uL, created
	           FROM planned_children WHERE session_id = ? ORDER BY ordinal`
	rows, err := s.db.QueryContext(ctx, q, string(sess.ID))
	if err != nil {
		return fmt.Errorf("load children: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var c store.PlannedChild
		if err := rows.Scan(&c.Ordinal, &c.ChildTubeID, &c.PlannedVolumeUL, &c.Created); err != nil {
			return fmt.Errorf("scan child: %w", err)
		}
		sess.Children = append(sess.Children, c)
	}
	return rows.Err()
}

func (s *Store) loadEntries(ctx context.Context, sess *store.Session) error {
	const q = `SELECT seq, entry_type, quantity_uL, child_tube_id, operation_id
	           FROM volume_entries WHERE session_id = ? ORDER BY seq`
	rows, err := s.db.QueryContext(ctx, q, string(sess.ID))
	if err != nil {
		return fmt.Errorf("load entries: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var e volume.Entry
		if err := rows.Scan(&e.Seq, &e.Type, &e.QuantityUL, &e.ChildTubeID, &e.OperationID); err != nil {
			return fmt.Errorf("scan entry: %w", err)
		}
		sess.Entries = append(sess.Entries, e)
	}
	return rows.Err()
}

func (s *Store) loadEdges(ctx context.Context, sess *store.Session) error {
	const q = `SELECT session_id, parent_tube_id, child_tube_id, allocation_uL, state
	           FROM lineage_edges WHERE session_id = ? ORDER BY child_tube_id`
	rows, err := s.db.QueryContext(ctx, q, string(sess.ID))
	if err != nil {
		return fmt.Errorf("load edges: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var e lineage.Edge
		if err := rows.Scan(&e.SessionID, &e.ParentTubeID, &e.ChildTubeID, &e.AllocationUL, &e.State); err != nil {
			return fmt.Errorf("scan edge: %w", err)
		}
		sess.Edges = append(sess.Edges, e)
	}
	return rows.Err()
}

func (s *Store) loadVerifications(ctx context.Context, sess *store.Session) error {
	const q = `SELECT child_tube_id, sample_id, batch_id, revision, allocation_uL, freeze_run_id
	           FROM refreeze_verifications WHERE session_id = ?`
	rows, err := s.db.QueryContext(ctx, q, string(sess.ID))
	if err != nil {
		return fmt.Errorf("load verifications: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v lineage.Verification
		if err := rows.Scan(&v.ChildTubeID, &v.SampleID, &v.BatchID, &v.Revision, &v.AllocationUL, &v.FreezeRunID); err != nil {
			return fmt.Errorf("scan verification: %w", err)
		}
		sess.Verifications[v.ChildTubeID] = v
	}
	return rows.Err()
}

func (s *Store) loadManifest(ctx context.Context, sess *store.Session) error {
	const hq = `SELECT session_id, mother_tube_id, sample_id, batch_id, revision,
	                   frozen_volume_uL, total_loss_uL
	            FROM lineage_manifests WHERE session_id = ?`
	var m lineage.Manifest
	err := s.db.QueryRowContext(ctx, hq, string(sess.ID)).Scan(
		&m.SessionID, &m.MotherTubeID, &m.SampleID, &m.BatchID, &m.Revision,
		&m.FrozenVolumeUL, &m.TotalLossUL)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	sess.Manifest = &m

	const iq = `SELECT ordinal, child_tube_id, allocation_uL, freeze_run_id
	            FROM manifest_items WHERE session_id = ? ORDER BY ordinal`
	rows, err := s.db.QueryContext(ctx, iq, string(sess.ID))
	if err != nil {
		return fmt.Errorf("load manifest items: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item lineage.ManifestItem
		if err := rows.Scan(&item.Ordinal, &item.ChildTubeID, &item.AllocationUL, &item.FreezeRunID); err != nil {
			return fmt.Errorf("scan manifest item: %w", err)
		}
		sess.ManifestItems = append(sess.ManifestItems, item)
	}
	return rows.Err()
}

// motherStatus converts a persisted container status string into the aliquot
// state machine type.
func motherStatus(s string) aliquot.MotherStatus { return aliquot.MotherStatus(s) }

// loadEdgesTx loads lineage edges inside a transaction.
func loadEdgesTx(ctx context.Context, tx *sql.Tx, sessionID catalog.SessionID) ([]lineage.Edge, error) {
	const q = `SELECT session_id, parent_tube_id, child_tube_id, allocation_uL, state
	           FROM lineage_edges WHERE session_id = ? ORDER BY child_tube_id`
	rows, err := tx.QueryContext(ctx, q, string(sessionID))
	if err != nil {
		return nil, fmt.Errorf("load edges tx: %w", err)
	}
	defer rows.Close()
	var edges []lineage.Edge
	for rows.Next() {
		var e lineage.Edge
		if err := rows.Scan(&e.SessionID, &e.ParentTubeID, &e.ChildTubeID, &e.AllocationUL, &e.State); err != nil {
			return nil, fmt.Errorf("scan edge tx: %w", err)
		}
		edges = append(edges, e)
	}
	return edges, rows.Err()
}

// loadVerificationsTx loads refreeze verifications inside a transaction.
func loadVerificationsTx(ctx context.Context, tx *sql.Tx, sessionID catalog.SessionID) (map[catalog.TubeID]lineage.Verification, error) {
	const q = `SELECT child_tube_id, sample_id, batch_id, revision, allocation_uL, freeze_run_id
	           FROM refreeze_verifications WHERE session_id = ?`
	rows, err := tx.QueryContext(ctx, q, string(sessionID))
	if err != nil {
		return nil, fmt.Errorf("load verifications tx: %w", err)
	}
	defer rows.Close()
	verifications := make(map[catalog.TubeID]lineage.Verification)
	for rows.Next() {
		var v lineage.Verification
		if err := rows.Scan(&v.ChildTubeID, &v.SampleID, &v.BatchID, &v.Revision, &v.AllocationUL, &v.FreezeRunID); err != nil {
			return nil, fmt.Errorf("scan verification tx: %w", err)
		}
		verifications[v.ChildTubeID] = v
	}
	return verifications, rows.Err()
}
