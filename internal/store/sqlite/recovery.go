package sqlite

import (
	"context"
	"fmt"
)

// Recover verifies the database invariants on startup (domain rule 14, failure
// boundary 8). It checks for negative remaining volumes, orphaned child tubes,
// conservation violations, duplicate open occupancy, duplicate terminal
// outcomes, and manifest/lineage inconsistency. It returns a descriptive error
// naming the first offending session.
func (s *Store) Recover(ctx context.Context) error {
	if err := s.checkNegativeRemaining(ctx); err != nil {
		return err
	}
	if err := s.checkOrphanedChildren(ctx); err != nil {
		return err
	}
	if err := s.checkDuplicateOpenSessions(ctx); err != nil {
		return err
	}
	if err := s.checkSessionInvariants(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) checkNegativeRemaining(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tube_id, remaining_uL FROM containers WHERE remaining_uL < 0`)
	if err != nil {
		return fmt.Errorf("recover negative remaining: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var rem int64
		if err := rows.Scan(&id, &rem); err != nil {
			return err
		}
		return fmt.Errorf("recovery error: container %q has negative remaining volume %d", id, rem)
	}
	return rows.Err()
}

func (s *Store) checkOrphanedChildren(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT c.tube_id FROM containers c
		 LEFT JOIN lineage_edges e ON e.child_tube_id = c.tube_id
		 WHERE c.type = 'child' AND e.child_tube_id IS NULL`)
	if err != nil {
		return fmt.Errorf("recover orphaned children: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		return fmt.Errorf("recovery error: child tube %q has no parent lineage edge", id)
	}
	return rows.Err()
}

func (s *Store) checkDuplicateOpenSessions(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT mother_tube_id, COUNT(*) FROM aliquot_sessions
		 WHERE terminal_kind = '' GROUP BY mother_tube_id HAVING COUNT(*) > 1`)
	if err != nil {
		return fmt.Errorf("recover duplicate open sessions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var mother string
		var n int
		if err := rows.Scan(&mother, &n); err != nil {
			return err
		}
		return fmt.Errorf("recovery error: mother %q has %d open sessions", mother, n)
	}
	return rows.Err()
}

// checkSessionInvariants validates conservation, single terminal outcome, and
// manifest/lineage consistency for every session.
func (s *Store) checkSessionInvariants(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT session_id FROM aliquot_sessions`)
	if err != nil {
		return fmt.Errorf("recover sessions: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, id := range ids {
		if err := s.checkOneSessionInvariants(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) checkOneSessionInvariants(ctx context.Context, id string) error {
	// Conservation identity.
	var locked, allocated, loss int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT locked_volume_uL FROM aliquot_sessions WHERE session_id = ?`, id).Scan(&locked); err != nil {
		return err
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(CASE WHEN entry_type='child_allocation' THEN quantity_uL ELSE 0 END),0)
		 FROM volume_entries WHERE session_id = ?`, id).Scan(&allocated); err != nil {
		return err
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(CASE WHEN entry_type='recorded_loss' THEN quantity_uL ELSE 0 END),0)
		 FROM volume_entries WHERE session_id = ?`, id).Scan(&loss); err != nil {
		return err
	}
	var remaining int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT remaining_uL FROM containers WHERE session_id = ? AND type = 'mother'`, id).Scan(&remaining); err != nil {
		return err
	}
	if locked != allocated+loss+remaining {
		return fmt.Errorf("recovery error: session %q violates conservation locked=%d allocated=%d loss=%d remaining=%d",
			id, locked, allocated, loss, remaining)
	}

	// Single terminal outcome.
	var outcomes int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_outcomes WHERE session_id = ?`, id).Scan(&outcomes); err != nil {
		return err
	}
	if outcomes > 1 {
		return fmt.Errorf("recovery error: session %q has %d terminal outcomes", id, outcomes)
	}

	// Manifest consistency for finalized sessions.
	var terminal string
	if err := s.db.QueryRowContext(ctx,
		`SELECT terminal_kind FROM aliquot_sessions WHERE session_id = ?`, id).Scan(&terminal); err != nil {
		return err
	}
	if terminal == "finalized" {
		var manifestCount, edgeCount, itemCount int
		_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM lineage_manifests WHERE session_id = ?`, id).Scan(&manifestCount)
		_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM lineage_edges WHERE session_id = ?`, id).Scan(&edgeCount)
		_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM manifest_items WHERE session_id = ?`, id).Scan(&itemCount)
		if manifestCount != 1 || itemCount != edgeCount {
			return fmt.Errorf("recovery error: session %q manifest inconsistent manifests=%d edges=%d items=%d",
				id, manifestCount, edgeCount, itemCount)
		}
	}
	return nil
}
