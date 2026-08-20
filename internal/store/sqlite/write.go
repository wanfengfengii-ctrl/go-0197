package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
)

// Bootstrap transactionally registers catalog facts in an empty database and
// records the operation result so a repeated identical bootstrap is idempotent.
func (s *Store) Bootstrap(ctx context.Context, opID, fingerprint string, facts store.Facts) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bootstrap: %w", err)
	}
	defer tx.Rollback()

	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM specimens`).Scan(&n); err != nil {
		return fmt.Errorf("count specimens: %w", err)
	}
	if n > 0 {
		return store.ErrAlreadyBootstrapped
	}

	for _, spec := range facts.Specimens {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO specimens (sample_id, description) VALUES (?, ?)`,
			string(spec.ID), spec.Description); err != nil {
			return fmt.Errorf("insert specimen: %w", err)
		}
	}
	for _, br := range facts.BatchRevisions {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO batch_revisions (sample_id, batch_id, revision) VALUES (?, ?, ?)`,
			string(br.SampleID), string(br.BatchID), string(br.Revision)); err != nil {
			return fmt.Errorf("insert batch revision: %w", err)
		}
	}
	for _, m := range facts.Mothers {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO containers (tube_id, type, sample_id, batch_id, revision, remaining_uL, status, session_id)
			 VALUES (?, 'mother', ?, ?, ?, ?, 'available', '')`,
			string(m.TubeID), string(m.SampleID), string(m.BatchID), string(m.Revision), m.VolumeUL); err != nil {
			return fmt.Errorf("insert mother: %w", err)
		}
	}
	if err := writeOperationResult(ctx, tx, opID, fingerprint, "ok", "", "", 0, ""); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bootstrap: %w", err)
	}
	return nil
}

// Reserve persists a new reservation session atomically (acceptance 1, 2, 5).
func (s *Store) Reserve(ctx context.Context, r store.ReserveRequest) (store.CommandResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("begin reserve: %w", err)
	}
	defer tx.Rollback()

	// Conditional update: the mother must still be available (belt-and-suspenders
	// on top of the per-mother in-process lock).
	res, err := tx.ExecContext(ctx,
		`UPDATE containers SET status = ?, session_id = ?
		 WHERE tube_id = ? AND type = 'mother' AND status = 'available' AND session_id = ''`,
		catalog.ContainerStatusReserved, string(r.SessionID), string(r.MotherTubeID))
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("claim mother: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.CommandResult{}, aliquot.NewError(aliquot.CodeMotherAlreadyReserved, 0, "mother not available for reservation")
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO aliquot_sessions (session_id, mother_tube_id, sample_id, batch_id, revision,
		        locked_volume_uL, status, revision_num, terminal_kind, terminal_reason, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, 'reserved', 0, '', '', ?)`,
		string(r.SessionID), string(r.MotherTubeID), string(r.SampleID), string(r.BatchID),
		string(r.Revision), r.LockedVolumeUL, now); err != nil {
		return store.CommandResult{}, fmt.Errorf("insert session: %w", err)
	}

	for _, c := range r.Children {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO planned_children (session_id, ordinal, child_tube_id, planned_volume_uL, created)
			 VALUES (?, ?, ?, ?, 0)`,
			string(r.SessionID), c.Ordinal, string(c.ChildTubeID), c.PlannedVolumeUL); err != nil {
			if isUniqueViolation(err) {
				return store.CommandResult{}, aliquot.NewError(aliquot.CodeChildNumberConflict, 0, string(c.ChildTubeID))
			}
			return store.CommandResult{}, fmt.Errorf("insert planned child: %w", err)
		}
	}

	if err := writeOperationResult(ctx, tx, r.OperationID, r.Fingerprint, "ok", "", string(r.SessionID), 0, ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeEvent(ctx, tx, string(r.SessionID), 0, "reserve", "ok", r.OperationID); err != nil {
		return store.CommandResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.CommandResult{}, fmt.Errorf("commit reserve: %w", err)
	}
	return store.CommandResult{Revision: 0}, nil
}

// Thaw persists a confirmed thaw transition to the thawed state.
func (s *Store) Thaw(ctx context.Context, r store.ThawRequest) (store.CommandResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("begin thaw: %w", err)
	}
	defer tx.Rollback()

	rev, _, _, err := loadSessionRowTx(ctx, tx, r.SessionID)
	if err != nil {
		return store.CommandResult{}, err
	}
	if err := checkRevision(rev, r.ExpectedRevision); err != nil {
		return store.CommandResult{}, err
	}

	newRev := rev + 1
	if err := bumpSession(ctx, tx, r.SessionID, aliquot.StatusThawed, newRev, "", ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeOperationResult(ctx, tx, r.OperationID, r.Fingerprint, "ok", "", string(r.SessionID), newRev, ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeEvent(ctx, tx, string(r.SessionID), newRev, "thaw", "ok", r.OperationID); err != nil {
		return store.CommandResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.CommandResult{}, fmt.Errorf("commit thaw: %w", err)
	}
	return store.CommandResult{Revision: newRev}, nil
}

// CreateChild creates exactly one planned child and debits the mother volume in
// the same transaction, writing the container, ledger entry, lineage edge, plan
// completion mark, session revision, and operation result together (acceptance 5,
// failure boundary 2).
func (s *Store) CreateChild(ctx context.Context, r store.ChildRequest) (store.CommandResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("begin create child: %w", err)
	}
	defer tx.Rollback()

	rev, _, motherID, err := loadSessionRowTx(ctx, tx, r.SessionID)
	if err != nil {
		return store.CommandResult{}, err
	}
	if err := checkRevision(rev, r.ExpectedRevision); err != nil {
		return store.CommandResult{}, err
	}

	// The allocation is the frozen planned volume of this specific child.
	var planned int64
	err = tx.QueryRowContext(ctx,
		`SELECT planned_volume_uL FROM planned_children
		 WHERE session_id = ? AND child_tube_id = ? AND created = 0`,
		string(r.SessionID), string(r.ChildTubeID)).Scan(&planned)
	if err == sql.ErrNoRows {
		return store.CommandResult{}, aliquot.NewError(aliquot.CodeInvalidState, rev, "child not planned or already created")
	}
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("read planned child: %w", err)
	}

	sampleID, batchID, revision := s.sessionCatalog(ctx, tx, r.SessionID)

	// 1. Insert the child container.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO containers (tube_id, type, sample_id, batch_id, revision, remaining_uL, status, session_id)
		 VALUES (?, 'child', ?, ?, ?, ?, 'available', ?)`,
		string(r.ChildTubeID), string(sampleID), string(batchID), string(revision), planned, string(r.SessionID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("insert child container: %w", err)
	}
	if err := s.check(store.CheckpointAfterChildInsert); err != nil {
		return store.CommandResult{}, err
	}

	// 2. Debit the mother and write the ledger entry.
	if _, err := tx.ExecContext(ctx,
		`UPDATE containers SET remaining_uL = remaining_uL - ? WHERE tube_id = ?`,
		planned, string(motherID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("debit mother: %w", err)
	}
	if err := s.insertEntry(ctx, tx, r.SessionID, volumeEntryChildAllocation, planned, string(r.ChildTubeID), r.OperationID); err != nil {
		return store.CommandResult{}, err
	}
	if err := s.check(store.CheckpointAfterVolumeDebit); err != nil {
		return store.CommandResult{}, err
	}

	// 3. Insert the pending lineage edge and mark the plan complete.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO lineage_edges (session_id, parent_tube_id, child_tube_id, allocation_uL, state)
		 VALUES (?, ?, ?, ?, 'pending')`,
		string(r.SessionID), string(motherID), string(r.ChildTubeID), planned); err != nil {
		return store.CommandResult{}, fmt.Errorf("insert lineage edge: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE planned_children SET created = 1 WHERE session_id = ? AND child_tube_id = ?`,
		string(r.SessionID), string(r.ChildTubeID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("mark child created: %w", err)
	}
	if err := s.check(store.CheckpointAfterLineageInsert); err != nil {
		return store.CommandResult{}, err
	}

	// 4. Advance the session revision and status.
	newRev := rev + 1
	if err := bumpSession(ctx, tx, r.SessionID, r.NewStatus, newRev, "", ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeOperationResult(ctx, tx, r.OperationID, r.Fingerprint, "ok", "", string(r.SessionID), newRev, ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeEvent(ctx, tx, string(r.SessionID), newRev, "create_child", "ok", r.OperationID); err != nil {
		return store.CommandResult{}, err
	}
	if err := s.check(store.CheckpointBeforeCommit); err != nil {
		return store.CommandResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.CommandResult{}, fmt.Errorf("commit create child: %w", err)
	}
	return store.CommandResult{Revision: newRev}, nil
}

// RecordLoss records a non-negative integer loss and debits the mother volume.
func (s *Store) RecordLoss(ctx context.Context, r store.LossRequest) (store.CommandResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("begin record loss: %w", err)
	}
	defer tx.Rollback()

	rev, _, motherID, err := loadSessionRowTx(ctx, tx, r.SessionID)
	if err != nil {
		return store.CommandResult{}, err
	}
	if err := checkRevision(rev, r.ExpectedRevision); err != nil {
		return store.CommandResult{}, err
	}

	var remaining int64
	if err := tx.QueryRowContext(ctx,
		`SELECT remaining_uL FROM containers WHERE tube_id = ?`, string(motherID)).Scan(&remaining); err != nil {
		return store.CommandResult{}, fmt.Errorf("read mother remaining: %w", err)
	}
	if r.LossUL > remaining {
		return store.CommandResult{}, aliquot.NewError(aliquot.CodeLossExceedsOutstanding, rev, "loss exceeds remaining volume")
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE containers SET remaining_uL = remaining_uL - ? WHERE tube_id = ?`,
		r.LossUL, string(motherID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("debit loss: %w", err)
	}
	if err := s.insertEntry(ctx, tx, r.SessionID, volumeEntryRecordedLoss, r.LossUL, "", r.OperationID); err != nil {
		return store.CommandResult{}, err
	}

	newRev := rev + 1
	if err := bumpSession(ctx, tx, r.SessionID, r.NewStatus, newRev, "", ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeOperationResult(ctx, tx, r.OperationID, r.Fingerprint, "ok", "", string(r.SessionID), newRev, ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeEvent(ctx, tx, string(r.SessionID), newRev, "record_loss", "ok", r.OperationID); err != nil {
		return store.CommandResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.CommandResult{}, fmt.Errorf("commit record loss: %w", err)
	}
	return store.CommandResult{Revision: newRev}, nil
}

// VerifyRefreeze persists a per-child refreeze verification. Each child keeps at
// most one verification (unique child_tube_id).
func (s *Store) VerifyRefreeze(ctx context.Context, r store.VerifyRequest) (store.CommandResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("begin verify: %w", err)
	}
	defer tx.Rollback()

	rev, status, _, err := loadSessionRowTx(ctx, tx, r.SessionID)
	if err != nil {
		return store.CommandResult{}, err
	}
	if err := checkRevision(rev, r.ExpectedRevision); err != nil {
		return store.CommandResult{}, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO refreeze_verifications (child_tube_id, session_id, sample_id, batch_id, revision,
		        allocation_uL, freeze_run_id, operation_id, verified_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(r.ChildTubeID), string(r.SessionID), string(r.SampleID), string(r.BatchID),
		string(r.Revision), r.AllocationUL, r.FreezeRunID, r.OperationID, now); err != nil {
		if isUniqueViolation(err) {
			return store.CommandResult{}, aliquot.NewError(aliquot.CodeInvalidState, rev, "child already verified")
		}
		return store.CommandResult{}, fmt.Errorf("insert verification: %w", err)
	}

	newRev := rev + 1
	if err := bumpSession(ctx, tx, r.SessionID, status, newRev, "", ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeOperationResult(ctx, tx, r.OperationID, r.Fingerprint, "ok", "", string(r.SessionID), newRev, ""); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeEvent(ctx, tx, string(r.SessionID), newRev, "verify_refreeze", "ok", r.OperationID); err != nil {
		return store.CommandResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.CommandResult{}, fmt.Errorf("commit verify: %w", err)
	}
	return store.CommandResult{Revision: newRev}, nil
}

// Finalize produces the unique immutable lineage manifest and freezes all
// pending edges (acceptance 6, 7; failure boundary 3).
func (s *Store) Finalize(ctx context.Context, r store.FinalizeRequest) (store.CommandResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("begin finalize: %w", err)
	}
	defer tx.Rollback()

	rev, status, motherID, err := loadSessionRowTx(ctx, tx, r.SessionID)
	if err != nil {
		return store.CommandResult{}, err
	}
	if err := checkRevision(rev, r.ExpectedRevision); err != nil {
		return store.CommandResult{}, err
	}
	if status != aliquot.StatusDepleted {
		return store.CommandResult{}, aliquot.NewError(aliquot.CodeInvalidState, rev, "mother not depleted")
	}

	sampleID, batchID, revision := s.sessionCatalog(ctx, tx, r.SessionID)
	locked, totalLoss := s.volumeTotals(ctx, tx, r.SessionID)

	edges, err := loadEdgesTx(ctx, tx, r.SessionID)
	if err != nil {
		return store.CommandResult{}, err
	}
	verifications, err := loadVerificationsTx(ctx, tx, r.SessionID)
	if err != nil {
		return store.CommandResult{}, err
	}

	g := lineage.NewGraph(r.SessionID, motherID)
	for _, e := range edges {
		g.AddEdge(e)
	}
	for _, v := range verifications {
		g.AddVerification(v)
	}
	header, items, err := lineage.ManifestFromGraph(r.SessionID, motherID, sampleID, batchID, revision, locked, totalLoss, g)
	if err != nil {
		return store.CommandResult{}, aliquot.NewError(aliquot.CodeMissingVerification, rev, err.Error())
	}

	// Claim the terminal outcome.
	if _, err := tx.ExecContext(ctx,
		`UPDATE aliquot_sessions SET status = 'finalized', terminal_kind = 'finalized', terminal_reason = '', revision_num = ?
		 WHERE session_id = ?`, rev+1, string(r.SessionID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("finalize session: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO session_outcomes (session_id, terminal_kind, revision, reason, summary)
		 VALUES (?, 'finalized', ?, '', 'lineage finalized')`,
		string(r.SessionID), rev+1); err != nil {
		return store.CommandResult{}, fmt.Errorf("insert outcome: %w", err)
	}
	if err := s.check(store.CheckpointAfterTerminalClaim); err != nil {
		return store.CommandResult{}, err
	}

	// Freeze edges.
	if _, err := tx.ExecContext(ctx,
		`UPDATE lineage_edges SET state = 'finalized' WHERE session_id = ?`, string(r.SessionID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("freeze edges: %w", err)
	}
	// Insert the manifest header and items.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO lineage_manifests (session_id, mother_tube_id, sample_id, batch_id, revision, frozen_volume_uL, total_loss_uL)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		string(header.SessionID), string(header.MotherTubeID), string(header.SampleID), string(header.BatchID),
		string(header.Revision), header.FrozenVolumeUL, header.TotalLossUL); err != nil {
		return store.CommandResult{}, fmt.Errorf("insert manifest: %w", err)
	}
	for _, item := range items {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO manifest_items (session_id, ordinal, child_tube_id, allocation_uL, freeze_run_id)
			 VALUES (?, ?, ?, ?, ?)`,
			string(r.SessionID), item.Ordinal, string(item.ChildTubeID), item.AllocationUL, item.FreezeRunID); err != nil {
			return store.CommandResult{}, fmt.Errorf("insert manifest item: %w", err)
		}
	}
	if err := s.check(store.CheckpointAfterManifestInsert); err != nil {
		return store.CommandResult{}, err
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE containers SET status = 'finalized' WHERE tube_id = ?`, string(motherID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("finalize mother: %w", err)
	}

	newRev := rev + 1
	if err := writeOperationResult(ctx, tx, r.OperationID, r.Fingerprint, "ok", "", string(r.SessionID), newRev, store.TerminalFinalized); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeEvent(ctx, tx, string(r.SessionID), newRev, "finalize", "ok", r.OperationID); err != nil {
		return store.CommandResult{}, err
	}
	if err := s.check(store.CheckpointBeforeTerminalCommit); err != nil {
		return store.CommandResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.CommandResult{}, fmt.Errorf("commit finalize: %w", err)
	}
	return store.CommandResult{Revision: newRev, Terminal: store.TerminalFinalized}, nil
}

// Quarantine freezes the whole session and its affected tubes into a
// quarantined terminal (business flow 6).
func (s *Store) Quarantine(ctx context.Context, r store.QuarantineRequest) (store.CommandResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return store.CommandResult{}, fmt.Errorf("begin quarantine: %w", err)
	}
	defer tx.Rollback()

	rev, _, motherID, err := loadSessionRowTx(ctx, tx, r.SessionID)
	if err != nil {
		return store.CommandResult{}, err
	}
	if err := checkRevision(rev, r.ExpectedRevision); err != nil {
		return store.CommandResult{}, err
	}

	newRev := rev + 1
	if _, err := tx.ExecContext(ctx,
		`UPDATE aliquot_sessions SET status = 'quarantined', terminal_kind = 'quarantined', terminal_reason = ?, revision_num = ?
		 WHERE session_id = ?`, r.Reason, newRev, string(r.SessionID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("quarantine session: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO session_outcomes (session_id, terminal_kind, revision, reason, summary)
		 VALUES (?, 'quarantined', ?, ?, 'batch quarantined')`,
		string(r.SessionID), newRev, r.Reason); err != nil {
		return store.CommandResult{}, fmt.Errorf("insert outcome: %w", err)
	}
	if err := s.check(store.CheckpointAfterTerminalClaim); err != nil {
		return store.CommandResult{}, err
	}

	// Quarantine the mother and every created child container.
	if _, err := tx.ExecContext(ctx,
		`UPDATE containers SET status = 'quarantined' WHERE tube_id = ?`, string(motherID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("quarantine mother: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE containers SET status = 'quarantined' WHERE session_id = ? AND type = 'child'`,
		string(r.SessionID)); err != nil {
		return store.CommandResult{}, fmt.Errorf("quarantine children: %w", err)
	}

	if err := writeOperationResult(ctx, tx, r.OperationID, r.Fingerprint, "ok", "", string(r.SessionID), newRev, store.TerminalQuarantined); err != nil {
		return store.CommandResult{}, err
	}
	if err := writeEvent(ctx, tx, string(r.SessionID), newRev, "quarantine", "ok", r.OperationID); err != nil {
		return store.CommandResult{}, err
	}
	if err := s.check(store.CheckpointBeforeTerminalCommit); err != nil {
		return store.CommandResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return store.CommandResult{}, fmt.Errorf("commit quarantine: %w", err)
	}
	return store.CommandResult{Revision: newRev, Terminal: store.TerminalQuarantined}, nil
}

// RecordRejection persists a deterministic business rejection so a retry of the
// same operation ID returns the same stable rejection (domain rule 12).
func (s *Store) RecordRejection(ctx context.Context, opID, fingerprint string, e *aliquot.Error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin record rejection: %w", err)
	}
	defer tx.Rollback()
	code := string(e.Code)
	if code == "" {
		code = "REJECTED"
	}
	if err := writeOperationResult(ctx, tx, opID, fingerprint, code, e.Message, "", e.Revision, ""); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit record rejection: %w", err)
	}
	return nil
}

const (
	volumeEntryChildAllocation = "child_allocation"
	volumeEntryRecordedLoss    = "recorded_loss"
)

// insertEntry appends a ledger entry with the next sequence number.
func (s *Store) insertEntry(ctx context.Context, tx *sql.Tx, sessionID catalog.SessionID, typ string, quantity int64, childTubeID, opID string) error {
	var seq int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), -1) + 1 FROM volume_entries WHERE session_id = ?`,
		string(sessionID)).Scan(&seq); err != nil {
		return fmt.Errorf("next entry seq: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO volume_entries (session_id, seq, entry_type, quantity_uL, child_tube_id, operation_id)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		string(sessionID), seq, typ, quantity, childTubeID, opID); err != nil {
		return fmt.Errorf("insert volume entry: %w", err)
	}
	return nil
}

// sessionCatalog returns the frozen sample/batch/revision of a session.
func (s *Store) sessionCatalog(ctx context.Context, tx *sql.Tx, sessionID catalog.SessionID) (catalog.SampleID, catalog.BatchID, catalog.Revision) {
	var sample catalog.SampleID
	var batch catalog.BatchID
	var revision catalog.Revision
	_ = tx.QueryRowContext(ctx,
		`SELECT sample_id, batch_id, revision FROM aliquot_sessions WHERE session_id = ?`,
		string(sessionID)).Scan(&sample, &batch, &revision)
	return sample, batch, revision
}

// volumeTotals returns the frozen locked volume and total recorded loss.
func (s *Store) volumeTotals(ctx context.Context, tx *sql.Tx, sessionID catalog.SessionID) (int64, int64) {
	var locked int64
	_ = tx.QueryRowContext(ctx,
		`SELECT locked_volume_uL FROM aliquot_sessions WHERE session_id = ?`, string(sessionID)).Scan(&locked)
	var loss int64
	_ = tx.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(quantity_uL), 0) FROM volume_entries WHERE session_id = ? AND entry_type = 'recorded_loss'`,
		string(sessionID)).Scan(&loss)
	return locked, loss
}

// loadSessionRowTx loads the session's revision, status, terminal kind, and
// mother tube inside a transaction.
func loadSessionRowTx(ctx context.Context, tx *sql.Tx, sessionID catalog.SessionID) (int64, aliquot.MotherStatus, catalog.TubeID, error) {
	var revision int64
	var status string
	var terminal string
	var mother catalog.TubeID
	err := tx.QueryRowContext(ctx,
		`SELECT revision_num, status, terminal_kind, mother_tube_id FROM aliquot_sessions WHERE session_id = ?`,
		string(sessionID)).Scan(&revision, &status, &terminal, &mother)
	if err == sql.ErrNoRows {
		return 0, "", "", aliquot.NewError(aliquot.CodeInvalidState, 0, "session not found")
	}
	if err != nil {
		return 0, "", "", fmt.Errorf("load session row: %w", err)
	}
	if terminal != "" {
		return revision, aliquot.MotherStatus(status), mother, aliquot.NewError(aliquot.CodeTerminalConflict, revision, "session already terminal")
	}
	return revision, aliquot.MotherStatus(status), mother, nil
}

func checkRevision(current, expected int64) error {
	if current != expected {
		return aliquot.NewError(aliquot.CodeRevisionConflict, current, "revision mismatch")
	}
	return nil
}

// bumpSession advances a session's revision and status.
func bumpSession(ctx context.Context, tx *sql.Tx, sessionID catalog.SessionID, status aliquot.MotherStatus, revision int64, terminal store.TerminalKind, reason string) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE aliquot_sessions SET status = ?, revision_num = ?, terminal_kind = ?, terminal_reason = ?
		 WHERE session_id = ?`,
		string(status), revision, string(terminal), reason, string(sessionID))
	if err != nil {
		return fmt.Errorf("bump session: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE containers SET status = ?
		 WHERE type = 'mother' AND tube_id = (
			SELECT mother_tube_id FROM aliquot_sessions WHERE session_id = ?
		)`, string(status), string(sessionID)); err != nil {
		return fmt.Errorf("sync mother status: %w", err)
	}
	return nil
}

func writeOperationResult(ctx context.Context, tx *sql.Tx, opID, fingerprint, code, message, sessionID string, revision int64, terminal store.TerminalKind) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO operation_results (operation_id, fingerprint, status_code, message, revision, terminal, session_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		opID, fingerprint, code, message, revision, string(terminal), sessionID)
	if err != nil {
		return fmt.Errorf("write operation result: %w", err)
	}
	return nil
}

func writeEvent(ctx context.Context, tx *sql.Tx, sessionID string, revision int64, commandType, result, opID string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO session_events (session_id, revision, command_type, result, operation_id)
		 VALUES (?, ?, ?, ?, ?)`,
		sessionID, revision, commandType, result, opID)
	if err != nil {
		return fmt.Errorf("write event: %w", err)
	}
	return nil
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
